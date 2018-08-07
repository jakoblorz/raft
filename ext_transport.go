package raft

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
)

type ExtendedTransport struct {
	connPool     map[raft.ServerAddress][]*netConn
	connPoolLock sync.Mutex

	consumeCh chan raft.RPC

	logger *log.Logger

	maxPool int

	serverAddressProvider raft.ServerAddressProvider

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	stream raft.StreamLayer

	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout      time.Duration
	TimeoutScale int
}

type ExtendedTransportConfig struct {
	ServerAddressProvider raft.ServerAddressProvider

	Logger *log.Logger

	Stream raft.StreamLayer

	MaxPool int

	Timeout time.Duration
}

type netConn struct {
	target raft.ServerAddress
	conn   net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

func NewExtendedTransport(
	stream raft.StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *ExtendedTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := log.New(logOutput, "", log.LstdFlags)
	config := &ExtendedTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewExtendedTransportWithConfig(config)
}

func NewExtendedTransportWithConfig(
	config *ExtendedTransportConfig,
) *ExtendedTransport {
	if config.Logger == nil {
		config.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	trans := &ExtendedTransport{
		connPool:              make(map[raft.ServerAddress][]*netConn),
		consumeCh:             make(chan raft.RPC),
		logger:                config.Logger,
		maxPool:               config.MaxPool,
		shutdownCh:            make(chan struct{}),
		stream:                config.Stream,
		timeout:               config.Timeout,
		TimeoutScale:          raft.DefaultTimeoutScale,
		serverAddressProvider: config.ServerAddressProvider,
	}

	trans.setupStreamContext()
	go trans.listen()

	return trans
}

func (p *ExtendedTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	p.streamCtx = ctx
	p.streamCancel = cancel
}

func (p *ExtendedTransport) getStreamContext() context.Context {
	p.streamCtxLock.RLock()
	defer p.streamCtxLock.RUnlock()
	return p.streamCtx
}

func (p *ExtendedTransport) CloseStreams() {
	p.connPoolLock.Lock()
	defer p.connPoolLock.Unlock()

	for k, e := range p.connPool {
		for _, conn := range e {
			conn.Release()
		}

		delete(p.connPool, k)
	}

	p.streamCtxLock.Lock()
	defer p.streamCtxLock.Unlock()
	p.streamCancel()
	p.setupStreamContext()
}

func (p *ExtendedTransport) Close() error {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()

	if !p.shutdown {
		close(p.shutdownCh)
		p.stream.Close()
		p.shutdown = true
	}
	return nil
}

func (p *ExtendedTransport) IsShutdown() bool {
	select {
	case <-p.shutdownCh:
		return true
	default:
		return false
	}
}

func (p *ExtendedTransport) getPooledConn(target raft.ServerAddress) *netConn {
	p.connPoolLock.Lock()
	defer p.connPoolLock.Unlock()

	conns, ok := p.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	p.connPool[target] = conns[:num-1]
	return conn
}

func (p *ExtendedTransport) getConnFromAddressProvider(id raft.ServerID, target raft.ServerAddress) (*netConn, error) {
	address := p.getProviderAddressOrFallback(id, target)
	return p.getConn(address)
}

func (p *ExtendedTransport) getProviderAddressOrFallback(id raft.ServerID, target raft.ServerAddress) raft.ServerAddress {
	if p.serverAddressProvider != nil {
		serverAddressOverride, err := p.serverAddressProvider.ServerAddr(id)
		if err != nil {
			p.logger.Printf("[WARN] raft extension: Unable to get address for server id %v, using fallback address %v: %v", id, target, err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

func (p *ExtendedTransport) getConn(target raft.ServerAddress) (*netConn, error) {

	if conn := p.getPooledConn(target); conn != nil {
		return conn, nil
	}

	conn, err := p.stream.Dial(target, p.timeout)
	if err != nil {
		return nil, err
	}

	netConn := &netConn{
		target: target,
		conn:   conn,
		r:      bufio.NewReader(conn),
		w:      bufio.NewWriter(conn),
	}

	netConn.dec = codec.NewDecoder(netConn.r, &codec.MsgpackHandle{})
	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	return netConn, nil
}

func (p *ExtendedTransport) returnConn(conn *netConn) {
	p.connPoolLock.Lock()
	defer p.connPoolLock.Unlock()

	key := conn.target
	conns, _ := p.connPool[key]

	if !p.IsShutdown() && len(conns) < p.maxPool {
		p.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

func (p *ExtendedTransport) JoinCluster(target raft.ServerAddress, args *JoinClusterRequest, resp *JoinClusterResponse) error {
	return p.genericRPC(target, rpcJoinCluster, args, resp)
}

func (p *ExtendedTransport) genericRPC(target raft.ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	conn, err := p.getConn(target)
	if err != nil {
		return nil
	}

	defer conn.Release()

	if p.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(p.timeout))
	}

	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	_, err = decodeResponse(conn, resp)

	return err
}

func (p *ExtendedTransport) EncodePeer(id raft.ServerID, a raft.ServerAddress) []byte {
	address := p.getProviderAddressOrFallback(id, a)
	return []byte(address)
}

func (p *ExtendedTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

func (p *ExtendedTransport) listen() {

	for {
		conn, err := p.stream.Accept()
		if err != nil {
			if p.IsShutdown() {
				return
			}
			p.logger.Printf("[ERR] raft-net extension: Failed to accept connection: %v", err)
			continue
		}

		p.logger.Printf("[DEBUG] raft-net extension: %v accepted connection from: %v", p.stream.Addr(), conn.RemoteAddr())

		go p.handleConn(p.getStreamContext(), conn)
	}
}

func (p *ExtendedTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-connCtx.Done():
			p.logger.Println("[DEBUG] raft-net extension: stream layer is closed")
			return
		default:
		}

		if err := p.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				p.logger.Printf("[ERR] raft-net extension: Failed to decode incoming command: %v", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			p.logger.Printf("[ERR] raft-net extension: Failed to flush response: %v", err)
			return
		}
	}

}

func (p *ExtendedTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		RespChan: respCh,
	}

	switch rpcType {
	case rpcJoinCluster:
		var req JoinClusterRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	select {
	case p.consumeCh <- rpc:
	case <-p.shutdownCh:
		return raft.ErrTransportShutdown
	}

	select {
	case resp := <-respCh:
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-p.shutdownCh:
		return raft.ErrTransportShutdown
	}

	return nil

}

func decodeResponse(conn *netConn, resp interface{}) (bool, error) {

	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		conn.Release()
		return false, err
	}

	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}

func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {

	if err := conn.w.WriteByte(rpcType); err != nil {
		conn.Release()
		return err
	}

	if err := conn.enc.Encode(args); err != nil {
		conn.Release()
		return err
	}

	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}
