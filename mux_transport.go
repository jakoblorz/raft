package raft

import (
	"bufio"
	"errors"
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

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	_
	rpcJoinCluster
)

var (
	protocolHeaders = []byte{rpcAppendEntries, rpcRequestVote, rpcInstallSnapshot}
	extendedHeaders = []byte{rpcJoinCluster}

	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// Mux represents a multiplexer for a net.Listener.
type mux struct {
	ln   net.Listener
	once sync.Once
	wg   sync.WaitGroup

	handlers map[byte]*handler

	Timeout   time.Duration
	LogOutput io.Writer
}

// newMultiplexer returns a new instance of Mux.
func newMultiplexer(ln net.Listener) *mux {
	return &mux{
		ln:        ln,
		handlers:  make(map[byte]*handler),
		Timeout:   30 * time.Second,
		LogOutput: os.Stderr,
	}
}

// Close closes the underlying listener.
func (mux *mux) Close() (err error) {
	mux.once.Do(func() {
		// Close underlying listener.
		if mux.ln != nil {
			err = mux.ln.Close()
		}

		// Wait for open connections to close and then close handlers.
		mux.wg.Wait()
		for _, h := range mux.handlers {
			h.Close()
		}
	})
	return
}

// Serve handles connections from ln and multiplexes then across registered listener.
func (mux *mux) Serve() error {
	logger := log.New(mux.LogOutput, "", log.LstdFlags)

	for {
		// Handle incoming connections. Retry temporary errors.
		conn, err := mux.ln.Accept()
		if err, ok := err.(interface {
			Temporary() bool
		}); ok && err.Temporary() {
			logger.Printf("tcp.Mux: temporary error: %s", err)
			continue
		}

		// Other errors should close the muxer and wait for outstanding conns.
		if err != nil {
			mux.Close()
			return err
		}

		// Hand off connection to a separate goroutine.
		mux.wg.Add(1)
		go func(conn net.Conn) {
			defer mux.wg.Done()
			if err := mux.handleConn(conn); err != nil {
				conn.Close()
				logger.Printf("tcp.Mux: %s", err)
			}
		}(conn)
	}
}

func (mux *mux) handleConn(conn net.Conn) error {
	// Wrap in a buffered connection in order to peek at the first byte.
	bufConn := newBufConn(conn)

	// Set a read deadline so connections with no data timeout.
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		return fmt.Errorf("set read deadline: %s", err)
	}

	// Peek at first byte from connection to determine handler.
	typ, err := bufConn.r.ReadByte()
	if err != nil {
		return fmt.Errorf("read header byte: %s", err)
	} else if err = bufConn.r.UnreadByte(); err != nil {
		return fmt.Errorf("unread header byte: %s", err)
	}

	// Reset read deadline and let the listener handle that.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("reset set read deadline: %s", err)
	}

	// Lookup handler.
	h := mux.handlers[typ]
	if h == nil {
		return fmt.Errorf("unregistered header byte: 0x%02x", typ)
	}

	// Hand off connection to handler.
	h.c <- bufConn
	return nil
}

// Listen returns a listener that receives connections from any byte in hdrs.
// Re-registering hdr bytes will overwrite existing handlers.
func (mux *mux) Listen(hdrs []byte) net.Listener {
	// Create new handler.
	h := mux.handler()

	// Register each header byte.
	for _, hdr := range hdrs {
		// Create a new listener and assign it.
		mux.handlers[hdr] = h
	}

	return h
}

// handler returns a new instance of handler.
func (mux *mux) handler() *handler {
	return &handler{
		mux: mux,
		c:   make(chan net.Conn),
	}
}

// handler is a receiver for connections received by Mux. Implements net.Listener.
type handler struct {
	mux  *mux
	c    chan net.Conn
	once sync.Once
}

// Accept waits for and returns the next connection.
func (h *handler) Accept() (c net.Conn, err error) {
	conn, ok := <-h.c
	if !ok {
		return nil, errors.New("network connection closed")
	}
	return conn, nil
}

// Close closes the original listener.
func (h *handler) Close() error {
	h.once.Do(func() { close(h.c) })
	return nil
}

// Addr returns the address of the original listener.
func (h *handler) Addr() net.Addr { return h.mux.ln.Addr() }

// bufConn represents a buffered connection.
type bufConn struct {
	conn net.Conn
	r    *bufio.Reader
}

// newBufConn returns a new instance of bufConn.
func newBufConn(conn net.Conn) *bufConn {
	return &bufConn{
		conn: conn,
		r:    bufio.NewReader(conn),
	}
}

func (c *bufConn) Read(b []byte) (n int, err error)   { return c.r.Read(b) }
func (c *bufConn) Write(b []byte) (n int, err error)  { return c.conn.Write(b) }
func (c *bufConn) Close() error                       { return c.conn.Close() }
func (c *bufConn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *bufConn) RemoteAddr() net.Addr               { return c.conn.RemoteAddr() }
func (c *bufConn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *bufConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *bufConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

type MuxTCPStreamLayer struct {
	advertise net.Addr

	consumeCh chan raft.RPC

	protocolLis *net.TCPListener
	extendedLis *net.TCPListener

	mux *mux

	logger *log.Logger

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	timeout time.Duration
}

func NewMuxTCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*raft.NetworkTransport, error) {
	return newMuxTCPTransport(bindAddr, advertise, func(stream raft.StreamLayer) *raft.NetworkTransport {
		return raft.NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

func NewMuxTCPTransportWithLogger(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logger *log.Logger,
) (*raft.NetworkTransport, error) {
	return newMuxTCPTransport(bindAddr, advertise, func(stream raft.StreamLayer) *raft.NetworkTransport {
		return raft.NewNetworkTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

func NewMuxTCPTransportWithConfig(
	bindAddr string,
	advertise net.Addr,
	config *raft.NetworkTransportConfig,
) (*raft.NetworkTransport, error) {
	return newMuxTCPTransport(bindAddr, advertise, func(stream raft.StreamLayer) *raft.NetworkTransport {
		return raft.NewNetworkTransportWithConfig(config)
	})
}

func newMuxTCPTransport(bindAddr string,
	advertise net.Addr,
	transportCreator func(stream raft.StreamLayer) *raft.NetworkTransport) (*raft.NetworkTransport, error) {
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	mux := newMultiplexer(list)
	go mux.Serve()

	stream := &MuxTCPStreamLayer{
		advertise:   advertise,
		protocolLis: (mux.Listen(protocolHeaders)).(*net.TCPListener),
		extendedLis: (mux.Listen(extendedHeaders)).(*net.TCPListener),
		mux:         mux,
	}

	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}

	if addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}

	go stream.listen()

	return transportCreator(stream), nil
}

func (t *MuxTCPStreamLayer) listen() error {

	for {
		conn, err := t.extendedLis.Accept()
		if err != nil {
			t.logger.Printf("[ERR] raft-net extension: Failed to accept connection: %v", err)
			continue
		}

		t.logger.Printf("[DEBUG] raft-net extension: %v accepted connection from: %v", t.Addr().String(), conn.RemoteAddr())

		go t.handleConn(conn)
	}
}

func (t *MuxTCPStreamLayer) handleConn(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		if err := t.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				t.logger.Printf("[ERR] raft-net extension: Failed to decode incoming command: %v", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			t.logger.Printf("[ERR] raft-net extension: Failed to flush response: %v", err)
			return
		}
	}
}

func (t *MuxTCPStreamLayer) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {

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
	case t.consumeCh <- rpc:
	case <-t.shutdownCh:
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
	case <-t.shutdownCh:
		return raft.ErrTransportShutdown
	}

	return nil
}

func (t *MuxTCPStreamLayer) genericRPC(target raft.ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {

	conn, err := t.getConn(target)
	if err != nil {
		return nil
	}

	defer conn.Release()

	if t.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(t.timeout))
	}

	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	_, err = decodeResponse(conn, resp)

	return err

}

func (t *MuxTCPStreamLayer) getConn(target raft.ServerAddress) (*netConn, error) {

	conn, err := t.Dial(target, t.timeout)
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

func (t *MuxTCPStreamLayer) JoinCluster(target raft.ServerAddress, args *JoinClusterRequest, resp *JoinClusterResponse) error {
	return t.genericRPC(target, rpcJoinCluster, args, resp)
}

func (t *MuxTCPStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

func (t *MuxTCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.protocolLis.Accept()
}

func (t *MuxTCPStreamLayer) Close() (err error) {
	t.shutdownLock.Lock()
	defer t.shutdownLock.Unlock()

	if !t.shutdown {
		close(t.shutdownCh)
		if err := t.mux.Close(); err != nil {
			return err
		}
		t.shutdown = true
	}

	return nil
}

func (t *MuxTCPStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}

	return t.protocolLis.Addr()
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
