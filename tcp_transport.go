package raft

import (
	"errors"
	"io"
	"log"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// tcpStreamLayer implements StreamLayer interface for plain TCP.
type tcpStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

// NewTCPTransport returns a NetworkTransport that is built on top of
// a TCP streaming transport layer.
func NewTCPTransport(
	bindAddr string,
	advertise net.Addr,
	matcher MessageMatcher,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*extendedTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream raft.StreamLayer) *extendedTransport {
		return NewExtendedTransport(stream, matcher, maxPool, timeout, logOutput)
	})
}

// NewTCPTransportWithLogger returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, with log output going to the supplied Logger
func NewTCPTransportWithLogger(
	bindAddr string,
	advertise net.Addr,
	matcher MessageMatcher,
	maxPool int,
	timeout time.Duration,
	logger *log.Logger,
) (*extendedTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream raft.StreamLayer) *extendedTransport {
		return NewExtendedTransportWithLogger(stream, matcher, maxPool, timeout, logger)
	})
}

// NewTCPTransportWithConfig returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, using the given config struct.
func NewTCPTransportWithConfig(
	bindAddr string,
	advertise net.Addr,
	config *ExtendedTransportConfig,
) (*extendedTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream raft.StreamLayer) *extendedTransport {
		config.Stream = stream
		return NewExtendedTransportWithConfig(config)
	})
}

func newTCPTransport(bindAddr string,
	advertise net.Addr,
	transportCreator func(stream raft.StreamLayer) *extendedTransport) (*extendedTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create stream
	stream := &tcpStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}

	// Verify that we have a usable advertise address
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}
	if addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}

	// Create the network transport
	trans := transportCreator(stream)
	return trans, nil
}

// Dial implements the StreamLayer interface.
func (t *tcpStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept implements the net.Listener interface.
func (t *tcpStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *tcpStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *tcpStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
