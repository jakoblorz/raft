package raft

import (
	"errors"
	"time"
)

// MessageMatcher represents a protocol struct which can match and
// unmarshall incomming non-raft messages
type MessageMatcher interface {

	// Match matches a rpc type to a umarshalled struct ptr (the interface{})
	// and an indicator if the rpc type is known (the bool)
	Match(uint8) (interface{}, bool)
}

// MessageNotificator represents a protocol interface which is
// notified about incoming messages of which the rpc type is known
// (see MessageMatcher's Match() func for more information about
// known rpc types)
type MessageNotificator interface {

	// Notify is called when a message was recieved and properly
	// decoded; return either an error or an struct ptr; both will
	// be sent back to the requesting client
	Notify(uint8, interface{}) (interface{}, error)
}

// RemoteNode is a raft node which is not the local node.
// This is supposed to be only a reference to this node.
type RemoteNode interface {
	GetID() string
	GetAddress() string
}

// LocalNode is a raft node which is the local node.
type LocalNode interface {
	AppendLogMessage([]byte, time.Duration) error
	RemoteProcedureCall(RemoteNode, uint8, interface{}, interface{}) error
	RemoteNodes() ([]RemoteNode, error)
}

// LocalNodeReceiver represents a protocol interface which
// receives a reference to a LocalNode once a cluster has been
// joined / created.
type LocalNodeReceiver interface {
	ReceiveLocalNode(LocalNode)
}

type StatefulProtocol interface {
	SharedState() SharedState
}

type MessageProtocol interface {
	StatefulProtocol

	MessageMatcher
	MessageNotificator
	LocalNodeReceiver
}

type protocolWrapper struct {
	joinProtoc MessageProtocol
	custProtoc MessageProtocol
}

func (c *protocolWrapper) SharedState() SharedState {

	if c.custProtoc != nil {
		return c.custProtoc.SharedState()
	}

	return nil
}

func (c *protocolWrapper) Match(rpcType uint8) (interface{}, bool) {
	ji, jb := c.joinProtoc.Match(rpcType)
	if jb {
		return ji, jb
	}

	if c.custProtoc == nil {
		return ji, jb
	}

	// block any rpcTypes that might interfere with raft protocol
	if rpcType < RPCHeaderOffset {
		return nil, false
	}

	return c.custProtoc.Match(rpcType)
}

func (c *protocolWrapper) Notify(u uint8, i interface{}) (interface{}, error) {
	ji, je := c.joinProtoc.Notify(u, i)
	if !(je == nil && ji == nil) {
		return ji, je
	}

	if c.custProtoc == nil {
		return ji, je
	}

	// block any rpcType that might interfere with raft protocol
	if u <= rpcJoinCluster {
		return nil, errors.New("[ERR] raft internal rpc message reached custom protocol")
	}

	return c.custProtoc.Notify(u, i)
}

func (c *protocolWrapper) ReceiveLocalNode(node LocalNode) {
	c.joinProtoc.ReceiveLocalNode(node)

	if c.custProtoc != nil {
		c.custProtoc.ReceiveLocalNode(node)
	}
}
