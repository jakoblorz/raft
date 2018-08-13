package raft

import (
	"errors"
	"time"
)

type MessagePtrFactory func() interface{}

type TypeTranslator map[uint8]MessagePtrFactory

// TypeTranslatorGetter represents a protocol struct which can match and
// unmarshall incomming non-raft messages
type TypeTranslatorGetter interface {

	// GetTypeTranslator matches a rpc type to a umarshalled struct ptr (the interface{})
	// and an indicator if the rpc type is known (the bool)
	GetTypeTranslator() TypeTranslator
}

// MessageReceiveEventHandler represents a protocol interface which is
// notified about incoming messages of which the rpc type is known
// (see MessageMatcher's Match() func for more information about
// known rpc types)
type MessageReceiveEventHandler interface {

	// OnMessageReceive is called when a message was recieved and properly
	// decoded; return either an error or an struct ptr; both will
	// be sent back to the requesting client
	OnMessageReceive(uint8, interface{}) (interface{}, error)
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
	GetRemoteNodes() ([]RemoteNode, error)
}

// LocalNodeSetter represents a protocol interface which
// receives a reference to a LocalNode once a cluster has been
// joined / created.
type LocalNodeSetter interface {
	SetLocalNode(LocalNode)
}

type SharedStateGetter interface {
	GetSharedState() SharedState
}

type MessageProtocol interface {
	SharedStateGetter
	TypeTranslatorGetter
	LocalNodeSetter

	MessageReceiveEventHandler
}

type protocolWrapper struct {
	joinProtoc MessageProtocol
	custProtoc MessageProtocol
}

func (c *protocolWrapper) GetSharedState() SharedState {

	if c.custProtoc != nil {
		return c.custProtoc.GetSharedState()
	}

	return nil
}

func (c *protocolWrapper) GetTypeTranslator() TypeTranslator {

	jtt := c.joinProtoc.GetTypeTranslator()

	if c.custProtoc == nil {
		return jtt
	}

	ctt := c.custProtoc.GetTypeTranslator()
	for k, v := range ctt {

		// block any rpcTypes that might interfere with raft protocol
		if k < RPCHeaderOffset {
			return nil
		}

		jtt[k] = v
	}

	return jtt
}

func (c *protocolWrapper) OnMessageReceive(u uint8, i interface{}) (interface{}, error) {
	ji, je := c.joinProtoc.OnMessageReceive(u, i)
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

	return c.custProtoc.OnMessageReceive(u, i)
}

func (c *protocolWrapper) SetLocalNode(node LocalNode) {
	c.joinProtoc.SetLocalNode(node)

	if c.custProtoc != nil {
		c.custProtoc.SetLocalNode(node)
	}
}
