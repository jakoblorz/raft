package raft

import (
	"errors"
	"time"
)

// MessagePtrFactory emits a new request struct ptr
type MessagePtrFactory func() interface{}

// MessageTypeTranslator is a map associating rpc types
// (uint8) to MessagePtrFactorys
type MessageTypeTranslator map[uint8]MessagePtrFactory

// MessageTypeTranslatorGetter implements
// GetTypeTranslator() func
type MessageTypeTranslatorGetter interface {

	// GetTypeTranslator returns the protocol's
	// type translator
	GetTypeTranslator() MessageTypeTranslator
}

// MessageReceiveEventHandler represents a protocol interface which is
// notified about incoming messages, properly decoded into the
// interface{} with the rpc type
type MessageReceiveEventHandler interface {

	// OnMessageReceive is called when a message was recieved and has been
	// properly decoded; return either an error or an struct ptr; both will
	// be sent back to the requesting client
	OnMessageReceive(uint8, interface{}) (interface{}, error)
}

// RemoteNode is a raft node which is not the local node.
// This is supposed to only be a reference to this node.
type RemoteNode interface {
	GetID() string
	GetAddress() string
}

// LocalNode is a raft node which is the local node.
type LocalNode interface {
	RemoteNode

	AppendLogMessage([]byte, time.Duration) error
	RemoteProcedureCall(RemoteNode, uint8, interface{}, interface{}) error
	GetRemoteNodes() ([]RemoteNode, error)
	GetAuthToken() string
}

// LocalNodeSetter represents a protocol interface which
// receives a reference to a LocalNode once a cluster has been
// joined / created.
type LocalNodeSetter interface {
	SetLocalNode(LocalNode)
}

// SharedStateGetter implements the GetSharedState() func
type SharedStateGetter interface {

	// GetSharedState returns the SharedState
	GetSharedState() SharedState
}

// MessageProtocol is a message oriented protocol
// sharing a common state over all nodes of the "overlay"
// network
type MessageProtocol interface {
	SharedStateGetter
	LocalNodeSetter
	MessageTypeTranslatorGetter
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

func (c *protocolWrapper) GetTypeTranslator() MessageTypeTranslator {

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
