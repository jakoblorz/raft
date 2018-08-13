package main

import (
	"errors"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/jakoblorz/raft"
)

const (
	rpcGetRequest    uint8 = iota + raft.RPCHeaderOffset
	rpcSetRequest          = iota
	rpcDeleteRequest       = iota
)

type Protocol struct {
	node raft.LocalNode
	fsm  *fsm
}

func (p *Protocol) GetSharedState() raft.SharedState {
	return p.fsm
}

func (p *Protocol) GetTypeTranslator() raft.MessageTypeTranslator {

	var m = make(map[uint8]raft.MessagePtrFactory)
	m[rpcGetRequest] = func() interface{} {
		return &GetRequest{}
	}
	m[rpcSetRequest] = func() interface{} {
		return &SetRequest{}
	}
	m[rpcDeleteRequest] = func() interface{} {
		return &DeleteRequest{}
	}

	return m
}

func (p *Protocol) OnMessageReceive(u uint8, req interface{}) (interface{}, error) {

	if get, ok := req.(*GetRequest); u == rpcGetRequest && ok {
		return &GetResponse{
			Value: p.fsm.Get(get.Key),
		}, nil
	}

	if set, ok := req.(*SetRequest); u == rpcSetRequest && ok {
		var b []byte
		set.Encode(codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}))

		err := p.node.AppendLogMessage(b, 30*time.Second)
		if err != nil {
			return nil, err
		}

		return &SetResponse{
			Key: set.Key,
		}, nil
	}

	if del, ok := req.(*DeleteRequest); u == rpcDeleteRequest && ok {
		var b []byte
		del.Encode(codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}))

		err := p.node.AppendLogMessage(b, 30*time.Second)
		if err != nil {
			return nil, err
		}

		return &DeleteResponse{
			Key: del.Key,
		}, nil
	}

	return nil, errors.New("request could not be identified")
}

func (p *Protocol) SetLocalNode(node raft.LocalNode) {
	p.node = node
}
