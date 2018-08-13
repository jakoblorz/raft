package main

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/jakoblorz/raft"
)

const (
	rpcGetRequest uint8 = iota + raft.RPCHeaderOffset
	rpcSetRequest       = iota
)

type Protocol struct {
	node raft.LocalNode `codec:"-"`

	valueLock sync.Mutex `codec:"-"`
	values    map[string]interface{}
}

func (p *Protocol) set(key string, value interface{}) {
	p.valueLock.Lock()
	defer p.valueLock.Unlock()

	p.values[key] = value
}

func (p *Protocol) get(key string) interface{} {
	p.valueLock.Lock()
	defer p.valueLock.Unlock()

	return p.values[key]
}

func (p *Protocol) SharedState() raft.SharedState {
	return p
}

func (p *Protocol) AppendLogMessage(c []byte) {
	command := SetCommand{}

	dec := codec.NewDecoder(bytes.NewReader(c), &codec.MsgpackHandle{})
	dec.Decode(&command)

	p.set(command.Key, command.Value)
}

func (p *Protocol) Encode(w io.Writer) error {
	p.valueLock.Lock()
	defer p.valueLock.Unlock()

	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})
	return enc.Encode(&p)
}

func (p *Protocol) Decode(r io.Reader) error {
	p.valueLock.Lock()
	defer p.valueLock.Unlock()

	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	return dec.Decode(&p)
}

func (p *Protocol) Match(i uint8) (interface{}, bool) {

	if i == rpcGetRequest {
		return &GetRequest{}, true
	}

	if i == rpcSetRequest {
		return &SetRequest{}, true
	}

	return nil, false
}

func (p *Protocol) Notify(req interface{}) (interface{}, error) {

	if get, ok := req.(*GetRequest); ok {
		return &GetResponse{
			Value: p.get(get.Key),
		}, nil
	}

	if set, ok := req.(*SetRequest); ok {
		command := SetCommand{
			Key:   set.Key,
			Value: set.Value,
		}

		var b []byte
		enc := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{})
		enc.Encode(&command)

		err := p.node.AppendLogMessage(b, raft.DefaultTimeoutScale*time.Second)
		if err != nil {
			return nil, err
		}

		return &SetResponse{
			Key: set.Key,
		}, nil
	}

	return nil, errors.New("request could not be identified")
}

func (p *Protocol) ReceiveLocalNode(node raft.LocalNode) {
	p.node = node
}
