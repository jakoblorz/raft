package main

import (
	"io"
	"sync"

	"github.com/hashicorp/go-msgpack/codec"
)

type fsm struct {
	valuesLock sync.Mutex `codec:"-"`
	values     map[string]interface{}
}

func (f *fsm) AppendLogMessage(c []byte) {

	dec := codec.NewDecoderBytes(c, &codec.MsgpackHandle{})

	var rpc uint8
	if err := dec.Decode(&rpc); err != nil {
		return
	}

	if rpc == rpcSetRequest {
		var req = &SetRequest{}
		if err := req.Decode(dec); err != nil {
			return
		}

		f.Set(req.Key, req.Value)
		return

	} else if rpc == rpcDeleteRequest {
		var req = &DeleteRequest{}
		if err := req.Decode(dec); err != nil {
			return
		}

		f.Delete(req.Key)
		return
	}

	return
}

func (f *fsm) Get(key string) interface{} {
	f.valuesLock.Lock()
	defer f.valuesLock.Unlock()

	return f.values[key]
}

func (f *fsm) Set(key string, val interface{}) {
	f.valuesLock.Lock()
	defer f.valuesLock.Unlock()

	f.values[key] = val
}

func (f *fsm) Delete(key string) {
	f.valuesLock.Lock()
	defer f.valuesLock.Unlock()

	delete(f.values, key)
}

func (f *fsm) Encode(w io.Writer) error {
	f.valuesLock.Lock()
	defer f.valuesLock.Unlock()

	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})
	return enc.Encode(f)
}

func (f *fsm) Decode(r io.Reader) error {
	f.valuesLock.Lock()
	defer f.valuesLock.Unlock()

	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	return dec.Decode(f)
}
