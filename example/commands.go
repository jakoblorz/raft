package main

import "github.com/hashicorp/go-msgpack/codec"

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value interface{}
}

type DeleteRequest GetRequest

func (d *DeleteRequest) Encode(enc *codec.Encoder) error {
	if err := enc.Encode(rpcDeleteRequest); err != nil {
		return err
	}
	if err := enc.Encode(&d); err != nil {
		return err
	}

	return nil
}

func (d *DeleteRequest) Decode(dec *codec.Decoder) error {
	// rpc type byte was already decoded
	return dec.Decode(d)
}

type DeleteResponse SetResponse

type SetRequest struct {
	Key   string
	Value interface{}
}

func (s *SetRequest) Encode(enc *codec.Encoder) error {
	if err := enc.Encode(rpcSetRequest); err != nil {
		return err
	}
	if err := enc.Encode(&s); err != nil {
		return err
	}

	return nil
}

func (s *SetRequest) Decode(dec *codec.Decoder) error {
	// rpc type byte was already decoded
	return dec.Decode(s)
}

type SetResponse struct {
	Key string
}
