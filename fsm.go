package raft

import (
	"io"

	"github.com/hashicorp/raft"
)

type SharedState interface {
	Apply([]byte)
	Encode() []byte
	Decode(r io.ReadCloser)
}

type fsm struct {
	state SharedState
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	f.state.Apply(l.Data)
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	f.state.Decode(r)
	return nil
}

func (f *fsm) Persist(sink raft.SnapshotSink) error {

	err := func() error {
		if _, err := sink.Write(f.state.Encode()); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsm) Release() {}
