package raft

import (
	"bytes"
	"io"

	"github.com/hashicorp/raft"
)

type SharedState interface {
	Apply(io.Reader)
	Encode(io.Writer) error
	Decode(io.Reader) error
}

type fsm struct {
	state SharedState
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	f.state.Apply(bytes.NewReader(l.Data))
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	defer r.Close()

	f.state.Decode(r)
	return nil
}

func (f *fsm) Persist(sink raft.SnapshotSink) error {

	err := func() error {

		if err := f.state.Encode(sink); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
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
