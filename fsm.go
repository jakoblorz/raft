package raft

import (
	"io"

	"github.com/hashicorp/raft"
)

// SharedState is a interface representing
// a log / transaction based state which
// can be shared over multiple nodes.
// The raft protocol ensures that the state
// is equalized on all nodes
type SharedState interface {

	// AppendLogMessage decodes the log message
	// and applies the represented change to the
	// local state
	AppendLogMessage([]byte)

	// Encode encodes the current state into
	// the writer
	Encode(io.Writer) error

	// Decode decodes a state from the reader
	// into the local state
	Decode(io.Reader) error
}

type fsm struct {
	state SharedState
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	f.state.AppendLogMessage(l.Data)
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
