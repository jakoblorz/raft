package raft

import (
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft-boltdb"

	"github.com/hashicorp/raft"
	uuid "github.com/satori/go.uuid"
)

type Node struct {
	NodeID      raft.ServerID
	SnapshotDir string
	DatabaseDir string
	BindAddr    string

	state         SharedState
	raft          *raft.Raft
	transport     raft.Transport
	stream        *MuxTCPStreamLayer
	snapshotStore raft.SnapshotStore
	boltStore     *raftboltdb.BoltStore
}

func nodeWithID() *Node {

	id, _ := uuid.NewV4()

	return &Node{
		NodeID: raft.ServerID(id.String()),
	}
}

func Join(state SharedState, addr, token string) (*Node, error) {

	n := nodeWithID()
	n.state = state

	if err := n.Open(); err != nil {
		return nil, err
	}

	n.stream.JoinCluster(raft.ServerAddress(addr), &JoinClusterRequest{
		NodeID:     string(n.NodeID),
		Token:      token,
		RemoteAddr: n.BindAddr,
	}, &JoinClusterResponse{})

	return nil, nil
}

func Init(state SharedState) (*Node, error) {
	n := nodeWithID()

	n.state = state

	if err := n.Open(); err != nil {
		return nil, err
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      n.NodeID,
				Address: n.transport.LocalAddr(),
			},
		},
	}

	n.raft.BootstrapCluster(configuration)

	return n, nil
}

func (n *Node) Open() error {

	config := raft.DefaultConfig()
	config.LocalID = n.NodeID

	addr, err := net.ResolveTCPAddr("tcp", n.BindAddr)
	if err != nil {
		return err
	}

	transport, err := newMuxTCPTransport(n.BindAddr, addr, func(stream raft.StreamLayer) *raft.NetworkTransport {
		n.stream = stream.(*MuxTCPStreamLayer)
		return raft.NewNetworkTransport(stream, 3, 10*time.Second, os.Stderr)
	})

	if err != nil {
		return err
	}

	n.transport = transport

	snapshots, err := raft.NewFileSnapshotStore(n.SnapshotDir, 2, os.Stderr)
	if err != nil {
		return err
	}

	n.snapshotStore = snapshots

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(n.DatabaseDir, "log.db"))
	if err != nil {
		return err
	}

	n.boltStore = boltDB

	r, err := raft.NewRaft(config, &fsm{state: n.state}, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return err
	}

	n.raft = r

	return nil
}
