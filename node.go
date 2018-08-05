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
	NodeID      string
	SnapshotDir string
	DatabaseDir string
	BindAddr    string

	state SharedState
	raft  *raft.Raft
	init  bool
}

func nodeWithID() *Node {

	id, _ := uuid.NewV4()

	return &Node{
		NodeID: id.String(),
	}
}

func Join(state SharedState, nodeID, addr string) (*Node, error) {
	n := nodeWithID()

	n.init = false
	n.state = state

	if err := n.Open(); err != nil {
		return nil, err
	}

	configurationFut := n.raft.GetConfiguration()
	if err := configurationFut.Error(); err != nil {
		return nil, err
	}

	for _, srv := range configurationFut.Configuration().Servers {

		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				return nil, nil
			}
		}
	}

	return n, nil
}

func Init(state SharedState) (*Node, error) {
	n := nodeWithID()

	n.init = true
	n.state = state

	return n, n.Open()
}

func (n *Node) Open() error {

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.NodeID)

	addr, err := net.ResolveTCPAddr("tcp", n.BindAddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(n.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(n.SnapshotDir, 2, os.Stderr)
	if err != nil {
		return err
	}

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(n.DatabaseDir, "log.db"))
	if err != nil {
		return err
	}

	r, err := raft.NewRaft(config, &fsm{state: n.state}, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return err
	}

	n.raft = r

	if n.init {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		r.BootstrapCluster(configuration)
	}

	return nil
}
