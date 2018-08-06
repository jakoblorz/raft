package raft

import (
	"fmt"
	"log"
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
	logger        *log.Logger
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

	go n.consume()

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

	go n.consume()

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

func (n *Node) consume() {

FOR:
	for {
		select {
		case rpc := <-n.stream.consumeCh:

			if join, ok := rpc.Command.(*JoinClusterRequest); ok {
				n.logger.Printf("[INFO] received join request from remote node %s at %s", join.NodeID, join.RemoteAddr)

				configFuture := n.raft.GetConfiguration()
				if err := configFuture.Error(); err != nil {
					n.logger.Printf("[ERR] failed to get raft configuration: %v", err)
					rpc.Respond(nil, err)
					continue FOR
				}

				for _, srv := range configFuture.Configuration().Servers {
					if srv.ID == raft.ServerID(join.NodeID) || srv.Address == raft.ServerAddress(join.RemoteAddr) {

						if srv.Address == raft.ServerAddress(join.RemoteAddr) && srv.ID == raft.ServerID(join.NodeID) {
							n.logger.Printf("[WARN] node %s at %s already member of cluster, ignoring join request", join.NodeID, join.RemoteAddr)
						} else {
							future := n.raft.RemoveServer(srv.ID, 0, 0)
							if err := future.Error(); err != nil {
								err = fmt.Errorf("[ERR] error removing existing node %s at %s: %s", join.NodeID, join.RemoteAddr, err)
								n.logger.Printf("%s", err)
								rpc.Respond(nil, err)
								continue FOR
							}
						}
					}
				}

				future := n.raft.AddVoter(raft.ServerID(join.NodeID), raft.ServerAddress(join.RemoteAddr), 0, 0)
				if err := future.Error(); err != nil {
					err = fmt.Errorf("[ERR] failed to add new voter %s at %s: %s", join.NodeID, join.RemoteAddr, err)
					n.logger.Printf("%s", err)
					rpc.Respond(nil, err)
					continue FOR
				}

				n.logger.Printf("[INFO] node %s at %s joined successfully", join.NodeID, join.RemoteAddr)
				rpc.Respond(&JoinClusterResponse{
					LeaderAddr: string(n.raft.Leader()),
					LastIndex:  n.raft.LastIndex(),
				}, nil)

			}

		}
	}
}
