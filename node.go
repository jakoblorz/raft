package raft

import (
	"encoding/base64"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft-boltdb"

	"github.com/hashicorp/raft"
	uuid "github.com/satori/go.uuid"
)

const (
	rpcJoinCluster uint8 = iota + rpcHeaderOffset

	// RPCHeaderOffset is the min header byte free to
	// be used for a custom protocol
	RPCHeaderOffset = rpcJoinCluster + 1
)

type RemoteNode struct {
	ID      raft.ServerID
	Address raft.ServerAddress
}

type Node struct {
	NodeID      raft.ServerID
	SnapshotDir string
	DatabaseDir string
	BindAddr    string

	consumeCh <-chan raft.RPC

	raft *raft.Raft

	token     string
	transport *ExtendedTransport
	protocol  MessageProtocol

	state SharedState

	logger *log.Logger
}

func (n *Node) prepare(cProtocol MessageProtocol) error {

	config := raft.DefaultConfig()
	config.LocalID = n.NodeID

	advertise, err := net.ResolveTCPAddr("tcp", n.BindAddr)
	if err != nil {
		return err
	}

	if n.token == "" {
		return errors.New("token not set")
	}

	// remember to set logger and raft implementation
	protocol := &customRPCWrapper{
		joinProtoc: &joinRPCMatcher{
			logger: nil,
			raft:   nil,
			token:  n.token,
		},
		custProtoc: cProtocol,
	}

	transport, err := NewTCPTransport(n.BindAddr, advertise, protocol, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	n.transport = transport
	n.consumeCh = transport.CustomConsumeCh()
	n.logger = transport.logger

	// setting logger
	protocol.joinProtoc.logger = transport.logger

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

	// setting raft
	n.raft = r
	protocol.joinProtoc.raft = r

	protocol.ReceiveInterface(func(r *RemoteNode, t uint8, args interface{}, resp interface{}) error {

		if t < RPCHeaderOffset {
			return errors.New("cannot send message with raft specific header")
		}

		return n.transport.genericRPC(r.ID, r.Address, t, args, resp)
	})

	n.protocol = protocol

	return nil
}

func (n *Node) listen() {

	for {
		if n.consumeCh != nil {
			select {
			case rpc := <-n.consumeCh:
				resp, err := n.protocol.Notify(rpc.Command)
				rpc.Respond(resp, err)
			default:
			}
		}
	}
}

func (n *Node) JoinCluster(id raft.ServerID, target raft.ServerAddress, args *JoinClusterRequest, resp *JoinClusterResponse) error {
	return n.transport.genericRPC(id, target, rpcJoinCluster, args, resp)
}

func (n *Node) Apply(cmd []byte, timeout time.Duration) error {
	return n.raft.Apply(cmd, timeout).Error()
}

func (n *Node) GetToken() string {
	return n.token
}

func (n *Node) GetRemoteNodes() ([]*RemoteNode, error) {

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		n.logger.Printf("[ERR] failed to get raft configuration: %v", err)
		return nil, err
	}

	nodes := make([]*RemoteNode, 0)
	for _, srv := range configFuture.Configuration().Servers {
		nodes = append(nodes, &RemoteNode{Address: srv.Address, ID: srv.ID})
	}

	return nodes, nil
}

func nodeWithID(state SharedState) *Node {

	id, _ := uuid.NewV4()

	n := &Node{
		NodeID: raft.ServerID(id.String()),
	}

	n.state = state

	return n
}

// Join creates a new Node and tries to connect it to an existing cluster
// of raft nodes
func Join(state SharedState, customProtocol MessageProtocol, addr, token string) (*Node, error) {

	n := nodeWithID(state)

	n.token = token

	if err := n.prepare(customProtocol); err != nil {
		return nil, err
	}

	err := n.JoinCluster(raft.ServerID(""), raft.ServerAddress(addr), &JoinClusterRequest{
		NodeID:     string(n.NodeID),
		Token:      token,
		RemoteAddr: n.BindAddr,
	}, &JoinClusterResponse{})
	if err != nil {
		return nil, err
	}

	go n.listen()

	return nil, nil
}

// Init creates a new Node which seeds a new cluster of raft nodes;
// each consecutive raft node needs the token to connect to this node
func Init(state SharedState, customProtocol MessageProtocol) (*Node, string, error) {
	n := nodeWithID(state)

	tk, _ := uuid.NewV4()

	n.token = base64.StdEncoding.EncodeToString(tk.Bytes())

	if err := n.prepare(customProtocol); err != nil {
		return nil, "", err
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

	go n.listen()

	return n, n.token, nil
}
