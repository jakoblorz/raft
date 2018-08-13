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

type remoteNode struct {
	ID      raft.ServerID
	Address raft.ServerAddress
}

func (r *remoteNode) GetID() string {
	return string(r.ID)
}

func (r *remoteNode) GetAddress() string {
	return string(r.Address)
}

type localNode struct {
	NodeID      raft.ServerID
	SnapshotDir string
	DatabaseDir string
	BindAddr    string

	consumeCh <-chan rpc

	raft *raft.Raft

	token     string
	transport *extendedTransport
	protocol  MessageProtocol

	state SharedState

	logger *log.Logger
}

func (n *localNode) prepare(cProtocol MessageProtocol) error {

	logger := log.New(os.Stderr, "", log.LstdFlags)

	config := raft.DefaultConfig()
	config.LocalID = n.NodeID

	advertise, err := net.ResolveTCPAddr("tcp", n.BindAddr)
	if err != nil {
		return err
	}

	if n.token == "" {
		return errors.New("token not set")
	}

	jProtocol := &joinProtocol{
		addr:   advertise.String(),
		token:  n.token,
		Logger: logger,
	}

	protocol := &protocolWrapper{
		joinProtoc: jProtocol,
		custProtoc: cProtocol,
	}

	n.state = protocol.SharedState()

	transport, err := NewTCPTransportWithLogger(n.BindAddr, advertise, protocol, 3, 10*time.Second, logger)
	if err != nil {
		return err
	}

	n.transport = transport
	n.consumeCh = transport.CustomConsumeCh()
	n.logger = transport.logger

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
	jProtocol.raft = r

	protocol.ReceiveLocalNode(n)
	n.protocol = protocol

	return nil
}

func (n *localNode) listen() {

	for {
		if n.consumeCh != nil {
			select {
			case rpc := <-n.consumeCh:
				resp, err := n.protocol.Notify(rpc.CommandType, rpc.Command)
				rpc.Respond(resp, err)
			default:
			}
		}
	}
}

func (n *localNode) join(id raft.ServerID, target raft.ServerAddress, args *JoinClusterRequest, resp *JoinClusterResponse) error {
	return n.transport.genericRPC(id, target, rpcJoinCluster, args, resp)
}

func (n *localNode) AppendLogMessage(m []byte, t time.Duration) error {
	return n.raft.Apply(m, t).Error()
}

func (n *localNode) RemoteProcedureCall(r RemoteNode, t uint8, a interface{}, res interface{}) error {

	if t < RPCHeaderOffset {
		return errors.New("cannot send message with raft specific header")
	}

	return n.transport.genericRPC(raft.ServerID(r.GetID()), raft.ServerAddress(r.GetAddress()), t, a, res)
}

func (n *localNode) RemoteNodes() ([]RemoteNode, error) {

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		n.logger.Printf("[ERR] failed to get raft configuration: %v", err)
		return nil, err
	}

	nodes := make([]RemoteNode, 0)
	for _, srv := range configFuture.Configuration().Servers {
		nodes = append(nodes, &remoteNode{Address: srv.Address, ID: srv.ID})
	}

	return nodes, nil
}

func nodeWithID() *localNode {

	id, _ := uuid.NewV4()

	n := &localNode{
		NodeID: raft.ServerID(id.String()),
	}

	return n
}

// Join creates a new localNode and tries to connect it to an existing cluster
// of raft nodes
func Join(customProtocol MessageProtocol, addr, token string) error {

	n := nodeWithID()

	n.token = token

	if err := n.prepare(customProtocol); err != nil {
		return err
	}

	err := n.join(
		raft.ServerID(""),
		raft.ServerAddress(addr),
		&JoinClusterRequest{
			NodeID:     string(n.NodeID),
			Token:      token,
			RemoteAddr: n.BindAddr,
		},
		&JoinClusterResponse{})
	if err != nil {
		return err
	}

	go n.listen()

	return nil
}

// Init creates a new localNode which seeds a new cluster of raft nodes;
// each consecutive raft node needs the token to connect to this node
func Init(customProtocol MessageProtocol) (string, error) {
	n := nodeWithID()

	tk, _ := uuid.NewV4()
	n.token = base64.StdEncoding.EncodeToString(tk.Bytes())

	if err := n.prepare(customProtocol); err != nil {
		return "", err
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

	return n.token, nil
}
