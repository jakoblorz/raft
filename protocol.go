package raft

import (
	"fmt"
	"log"

	"github.com/hashicorp/raft"
)

// MessageMatcher represents a protocol struct which can match and
// unmarshall incomming non-raft messages
type MessageMatcher interface {
	// Match matches a rpc type to a umarshalled struct ptr (the interface{})
	// and an indicator if the rpc type is known (the bool)
	Match(uint8) (interface{}, bool)
}

type MessageNotificator interface {
	Notify(interface{}) (interface{}, error)
}

type RPCInterface func(*RemoteNode, uint8, interface{}, interface{}) error

type InterfaceReceiver interface {
	ReceiveInterface(RPCInterface)
}

type MessageProtocol interface {
	MessageMatcher
	MessageNotificator
	InterfaceReceiver
}

type joinRPCMatcher struct {
	raft      *raft.Raft
	logger    *log.Logger
	token     string
	localAddr raft.ServerAddress
	rpc       RPCInterface
}

func (j *joinRPCMatcher) Match(rpcType uint8) (interface{}, bool) {

	switch rpcType {
	case rpcJoinCluster:
		return &JoinClusterRequest{}, true
	}

	return nil, false
}

func (j *joinRPCMatcher) Notify(req interface{}) (interface{}, error) {

	// check if the req can be parsed to a JoinClusterRequest pointer
	if join, ok := req.(*JoinClusterRequest); ok {
		j.logger.Printf("[INFO] received join request from remote node %s at %s", join.NodeID, join.RemoteAddr)

		// compare tokens; in case this node is a leader or a
		// follower, the node will have to correct token locally
		if join.Token != j.token {
			err := fmt.Errorf("[ERR] join request contained wrong join token %s", join.Token)
			j.logger.Printf("%s", err)
			return nil, err
		}

		// only leaders can add new nodes to the cluster
		if leader := j.raft.Leader(); leader != j.localAddr {
			j.logger.Printf("[INFO] this is not a leader, join request will be forwarded to leader at %s", leader)

			var res = &JoinClusterResponse{}
			err := j.rpc(&RemoteNode{Address: leader}, rpcJoinCluster, req, res)
			return res, err
		}

		// obtain configuration about registered nodes
		configFuture := j.raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			j.logger.Printf("[ERR] failed to get raft configuration: %v", err)
			return nil, err
		}

		for _, srv := range configFuture.Configuration().Servers {
			if srv.ID == raft.ServerID(join.NodeID) || srv.Address == raft.ServerAddress(join.RemoteAddr) {

				if srv.Address == raft.ServerAddress(join.RemoteAddr) && srv.ID == raft.ServerID(join.NodeID) {
					j.logger.Printf("[WARN] node %s at %s already member of cluster, ignoring join request", join.NodeID, join.RemoteAddr)
				} else {
					future := j.raft.RemoveServer(srv.ID, 0, 0)
					if err := future.Error(); err != nil {
						err = fmt.Errorf("[ERR] error removing existing node %s at %s: %s", join.NodeID, join.RemoteAddr, err)
						j.logger.Printf("%s", err)
						return nil, err
					}
				}
			}
		}

		future := j.raft.AddVoter(raft.ServerID(join.NodeID), raft.ServerAddress(join.RemoteAddr), 0, 0)
		if err := future.Error(); err != nil {
			err = fmt.Errorf("[ERR] failed to add new voter %s at %s: %s", join.NodeID, join.RemoteAddr, err)
			j.logger.Printf("%s", err)
			return nil, err
		}

		j.logger.Printf("[INFO] node %s at %s joined successfully", join.NodeID, join.RemoteAddr)
		return &JoinClusterResponse{
			LeaderAddr: string(j.raft.Leader()),
			LastIndex:  j.raft.LastIndex(),
		}, nil
	}

	return nil, nil
}

func (j *joinRPCMatcher) ReceiveInterface(rpc RPCInterface) {
	j.rpc = rpc
}

type customRPCWrapper struct {
	joinProtoc *joinRPCMatcher
	custProtoc MessageProtocol
}

func (c *customRPCWrapper) Match(rpcType uint8) (interface{}, bool) {
	ji, jb := c.joinProtoc.Match(rpcType)
	if jb {
		return ji, jb
	}

	if c.custProtoc == nil {
		return ji, jb
	}

	// block any rpcTypes that might interfere with raft protocol
	if rpcType < RPCHeaderOffset {
		return nil, false
	}

	return c.custProtoc.Match(rpcType)
}

func (c *customRPCWrapper) Notify(i interface{}) (interface{}, error) {
	ji, je := c.joinProtoc.Notify(i)
	if !(je == nil && ji == nil) {
		return ji, je
	}

	if c.custProtoc == nil {
		return ji, je
	}

	// block any rpcType that might interfere with raft protocol
	// if u <= rpcJoinCluster {
	// 	return nil, errors.New("[ERR] raft internal rpc message reached custom protocol")
	// }

	return c.custProtoc.Notify(i)
}

func (c *customRPCWrapper) ReceiveInterface(i RPCInterface) {
	c.joinProtoc.ReceiveInterface(i)

	if c.custProtoc != nil {
		c.custProtoc.ReceiveInterface(i)
	}
}
