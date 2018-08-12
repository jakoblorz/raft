package raft

import (
	"fmt"
	"log"

	"github.com/hashicorp/raft"
)

type joinProtocol struct {
	raft  *raft.Raft
	node  LocalNode
	token string
	addr  string

	Logger *log.Logger
}

func (j *joinProtocol) SharedState() SharedState {
	return nil
}

func (j *joinProtocol) Match(t uint8) (interface{}, bool) {

	if t == rpcJoinCluster {
		return &JoinClusterRequest{}, true
	}

	return nil, false
}

func (j *joinProtocol) Notify(req interface{}) (interface{}, error) {

	// check if the req can be parsed to a JoinClusterRequest pointer
	if join, ok := req.(*JoinClusterRequest); ok {
		j.Logger.Printf("[INFO] received join request from remote node %s at %s", join.NodeID, join.RemoteAddr)

		// compare tokens; in case this node is a leader or a
		// follower, the node will have to correct token locally
		if join.Token != j.token {
			err := fmt.Errorf("[ERR] join request contained wrong join token %s", join.Token)
			j.Logger.Printf("%s", err)
			return nil, err
		}

		// only leaders can add new nodes to the cluster
		if leader := j.raft.Leader(); string(leader) != j.addr {
			j.Logger.Printf("[INFO] this is not a leader, join request will be forwarded to leader at %s", leader)

			var res = &JoinClusterResponse{}
			err := j.node.RemoteProcedureCall(&remoteNode{Address: leader}, rpcJoinCluster, req, res)
			return res, err
		}

		// obtain configuration about registered nodes
		configFuture := j.raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			j.Logger.Printf("[ERR] failed to get raft configuration: %v", err)
			return nil, err
		}

		for _, srv := range configFuture.Configuration().Servers {
			if srv.ID == raft.ServerID(join.NodeID) || srv.Address == raft.ServerAddress(join.RemoteAddr) {

				if srv.Address == raft.ServerAddress(join.RemoteAddr) && srv.ID == raft.ServerID(join.NodeID) {
					j.Logger.Printf("[WARN] node %s at %s already member of cluster, ignoring join request", join.NodeID, join.RemoteAddr)
				} else {
					future := j.raft.RemoveServer(srv.ID, 0, 0)
					if err := future.Error(); err != nil {
						err = fmt.Errorf("[ERR] error removing existing node %s at %s: %s", join.NodeID, join.RemoteAddr, err)
						j.Logger.Printf("%s", err)
						return nil, err
					}
				}
			}
		}

		future := j.raft.AddVoter(raft.ServerID(join.NodeID), raft.ServerAddress(join.RemoteAddr), 0, 0)
		if err := future.Error(); err != nil {
			err = fmt.Errorf("[ERR] failed to add new voter %s at %s: %s", join.NodeID, join.RemoteAddr, err)
			j.Logger.Printf("%s", err)
			return nil, err
		}

		j.Logger.Printf("[INFO] node %s at %s joined successfully", join.NodeID, join.RemoteAddr)
		return &JoinClusterResponse{
			LeaderAddr: string(j.raft.Leader()),
			LastIndex:  j.raft.LastIndex(),
		}, nil
	}

	return nil, nil
}

func (j *joinProtocol) ReceiveLocalNode(node LocalNode) {
	j.node = node
}
