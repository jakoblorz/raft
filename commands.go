package raft

import "github.com/hashicorp/raft"

type joinClusterRequest struct {
	raft.RPCHeader

	Token      string
	RemoteAddr string
	NodeID     string
}

func (j *joinClusterRequest) GetRPCHeader() raft.RPCHeader {
	return j.RPCHeader
}

type joinClusterResponse struct {
	raft.RPCHeader

	LastIndex  uint64
	LeaderAddr string
}

func (j *joinClusterResponse) GetRPCHeader() raft.RPCHeader {
	return j.RPCHeader
}
