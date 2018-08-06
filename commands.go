package raft

import "github.com/hashicorp/raft"

type JoinClusterRequest struct {
	raft.RPCHeader

	Token      string
	RemoteAddr string
	NodeID     string
}

func (j *JoinClusterRequest) GetRPCHeader() raft.RPCHeader {
	return j.RPCHeader
}

type JoinClusterResponse struct {
	raft.RPCHeader

	Term   uint64
	Leader []byte
}

func (j *JoinClusterResponse) GetRPCHeader() raft.RPCHeader {
	return j.RPCHeader
}
