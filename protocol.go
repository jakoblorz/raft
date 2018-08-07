package raft

type MessageMatcher interface {
	// Match matches a rpc type to a request ptr
	Match(rpcType uint8) (interface{}, bool)
}
