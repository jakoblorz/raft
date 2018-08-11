package main

import (
	"log"
	"sync"

	"github.com/jakoblorz/raft"
)

var (
	node *raft.Node
)

func main() {

	p := &Protocol{
		valueLock: sync.Mutex{},
		values:    make(map[string]interface{}),
	}

	node, token, err := raft.Init(p, p)
	if err != nil {
		log.Fatalf("error occured: %v", err)
	}

}
