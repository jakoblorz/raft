package main

import (
	"log"
	"sync"

	"github.com/jakoblorz/raft"
)

func main() {

	p := &Protocol{
		valueLock: sync.Mutex{},
		values:    make(map[string]interface{}),
	}

	_, err := raft.Init(p)
	if err != nil {
		log.Fatalf("error occured: %v", err)
	}

}
