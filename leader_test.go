package main

import (
	"fmt"
	"testing"
	"time"
	lib "github.com/persona-mp3/node"
)

func TestLeaderElectionSingleLeader(t *testing.T) {
	addrs := []string{
		"127.0.0.1:19001",
		"127.0.0.1:19002",
		"127.0.0.1:19003",
	}

	nodes := make([]*lib.Node, len(addrs))
	for i, addr := range addrs {
		nodes[i] = lib.CreateNode(fmt.Sprintf("%d", i+1), addr, addrs)
		server := lib.CreateServer(addr, nodes[i])
		go server.Run()
	}

	time.Sleep(5 * time.Second)

	leaders := 0
	for _, n := range nodes {
		if n.State == lib.Leader {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("expected 1 leader, got %d", leaders)
	}
}
