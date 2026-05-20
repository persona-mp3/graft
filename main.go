package main

import (
	"flag"
	"fmt"
	"graft/node"
	"log"
	"math/rand"
	"sync"
)

var (
	defaultNodes = 3
)

func main() {
	var nodes int

	flag.IntVar(&nodes, "nodes", 3, "number of nodes to start in a cluster. By default creates 3 nodes")
	flag.Parse()

	if nodes <= 0 || nodes%2 == 0 {
		fmt.Printf("cannot create an even cluster of nodes for raft. Using default,  %d nodes\n", defaultNodes)
	}

	cluster := []*std.Node{}
	peerAddress := []string{}

	for i := range nodes {
		addr := getRandomListenAddr()
		node := std.CreateNode(i+1, addr)

		cluster = append(cluster, node)
		peerAddress = append(peerAddress, addr)
	}

	wg := sync.WaitGroup{}
	for _, node := range cluster {
		wg.Go(func() {
			if err := node.Start(peerAddress); err != nil {
				log.Println(err)
				return
			}
		})
	}

	wg.Wait()
}

func getRandomListenAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", rand.Intn(50000)+10000)
}
