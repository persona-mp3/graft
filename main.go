package main

import (
	"math/rand"
	"fmt"
	"flag"
	"sync"
	graft "github.com/persona-mp3/node"
)

var (
	defaultInstances = 2
)


func main() {
	var instances int
	flag.IntVar(&instances, "instances", defaultInstances, "number of instances to run in a cluster. Default is 2")
	flag.Parse()

	addrs := []string{}
	for i := range instances {
		_ = i
		addrs = append(addrs, generateRandomListenAddr())
	}

	cluster := []*graft.Server{}
	for id, addr := range addrs {
		node := graft.CreateNode(fmt.Sprintf("%d", id+1), addr,  addrs)
		server := graft.CreateServer(addr, node)
		cluster = append(cluster, server)
	}

	wg := &sync.WaitGroup{}

	for _, server := range cluster {
		wg.Add(1)
		go func(server *graft.Server){
			defer wg.Done()
			server.Run()
		}(server)
	}



	wg.Wait()

}

func generateRandomListenAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", rand.Intn(5000)+1000)
}

