package main

import (
	"sync"
	"log"
	nd "github.com/persona-mp3/node"
)

func main() {
	addr := "127.0.0.1:8713"
	node := nd.CreateNode("1", []string{})
	server := nd.CreateServer(addr, node)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func(){
		defer wg.Done()
		server.Run()
	}()


	stats := node.GetStatus()
	log.Println(stats)

	wg.Wait()

}
