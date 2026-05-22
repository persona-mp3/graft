package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var nodes int
	flag.IntVar(&nodes, "nodes", 2, "provide number of nodes to run")
	flag.Parse()

	cluster := []*Node{}
	peerAddresses := []string{}
	for i := range nodes {
		addr := generateRandomListenAddr()
		node := createNode(fmt.Sprint(i+1), addr)
		cluster = append(cluster, node)
		peerAddresses = append(peerAddresses, addr)
	}

	wg := &sync.WaitGroup{}
	for _, node := range cluster {
		wg.Go(func() {
			node.Start(peerAddresses)
		})
	}

	manager := &Cluster{cluster}
	go manager.run()
	wg.Wait()
	log.Println("application done")
}

func (node Node) monitor(ctx context.Context) {
	// for every node.electionTimeout
	// if we dont recv hearbeat become candidate
	ticker := time.NewTicker(node.electionTimeout)
	defer func() {
		ticker.Stop()
		log.Println("stopped monitor's ticker")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("node_%s should transition state into Candidate after %+v\n", node.id, node.electionTimeout)
			// ticker.Stop()
			node.State = Candidate
			success := node.Campaign()
			if success {
				node.State = Leader
				log.Printf("node_%s turned leader::\n", node.id)
				// do leader things
				go node.sendHeartbeats()
				// todo: node.updateTerm()
			} else {
				log.Printf("todo: node_%s lost the campaign for this term, become follower or candidate\n", node.id)
				node.State = Follower
				// todo: node.updateTerm()
			}
			ticker.Reset(node.electionTimeout)

		case <-node.resetTimer:
			if node.State == Candidate {
				log.Printf("node_%s previously candiadte, dropping to follower\n", node.id)
				node.State = Follower
			}
			log.Printf("[follower_%s]Reseting electionTimeout\n", node.id)
			ticker.Reset(node.electionTimeout)

		case <-node.killMonitor:
			log.Println("killing monitor because 1. currentNode is leader")
			return
		}
	}

}

// TODO: a node sends heartbeats to itself. although it seems better to
// not do that, at the moment, that isn't neccessary as of now because we'll
// also need to tell the node to wait for heartbearts in the handleIncoming(conn)
// or make the monitor a switch. Actually we can do that. If leader, turn of monitor
// if Follower turn on monitor, if Candidate turn off monitor
func (node *Node) sendHeartbeats() {
	log.Printf("[leader]node_%s sending heartbeat as %+v\n", node.id, node.State)
	ctx, cancel := context.WithCancel(node.lifeCtx)
	log.Println("sending message to kill monitor")
	node.killMonitor <- true

	for _, addr := range node.peers {
		if addr == node.addr {
			continue
		}
		go func(ctx context.Context, addr string) {
			ticker := time.NewTicker(node.electionTimeout)
			defer ticker.Stop()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Printf("[leader]node_%s: could not dial %s. Reason: %s\n", node.id, addr, err)
				return
			}

			c := newConn(conn)

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// every node.electionTimeout, send
					msg := fmt.Sprintf("[%s][ <ping> ]", node.id)
					if err := c.Write(msg); err != nil {
						log.Printf("[leader]node_%s: could not write to %s. Reason: %s\n", node.id, addr, err)
						return
					}

					// wait for their response
					log.Printf(" >> sent pings...")
					resp, err := c.Read()
					if err != nil {
						log.Printf("[leader]node_%s: could not recv response back from %s. Reason: %s\n", node.id, addr, err)
						return
					}

					if strings.Contains(resp, "<pong>") {
						log.Printf(" >> got pong from %s\n", addr)
					}

					// note: we can't do this here, as peers may be slow to respond
					// but then we can set read-deadlines instead and since this is inside it's own routine
					// i think it's okay
					ticker.Reset(node.electionTimeout)
				}
			}
		}(ctx, addr)
	}

	<-ctx.Done()
	cancel()
}

func (node *Node) Campaign() bool {
	log.Println("")
	log.Printf("%s is currently campaiging\n", node.id)

	wg := &sync.WaitGroup{}

	voteCount := atomic.Uint32{}
	// according to the raft paper, each node can vote itself
	voteCount.Add(1)

	for _, addr := range node.peers {
		if addr == node.addr {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Printf("node_%s: could not dial %s. Reason: %s\n", node.id, addr, err)
				return
			}

			c := newConn(conn)
			msg := fmt.Sprintf("[%s][ <rqv> give me your vote]", node.id)
			if err := c.Write(msg); err != nil {
				log.Printf("node_%s: could not write to %s. Reason: %s\n", node.id, addr, err)
				return
			}

			// wait for their response

			resp, err := c.Read()
			if err != nil {
				log.Printf("node_%s: could not recv response back from %s. Reason: %s\n", node.id, addr, err)
				return
			}

			if strings.Contains(resp, "<yes>") {
				log.Printf("%s recvd vote from %s\n", node.id, addr)
				voteCount.Add(1)
			}

		}(addr)
	}

	// note: at some point,we don't have to wait for all nodes in the cluster to respond, so we don't
	// need to wait for all the go-routines to respond. Infact, we might also want to collate votes asynchronously
	wg.Wait()
	// now we need a way to determine if this node actually won the election
	return voteCount.Load() > 1
}

func generateRandomListenAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", rand.Intn(5000)+1000)
}
