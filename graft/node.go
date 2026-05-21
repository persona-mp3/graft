package graft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type State int

type RequestType int

const (
	Unit State = iota
	Candidate
	Leader
	Follower
)

const (
	HeartBeat RequestType = iota
	Unknown
)

type Request struct {
	body string
	RequestType
}

type Node struct {
	State
	id          int
	addr        string
	timeout     time.Duration
	lifeTimeCtx context.Context
	listener    net.Listener
	cancelFunc  context.CancelFunc
	peers       []string
}

func randomNumber() int {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return rng.Intn(100) * 1
}

func CreateNode(id int, addr string) *Node {
	duration := time.Duration(randomNumber() * int(time.Millisecond))
	return &Node{
		id:      id,
		addr:    addr,
		timeout: duration,
		State:   Unit,
	}
}

func (node *Node) Start(peers []string) error {
	filteredPeers := []string{}
	for _, peer := range peers {
		if peer != node.addr {
			filteredPeers = append(filteredPeers, peer)
		}
	}

	ln, err := net.Listen("tcp", node.addr)
	if err != nil {
		log.Printf("node_%d could not start. Reason: %s\n", node.id, err)
		return fmt.Errorf("node_%d could not start. Reason: %s\n", node.id, err)
	}

	node.listener = ln
	node.peers = filteredPeers

	log.Printf("node_%d started at %s\n", node.id, node.addr)
	ctx, cancel := context.WithCancel(context.Background())

	// so two people can write to this channel, and manageState is the only
	// reader. Inside manageState, the nodesTimerMonitor will always be monitoring
	// if it recvd a heartBeat request or not. If it does, it resets the timer
	// and doesnt tell manageState anything, otherwise it does
	transitionSignal := make(chan State)
	reset := make(chan bool)
	go node.manageState(transitionSignal)
	go node.startMonitor(reset, transitionSignal)

	node.lifeTimeCtx = ctx
	node.cancelFunc = cancel
	for {
		conn, err := ln.Accept()
		if err != nil && errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("listener for node_%d closed at %s\n", node.id, node.addr)
		} else if err != nil {
			log.Printf("could not accept connection. Reason: %s\n", err)
			continue
		}

		log.Println("accepted connection from", conn.LocalAddr())
		peer := peer{
			conn: conn,
		}

		// check if its a heartbeat first
		// todo (this might be blocking here, so might want to set a readtimeout)
		node.handleConn(peer, reset)
	}

}

/*
	We have a bunch of things that can alter the state of a node.
	1. Unit, at the start ie not a candidate, neither a leader, nor a follower
	2. Candidate, carrying out an electoral process
	3. Follower, follows a leader
	4. Leader, wins an electoral process and all other nodes follow him

	State can be changed dependent on these factors:
	1. HeartBeat, leader didn't send heartbeat on time
	   _Follower -> Candidate

	2. Won an election
		_Candidate -> Leader

	So the only tigger as at now is the heartbeat. If the leader didn't send
	a heartbeat request for x_ms, we can then transition from _Follower -> Candidate
*/

func (node *Node) manageState(transition <-chan State) {
	done := make(chan bool, 2)
	for {
		to := <-transition
		switch to {
		case Candidate:
			fmt.Println("[debug]:: switching to canditate state")
			node.State = Candidate
			success, peerConns := node.Campaign()
			if success {
				node.State = Leader
				// do leader stuff
				log.Printf("node_%d will do leader stuff\n", node.id)
				go func() {
					select {
					case <-done:
						return
					default:
						node.sendHeartBeats(peerConns)
					}
				}()
			} else {
				node.State = Follower
				done <- true
				// do follower stuff
				log.Printf("node_%d will do follower stuff\n", node.id)
			}

		}
	}
}

func (node *Node) sendHeartBeats(conns []peer) {
	ticker := time.NewTicker(node.timeout)
	for i := range ticker.C {
		_ = i
		for _, peer := range conns {
			peer.Write("this that heartbeat")
		}
		ticker.Reset(node.timeout)
	}
}

func (node *Node) Campaign() (bool, []peer) {
	log.Printf("node_%d is sending out campaign\n", node.id)
	fmt.Printf("sending out requests to %+v\n", node.peers)
	wg := &sync.WaitGroup{}

	votes := []string{}
	openConns := []peer{}
	// theres going to be alot of chitchat here and there later on
	for _, addr := range node.peers {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("[debug]:: could not dial %s. Reason: %s\n", addr, err)
			continue
		}

		wg.Go(func() {
			req := fmt.Sprintf("node_%d is now leader. I will now be sending out hearbeats", node.id)
			if _, err := conn.Write([]byte(req)); err != nil {
				log.Printf("could not write to addr: %s. Reason: %s\n", addr, err)
				conn.Close()
				return
			}

			buffer := make([]byte, 1024)
			n, err := conn.Read(buffer)
			if err != nil {
				fmt.Printf("[debug]:: could not read from %s. Reason: %s\n", addr, err)
				conn.Close()
				return
			}

			msg := string(buffer[:n])
			votes = append(votes, msg)
			openConns = append(openConns, peer{conn})
		})
	}

	if len(votes) >= 1 {
		fmt.Printf("[debug]:: node_%d recvd votes -> %+v\n", node.id, votes)
		return true, openConns
	}
	return false, openConns
}

// So for this one, everything is tiggered to go on the offensive
// from Follower -> Candidate
// But we'll need to transition from Candidate -> Follower or Follower -> Leader
// I think that can be done inside managedState()
func (node *Node) startMonitor(reset <-chan bool, transition chan<- State) {
	ticker := time.NewTicker(node.timeout)
	for {
		select {
		case <-reset:
			ticker.Reset(node.timeout)
		case <-ticker.C:
			fmt.Printf("[debug]:: %d recvd nothing from leader after %+v\n", node.id, node.timeout)
			// always make sure that monitor always requests to transition to candidate
			// if node is a follower and not a leader
			if node.State == Follower || node.State == Unit {
				switch node.State {
				case Follower:
					transition <- Candidate
				case Unit:
					transition <- Candidate
				}

				// todo(need to be careful here as it might take longer that the timeout to
				// finish an election.
				// ticker.Reset(node.timeout)
				ticker.Stop()
			} else if node.State == Leader {
				ticker.Reset(node.timeout)
				continue
			}
			// todo(need to be careful here as it might take longer that the timeout to
			// finish an election.
			fmt.Printf("[debug]:: beware here, unknown state -> %+v\n", node.State)
			time.Sleep(3 * time.Second)
			ticker.Stop()
		}
	}
}

func (node *Node) handleConn(peer peer, reset chan<- bool) {
	rawContent, err := peer.Read()
	if err != nil && errors.Is(err, io.EOF) {
		log.Printf("client has disconnected")
		return
	} else if err != nil {
		log.Printf("peer read error: %s\n", err)
		return
	}

	msg := string(rawContent)
	if strings.Contains(msg, "heartbeat") || strings.Contains(msg, "leader") {
		reset <- true
		fmt.Printf("[debug]:: I node_%d, have given my allegiance\n", node.id)
		if err := peer.Write(fmt.Sprintf("node_%d gives you my loyal alleigance", node.id)); err != nil {
			fmt.Printf("[error]:: could not cast vote to leader: %s\n", err)
		}
	}

	// inspectRequest(peer)

	log.Printf("request from %s, %s\n", peer.conn.RemoteAddr(), msg)
}

func (node *Node) Stop() {
	node.cancelFunc()
	node.listener.Close()
	log.Printf("successfully stopped node_%d\n", node.id)
}
