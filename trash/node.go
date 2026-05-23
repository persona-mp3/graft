package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"
)

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

type Node struct {
	id              string
	addr            string
	peers           []string
	electionTimeout time.Duration
	resetTimer      chan bool
	lifeCtx         context.Context
	cancelCtx       context.CancelFunc
	ln              net.Listener
	killMonitor     chan bool
	State
}

// Note: the timer here is set to seconds so it can easily be debuged
func createNode(id string, addr string) *Node {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := (rng.IntN(8) + 1)
	electionTimeout := time.Duration(duration * int(time.Second))
	return &Node{
		id:              id,
		addr:            addr,
		State:           Follower,
		resetTimer:      make(chan bool),
		killMonitor:     make(chan bool),
		electionTimeout: electionTimeout,
	}
}

func (node *Node) Start(peers []string) {
	ln, err := net.Listen("tcp", node.addr)
	if err != nil {
		log.Printf("could not start node_%s. Reason: %s\n", node.id, err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node.lifeCtx = ctx
	node.cancelCtx = cancel
	node.peers = peers
	// listener is closed on shutdown via node.Shutdown()
	node.ln = ln

	go node.monitor(ctx)

	// TODO: Server with rcp instead
	log.Printf("node_%s started at tcp:%s\n", node.id, node.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Printf("error: ln for node_%s has been closed\n", node.id)
				return
			} else {
				log.Printf("could not accept client. Reason: %s\n", err)
				continue
			}
		}

		go node.handleIncoming(conn)
	}
}

func (node *Node) handleIncoming(conn net.Conn) {
	c := newConn(conn)

	defer func() {
		conn.Close()
	}()

	for {
		req, err := c.Read()
		if err != nil {
			log.Println("peer read error: ", err)
			return
		}

		if strings.Contains(req, "<rqv>") {
			node.resetTimer <- true
			// node.updateState()
			// note: this is actually harder than i expected, what did you do!
			// now, we are not supposed to reset timer here, but it would be
			// beneficial to be more explicit that we're in a CandiateState,
			// and then when we recv a <request_for_your_vote>, we shoul drop to Follower
			if err := c.Write(fmt.Sprintf("[%s][ <yes> ]", node.id)); err != nil {
				log.Printf("could not write to candid -> %s\n", err)
				return
			}

			log.Printf("<< %s just swore away allegiance\n", node.id)
			continue

		} else if strings.Contains(req, "<ping>") {
			if err := c.Write(fmt.Sprintf("[%s][ <pong> ]", node.id)); err != nil {
				log.Printf("could not pong to leader -> %s\n", err)
				return
			}
			node.resetTimer <- true
			log.Printf("node_%s recvd ping from leader: %s\n", node.id, req)
			continue
		}

		log.Println("revd an odd request>>>", req)
	}
}

func (node *Node) Shutdown() {
	node.cancelCtx()
	node.ln.Close()

}
