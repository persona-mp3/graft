package std

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"time"
)

type State int

const (
	Unit State = iota
	Leader
	Follower
)

type Node struct {
	State
	id          int
	addr        string
	timeout     time.Duration
	lifeTimeCtx context.Context
	listener    net.Listener
	stopFunc    context.CancelFunc
	peers       []string
}

func randomGen() int {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return rng.Intn(100) * 1
}

func CreateNode(id int, addr string) *Node {
	duration := time.Duration(randomGen() * int(time.Millisecond))
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

	log.Printf("node_%d started at %s\n", node.id, node.addr)
	for {
		conn, err := ln.Accept()
		if err != nil && errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("listener for node_%d closed at %s\n", node.id, node.addr)
		} else if err != nil {
			log.Printf("could not accept connection. Reason: %s\n", err)
			continue
		}

		log.Println("accpeted connection from", conn.LocalAddr())
		peer := peer{
			conn: conn,
		}

		handleConn(peer)
	}

}

type peer struct {
	conn net.Conn
}

func (p peer) Read() ([]byte, error) {
	buffer := make([]byte, 1024)
	n, err := p.conn.Read(buffer)
	return buffer[:n], err
}

func handleConn(peer peer) {
	request, err := peer.Read()
	if err != nil && errors.Is(err, io.EOF) {
		log.Printf("client has disconnected")
		return
	} else if err != nil {
		log.Printf("peer read error: %s\n", err)
		return
	}

	log.Printf("request from %s, %s\n", peer.conn.RemoteAddr(), string(request))
}
