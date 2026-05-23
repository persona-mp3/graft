package node

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	Addr string
	mu   sync.Mutex
	node *Node
}

func CreateServer(addr string, node *Node) *Server {
	return &Server{
		Addr: addr,
		mu:   sync.Mutex{},
		node: node,
	}
}

/*
So inside here is where the first leader election process will reside.
1. Set a heartBeat timer, if the timer fires, this node becomes candidate
*/
func (s *Server) RequestVote(req RequestVoteArgs, res *ResponseVote) error {

	if (s.node.State == Candidate || s.node.State == Follower ) && s.node.GetTerm() < req.Term {
		lgr.Println("dropping from inside while candid|follower")
		res.Id = s.node.Id
		res.RecvdVote = true
		s.node.RecvdHeartBeatCh <- true
		s.node.updateTerm()
		lgr.Printf("%s's vote got requested: %s\n", s.node.Id, req.toString())
		return nil
	}

	return nil
}

func (s *Server) Ping(req HeartBeatRequest, res *HeartBeatResponse) error {
	res.From = s.node.Id
	res.Term = s.node.GetTerm()
	s.node.RecvdHeartBeatCh <- true
	lgr.Printf("%s got pinged: %s\n", s.node.Id, req.toString())
	return nil
}

func (s *Server) Run() {
	svr := rpc.NewServer()
	if err := svr.Register(s); err != nil {
		lgr.Fatalf("could not register rcp for node_%s. reason: %s\n", s.node.Id, err)
	}

	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		lgr.Fatalf("Could not start tcp server for Node_%s at %s. Reason: %s", s.node.Id, s.node.Addr, err)
	}

	lgr.Printf("%s listening on %s", s.node.Id, s.Addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.node.Start(ctx)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				lgr.Printf("%s server has been shutdown\n", s.node.Id)
				return
			}

			lgr.Printf("%s could not accept connection. Reason: %s\n", s.node.Id, err)
			continue
		}

		lgr.Printf("%s accepted connection from: %s\n", s.node.Id, conn.RemoteAddr())
		go svr.ServeConn(conn)

	}

}
