package node

import (
	"net"
	"errors"
	"net/rpc"
	"sync"
)

type Server struct {
	Addr   string
	mu     sync.Mutex
	node   *Node
}

type RequestVoteArgs struct {
	Id       string
	Term     int
	LogCount int
}

type ResponseVote struct {
	Id          string
	RecvdVote   bool
}


func CreateServer(addr string, node *Node) *Server {
	return &Server {
		Addr:   addr,
		mu:     sync.Mutex{},
		node:   node,
	}
}

func (s *Server) RequestVote(req *RequestVoteArgs, res *ResponseVote) error {
	res.Id = s.node.Id
	res.RecvdVote = true
	return nil
}


func (s *Server) Run() {
	if err := rpc.Register(s); err != nil {
		lgr.Fatalf("Could not register rcp for Node_%s. Reason: %s\n", s.node.Id, err)
	}

	ln, err := net.Listen("tcp", s.Addr) 
	if err != nil {
		lgr.Fatalf("Could not start tcp server for Node_%s at %s. Reason: ", s.node.Id, s.node.Addr,  err)
	}

	lgr.Printf("%s listening on %s", s.node.Id, s.Addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				lgr.Printf("%s server has been shutdown\n", s.node.Id)
				return
			}

			lgr.Printf("%s could not accept connection. Reason: %s\n", s.node.Id, err )
			continue
		}

		lgr.Printf("%s accepted connection from: %s\n", s.node.Id, conn.LocalAddr())
		rpc.ServeConn(conn)
		
	}

}
