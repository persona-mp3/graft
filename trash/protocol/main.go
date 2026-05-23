package protocol

import (
	"log"
	"net/rpc"
	"sync/mutex"
)

type Server struct {
	mu   sync.Mutex
	node *Node
}

type RequestVoteArgs struct {
	Id       string
	Term     int
	LogCount int
}

type ResponseVote struct {
	Id         string
	Reason     string
	RecvdVote  bool
}

func (s *Server) RequestVote(req RequestVoteArgs, res *ResponseVote ) error {
	res.Id = s.node.Id
	res.RecvdVote = false

	switch s.node.getState() {
	case Leader:
		if req.Term > s.node.getTerm(){
			log.Printf("%s is stepping down from leader because %s has a higher term\n", s.node.Id, req.Id)
			res.RecvdVote = true
			res.Id = fmt.Sprintf("%s", node.Id)
			s.node.updateTerm()
		} else {
			res.RecvdVote = false
			res.Id = fmt.Sprintf("%s", node.Id)
		}
		
		return nil
		// Note: we need to be able to collate different candidate votes
	case Candidate:
		log.Printf("<< %s gave up vote to %s. Updating term\n", res.Id,  req.Id)
		if req.Term > s.node.getTerm() {
			res.RecvdVote = true
			s.node.updateTerm()
		}
		return nil

	case Follower: 
	if req.Term > s.node.getTerm() && req.LogCount > s.node.getLogCount() {
		log.Printf(" ** %s Updating term due to new leader\n", res.Id,  req.Id)
		if req.Term > s.node.getTerm() {
			res.RecvdVote = true
			s.node.updateTerm()
		}
	}

	default:
		return nil
	}
	return nil
}
