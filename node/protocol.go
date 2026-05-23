package node

import "fmt"

type RequestVoteArgs struct {
	Id       string
	Term     int
	LogCount int
}

type ResponseVote struct {
	Id        string
	RecvdVote bool
}

type HeartBeatRequest struct {
	From string
	Term int
}

type HeartBeatResponse struct {
	From string
	Term int
}

func (req *RequestVoteArgs) toString() string {
	return fmt.Sprintf("RequestVoteArgs { Id: %s, Term: %d, LogCount: %d }",
		req.Id, req.Term, req.LogCount,
	)
}

func (res *ResponseVote) toString() string {
	return fmt.Sprintf("ResponseVote { Id: %s, RecvdVote: %t}",
		res.Id, res.RecvdVote,
	)
}

func (req *HeartBeatRequest) toString() string {
	return fmt.Sprintf("HeartBeatRequest: {From: %s, Term: %d}", req.From, req.Term)
}

func (res *HeartBeatResponse) toString() string {
	return fmt.Sprintf("HeartBeatRequest: {From: %s, Term: %d}", res.From, res.Term)
}
