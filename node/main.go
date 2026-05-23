package node

import (
	"context"
	"fmt"
	"github.com/persona-mp3/logger"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

var (
	lgr = logger.NewLogger(nil)
)

const (
	heartBeatInterval = 100 // in ms

)

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

// not sure if i want to use a mutex here later for the State
type Node struct {
	Id                   string
	term                 atomic.Int64
	Addr                 string
	Peers                []string
	HeartBeat            time.Duration
	RecvdHeartBeatCh     chan bool
	transitionToFollower chan bool
	ElectionTimeout      time.Duration
	logCount             int
	State
}


var (
	seed = time.Now().UnixNano()
	rng = rand.New(rand.NewSource(seed))
)
func randomNumberGenerator(limit int) int {
	return rng.Intn(limit) + 1
}

func CreateNode(id string, addr string, peers []string) *Node {
	electionTimeout := time.Duration(randomNumberGenerator(6)) * time.Second
	// for easier debugging
	heartBeatTimeout := time.Duration(randomNumberGenerator(4)) * time.Second
	return &Node{
		Id:                   id,
		Peers:                peers,
		Addr:                 addr,
		term:                 atomic.Int64{},
		HeartBeat:            heartBeatTimeout,
		RecvdHeartBeatCh:     make(chan bool, 100),
		transitionToFollower: make(chan bool, 100),
		ElectionTimeout:      electionTimeout,
		State:                Follower,
	}
}

// 1. Start the timer to watch for the heartbeats
func (node *Node) Start(ctx context.Context) {
	transitionToLeader := make(chan bool)
	transitionToFollower := make(chan bool)

	killLeader := make(chan bool)

	go node.monitorHeartBeats(transitionToLeader)

	wg := &sync.WaitGroup{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-transitionToLeader:
			lgr.Println("transitioned into leader state, sending heartbeats")
			lgr.Println("status", node.GetStatus())
			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					lgr.Println("stopped leader-routine")
				}()
				lgr.Println("starting leader-routine")
				node.sendHeartBeats(killLeader)
			}()
		case <-transitionToFollower:
			if node.State == Leader {
				killLeader <- true
			}
			node.updateTerm()
			node.updateState(Follower)

			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					lgr.Println("dropped monitor-routine")
				}()
				node.monitorHeartBeats(transitionToLeader)
			}()
		}
	}
}

func (node *Node) sendHeartBeats(offswitch <-chan bool) {

	wg := sync.WaitGroup{}

	for _, peer := range node.Peers {
		if peer == node.Addr {
			continue
		}

		wg.Add(1)
		go func(peer string) {
			ticker := time.NewTicker(node.HeartBeat)
			defer func() {
				wg.Done()
				ticker.Stop()
			}()

			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				lgr.Printf("could not dial Follower. Reason: %s\n", err)
				return
			}

			for {
				select {
				case <-offswitch:
					return
				case <-ticker.C:
					req := HeartBeatRequest{From: node.Id, Term: node.GetTerm()}
					res := &HeartBeatResponse{}
					if err := client.Call("Server.Ping", req, res); err != nil {
						lgr.Printf("could not get pong from Follower. Reason: %s\n", err)
						return
					}
					lgr.Printf("PingResponse from Follower_%s-> %+v\n", peer, res.toString())
					ticker.Reset(node.HeartBeat)
				}
			}
		}(peer)
	}

	wg.Wait()
}

func (node *Node) monitorHeartBeats(transition chan<- bool) {
	ticker := time.NewTicker(node.HeartBeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lgr.Printf("%s turning into candidate. Heartbeat not met in %+v\n", node.Id, node.HeartBeat)
			node.updateTerm()
			node.updateState(Candidate)
			lgr.Println(node.GetStatus())
			// TODO: use the electionTimeout here
			success := node.Campaign()
			if success {
				node.updateState(Leader)
				transition <- true
				return
			} else {
				lgr.Printf("%s lost campaign, sending transit to become a follower\n", node.Id)
				node.transitionToFollower <- true
			}
			return
		case <-node.RecvdHeartBeatCh:
			lgr.Printf("%s heartbeat signal met, restarting ticker\n", node.Id)
			ticker.Reset(node.HeartBeat)
		}
	}

}

// TODO: Add an election timer here
func (node *Node) Campaign() bool {
	failedConns := atomic.Int64{}
	totalPeers := len(node.Peers)

	successfulVotes := atomic.Int64{}
	failedVotes := atomic.Int64{}

	// Nodes/servers can vote themseleves
	successfulVotes.Add(1)

	if totalPeers == 0 {
		lgr.Printf("warning: %s has no peers or nodes started along side with it becoming leader\n", node.Id)
		return true
	}

	wg := &sync.WaitGroup{}
	for _, peer := range node.Peers {
		if peer == node.Addr {
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer func() {
				wg.Done()
			}()

			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				lgr.Printf("%s could not dial %s. Reason: %s\n", node.Id, peer, err)
				failedConns.Add(1)
				return
			}

			req := RequestVoteArgs{
				Id:       node.Id,
				Term:     node.GetTerm(),
				LogCount: node.logCount,
			}

			res := &ResponseVote{}

			if err := client.Call("Server.RequestVote", req, res); err != nil {
				lgr.Printf("%s could not call rpcMethod: server.RequestVote on %s. Reason: %s\n", node.Id, peer, err)
				return
			}

			if res.RecvdVote {
				lgr.Printf("%s recvd valid vote: %s\n", node.Id, res.toString())
				successfulVotes.Add(1)
			} else {
				lgr.Printf("%s recvd negative vote: %s\n", node.Id, res.toString())
				failedVotes.Add(1)
			}
		}(peer)
	}

	// Election logic here

	wg.Wait()

	if int(failedConns.Load()) == totalPeers {
		lgr.Printf("warning: all peers appeared to have to failed from %s perspective. Check cluster\n", node.Id)
		return true
	}

	// TODO(persona): Heres the thing, what if two people become leaders at the same time?
	// thats why there it's advised to always be an odd number of nodes. But that still dosen't
	// stop it yet. You can have 5 nodes in a cluster, and maybe 3 become candidates, now two nodes
	// can end up as leaders and how do we resolve that?

	tally := successfulVotes.Load()
	if tally > 1 && failedVotes.Load() < tally {
		lgr.Printf("%s is running for president\n", node.Id)
		return true
	}

	lgr.Printf("%s is dropping to Follower. FailedVotes: %d, Success: %d\n", node.Id, failedVotes.Load(), tally)
	return false
}

func (node *Node) GetStatus() string {
	fmt := fmt.Sprintf("Status: {Id: %s, State: %s, Term: %d}", node.Id, node.getState(), node.GetTerm())
	return fmt
}

func (node *Node) getState() string {
	switch node.State {
	case 0:
		return "Leader"
	case 1:
		return "Candidate"
	case 2:
		return "Follower"
	default:
		return fmt.Sprintf("Unregistered state: %+v\n", node.State)
	}
}

func (node *Node) updateTerm() {
	node.term.Add(1)
}

func (node *Node) GetTerm() int {
	return int(node.term.Load())
}

func (node *Node) updateState(state State) {
	node.State = state
}
