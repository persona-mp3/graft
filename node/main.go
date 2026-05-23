package node

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"github.com/persona-mp3/logger"
	"time"
)

var (
	lgr  = logger.NewLogger(nil)
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
	State
}

func randomNumberGenerator() int{
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	return rng.Intn(500) + 100
}


func CreateNode(id string, peers []string) *Node {
 electionTimeout := time.Duration(randomNumberGenerator()) * time.Second

	return &Node {
		Id:                  id, 
		Peers:               peers, 
		term:                atomic.Int64{},
		HeartBeat:           time.Duration(time.Millisecond * heartBeatInterval),
		RecvdHeartBeatCh:    make(chan bool, 100), 
		ElectionTimeout:     electionTimeout, 
		State:               Follower,
	}
}


// 1. Start the timer to watch for the heartbeats
func (node *Node) Start(ctx context.Context){
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
				defer func (){
					wg.Done()
					lgr.Println("stopped leader-routine")
				}()
				lgr.Println("starting leader-routine")
				node.sendHeartBeats(killLeader)
			}()
		case <-transitionToFollower:
			node.updateTerm()
			node.updateState(Follower)
			killLeader <- true

			wg.Add(1)
			go func (){
				defer func(){
					wg.Done()	
					lgr.Println("dropped moinitor-routine")
				}()
				node.monitorHeartBeats(transitionToLeader)
			}()
		}
	}
}


func (node *Node) sendHeartBeats(offswitch <-chan bool) {
	for {
		select {
		case <-offswitch:
			return
		default:
			time.Sleep(10 * time.Second)
			lgr.Println("sent all heartbeats")
		}
	}
}



func (node *Node) monitorHeartBeats(transition chan<- bool){
	ticker := time.NewTicker(node.HeartBeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lgr.Printf("%s turning into candidate. Heartbeat not met in %+v\n", node.Id, node.HeartBeat )
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

	lgr.Printf("%s starting heartbeat monitor...\n", node.Id)
}


func (node *Node) Campaign() bool {
	failedConns := atomic.Int64{}
	totalPeers := len(node.Peers)

	if totalPeers == 0 {
		lgr.Printf("warning: %s has no peers or nodes started along side with it becoming leader\n", node.Id)
		return true
	}

	
	wg := &sync.WaitGroup{}
	_ = wg
	// Election logic here

	if int(failedConns.Load()) == totalPeers {
		lgr.Printf("warning: all peers appeared to have to failed from %s perspective. Check cluter\n", node.Id)
		return true
	}
	

	// Tally votes
	lgr.Printf("%s is running for president\n", node.Id)
	time.Sleep(5 * 3 * time.Second)
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


func (node *Node) updateTerm(){
	node.term.Add(1)
}

func (node *Node) GetTerm() int{
	return int(node.term.Load())
}

func (node *Node) updateState(state State){
	node.State = state
}

