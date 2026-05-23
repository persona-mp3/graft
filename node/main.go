package node

import (
	"fmt"
	"math/rand"
	"time"
	"github.com/persona-mp3/logger"
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

type Node struct {
	Id              string
	Addr            string
	Peers           []string
	Heartbeat       time.Duration
	ElectionTimeout time.Duration
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
		Id: id, 
		Peers: peers, 
		Heartbeat: time.Duration(time.Millisecond * heartBeatInterval),
		ElectionTimeout: electionTimeout, 
		State: Follower,
	}
}


func (node *Node) Start(){
}

func (node *Node) GetStatus() string {
	fmt := fmt.Sprintf("Status: {Id: %s, State: %s}", node.Id, node.getState() ) 
	return fmt
}

func (node *Node) getState() string {
	switch node.State {
	case 1:
		return "Leader"
	case 2:
		return "Candidate"
	case 3:
		return "Follower"
	default:
		return fmt.Sprintf("Unregistered state: %+v\n", node.State)
	}
}
