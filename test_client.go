package main

import (
	"net/rpc"
	"fmt"
	"github.com/persona-mp3/node"
)


func main() {
	client, err := rpc.Dial("tcp", "127.0.0.1:8713")
	if err != nil {
		fmt.Println("could not dial rpcServer. Reason: ", err)
		return
	}

	req := &node.RequestVoteArgs {
		Id: "test_client",
		Term: 0,
		LogCount: 0,
	}


	res := &node.ResponseVote{}
	if err := client.Call("Server.RequestVote", req, res); err != nil {
		fmt.Println("could not RequestVote from server. Reason: ", err)
		return
	}

	fmt.Printf("Response: %+v\n", res)
}
