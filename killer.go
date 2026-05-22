package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Cluster struct {
	nodes []*Node
}

func (cluster *Cluster) run() {
	fmt.Println("[cluster-manager] actively running")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		args := strings.Split(input, " ")
		if len(args) < 2 {
			fmt.Println("[cluster-manager] invalid use [cmd] [node_id]")
			continue
		}

		cmd := args[0]
		nodeId := args[1]

		switch cmd {
		case "kill":
			cluster.killNodeById(nodeId)
			fmt.Println("[cluster-manager] killing")
		case "restart":
			if cluster.restartNode(nodeId) {
				fmt.Println("[cluster-manager] started node succeessfully")
			} else {
				fmt.Println("[cluster-manager] could started node ", nodeId)
			}
		}
		// if !strings.Contains(cmd, "kill") {
		// 	fmt.Println("[cluster-manager] cmd not undertstood")
		// 	continue
		// }

	}
}

func (cluster *Cluster) restartNode(id string) bool {
	for _, node := range cluster.nodes {
		if node.id == id {
			node.Start(node.peers)
			return true
		}
	}
	return false
}

func (cluster *Cluster) killNodeById(id string) {
	for _, node := range cluster.nodes {
		if node.id == id {
			node.Shutdown()
			return
		}
	}
}
