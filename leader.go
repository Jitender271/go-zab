package main

import (
	"log"
)

// ElectLeader simulates leader election
func ElectLeader(nodes []*Node, net *Network) *Node {
	// Re-elect leader and ensure recovery
	var leader *Node
	highestEpoch := int64(0)

	for _, node := range nodes {
		if node.CheckHealth() && node.CurrentEpoch > highestEpoch {
			highestEpoch = node.CurrentEpoch
			leader = node
		}
	}

	if leader == nil {
		log.Println("No healthy leader found, retrying...")
		return ElectLeader(nodes, net)
	}

	leader.Role = Leader
	for _, node := range nodes {
		if node.ID != leader.ID && node.CheckHealth() {
			node.Role = Follower
			node.LeaderID = leader.ID
		}
	}

	log.Printf("Leader elected: Node %d (Epoch: %d)", leader.ID, highestEpoch)
	leader.Synchronize(nodes)
	return leader
}
