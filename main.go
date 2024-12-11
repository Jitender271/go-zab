package main

import (
	"log"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	// Initialize network
	net := NewNetwork()

	// Initialize Nodes
	nodes := []*Node{
		{ID: 1, CurrentEpoch: 1, State: &NodeState{}},
		{ID: 2, CurrentEpoch: 2, State: &NodeState{}},
		{ID: 3, CurrentEpoch: 1, State: &NodeState{}},
		{ID: 4, CurrentEpoch: 2, State: &NodeState{}},
		{ID: 5, CurrentEpoch: 2, State: &NodeState{}},
	}

	// Add Observers
	observers := []*Node{
		{ID: 6, Role: Observer, State: &NodeState{}},
		{ID: 7, Role: Observer, State: &NodeState{}},
	}

	// Register all nodes in network
	for _, node := range nodes {
		net.RegisterNode(node)
	}
	for _, observer := range observers {
		net.RegisterNode(observer)
	}

	// Leader Election
	leader := ElectLeader(nodes, net)

	// Synchronization Phase
	var followers []*Node
	for _, node := range nodes {
		if node.Role == Follower {
			followers = append(followers, node)
		}
	}

	leader.Synchronize(followers)

	// Broadcast Transactions
	leader.BroadcastTransaction("TX1", followers)
	leader.BroadcastTransaction("TX2", followers)

	// Observers reading state
	for _, observer := range observers {
		observer.ObserveState()
	}

	// Simulate failure and recovery
	log.Println("Simulating failure and recovery...")
	if !leader.CheckHealth() {
		log.Println("Leader failed, triggering re-election")
		leader = ElectLeader(nodes, net)
		leader.Synchronize(followers)
	}
}
