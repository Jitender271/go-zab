package main

import (
	"log"
	"math/rand"
	"sync"
)

// NodeRole defines the roles in ZAB
type NodeRole string

const (
	Leader   NodeRole = "LEADER"
	Follower NodeRole = "FOLLOWER"
	Observer NodeRole = "OBSERVER"
)

// Node represents a single node in the cluster
type Node struct {
	ID           int
	Role         NodeRole
	CurrentEpoch int64
	LeaderID     int
	State        *NodeState
	mu           sync.Mutex
}

// NodeState holds the state of a node
type NodeState struct {
	Zxid       int64
	Committed  int64
	LogEntries []string // Transaction log
}

func (n *Node) CheckHealth() bool {
	return rand.Intn(10) > 6 // Simulate 20% failure
}

// ReceiveMessage processes incoming messages
func (n *Node) ReceiveMessage(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch msg.Type {
	case Transaction:
		n.State.LogEntries = append(n.State.LogEntries, msg.Payload)
		log.Printf("Node %d received transaction: %s", n.ID, msg.Payload)
	case Sync:
		n.State.Zxid++
		log.Printf("Node %d synchronized state with leader", n.ID)
	}
}

func (n *Node) ObserveState() {
	log.Printf("Observer Node %d reads committed log: %v", n.ID, n.State.LogEntries)
}
