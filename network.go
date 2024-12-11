package main

import (
	"log"
	"sync"
)

// Message types for communication
type MessageType string

const (
	Transaction MessageType = "TRANSACTION"
	Sync        MessageType = "SYNC"
	Ack         MessageType = "ACK"
)

// Message structure for node communication
type Message struct {
	FromNodeID int
	Type       MessageType
	Payload    string
}

// Network simulates node-to-node communication
type Network struct {
	mu        sync.Mutex
	connected map[int]*Node
}

// NewNetwork initializes a new network
func NewNetwork() *Network {
	return &Network{
		connected: make(map[int]*Node),
	}
}

// SendMessage sends a message to a specific node
func (net *Network) SendMessage(toNodeID int, msg Message) {
	net.mu.Lock()
	defer net.mu.Unlock()

	if node, ok := net.connected[toNodeID]; ok {
		node.ReceiveMessage(msg)
	} else {
		log.Printf("Node %d not reachable", toNodeID)
	}
}

// RegisterNode adds a node to the network
func (net *Network) RegisterNode(node *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.connected[node.ID] = node
	log.Printf("Node %d registered in the network", node.ID)
}

// Broadcast sends a message to all nodes
func (net *Network) Broadcast(msg Message) {
	net.mu.Lock()
	defer net.mu.Unlock()

	for _, node := range net.connected {
		node.ReceiveMessage(msg)
	}
}
