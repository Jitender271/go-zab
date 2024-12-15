package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// --- Node Roles and Message Types ---

type NodeRole string

type MessageType string

const (
	Leader   NodeRole = "LEADER"
	Follower NodeRole = "FOLLOWER"
	Observer NodeRole = "OBSERVER"

	Transaction MessageType = "TRANSACTION"
	Sync        MessageType = "SYNC"
	Ack         MessageType = "ACK"
)

// --- Proposal and Message Definitions ---

type Proposal struct {
	Epoch int64
	Zxid  int64
	Tx    string
}

func (p Proposal) String() string {
	return fmt.Sprintf("Epoch:%d,Zxid:%d,Tx:%s", p.Epoch, p.Zxid, p.Tx)
}

func ParseProposal(data string) Proposal {
	var epoch, zxid int64
	var tx string
	fmt.Sscanf(data, "Epoch:%d,Zxid:%d,Tx:%s", &epoch, &zxid, &tx)
	return Proposal{Epoch: epoch, Zxid: zxid, Tx: tx}
}

type Message struct {
	FromNodeID       int
	Type             MessageType
	Payload          string
	PayloadZxid      int64
	PayloadCommitted int64
}

// --- Node State and Behavior ---

type NodeState struct {
	Zxid       int64
	Committed  int64
	LogEntries []string
}

type Node struct {
	ID                int
	Role              NodeRole
	CurrentEpoch      int64
	LeaderID          int
	State             *NodeState
	IdempotentTracker map[string]bool
	mu                sync.Mutex
	healthCheckTimer  *time.Timer
}

func (n *Node) CheckHealth() bool {
	return rand.Intn(10) > 2
}

func (n *Node) ReceiveMessage(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch msg.Type {
	case Transaction:
		proposal := ParseProposal(msg.Payload)
		if proposal.Zxid <= n.State.Committed {
			log.Printf("Node %d: Ignoring duplicate or outdated proposal %s", n.ID, msg.Payload)
			return
		}
		n.State.LogEntries = append(n.State.LogEntries, msg.Payload)
		n.State.Zxid = proposal.Zxid
		log.Printf("Node %d received transaction: %s", n.ID, proposal.Tx)

	case Sync:
		n.State.Zxid = msg.PayloadZxid
		n.State.Committed = msg.PayloadCommitted
		log.Printf("Node %d synchronized with leader", n.ID)

	case Ack:
		log.Printf("Node %d acknowledged message", n.ID)
	}
}

func (n *Node) ObserveState() {
	log.Printf("Observer Node %d reads committed log: %v", n.ID, n.State.LogEntries)
}

// --- Leader Election ---

func ElectLeader(nodes []*Node, net *Network) *Node {
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
		time.Sleep(time.Second)
		return ElectLeader(nodes, net)
	}

	leader.mu.Lock()
	leader.Role = Leader
	leader.CurrentEpoch++
	leader.mu.Unlock()

	for _, node := range nodes {
		if node.ID != leader.ID {
			node.Role = Follower
			node.LeaderID = leader.ID
		}
	}

	log.Printf("Leader elected: Node %d (Epoch: %d)", leader.ID, highestEpoch)
	leader.Synchronize(nodes)
	return leader
}

// --- Synchronization ---

func (n *Node) Synchronize(followers []*Node) {
	if n.Role != Leader {
		log.Println("Only the leader can initiate synchronization")
		return
	}

	if len(n.State.LogEntries) == 0 {
		log.Println("Leader has no log entries to synchronize")
		return
	}

	for _, follower := range followers {
		follower.mu.Lock()
		if follower.State.Committed >= int64(len(n.State.LogEntries)) {
			log.Printf("Warning: Follower %d committed index (%d) is out of range (LogEntries length: %d)",
				follower.ID, follower.State.Committed, len(n.State.LogEntries))
			follower.mu.Unlock()
			continue
		}

		missingEntries := n.State.LogEntries[follower.State.Committed:]
		follower.State.LogEntries = append(follower.State.LogEntries, missingEntries...)
		follower.State.Zxid = n.State.Zxid
		follower.State.Committed = n.State.Committed

		follower.mu.Unlock()
		log.Printf("Synchronized Node %d with Leader (Committed: %d)", follower.ID, n.State.Committed)
	}
}

func (n *Node) TruncateLogs() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.State.LogEntries) > 1000 {
		n.State.LogEntries = n.State.LogEntries[len(n.State.LogEntries)-1000:]
		log.Printf("Node %d truncated logs to the last 1000 entries", n.ID)
	}
}

// --- Broadcasting Transactions ---

func (n *Node) BroadcastTransaction(tx string, followers []*Node, net *Network) {
	if n.Role != Leader {
		log.Println("Only the leader can broadcast transactions")
		return
	}

	proposal := Proposal{
		Epoch: n.CurrentEpoch,
		Zxid:  n.State.Zxid + 1,
		Tx:    tx,
	}

	n.mu.Lock()
	n.State.Zxid = proposal.Zxid
	n.State.LogEntries = append(n.State.LogEntries, proposal.String())
	n.mu.Unlock()

	acks := 0
	for _, follower := range followers {
		success := net.SendReliableMessage(follower.ID, Message{
			FromNodeID: n.ID,
			Type:       Transaction,
			Payload:    proposal.String(),
		})
		if success {
			acks++
			log.Printf("Transaction %s sent to Node %d", tx, follower.ID)
		}
	}

	if acks >= (len(followers)/2)+1 {
		n.mu.Lock()
		n.State.Committed = proposal.Zxid
		n.mu.Unlock()
		log.Printf("Transaction %s committed by Leader", tx)
	} else {
		log.Println("Quorum not reached. Transaction aborted")
	}
}

// --- Network Simulation ---

type Network struct {
	mu        sync.Mutex
	connected map[int]*Node
}

func NewNetwork() *Network {
	return &Network{
		connected: make(map[int]*Node),
	}
}

func (net *Network) SendReliableMessage(toNodeID int, msg Message) bool {
	net.mu.Lock()
	defer net.mu.Unlock()

	if node, ok := net.connected[toNodeID]; ok {
		node.ReceiveMessage(msg)
		return true
	}
	log.Printf("Node %d not reachable", toNodeID)
	return false
}

func (net *Network) RegisterNode(node *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.connected[node.ID] = node
	log.Printf("Node %d registered in the network", node.ID)
}

// --- Main ---

func main() {
	rand.Seed(time.Now().UnixNano())
	net := NewNetwork()

	// Initialize nodes and observers
	nodes := []*Node{
		{ID: 1, CurrentEpoch: 1, State: &NodeState{LogEntries: []string{}}},
		{ID: 2, CurrentEpoch: 2, State: &NodeState{LogEntries: []string{}}},
		{ID: 3, CurrentEpoch: 1, State: &NodeState{LogEntries: []string{}}},
	}
	observers := []*Node{
		{ID: 6, Role: Observer, State: &NodeState{}},
	}

	// Register nodes and observers
	for _, node := range nodes {
		net.RegisterNode(node)
	}
	for _, observer := range observers {
		net.RegisterNode(observer)
	}

	// Leader election
	leader := ElectLeader(nodes, net)
	var followers []*Node
	for _, node := range nodes {
		if node.Role == Follower {
			followers = append(followers, node)
		}
	}

	// Synchronize followers and validate
	leader.Synchronize(followers)
	leader.BroadcastTransaction("TX1", followers, net)
	leader.BroadcastTransaction("TX2", followers, net)

	// Simulate leader failure
	log.Println("Simulating leader failure...")
	leader.Role = Follower
	log.Println("Checking debug logs during re-election...")
	leader = ElectLeader(nodes, net)
	log.Printf("Debug: New leader elected with ID %d", leader.ID)
	// Add transactions and test log truncation
	for i := 1; i <= 10; i++ {
		leader.BroadcastTransaction(fmt.Sprintf("TX%d", i), followers, net)
	}
	leader.mu.Lock()
	if len(leader.State.LogEntries) > 5 {
		leader.State.LogEntries = leader.State.LogEntries[len(leader.State.LogEntries)-5:]
	}
	leader.mu.Unlock()
	log.Printf("Logs after truncation: %v", leader.State.LogEntries)

	// Simulate follower failure
	log.Println("Simulating follower failure...")
	failedFollower := followers[0]
	net.mu.Lock()
	delete(net.connected, failedFollower.ID)
	net.mu.Unlock()

	for _, follower := range followers {
		if _, ok := net.connected[follower.ID]; ok {
			log.Printf("Follower %d is still active", follower.ID)
		} else {
			log.Printf("Follower %d is down", follower.ID)
		}
	}

	leader.BroadcastTransaction("TX_FAILOVER", followers, net)

	// Validate observer state
	for _, observer := range observers {
		observer.ObserveState()
	}

	leader.Synchronize(followers)
	leader.mu.Lock()
	log.Printf("Debug: Leader committed Zxid: %d", leader.State.Committed)
	leader.mu.Unlock()

	log.Println("Test scenarios completed")

	log.Println("Test scenarios completed")
}
