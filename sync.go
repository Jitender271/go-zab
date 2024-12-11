package main

import "log"

// Synchronize ensures all followers sync state with the leader
func (n *Node) Synchronize(followers []*Node) {
	if n.Role != Leader {
		log.Println("Only the leader can initiate synchronization")
		return
	}

	for _, follower := range followers {
		follower.mu.Lock()
		follower.State.Zxid = n.State.Zxid
		follower.State.Committed = n.State.Committed
		follower.State.LogEntries = append([]string{}, n.State.LogEntries...)
		follower.mu.Unlock()
		log.Printf("Synchronized Node %d with Leader", follower.ID)
	}
}
