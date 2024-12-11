package main

import "log"

// BroadcastTransaction sends a transaction to all followers
func (n *Node) BroadcastTransaction(tx string, followers []*Node) {
	if n.Role != Leader {
		log.Println("Only the leader can broadcast transactions")
		return
	}

	n.State.Zxid++
	n.State.LogEntries = append(n.State.LogEntries, tx)
	acks := 0

	for _, follower := range followers {
		follower.mu.Lock()
		follower.State.LogEntries = append(follower.State.LogEntries, tx)
		follower.mu.Unlock()
		acks++
		log.Printf("Transaction %s sent to Node %d", tx, follower.ID)
	}

	if acks >= (len(followers)/2)+1 {
		n.State.Committed = n.State.Zxid
		log.Printf("Transaction %s committed by Leader", tx)
	} else {
		log.Println("Quorum not reached. Transaction aborted")
	}
}
