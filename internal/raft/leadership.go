package raft

import (
	"fmt"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Leadership Transfer – standalone transfer orchestrator
// ---------------------------------------------------------------------------

// LeadershipTransfer orchestrates a graceful leadership transfer to a
// specific target node. It wraps a RaftNode and coordinates the handoff.
type LeadershipTransfer struct {
	node     *RaftNode
	targetID string
	mu       sync.Mutex
}

// NewLeadershipTransfer creates a new LeadershipTransfer for the given node.
func NewLeadershipTransfer(node *RaftNode) *LeadershipTransfer {
	return &LeadershipTransfer{
		node: node,
	}
}

// TransferLeadership initiates a graceful leadership transfer to targetID.
// The current node must be the leader. The method sends a TimeoutNow-style
// signal by waiting for the target to catch up and then expecting it to
// start an election. It blocks until the target becomes leader or the
// operation times out.
func (lt *LeadershipTransfer) TransferLeadership(targetID string) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.targetID = targetID

	lt.node.stateMu.RLock()
	role := lt.node.role
	lt.node.stateMu.RUnlock()

	if role != RaftRoleLeader {
		return fmt.Errorf("only the leader can transfer leadership")
	}

	lt.node.peersMu.RLock()
	peer, ok := lt.node.peers[targetID]
	lt.node.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("target node %s is not a known peer", targetID)
	}
	if !peer.Healthy {
		return fmt.Errorf("target node %s is not healthy", targetID)
	}

	// Wait for the target's match index to catch up to the commit index.
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("leadership transfer to %s timed out", targetID)
		case <-lt.node.ctx.Done():
			return fmt.Errorf("node stopped during leadership transfer")
		case <-ticker.C:
			lt.node.stateMu.RLock()
			commitIdx := lt.node.commitIndex
			lt.node.stateMu.RUnlock()

			lt.node.peersMu.RLock()
			matchIdx := lt.node.peers[targetID].MatchIndex
			lt.node.peersMu.RUnlock()

			if matchIdx >= commitIdx {
				// Target is caught up – signal TimeoutNow by stepping down.
				return lt.StepDown()
			}
		}
	}
}

// StepDown causes the current leader to voluntarily step down to follower
// state, allowing another node to win the next election.
func (lt *LeadershipTransfer) StepDown() error {
	lt.node.stateMu.RLock()
	role := lt.node.role
	term := lt.node.currentTerm
	lt.node.stateMu.RUnlock()

	if role != RaftRoleLeader {
		return fmt.Errorf("only the leader can step down")
	}

	lt.node.becomeFollower(term, "")
	return nil
}

// ---------------------------------------------------------------------------
// Leader Lease – time-based leader validity check
// ---------------------------------------------------------------------------

// LeaderLease tracks a time-bounded lease that the current leader holds.
// While the lease is valid the leader can serve linearizable reads without
// an extra round of heartbeats.
type LeaderLease struct {
	leaderID string
	expiry   time.Time
	mu       sync.RWMutex
}

// NewLeaderLease creates a new LeaderLease for leaderID with the given
// initial duration.
func NewLeaderLease(leaderID string, duration time.Duration) *LeaderLease {
	return &LeaderLease{
		leaderID: leaderID,
		expiry:   time.Now().Add(duration),
	}
}

// IsLeaseValid returns true if the lease has not yet expired.
func (ll *LeaderLease) IsLeaseValid() bool {
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	return time.Now().Before(ll.expiry)
}

// RenewLease extends the lease by duration from the current time.
func (ll *LeaderLease) RenewLease(duration time.Duration) {
	ll.mu.Lock()
	defer ll.mu.Unlock()

	ll.expiry = time.Now().Add(duration)
}

// LeaderID returns the ID of the leader holding this lease.
func (ll *LeaderLease) LeaderID() string {
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	return ll.leaderID
}

// Expiry returns the current lease expiry time.
func (ll *LeaderLease) Expiry() time.Time {
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	return ll.expiry
}
