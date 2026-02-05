package raft

import (
	"sort"
	"sync/atomic"
	"time"
)

func (rn *RaftNode) heartbeatLoop() {
	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-rn.heartbeatTicker.C:
			rn.stateMu.RLock()
			isLeader := rn.role == RaftRoleLeader
			rn.stateMu.RUnlock()

			if !isLeader {
				return
			}

			rn.replicateToFollowers()
		}
	}
}

func (rn *RaftNode) replicateToFollowers() {
	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		peers = append(peers, p)
	}
	rn.peersMu.RUnlock()

	for _, peer := range peers {
		go rn.replicateToPeer(peer)
	}
}

func (rn *RaftNode) replicateToPeer(peer *RaftPeerState) {

	if atomic.LoadInt32(&peer.Inflight) >= int32(rn.config.MaxInflightRequests) {
		return
	}
	atomic.AddInt32(&peer.Inflight, 1)
	defer atomic.AddInt32(&peer.Inflight, -1)

	rn.peersMu.RLock()
	nextIndex := peer.NextIndex
	rn.peersMu.RUnlock()

	rn.stateMu.RLock()
	term := rn.currentTerm
	commitIndex := rn.commitIndex
	rn.stateMu.RUnlock()

	lastLogIndex := rn.log.LastIndex()
	var entries []*RaftLogEntry
	if nextIndex <= lastLogIndex {
		endIndex := nextIndex + uint64(rn.config.MaxAppendEntries) - 1
		if endIndex > lastLogIndex {
			endIndex = lastLogIndex
		}
		entries = rn.log.GetRange(nextIndex, endIndex)
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := rn.log.TermAt(prevLogIndex)

	req := &AppendEntriesRPCRequest{
		Term:         term,
		LeaderID:     rn.config.NodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	resp, err := rn.sendAppendEntries(peer, req)
	if err != nil {
		rn.peersMu.Lock()
		peer.Healthy = false
		rn.peersMu.Unlock()
		return
	}

	rn.peersMu.Lock()
	peer.LastSeen = time.Now()
	peer.Healthy = true
	rn.peersMu.Unlock()

	if resp.Term > term {
		rn.becomeFollower(resp.Term, "")
		return
	}

	if resp.Success {
		rn.peersMu.Lock()
		if len(entries) > 0 {
			peer.NextIndex = entries[len(entries)-1].Index + 1
			peer.MatchIndex = entries[len(entries)-1].Index
		}
		rn.peersMu.Unlock()

		rn.maybeAdvanceCommitIndex()
	} else {

		rn.peersMu.Lock()
		if resp.ConflictIndex > 0 {
			peer.NextIndex = resp.ConflictIndex
		} else if peer.NextIndex > 1 {
			peer.NextIndex--
		}
		rn.peersMu.Unlock()

		go rn.replicateToPeer(peer)
	}
}

func (rn *RaftNode) maybeAdvanceCommitIndex() {
	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	if rn.role != RaftRoleLeader {
		return
	}

	rn.peersMu.RLock()
	matchIndexes := make([]uint64, 0, len(rn.peers)+1)
	matchIndexes = append(matchIndexes, rn.log.LastIndex())
	for _, p := range rn.peers {
		matchIndexes = append(matchIndexes, p.MatchIndex)
	}
	rn.peersMu.RUnlock()

	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] > matchIndexes[j]
	})

	majority := len(matchIndexes) / 2
	newCommitIndex := matchIndexes[majority]

	if newCommitIndex > rn.commitIndex && rn.log.TermAt(newCommitIndex) == rn.currentTerm {
		oldCommitIndex := rn.commitIndex
		rn.commitIndex = newCommitIndex

		rn.proposalsMu.Lock()
		for idx := oldCommitIndex + 1; idx <= newCommitIndex; idx++ {
			if proposal, ok := rn.proposals[idx]; ok {
				proposal.Response <- nil
				delete(rn.proposals, idx)
			}
		}
		rn.proposalsMu.Unlock()

		go rn.applyCommitted()

		rn.leaseExpiry = time.Now().Add(rn.config.LeaseTimeout)
		rn.leaseExtended.Store(true)

		rn.notifyCommit(newCommitIndex)
	}
}
