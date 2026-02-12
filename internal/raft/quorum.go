package raft

import "time"

func (rn *RaftNode) quorumCheckLoop() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.config.ElectionTimeoutMin)
	defer ticker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-ticker.C:
			rn.checkQuorum()
		}
	}
}

func (rn *RaftNode) checkQuorum() {
	rn.stateMu.RLock()
	isLeader := rn.role == RaftRoleLeader
	rn.stateMu.RUnlock()

	if !isLeader {
		return
	}

	rn.peersMu.RLock()
	healthyCount := 1
	totalCount := len(rn.peers) + 1
	for _, p := range rn.peers {
		if p.Healthy && time.Since(p.LastSeen) < rn.config.ElectionTimeoutMax*2 {
			healthyCount++
		}
	}
	rn.peersMu.RUnlock()

	quorum := totalCount/2 + 1
	if healthyCount < quorum {

		rn.stateMu.RLock()
		term := rn.currentTerm
		rn.stateMu.RUnlock()
		rn.becomeFollower(term, "")
	}
}
