package raft

import (
	"math/rand"
	"time"
)

func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	timeoutRange := rn.config.ElectionTimeoutMax - rn.config.ElectionTimeoutMin
	timeout := rn.config.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(timeoutRange)))
	rn.electionTimer = time.AfterFunc(timeout, rn.electionTimeout)
}

func (rn *RaftNode) electionTimeout() {
	if !rn.running.Load() {
		return
	}

	rn.stateMu.RLock()
	role := rn.role
	rn.stateMu.RUnlock()

	if role == RaftRoleLeader {
		return
	}

	if rn.config.PreVoteEnabled {
		rn.startPreVote()
	} else {
		rn.startElection()
	}
}

func (rn *RaftNode) startPreVote() {
	rn.stateMu.Lock()
	rn.role = RaftRolePreCandidate
	term := rn.currentTerm + 1
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()
	rn.stateMu.Unlock()

	rn.notifyRoleChange(RaftRolePreCandidate)

	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		peers = append(peers, p)
	}
	rn.peersMu.RUnlock()

	votesNeeded := (len(peers)+1)/2 + 1
	votesCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p *RaftPeerState) {
			granted := rn.sendPreVote(p, term, lastLogIndex, lastLogTerm)
			votesCh <- granted
		}(peer)
	}

	votes := 1
	for i := 0; i < len(peers); i++ {
		select {
		case granted := <-votesCh:
			if granted {
				votes++
			}
		case <-time.After(rn.config.ElectionTimeoutMin):

		case <-rn.ctx.Done():
			return
		}

		if votes >= votesNeeded {
			rn.startElection()
			return
		}
	}

	rn.stateMu.Lock()
	rn.role = RaftRoleFollower
	rn.stateMu.Unlock()
	rn.notifyRoleChange(RaftRoleFollower)
	rn.resetElectionTimer()
}

func (rn *RaftNode) startElection() {
	rn.stateMu.Lock()
	rn.role = RaftRoleCandidate
	rn.currentTerm++
	rn.votedFor = rn.config.NodeID
	term := rn.currentTerm
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()
	rn.stateMu.Unlock()

	rn.notifyRoleChange(RaftRoleCandidate)
	_ = rn.saveState()

	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		peers = append(peers, p)
	}
	rn.peersMu.RUnlock()

	votesNeeded := (len(peers)+1)/2 + 1
	votesCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p *RaftPeerState) {
			granted := rn.sendRequestVote(p, term, lastLogIndex, lastLogTerm)
			votesCh <- granted
		}(peer)
	}

	votes := 1
	for i := 0; i < len(peers); i++ {
		select {
		case granted := <-votesCh:
			if granted {
				votes++
			}
		case <-time.After(rn.config.ElectionTimeoutMax):

		case <-rn.ctx.Done():
			return
		}

		rn.stateMu.RLock()
		currentRole := rn.role
		currentTerm := rn.currentTerm
		rn.stateMu.RUnlock()

		if currentRole != RaftRoleCandidate || currentTerm != term {
			return
		}

		if votes >= votesNeeded {
			rn.becomeLeader()
			return
		}
	}

	rn.resetElectionTimer()
}

func (rn *RaftNode) becomeLeader() {
	rn.stateMu.Lock()
	rn.role = RaftRoleLeader
	rn.leaderID = rn.config.NodeID
	term := rn.currentTerm
	lastIndex := rn.log.LastIndex()
	rn.stateMu.Unlock()

	rn.peersMu.Lock()
	for _, p := range rn.peers {
		p.NextIndex = lastIndex + 1
		p.MatchIndex = 0
	}
	rn.peersMu.Unlock()

	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  term,
		Type:  RaftLogNoop,
	}
	_ = rn.log.Append(entry)

	rn.heartbeatTicker = time.NewTicker(rn.config.HeartbeatInterval)
	go rn.heartbeatLoop()

	rn.notifyRoleChange(RaftRoleLeader)
	rn.notifyLeaderChange(rn.config.NodeID, term)
}

func (rn *RaftNode) becomeFollower(term uint64, leaderID string) {
	rn.stateMu.Lock()
	oldRole := rn.role
	rn.role = RaftRoleFollower
	rn.currentTerm = term
	rn.votedFor = ""
	rn.leaderID = leaderID
	rn.leaseExtended.Store(false)
	rn.stateMu.Unlock()

	if oldRole == RaftRoleLeader && rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
	}

	rn.resetElectionTimer()
	_ = rn.saveState()

	if oldRole != RaftRoleFollower {
		rn.notifyRoleChange(RaftRoleFollower)
	}
	if leaderID != "" {
		rn.notifyLeaderChange(leaderID, term)
	}
}
