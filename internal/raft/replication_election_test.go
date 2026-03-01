package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// newTestNode creates a RaftNode suitable for unit tests (no bind address, no disk).
func newTestNode(t *testing.T, nodeID string, peers []RaftPeer) *RaftNode {
	t.Helper()
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = nodeID
	config.BindAddr = ""
	config.Peers = peers
	config.HeartbeatInterval = 20 * time.Millisecond
	config.ElectionTimeoutMin = 50 * time.Millisecond
	config.ElectionTimeoutMax = 100 * time.Millisecond
	config.BatchingEnabled = false
	config.CheckQuorumEnabled = false
	config.SnapshotInterval = 0
	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}
	// Drain applyCh to prevent deadlocks in tests that trigger commit advancement.
	go func() {
		for {
			select {
			case <-node.ctx.Done():
				return
			case <-node.applyCh:
			}
		}
	}()
	return node
}

// --- Replication Tests ---

func TestHeartbeatLoopStopsOnContextCancel(t *testing.T) {
	node := newTestNode(t, "leader-1", nil)
	node.running.Store(true)

	// Make node a leader so heartbeatLoop doesn't exit immediately.
	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.stateMu.Unlock()
	node.heartbeatTicker = time.NewTicker(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		node.heartbeatLoop()
		close(done)
	}()

	// Cancel the context and verify the loop exits.
	node.cancel()
	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not stop after context cancel")
	}
}

func TestHeartbeatLoopExitsWhenNotLeader(t *testing.T) {
	node := newTestNode(t, "follower-1", nil)
	node.running.Store(true)

	// Node is a follower (default), so the loop should return immediately on tick.
	node.heartbeatTicker = time.NewTicker(5 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		node.heartbeatLoop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not exit for non-leader")
	}
	node.cancel()
}

func TestReplicateToFollowersContactsAllPeers(t *testing.T) {
	var contactCount int32

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&contactCount, 1)
		resp := AppendEntriesRPCResponse{Term: 1, Success: true}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&contactCount, 1)
		resp := AppendEntriesRPCResponse{Term: 1, Success: true}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv2.Close()

	node := newTestNode(t, "leader", nil)
	defer node.cancel()
	node.running.Store(true)

	node.peersMu.Lock()
	node.peers["peer-A"] = &RaftPeerState{
		ID: "peer-A", Addr: srv1.Listener.Addr().String(),
		NextIndex: 1, Healthy: true,
	}
	node.peers["peer-B"] = &RaftPeerState{
		ID: "peer-B", Addr: srv2.Listener.Addr().String(),
		NextIndex: 1, Healthy: true,
	}
	node.peersMu.Unlock()

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 2 // Different from log term to avoid commit advancement deadlock
	node.stateMu.Unlock()

	node.replicateToFollowers()
	time.Sleep(200 * time.Millisecond)

	count := atomic.LoadInt32(&contactCount)
	if count < 2 {
		t.Errorf("Expected at least 2 peer contacts, got %d", count)
	}
}

func TestReplicateToPeerUpdatesMatchIndex(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := AppendEntriesRPCResponse{Term: 2, Success: true}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	node := newTestNode(t, "leader", nil)
	defer node.cancel()
	node.running.Store(true)

	// Use term 2 for currentTerm but log entries at term 1 to avoid commit advancement deadlock.
	_ = node.log.Append(&RaftLogEntry{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("cmd1")})

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 2
	node.stateMu.Unlock()

	peer := &RaftPeerState{
		ID: "peer1", Addr: srv.Listener.Addr().String(),
		NextIndex: 1, Healthy: true,
	}
	node.peersMu.Lock()
	node.peers["peer1"] = peer
	node.peersMu.Unlock()

	node.replicateToPeer(peer)

	node.peersMu.RLock()
	if peer.MatchIndex != 1 {
		t.Errorf("Expected MatchIndex=1, got %d", peer.MatchIndex)
	}
	if peer.NextIndex != 2 {
		t.Errorf("Expected NextIndex=2, got %d", peer.NextIndex)
	}
	node.peersMu.RUnlock()
}

func TestReplicateToPeerMarksPeerUnhealthyOnError(t *testing.T) {
	node := newTestNode(t, "leader", nil)
	defer node.cancel()
	node.running.Store(true)

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 1
	node.stateMu.Unlock()

	// Use an unreachable address.
	peer := &RaftPeerState{
		ID: "bad-peer", Addr: "127.0.0.1:1",
		NextIndex: 1, Healthy: true,
	}
	node.peersMu.Lock()
	node.peers["bad-peer"] = peer
	node.peersMu.Unlock()

	node.replicateToPeer(peer)

	node.peersMu.RLock()
	if peer.Healthy {
		t.Error("Expected peer to be marked unhealthy after error")
	}
	node.peersMu.RUnlock()
}

func TestReplicateToPeerDecreasesNextIndexOnConflict(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			// First call: return conflict.
			resp := AppendEntriesRPCResponse{Term: 1, Success: false, ConflictIndex: 3}
			json.NewEncoder(w).Encode(resp)
		} else {
			// Subsequent retry: succeed so the loop stops.
			resp := AppendEntriesRPCResponse{Term: 1, Success: true}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer srv.Close()

	node := newTestNode(t, "leader", nil)
	defer node.cancel()
	node.running.Store(true)

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 2 // Different from log term to avoid commit advancement deadlock
	node.stateMu.Unlock()

	peer := &RaftPeerState{
		ID: "peer1", Addr: srv.Listener.Addr().String(),
		NextIndex: 5, Healthy: true,
	}
	node.peersMu.Lock()
	node.peers["peer1"] = peer
	node.peersMu.Unlock()

	node.replicateToPeer(peer)

	// Give async retry a moment.
	time.Sleep(200 * time.Millisecond)

	node.peersMu.RLock()
	// After conflict, NextIndex should have been set to ConflictIndex (3).
	if peer.NextIndex > 5 {
		t.Errorf("Expected NextIndex to decrease from 5, got %d", peer.NextIndex)
	}
	node.peersMu.RUnlock()
}

// --- Election Tests ---

func TestStartElectionBecomesLeaderOnQuorum(t *testing.T) {
	// Create 2 mock peers that grant votes.
	makeVoter := func(grant bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/raft/vote" {
				resp := RequestVoteRPCResponse{Term: 1, VoteGranted: grant}
				json.NewEncoder(w).Encode(resp)
			} else if r.URL.Path == "/raft/append" {
				resp := AppendEntriesRPCResponse{Term: 1, Success: true}
				json.NewEncoder(w).Encode(resp)
			}
		}))
	}

	srv1 := makeVoter(true)
	defer srv1.Close()
	srv2 := makeVoter(true)
	defer srv2.Close()

	node := newTestNode(t, "candidate", nil)
	defer node.cancel()
	node.running.Store(true)

	node.peersMu.Lock()
	node.peers["peer1"] = &RaftPeerState{ID: "peer1", Addr: srv1.Listener.Addr().String(), NextIndex: 1, Healthy: true}
	node.peers["peer2"] = &RaftPeerState{ID: "peer2", Addr: srv2.Listener.Addr().String(), NextIndex: 1, Healthy: true}
	node.peersMu.Unlock()

	node.startElection()

	if node.Role() != RaftRoleLeader {
		t.Errorf("Expected leader role after winning election, got %s", node.Role())
	}
	if node.Term() != 1 {
		t.Errorf("Expected term 1, got %d", node.Term())
	}
}

func TestStartElectionStaysFollowerWithoutQuorum(t *testing.T) {
	// Both peers deny votes.
	makeVoter := func(grant bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := RequestVoteRPCResponse{Term: 1, VoteGranted: grant}
			json.NewEncoder(w).Encode(resp)
		}))
	}

	srv1 := makeVoter(false)
	defer srv1.Close()
	srv2 := makeVoter(false)
	defer srv2.Close()

	node := newTestNode(t, "candidate", nil)
	defer node.cancel()
	node.running.Store(true)

	node.peersMu.Lock()
	node.peers["peer1"] = &RaftPeerState{ID: "peer1", Addr: srv1.Listener.Addr().String(), NextIndex: 1, Healthy: true}
	node.peers["peer2"] = &RaftPeerState{ID: "peer2", Addr: srv2.Listener.Addr().String(), NextIndex: 1, Healthy: true}
	node.peersMu.Unlock()

	node.startElection()

	// Without quorum, should not become leader.
	if node.Role() == RaftRoleLeader {
		t.Error("Should not become leader without quorum")
	}
}

func TestSplitVoteScenario(t *testing.T) {
	// One peer grants, one denies – with 3 nodes total, quorum is 2 (self + 1).
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/raft/vote" {
			resp := RequestVoteRPCResponse{Term: 1, VoteGranted: true}
			json.NewEncoder(w).Encode(resp)
		} else {
			resp := AppendEntriesRPCResponse{Term: 1, Success: true}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer srv1.Close()
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := RequestVoteRPCResponse{Term: 1, VoteGranted: false}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv2.Close()

	node := newTestNode(t, "candidate", nil)
	defer node.cancel()
	node.running.Store(true)

	node.peersMu.Lock()
	node.peers["peer1"] = &RaftPeerState{ID: "peer1", Addr: srv1.Listener.Addr().String(), NextIndex: 1, Healthy: true}
	node.peers["peer2"] = &RaftPeerState{ID: "peer2", Addr: srv2.Listener.Addr().String(), NextIndex: 1, Healthy: true}
	node.peersMu.Unlock()

	node.startElection()

	// Self + peer1 = 2 votes, quorum = 2, should become leader.
	if node.Role() != RaftRoleLeader {
		t.Errorf("Expected leader with split vote (self + 1 grant in 3-node cluster), got %s", node.Role())
	}
}

func TestElectionNetworkPartition(t *testing.T) {
	// Both peers unreachable (network partition).
	node := newTestNode(t, "candidate", nil)
	defer node.cancel()
	node.running.Store(true)
	node.transport = NewRaftTransport("", 20*time.Millisecond)

	node.peersMu.Lock()
	node.peers["peer1"] = &RaftPeerState{ID: "peer1", Addr: "127.0.0.1:1", NextIndex: 1, Healthy: true}
	node.peers["peer2"] = &RaftPeerState{ID: "peer2", Addr: "127.0.0.1:2", NextIndex: 1, Healthy: true}
	node.peersMu.Unlock()

	node.startElection()

	if node.Role() == RaftRoleLeader {
		t.Error("Should not become leader when all peers are unreachable")
	}
}

func TestBecomeLeaderInitializesPeerState(t *testing.T) {
	node := newTestNode(t, "leader", nil)
	defer node.cancel()
	node.running.Store(true)

	_ = node.log.Append(&RaftLogEntry{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("x")})
	_ = node.log.Append(&RaftLogEntry{Index: 2, Term: 1, Type: RaftLogCommand, Data: []byte("y")})

	node.peersMu.Lock()
	node.peers["peer1"] = &RaftPeerState{ID: "peer1", Addr: "127.0.0.1:1", NextIndex: 1, MatchIndex: 1}
	node.peersMu.Unlock()

	node.stateMu.Lock()
	node.role = RaftRoleCandidate
	node.currentTerm = 1
	node.stateMu.Unlock()

	node.becomeLeader()

	node.peersMu.RLock()
	peer := node.peers["peer1"]
	if peer.NextIndex != 3 {
		t.Errorf("Expected NextIndex=3 (lastIndex+1), got %d", peer.NextIndex)
	}
	if peer.MatchIndex != 0 {
		t.Errorf("Expected MatchIndex=0 after becoming leader, got %d", peer.MatchIndex)
	}
	node.peersMu.RUnlock()

	if node.Role() != RaftRoleLeader {
		t.Errorf("Expected leader role, got %s", node.Role())
	}
}

// --- Membership Tests ---

func TestAddNodeUpdatesClusterMembership(t *testing.T) {
	node := newTestNode(t, "leader", nil)
	defer node.cancel()
	node.running.Store(true)

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 1
	node.stateMu.Unlock()

	ctx := context.Background()
	err := node.AddNode(ctx, RaftPeer{ID: "new-node", Addr: "localhost:9999"})
	if err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	node.peersMu.RLock()
	p, ok := node.peers["new-node"]
	node.peersMu.RUnlock()
	if !ok {
		t.Fatal("New node not found in peers")
	}
	if p.Addr != "localhost:9999" {
		t.Errorf("Expected addr localhost:9999, got %s", p.Addr)
	}
}

func TestAddNodeFailsWhenNotLeader(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	ctx := context.Background()
	err := node.AddNode(ctx, RaftPeer{ID: "new-node", Addr: "localhost:9999"})
	if err == nil {
		t.Error("Expected error when adding node as non-leader")
	}
}

func TestRemoveNodeUpdatesClusterMembership(t *testing.T) {
	node := newTestNode(t, "leader", nil)
	defer node.cancel()
	node.running.Store(true)

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 1
	node.stateMu.Unlock()

	node.peersMu.Lock()
	node.peers["removable"] = &RaftPeerState{ID: "removable", Addr: "localhost:8888", Healthy: true}
	node.peersMu.Unlock()

	ctx := context.Background()
	err := node.RemoveNode(ctx, "removable")
	if err != nil {
		t.Fatalf("RemoveNode failed: %v", err)
	}

	node.peersMu.RLock()
	_, ok := node.peers["removable"]
	node.peersMu.RUnlock()
	if ok {
		t.Error("Removed node should not be in peers")
	}
}

func TestRemoveNodeFailsWhenNotLeader(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	ctx := context.Background()
	err := node.RemoveNode(ctx, "any-node")
	if err == nil {
		t.Error("Expected error when removing node as non-leader")
	}
}

func TestRemoveNodeFailsForUnknownNode(t *testing.T) {
	node := newTestNode(t, "leader", nil)
	defer node.cancel()

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 1
	node.stateMu.Unlock()

	ctx := context.Background()
	err := node.RemoveNode(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error when removing unknown node")
	}
}

// --- RPC Handler Tests ---

func TestHandleAppendEntriesRejectsStaleTerm(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	node.stateMu.Lock()
	node.currentTerm = 5
	node.stateMu.Unlock()

	req := AppendEntriesRPCRequest{
		Term:     3, // stale
		LeaderID: "old-leader",
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/append", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	node.handleAppendEntries(rr, httpReq)

	var resp AppendEntriesRPCResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp.Success {
		t.Error("Expected rejection for stale term")
	}
	if resp.Term != 5 {
		t.Errorf("Expected term 5, got %d", resp.Term)
	}
}

func TestHandleAppendEntriesAcceptsValidEntries(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	node.stateMu.Lock()
	node.currentTerm = 1
	node.role = RaftRoleFollower
	node.stateMu.Unlock()

	req := AppendEntriesRPCRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*RaftLogEntry{
			{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("cmd1")},
			{Index: 2, Term: 1, Type: RaftLogCommand, Data: []byte("cmd2")},
		},
		LeaderCommit: 0,
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/append", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	node.handleAppendEntries(rr, httpReq)

	var resp AppendEntriesRPCResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if !resp.Success {
		t.Error("Expected success for valid entries")
	}
	if node.log.LastIndex() != 2 {
		t.Errorf("Expected 2 log entries, got lastIndex=%d", node.log.LastIndex())
	}
	if node.Leader() != "leader-1" {
		t.Errorf("Expected leader to be leader-1, got %s", node.Leader())
	}
}

func TestHandleAppendEntriesUpdatesCommitIndex(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	_ = node.log.Append(
		&RaftLogEntry{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("a")},
		&RaftLogEntry{Index: 2, Term: 1, Type: RaftLogCommand, Data: []byte("b")},
	)

	node.stateMu.Lock()
	node.currentTerm = 1
	node.role = RaftRoleFollower
	node.stateMu.Unlock()

	req := AppendEntriesRPCRequest{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		LeaderCommit: 2,
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/append", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	node.handleAppendEntries(rr, httpReq)

	var resp AppendEntriesRPCResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if !resp.Success {
		t.Error("Expected success")
	}
	node.stateMu.RLock()
	ci := node.commitIndex
	node.stateMu.RUnlock()
	if ci != 2 {
		t.Errorf("Expected commitIndex=2, got %d", ci)
	}
}

func TestHandleRequestVoteGrantsVote(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	req := RequestVoteRPCRequest{
		Term:         1,
		CandidateID:  "candidate-1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/vote", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	node.handleRequestVote(rr, httpReq)

	var resp RequestVoteRPCResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if !resp.VoteGranted {
		t.Error("Expected vote to be granted")
	}
}

func TestHandleRequestVoteRejectsStaleTerm(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	node.stateMu.Lock()
	node.currentTerm = 5
	node.stateMu.Unlock()

	req := RequestVoteRPCRequest{
		Term:        3,
		CandidateID: "old-candidate",
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/vote", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	node.handleRequestVote(rr, httpReq)

	var resp RequestVoteRPCResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp.VoteGranted {
		t.Error("Should not grant vote for stale term")
	}
}

func TestHandlePreVoteGrantsForUpToDateLog(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	req := PreVoteRPCRequest{
		Term:         1,
		CandidateID:  "pre-candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/prevote", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	node.handlePreVote(rr, httpReq)

	var resp PreVoteRPCResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if !resp.VoteGranted {
		t.Error("Expected pre-vote to be granted for up-to-date log")
	}
}

func TestHandleInstallSnapshotUpdatesState(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	req := InstallSnapshotRPCRequest{
		Term:              2,
		LeaderID:          "leader",
		LastIncludedIndex: 50,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot-data"),
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/snapshot", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	node.handleInstallSnapshot(rr, httpReq)

	node.stateMu.RLock()
	if node.lastSnapshotIndex != 50 {
		t.Errorf("Expected snapshot index 50, got %d", node.lastSnapshotIndex)
	}
	if node.currentTerm != 2 {
		t.Errorf("Expected term 2, got %d", node.currentTerm)
	}
	node.stateMu.RUnlock()
}

func TestHandleAppendEntriesConflictDetection(t *testing.T) {
	node := newTestNode(t, "follower", nil)
	defer node.cancel()

	// Pre-populate log with entries at term 1.
	_ = node.log.Append(
		&RaftLogEntry{Index: 1, Term: 1, Type: RaftLogCommand, Data: []byte("a")},
		&RaftLogEntry{Index: 2, Term: 1, Type: RaftLogCommand, Data: []byte("b")},
	)

	node.stateMu.Lock()
	node.currentTerm = 2
	node.role = RaftRoleFollower
	node.stateMu.Unlock()

	// Try appending with PrevLogIndex=3 which doesn't exist.
	req := AppendEntriesRPCRequest{
		Term:         2,
		LeaderID:     "leader",
		PrevLogIndex: 3,
		PrevLogTerm:  1,
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/append", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	node.handleAppendEntries(rr, httpReq)

	var resp AppendEntriesRPCResponse
	json.NewDecoder(rr.Body).Decode(&resp)

	if resp.Success {
		t.Error("Expected failure when PrevLogIndex doesn't exist")
	}
	if resp.ConflictIndex == 0 {
		t.Error("Expected non-zero ConflictIndex")
	}
}
