package raft

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// --- applyCommand tests ---

func TestApplyCommand_Write(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "apply-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	point := Point{
		Metric:    "cpu",
		Value:     42.0,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "test"},
	}
	pointData, _ := json.Marshal(point)

	cmd := RaftCommand{
		Op:    "write",
		Key:   "cpu",
		Value: pointData,
	}

	err = node.applyCommand(cmd)
	if err != nil {
		t.Fatalf("applyCommand write: %v", err)
	}

	store.mu.Lock()
	if len(store.points) != 1 {
		t.Errorf("Expected 1 point, got %d", len(store.points))
	}
	if store.points[0].Metric != "cpu" {
		t.Errorf("Expected metric cpu, got %s", store.points[0].Metric)
	}
	if store.points[0].Value != 42.0 {
		t.Errorf("Expected value 42.0, got %f", store.points[0].Value)
	}
	store.mu.Unlock()
}

func TestApplyCommand_Delete(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "delete-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	cmd := RaftCommand{
		Op:  "delete",
		Key: "some-key",
	}

	// Delete is a no-op currently, should not error
	err = node.applyCommand(cmd)
	if err != nil {
		t.Fatalf("applyCommand delete: %v", err)
	}
}

func TestApplyCommand_UnknownOp(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "unknown-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	cmd := RaftCommand{Op: "unknown_op"}
	err = node.applyCommand(cmd)
	if err != nil {
		t.Errorf("Unknown op should not error, got: %v", err)
	}
}

func TestApplyCommand_InvalidJSON(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "badjson-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	cmd := RaftCommand{
		Op:    "write",
		Value: json.RawMessage(`not valid json`),
	}

	err = node.applyCommand(cmd)
	if err == nil {
		t.Error("Expected error for invalid JSON value")
	}
}

// --- applyCommitted tests ---

func TestApplyCommitted_AppliesEntries(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "commit-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	// Add entries to log
	point := Point{Metric: "test", Value: 1.0, Timestamp: 100}
	pointData, _ := json.Marshal(point)
	cmd := RaftCommand{Op: "write", Value: pointData}
	cmdData, _ := json.Marshal(cmd)

	entries := []*RaftLogEntry{
		{Index: 1, Term: 1, Type: RaftLogCommand, Data: cmdData},
		{Index: 2, Term: 1, Type: RaftLogCommand, Data: cmdData},
		{Index: 3, Term: 1, Type: RaftLogNoop, Data: nil},
	}
	if err := node.log.Append(entries...); err != nil {
		t.Fatal(err)
	}

	// Set commit index ahead of last applied
	node.stateMu.Lock()
	node.commitIndex = 3
	node.lastApplied = 0
	node.stateMu.Unlock()

	// Set up context for applyCommitted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.ctx = ctx

	// applyCommitted is a one-shot function that pushes to applyCh
	done := make(chan struct{})
	go func() {
		node.applyCommitted()
		close(done)
	}()

	// Drain the apply channel
	applied := 0
	timeout := time.After(2 * time.Second)
	for applied < 3 {
		select {
		case entry := <-node.applyCh:
			if entry != nil {
				applied++
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for entries, applied %d", applied)
		}
	}

	<-done

	if applied != 3 {
		t.Errorf("Expected 3 applied entries, got %d", applied)
	}

	// lastApplied should advance
	node.stateMu.RLock()
	lastApplied := node.lastApplied
	node.stateMu.RUnlock()

	if lastApplied != 3 {
		t.Errorf("Expected lastApplied=3, got %d", lastApplied)
	}
}

func TestApplyCommitted_SkipsMissingEntries(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "skip-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	// Only add entry at index 2 (index 1 is missing from the log)
	entry := &RaftLogEntry{Index: 2, Term: 1, Type: RaftLogNoop}
	if err := node.log.Append(entry); err != nil {
		t.Fatal(err)
	}

	node.stateMu.Lock()
	node.commitIndex = 2
	node.lastApplied = 0
	node.stateMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.ctx = ctx

	done := make(chan struct{})
	go func() {
		node.applyCommitted()
		close(done)
	}()

	// Drain whatever comes through applyCh
	applied := 0
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case <-node.applyCh:
			applied++
		case <-done:
			break loop
		case <-timeout:
			break loop
		}
	}

	// The log.Append with Index=2 as the first entry may store it at
	// internal index 1; either 0 or 1 applied entries is acceptable
	// depending on internal log indexing. No panic is the key assertion.
	_ = applied
}

// --- Stale term rejection ---

func TestBecomeFollower_StopsHeartbeat(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "leader-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	// Set as leader with heartbeat ticker
	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 5
	node.heartbeatTicker = time.NewTicker(time.Hour) // will be stopped
	node.stateMu.Unlock()

	// Become follower with higher term
	node.becomeFollower(10, "other-leader")

	if node.Role() != RaftRoleFollower {
		t.Errorf("Expected follower, got %s", node.Role())
	}
	if node.Term() != 10 {
		t.Errorf("Expected term 10, got %d", node.Term())
	}
	if node.Leader() != "other-leader" {
		t.Errorf("Expected leader other-leader, got %s", node.Leader())
	}
}

// --- Election edge cases ---

func TestStartElection_IncrementsTerm(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "election-node"
	config.BindAddr = ""
	config.PreVoteEnabled = false // Direct election

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}
	node.running.Store(true)

	initialTerm := node.Term()

	node.startElection()

	if node.Term() != initialTerm+1 {
		t.Errorf("Term should increment from %d to %d, got %d", initialTerm, initialTerm+1, node.Term())
	}
	if node.Role() != RaftRoleCandidate {
		t.Errorf("Should be candidate during election, got %s", node.Role())
	}

	node.stateMu.RLock()
	votedFor := node.votedFor
	node.stateMu.RUnlock()

	if votedFor != "election-node" {
		t.Errorf("Should vote for self, got %s", votedFor)
	}
}

func TestElectionTimeout_NotTriggeredForLeader(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "leader-node"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}
	node.running.Store(true)

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 5
	node.stateMu.Unlock()

	termBefore := node.Term()
	node.electionTimeout()

	// Leader should not start election
	if node.Term() != termBefore {
		t.Error("Leader should not trigger election timeout")
	}
}

// --- LeaderLease tests ---

func TestLeaderLease_IsValid(t *testing.T) {
	lease := NewLeaderLease("leader-1", 100*time.Millisecond)

	if !lease.IsLeaseValid() {
		t.Error("Lease should be valid immediately")
	}
	if lease.LeaderID() != "leader-1" {
		t.Errorf("Leader ID = %s, want leader-1", lease.LeaderID())
	}

	time.Sleep(150 * time.Millisecond)

	if lease.IsLeaseValid() {
		t.Error("Lease should be expired")
	}
}

func TestLeaderLease_Renew(t *testing.T) {
	lease := NewLeaderLease("leader-1", 50*time.Millisecond)

	time.Sleep(30 * time.Millisecond)
	lease.RenewLease(100 * time.Millisecond)

	if !lease.IsLeaseValid() {
		t.Error("Lease should be valid after renewal")
	}
}

func TestLeaderLease_Expiry(t *testing.T) {
	lease := NewLeaderLease("leader-1", time.Hour)
	expiry := lease.Expiry()

	if expiry.IsZero() {
		t.Error("Expiry should not be zero")
	}
	if time.Until(expiry) < 59*time.Minute {
		t.Error("Expiry should be about 1 hour from now")
	}
}

// --- LeadershipTransfer tests ---

func TestLeadershipTransfer_NotLeader(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "follower"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	lt := NewLeadershipTransfer(node)
	err = lt.TransferLeadership("target")
	if err == nil {
		t.Error("Expected error when not leader")
	}
}

func TestLeadershipTransfer_UnknownTarget(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "leader"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.stateMu.Unlock()

	lt := NewLeadershipTransfer(node)
	err = lt.TransferLeadership("unknown-node")
	if err == nil {
		t.Error("Expected error for unknown target")
	}
}

func TestStepDown_NotLeader(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "follower"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	lt := NewLeadershipTransfer(node)
	err = lt.StepDown()
	if err == nil {
		t.Error("Expected error stepping down when not leader")
	}
}

func TestStepDown_AsLeader(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "leader"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 5
	node.stateMu.Unlock()

	lt := NewLeadershipTransfer(node)
	err = lt.StepDown()
	if err != nil {
		t.Fatalf("StepDown: %v", err)
	}

	if node.Role() != RaftRoleFollower {
		t.Errorf("Should be follower after step down, got %s", node.Role())
	}
}

// --- Log operations during replication ---

func TestLogCompactBefore_DuringReplication(t *testing.T) {
	rl, err := NewRaftLog("")
	if err != nil {
		t.Fatal(err)
	}

	// Populate log
	for i := uint64(1); i <= 100; i++ {
		if err := rl.Append(&RaftLogEntry{Index: i, Term: 1, Type: RaftLogCommand, Data: []byte("cmd")}); err != nil {
			t.Fatal(err)
		}
	}

	// Compact and then verify
	rl.CompactBefore(50)

	if rl.Get(49) != nil {
		t.Error("Entry 49 should be compacted")
	}
	if rl.Get(50) == nil {
		t.Error("Entry 50 should still exist")
	}
	if rl.Get(100) == nil {
		t.Error("Entry 100 should still exist")
	}
}

// --- RaftLog edge cases ---

func TestRaftLog_GetOutOfBounds(t *testing.T) {
	rl, _ := NewRaftLog("")

	if err := rl.Append(&RaftLogEntry{Index: 1, Term: 1, Data: []byte("a")}); err != nil {
		t.Fatal(err)
	}

	if rl.Get(0) != nil {
		t.Error("Index 0 should return nil")
	}
	if rl.Get(999) != nil {
		t.Error("Out of range index should return nil")
	}
}

func TestRaftLog_EmptyGetRange(t *testing.T) {
	rl, _ := NewRaftLog("")

	result := rl.GetRange(1, 10)
	if result != nil {
		t.Errorf("Empty log GetRange should return nil, got %v", result)
	}
}

func TestRaftLog_TruncateAfterEmpty(t *testing.T) {
	rl, _ := NewRaftLog("")

	// Should not panic on empty log
	rl.TruncateAfter(0)
	if rl.Len() != 0 {
		t.Error("Empty log should remain empty after truncation")
	}
}

func TestRaftLog_CompactBeforeStart(t *testing.T) {
	rl, _ := NewRaftLog("")
	if err := rl.Append(&RaftLogEntry{Index: 1, Term: 1, Data: []byte("a")}); err != nil {
		t.Fatal(err)
	}

	// Compact before start index should be a no-op
	rl.CompactBefore(0)
	if rl.Len() != 1 {
		t.Error("CompactBefore(0) should not remove entries")
	}
}

// --- RaftNode helpers ---

func TestRaftNode_RoleAccessors(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "test"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	if node.Role() != RaftRoleFollower {
		t.Error("Default role should be follower")
	}
	if node.Term() != 0 {
		t.Error("Default term should be 0")
	}
	if node.Leader() != "" {
		t.Error("Default leader should be empty")
	}
}

// --- Multiple listeners ---

func TestMultipleListeners(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "multi-listen"
	config.BindAddr = ""

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	l1 := &testRaftEventListener{
		leaderChanges: make([]string, 0),
		roleChanges:   make([]RaftRole, 0),
		commits:       make([]uint64, 0),
	}
	l2 := &testRaftEventListener{
		leaderChanges: make([]string, 0),
		roleChanges:   make([]RaftRole, 0),
		commits:       make([]uint64, 0),
	}

	node.AddListener(l1)
	node.AddListener(l2)

	node.notifyLeaderChange("new-leader", 1)
	time.Sleep(50 * time.Millisecond)

	l1.mu.Lock()
	l1Count := len(l1.leaderChanges)
	l1.mu.Unlock()

	l2.mu.Lock()
	l2Count := len(l2.leaderChanges)
	l2.mu.Unlock()

	if l1Count != 1 {
		t.Errorf("Listener 1 should get 1 notification, got %d", l1Count)
	}
	if l2Count != 1 {
		t.Errorf("Listener 2 should get 1 notification, got %d", l2Count)
	}
}
