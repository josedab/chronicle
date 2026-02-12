package cluster

import (
	"testing"
	"time"
)

func TestClusterConfig(t *testing.T) {
	config := DefaultClusterConfig()

	if config.NodeID == "" {
		t.Error("NodeID should not be empty")
	}
	if config.ElectionTimeout == 0 {
		t.Error("ElectionTimeout should be set")
	}
	if config.HeartbeatInterval == 0 {
		t.Error("HeartbeatInterval should be set")
	}
	// Default is ReplicationQuorum (value 2)
	if config.ReplicationMode != ReplicationQuorum {
		t.Errorf("Expected ReplicationQuorum, got %v", config.ReplicationMode)
	}
}

func TestNodeState(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeStateFollower, "follower"},
		{NodeStateCandidate, "candidate"},
		{NodeStateLeader, "leader"},
	}

	for _, tc := range tests {
		if tc.state.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.state.String())
		}
	}
}

func TestClusterNode(t *testing.T) {
	node := &ClusterNode{
		ID:            "node1",
		Addr:          "localhost:8080",
		Healthy:       true,
		Term:          1,
		LastHeartbeat: time.Now(),
	}

	if node.ID != "node1" {
		t.Errorf("Expected node1, got %s", node.ID)
	}
	if !node.Healthy {
		t.Error("Node should be healthy")
	}
}

func TestLogEntry(t *testing.T) {
	entry := LogEntry{
		Index: 1,
		Term:  1,
		Type:  LogEntryWrite,
		Data:  []byte(`{"metric":"cpu","value":42}`),
	}

	if entry.Index != 1 {
		t.Errorf("Expected index 1, got %d", entry.Index)
	}
	if entry.Type != LogEntryWrite {
		t.Errorf("Expected LogEntryWrite, got %v", entry.Type)
	}
}

func TestVoteRequest(t *testing.T) {
	req := VoteRequest{
		Term:         1,
		CandidateID:  "node1",
		LastLogIndex: 10,
		LastLogTerm:  1,
	}

	if req.Term != 1 {
		t.Errorf("Expected term 1, got %d", req.Term)
	}
	if req.CandidateID != "node1" {
		t.Errorf("Expected node1, got %s", req.CandidateID)
	}
}

func TestVoteResponse(t *testing.T) {
	resp := VoteResponse{
		Term:        1,
		VoteGranted: true,
	}

	if !resp.VoteGranted {
		t.Error("Vote should be granted")
	}
}

func TestAppendEntriesRequest(t *testing.T) {
	req := AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader1",
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries:      []LogEntry{},
		LeaderCommit: 5,
	}

	if req.LeaderID != "leader1" {
		t.Errorf("Expected leader1, got %s", req.LeaderID)
	}
}

func TestAppendEntriesResponse(t *testing.T) {
	resp := AppendEntriesResponse{
		Term:    1,
		Success: true,
	}

	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestGossipMessage(t *testing.T) {
	nodes := []*ClusterNode{
		{ID: "node1", Addr: "localhost:8080"},
		{ID: "node2", Addr: "localhost:8081"},
	}

	msg := GossipMessage{
		Nodes: nodes,
	}

	if len(msg.Nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(msg.Nodes))
	}
}

func TestClusterStats(t *testing.T) {
	stats := ClusterStats{
		NodeID:       "node1",
		State:        "leader",
		Term:         5,
		LeaderID:     "node1",
		NodeCount:    3,
		HealthyNodes: 3,
		LogLength:    100,
		CommitIndex:  95,
	}

	if stats.NodeCount != 3 {
		t.Errorf("Expected 3 nodes, got %d", stats.NodeCount)
	}
	if stats.CommitIndex != 95 {
		t.Errorf("Expected commit index 95, got %d", stats.CommitIndex)
	}
}

func TestReplicationMode(t *testing.T) {
	// ReplicationAsync is iota (0)
	if ReplicationAsync != 0 {
		t.Error("ReplicationAsync should be 0")
	}
	// ReplicationSync is 1
	if ReplicationSync != 1 {
		t.Error("ReplicationSync should be 1")
	}
	// ReplicationQuorum is 2
	if ReplicationQuorum != 2 {
		t.Error("ReplicationQuorum should be 2")
	}
}

func TestLogEntryType(t *testing.T) {
	if LogEntryWrite != 0 {
		t.Error("LogEntryWrite should be 0")
	}
	if LogEntryConfig != 1 {
		t.Error("LogEntryConfig should be 1")
	}
	if LogEntryNoop != 2 {
		t.Error("LogEntryNoop should be 2")
	}
}

func TestNodesByPriority(t *testing.T) {
	nodes := nodesByPriority{
		{ID: "a", Term: 3},
		{ID: "b", Term: 1},
		{ID: "c", Term: 2},
	}

	// Test Len
	if nodes.Len() != 3 {
		t.Errorf("Expected length 3, got %d", nodes.Len())
	}

	// Test Less
	if !nodes.Less(1, 0) {
		t.Error("Node b (term 1) should be less than node a (term 3)")
	}

	// Test Swap
	nodes.Swap(0, 1)
	if nodes[0].ID != "b" {
		t.Error("Swap failed")
	}
}
