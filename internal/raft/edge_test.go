package raft

import (
	"testing"
	"time"
)

func TestRaftHealthChecker_NilNode(t *testing.T) {
	hc := NewRaftHealthChecker(nil, 5*time.Second)
	status := hc.Check()
	if status.Healthy {
		t.Error("expected unhealthy for nil node")
	}
	if len(status.HealthIssues) == 0 {
		t.Error("expected issues")
	}
}

func TestRaftHealthChecker_RecordContact(t *testing.T) {
	hc := NewRaftHealthChecker(nil, 5*time.Second)
	before := hc.lastContact
	time.Sleep(5 * time.Millisecond)
	hc.RecordContact()
	if !hc.lastContact.After(before) {
		t.Error("contact not updated")
	}
}

func TestRaftEdgeConfig_Defaults(t *testing.T) {
	cfg := DefaultRaftEdgeConfig()
	if !cfg.AdaptiveTimeout {
		t.Error("adaptive timeout should default to true")
	}
	if cfg.MinimumPeers != 1 {
		t.Errorf("minimum peers = %d, want 1", cfg.MinimumPeers)
	}
	if cfg.PartitionTolerance != 30*time.Second {
		t.Errorf("partition tolerance = %v", cfg.PartitionTolerance)
	}
	if !cfg.CompressSnapshots {
		t.Error("compress snapshots should default to true")
	}
}

func TestRaftLatencyTracker_Record(t *testing.T) {
	lt := NewRaftLatencyTracker(10)

	for i := 0; i < 20; i++ {
		lt.Record("peer1", time.Duration(i)*time.Millisecond)
	}

	// Should only keep last 10
	lt.mu.RLock()
	if len(lt.samples["peer1"]) != 10 {
		t.Errorf("samples = %d, want 10", len(lt.samples["peer1"]))
	}
	lt.mu.RUnlock()
}

func TestRaftLatencyTracker_Median(t *testing.T) {
	lt := NewRaftLatencyTracker(100)
	for i := 1; i <= 11; i++ {
		lt.Record("peer1", time.Duration(i)*time.Millisecond)
	}

	median := lt.Median("peer1")
	if median != 6*time.Millisecond {
		t.Errorf("median = %v, want 6ms", median)
	}
}

func TestRaftLatencyTracker_P99(t *testing.T) {
	lt := NewRaftLatencyTracker(100)
	for i := 1; i <= 100; i++ {
		lt.Record("peer1", time.Duration(i)*time.Millisecond)
	}

	p99 := lt.P99("peer1")
	if p99 < 99*time.Millisecond {
		t.Errorf("p99 = %v, expected >= 99ms", p99)
	}
}

func TestRaftLatencyTracker_EmptyPeer(t *testing.T) {
	lt := NewRaftLatencyTracker(100)
	if lt.Median("unknown") != 0 {
		t.Error("expected 0 for unknown peer")
	}
	if lt.P99("unknown") != 0 {
		t.Error("expected 0 for unknown peer")
	}
}

func TestRaftLatencyTracker_AdaptiveTimeout(t *testing.T) {
	lt := NewRaftLatencyTracker(100)
	for i := 0; i < 50; i++ {
		lt.Record("peer1", 5*time.Millisecond)
		lt.Record("peer2", 10*time.Millisecond)
	}

	timeout := lt.AdaptiveElectionTimeout(10)
	// Should be based on max p99 (10ms) * 10 = 100ms
	if timeout < 50*time.Millisecond {
		t.Errorf("timeout = %v, expected >= 50ms", timeout)
	}
}

func TestRaftLatencyTracker_AdaptiveTimeout_Empty(t *testing.T) {
	lt := NewRaftLatencyTracker(100)
	timeout := lt.AdaptiveElectionTimeout(10)
	if timeout != 150*time.Millisecond {
		t.Errorf("timeout = %v, want 150ms", timeout)
	}
}

func TestRaftLatencyTracker_AllPeers(t *testing.T) {
	lt := NewRaftLatencyTracker(100)
	lt.Record("peer1", 5*time.Millisecond)
	lt.Record("peer2", 10*time.Millisecond)

	peers := lt.AllPeers()
	if len(peers) != 2 {
		t.Errorf("peers = %d, want 2", len(peers))
	}
}

func TestNewRaftClusterTopology(t *testing.T) {
	peers := []RaftPeer{
		{ID: "node2", Addr: "localhost:8002"},
		{ID: "node3", Addr: "localhost:8003"},
	}
	cfg := DefaultRaftEdgeConfig()
	topo := NewRaftClusterTopology("node1", peers, cfg)

	if topo.Quorum != 2 {
		t.Errorf("quorum = %d, want 2", topo.Quorum)
	}
	if len(topo.Nodes) != 3 {
		t.Errorf("nodes = %d, want 3", len(topo.Nodes))
	}
	if topo.Nodes[0].ID != "node1" {
		t.Errorf("first node = %q", topo.Nodes[0].ID)
	}
}

func TestRaftClusterTopology_MarshalJSON(t *testing.T) {
	topo := NewRaftClusterTopology("n1", nil, DefaultRaftEdgeConfig())
	data, err := topo.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty json")
	}
}

func TestRaftReadConsistency_String(t *testing.T) {
	tests := []struct {
		c    RaftReadConsistency
		want string
	}{
		{RaftReadDefault, "DEFAULT"},
		{RaftReadLeader, "LEADER"},
		{RaftReadFollower, "FOLLOWER"},
		{RaftReadLeaderLease, "LEADER_LEASE"},
		{RaftReadConsistency(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.c.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.c, got, tt.want)
		}
	}
}

func TestRaftHealthStatus_Fields(t *testing.T) {
	status := RaftHealthStatus{
		NodeID:       "n1",
		Role:         "LEADER",
		Term:         5,
		CommitIndex:  100,
		LastApplied:  100,
		LeaderID:     "n1",
		PeerCount:    2,
		Healthy:      true,
		Uptime:       time.Minute,
		LastContact:  time.Now(),
	}

	if !status.Healthy {
		t.Error("expected healthy")
	}
	if status.CommitIndex != 100 {
		t.Error("wrong commit index")
	}
}
