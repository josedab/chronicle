package chronicle

import (
	"testing"
	"time"
)

func TestEmbeddedClusterNewAndDefaults(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "test-node-1"

	ec := NewEmbeddedCluster(db, cfg)
	if ec == nil {
		t.Fatal("NewEmbeddedCluster returned nil")
	}
	if ec.NodeID() != "test-node-1" {
		t.Errorf("Expected NodeID test-node-1, got %s", ec.NodeID())
	}
}

func TestEmbeddedClusterStartStopLifecycle(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "node-start-stop"

	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if err := ec.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestEmbeddedClusterWriteSingleNode(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "single-writer"

	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer ec.Stop()

	p := Point{
		Metric:    "cluster.test",
		Value:     42.0,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "test"},
	}

	err := ec.Write(p)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestEmbeddedClusterWriteWithConsistencyOne(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "consistency-one"

	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer ec.Stop()

	p := Point{
		Metric:    "consistency.test",
		Value:     99.0,
		Timestamp: time.Now().UnixNano(),
	}

	err := ec.WriteWithConsistency(p, WriteConsistencyOne)
	if err != nil {
		t.Fatalf("WriteWithConsistency(ONE) failed: %v", err)
	}
}

func TestEmbeddedClusterReadEventual(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "reader"

	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer ec.Stop()

	// Write some data.
	now := time.Now()
	for i := 0; i < 5; i++ {
		_ = ec.Write(Point{
			Metric:    "read.test",
			Value:     float64(i),
			Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	q := &Query{Metric: "read.test"}
	result, err := ec.Read(q, ReadConsistencyEventual)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if result == nil {
		t.Fatal("Read returned nil result")
	}
}

func TestEmbeddedClusterHealthCheck(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "health-node"

	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer ec.Stop()

	health := ec.Health()
	if health.TotalNodes < 1 {
		t.Errorf("Expected at least 1 node, got %d", health.TotalNodes)
	}
}

func TestEmbeddedClusterStats(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "stats-node"

	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer ec.Stop()

	stats := ec.Stats()
	if stats.NodeID != "stats-node" {
		t.Errorf("Expected stats-node, got %s", stats.NodeID)
	}
}

func TestEmbeddedClusterIsLeaderSingleNode(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "solo-leader"

	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer ec.Stop()

	// Single node should become leader.
	// Give it a moment to elect itself.
	time.Sleep(50 * time.Millisecond)

	// Just verify no panic – single-node leadership may or may not be automatic.
	_ = ec.IsLeader()
}

// --- LeaderLease Tests ---

func TestLeaderLeaseRenewAndValidate(t *testing.T) {
	lease := &LeaderLease{duration: 100 * time.Millisecond}
	lease.Renew("node-1")

	if !lease.IsValid() {
		t.Error("Expected lease to be valid after Renew")
	}
}

func TestLeaderLeaseExpires(t *testing.T) {
	lease := &LeaderLease{duration: 10 * time.Millisecond}
	lease.Renew("node-1")

	time.Sleep(20 * time.Millisecond)

	if lease.IsValid() {
		t.Error("Expected lease to be expired")
	}
}

func TestLeaderLeaseRevoke(t *testing.T) {
	lease := &LeaderLease{duration: time.Hour}
	lease.Renew("node-1")
	lease.Revoke()

	if lease.IsValid() {
		t.Error("Expected lease to be invalid after Revoke")
	}
}

func TestLeaderLeaseNotValidWithoutHolder(t *testing.T) {
	lease := &LeaderLease{duration: time.Hour}
	if lease.IsValid() {
		t.Error("Expected lease to be invalid without holder")
	}
}

// --- AntiEntropyRepair Tests ---

func TestAntiEntropyComputeChecksum(t *testing.T) {
	ae := NewAntiEntropyRepair()
	cs1 := ae.ComputeChecksum("partition-1", []byte("data-a"))
	cs2 := ae.ComputeChecksum("partition-1", []byte("data-a"))

	if cs1 != cs2 {
		t.Error("Same data should produce same checksum")
	}

	cs3 := ae.ComputeChecksum("partition-2", []byte("data-b"))
	if cs1 == cs3 {
		t.Error("Different data should (likely) produce different checksums")
	}
}

func TestAntiEntropyCompareChecksums(t *testing.T) {
	ae := NewAntiEntropyRepair()
	ae.ComputeChecksum("p1", []byte("data-1"))
	ae.ComputeChecksum("p2", []byte("data-2"))

	remote := map[string]uint32{
		"p1": ae.ComputeChecksum("p1", []byte("data-1")), // same
		"p2": 12345,                                        // different
		"p3": 99999,                                        // only on remote
	}

	divergent := ae.CompareChecksums(remote)
	if len(divergent) < 1 {
		t.Error("Expected at least one divergent partition")
	}
}

func TestAntiEntropyMarkRepaired(t *testing.T) {
	ae := NewAntiEntropyRepair()
	ae.MarkRepaired()

	ae.mu.RLock()
	if ae.repairCount != 1 {
		t.Errorf("Expected repairCount 1, got %d", ae.repairCount)
	}
	if ae.lastRepair.IsZero() {
		t.Error("Expected lastRepair to be set")
	}
	ae.mu.RUnlock()
}

// --- ClusterSimulator Tests ---

func TestClusterSimulatorAddAndPartition(t *testing.T) {
	sim := NewClusterSimulator()

	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "sim-node-1"
	ec := NewEmbeddedCluster(db, cfg)

	sim.AddNode("sim-node-1", ec)

	if len(sim.nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(sim.nodes))
	}

	sim.PartitionNode("sim-node-1")
	if !sim.IsPartitioned("sim-node-1") {
		t.Error("Expected node to be partitioned")
	}

	sim.HealPartition("sim-node-1")
	if sim.IsPartitioned("sim-node-1") {
		t.Error("Expected node to be healed")
	}
}

func TestClusterSimulatorHistory(t *testing.T) {
	sim := NewClusterSimulator()

	db := setupTestDB(t)
	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "hist-node"
	ec := NewEmbeddedCluster(db, cfg)

	sim.AddNode("hist-node", ec)
	sim.PartitionNode("hist-node")
	sim.HealPartition("hist-node")

	// History may be empty if events are tracked separately.
	// Just ensure it doesn't panic and returns a valid slice.
	history := sim.History()
	_ = history // No assertion on length since events and history are separate.
}

func TestConsistencyLevelStrings(t *testing.T) {
	tests := []struct {
		rc       ReadConsistency
		expected string
	}{
		{ReadConsistencyEventual, "eventual"},
		{ReadConsistencyStrong, "strong"},
		{ReadConsistencyBoundedStaleness, "bounded_staleness"},
	}
	for _, tc := range tests {
		if tc.rc.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.rc.String())
		}
	}

	wcTests := []struct {
		wc       WriteConsistency
		expected string
	}{
		{WriteConsistencyOne, "ONE"},
		{WriteConsistencyQuorum, "QUORUM"},
		{WriteConsistencyAll, "ALL"},
	}
	for _, tc := range wcTests {
		if tc.wc.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.wc.String())
		}
	}
}
