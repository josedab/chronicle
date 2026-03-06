package chronicle

import (
	"testing"
	"time"
)

// --- DefaultEmbeddedClusterConfig ---

func TestDefaultEmbeddedClusterConfig(t *testing.T) {
	cfg := DefaultEmbeddedClusterConfig()

	if cfg.ReplicationFactor != 3 {
		t.Errorf("ReplicationFactor = %d, want 3", cfg.ReplicationFactor)
	}
	if cfg.DefaultConsistency != ReadConsistencyEventual {
		t.Errorf("DefaultConsistency = %v, want eventual", cfg.DefaultConsistency)
	}
	if cfg.ElectionTimeout != time.Second {
		t.Errorf("ElectionTimeout = %v, want 1s", cfg.ElectionTimeout)
	}
	if cfg.HeartbeatInterval != 200*time.Millisecond {
		t.Errorf("HeartbeatInterval = %v, want 200ms", cfg.HeartbeatInterval)
	}
	if !cfg.AutoFailover {
		t.Error("AutoFailover should be true by default")
	}
	if !cfg.SplitBrainDetection {
		t.Error("SplitBrainDetection should be true by default")
	}
}

// --- ReadConsistency String ---

func TestReadConsistencyStringLifecycle(t *testing.T) {
	tests := []struct {
		rc     ReadConsistency
		expect string
	}{
		{ReadConsistencyEventual, "eventual"},
		{ReadConsistencyStrong, "strong"},
		{ReadConsistencyBoundedStaleness, "bounded_staleness"},
		{ReadConsistency(99), "unknown"},
	}
	for _, tc := range tests {
		if tc.rc.String() != tc.expect {
			t.Errorf("ReadConsistency(%d).String() = %q, want %q", tc.rc, tc.rc.String(), tc.expect)
		}
	}
}

// --- WriteConsistency String ---

func TestWriteConsistencyStringLifecycle(t *testing.T) {
	tests := []struct {
		wc     WriteConsistency
		expect string
	}{
		{WriteConsistencyOne, "ONE"},
		{WriteConsistencyQuorum, "QUORUM"},
		{WriteConsistencyAll, "ALL"},
		{WriteConsistency(99), "UNKNOWN"},
	}
	for _, tc := range tests {
		if tc.wc.String() != tc.expect {
			t.Errorf("WriteConsistency(%d).String() = %q, want %q", tc.wc, tc.wc.String(), tc.expect)
		}
	}
}

// --- LeaderLease ---

func TestLeaderLease_Lifecycle(t *testing.T) {
	lease := &LeaderLease{
		duration: 100 * time.Millisecond,
	}

	// Before Renew, no holder
	if lease.IsValid() {
		t.Error("Lease should be invalid before first renewal")
	}

	// Renew
	lease.Renew("node-1")
	if !lease.IsValid() {
		t.Error("Lease should be valid after renewal")
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)
	if lease.IsValid() {
		t.Error("Lease should be expired")
	}

	// Re-renew
	lease.Renew("node-1")
	if !lease.IsValid() {
		t.Error("Lease should be valid after re-renewal")
	}
}

func TestLeaderLease_Revoke(t *testing.T) {
	lease := &LeaderLease{
		duration: time.Hour,
	}
	lease.Renew("leader-1")

	if !lease.IsValid() {
		t.Error("Lease should be valid")
	}

	lease.Revoke()
	if lease.IsValid() {
		t.Error("Lease should be invalid after revoke")
	}
}

func TestLeaderLease_DifferentHolders(t *testing.T) {
	lease := &LeaderLease{
		duration: time.Hour,
	}

	lease.Renew("leader-1")
	lease.mu.RLock()
	holder := lease.holderID
	lease.mu.RUnlock()
	if holder != "leader-1" {
		t.Errorf("Holder = %s, want leader-1", holder)
	}

	lease.Renew("leader-2")
	lease.mu.RLock()
	holder = lease.holderID
	lease.mu.RUnlock()
	if holder != "leader-2" {
		t.Errorf("Holder = %s, want leader-2", holder)
	}
}

// --- AntiEntropyRepair ---

func TestAntiEntropyRepair_ComputeChecksum(t *testing.T) {
	ae := NewAntiEntropyRepair()

	data := []byte("test partition data")
	checksum := ae.ComputeChecksum("partition-1", data)

	if checksum == 0 {
		t.Error("Checksum should not be zero")
	}

	// Same data should produce same checksum
	checksum2 := ae.ComputeChecksum("partition-1", data)
	if checksum != checksum2 {
		t.Error("Same data should produce same checksum")
	}

	// Different data should produce different checksum
	checksum3 := ae.ComputeChecksum("partition-1", []byte("different data"))
	if checksum == checksum3 {
		t.Error("Different data should produce different checksum")
	}
}

func TestAntiEntropyRepair_CompareChecksums(t *testing.T) {
	ae := NewAntiEntropyRepair()

	ae.ComputeChecksum("part-1", []byte("data1"))
	ae.ComputeChecksum("part-2", []byte("data2"))
	ae.ComputeChecksum("part-3", []byte("data3"))

	// Remote has same checksum for part-1, different for part-2, missing part-3
	ae.mu.RLock()
	samePart1 := ae.checksums["part-1"]
	diffPart2 := ae.checksums["part-2"] + 1
	ae.mu.RUnlock()

	remote := map[string]uint32{
		"part-1": samePart1, // Same
		"part-2": diffPart2, // Different
		"part-4": 999,       // Only on remote
	}

	divergent := ae.CompareChecksums(remote)

	// Should include: part-2 (different), part-3 (only local), part-4 (only remote)
	if len(divergent) < 2 {
		t.Errorf("Expected at least 2 divergent partitions, got %d: %v", len(divergent), divergent)
	}

	divergentSet := make(map[string]bool)
	for _, d := range divergent {
		divergentSet[d] = true
	}

	if !divergentSet["part-2"] {
		t.Error("part-2 should be divergent (different checksum)")
	}
	if !divergentSet["part-3"] {
		t.Error("part-3 should be divergent (only local)")
	}
	if !divergentSet["part-4"] {
		t.Error("part-4 should be divergent (only remote)")
	}
}

func TestAntiEntropyRepair_MarkRepaired(t *testing.T) {
	ae := NewAntiEntropyRepair()

	ae.mu.RLock()
	if ae.repairCount != 0 {
		t.Errorf("Initial repair count should be 0, got %d", ae.repairCount)
	}
	ae.mu.RUnlock()

	ae.MarkRepaired()
	ae.mu.RLock()
	if ae.repairCount != 1 {
		t.Errorf("Repair count should be 1, got %d", ae.repairCount)
	}
	ae.mu.RUnlock()

	ae.MarkRepaired()
	ae.mu.RLock()
	if ae.repairCount != 2 {
		t.Errorf("Repair count should be 2, got %d", ae.repairCount)
	}
	lastRepair := ae.lastRepair
	ae.mu.RUnlock()

	if lastRepair.IsZero() {
		t.Error("lastRepair should not be zero")
	}
}

func TestAntiEntropyRepair_EmptyRemote(t *testing.T) {
	ae := NewAntiEntropyRepair()
	ae.ComputeChecksum("part-1", []byte("data"))

	divergent := ae.CompareChecksums(map[string]uint32{})
	if len(divergent) != 1 || divergent[0] != "part-1" {
		t.Errorf("Expected [part-1] divergent, got %v", divergent)
	}
}

func TestAntiEntropyRepair_EmptyLocal(t *testing.T) {
	ae := NewAntiEntropyRepair()

	remote := map[string]uint32{"part-1": 123}
	divergent := ae.CompareChecksums(remote)
	if len(divergent) != 1 || divergent[0] != "part-1" {
		t.Errorf("Expected [part-1] divergent, got %v", divergent)
	}
}

// --- NewEmbeddedCluster with temp DB ---

func TestNewEmbeddedCluster_WithDB(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "test-node"

	cluster := NewEmbeddedCluster(db, cfg)
	if cluster == nil {
		t.Fatal("NewEmbeddedCluster returned nil")
	}

	if cluster.NodeID() != "test-node" {
		t.Errorf("NodeID = %s, want test-node", cluster.NodeID())
	}
}

func TestNewEmbeddedCluster_GeneratesNodeID(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultEmbeddedClusterConfig()
	// NodeID left empty — should be auto-generated

	cluster := NewEmbeddedCluster(db, cfg)
	if cluster.NodeID() == "" {
		t.Error("Expected auto-generated NodeID, got empty string")
	}
}

// --- EmbeddedCluster Start/Stop ---

func TestEmbeddedCluster_StartStop(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "lifecycle-node"

	cluster := NewEmbeddedCluster(db, cfg)

	if err := cluster.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !cluster.running.Load() {
		t.Error("Expected cluster to be running")
	}

	// No peers → should self-elect as leader
	if !cluster.IsLeader() {
		t.Error("Single-node cluster should be leader")
	}

	if err := cluster.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if cluster.running.Load() {
		t.Error("Expected cluster to be stopped")
	}
}

func TestEmbeddedCluster_DoubleStart(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "double-start"

	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatalf("First Start: %v", err)
	}
	defer cluster.Stop()

	err := cluster.Start()
	if err == nil {
		t.Error("Second Start should return an error")
	}
}

// --- ClusterHealth ---

func TestClusterHealth_Struct(t *testing.T) {
	health := ClusterHealth{
		Healthy:      true,
		TotalNodes:   3,
		HealthyNodes: 3,
		LeaderID:     "node-1",
		SplitBrain:   false,
		MinorityMode: false,
	}

	if !health.Healthy {
		t.Error("Cluster should be healthy")
	}
	if health.SplitBrain {
		t.Error("Split brain should be false")
	}
}

func TestEmbeddedCluster_Health(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "health-node"

	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer cluster.Stop()

	h := cluster.Health()
	if h.TotalNodes != 1 {
		t.Errorf("TotalNodes = %d, want 1", h.TotalNodes)
	}
	if h.HealthyNodes != 1 {
		t.Errorf("HealthyNodes = %d, want 1", h.HealthyNodes)
	}
	if h.LeaderID != "health-node" {
		t.Errorf("LeaderID = %q, want %q", h.LeaderID, "health-node")
	}
}

// --- EmbeddedClusterStats ---

func TestEmbeddedClusterStats_Struct(t *testing.T) {
	stats := EmbeddedClusterStats{
		NodeID:           "node-1",
		IsLeader:         true,
		Term:             5,
		CommitIndex:      100,
		AppliedIndex:     95,
		ReplicatedWrites: 500,
	}

	if stats.NodeID != "node-1" {
		t.Errorf("NodeID = %s, want node-1", stats.NodeID)
	}
	if !stats.IsLeader {
		t.Error("Should be leader")
	}
}

func TestEmbeddedCluster_Stats(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "stats-node"

	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer cluster.Stop()

	stats := cluster.Stats()
	if stats.NodeID != "stats-node" {
		t.Errorf("NodeID = %q, want %q", stats.NodeID, "stats-node")
	}
	if !stats.IsLeader {
		t.Error("Single-node cluster stats should report leader")
	}
	if stats.Uptime <= 0 {
		t.Error("Uptime should be positive after start")
	}
}
