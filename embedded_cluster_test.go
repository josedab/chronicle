package chronicle

import (
	"path/filepath"
	"testing"
	"time"
)

func TestEmbeddedClusterConfig(t *testing.T) {
	cfg := DefaultEmbeddedClusterConfig()
	if cfg.ReplicationFactor != 3 {
		t.Errorf("expected replication factor 3, got %d", cfg.ReplicationFactor)
	}
	if cfg.DefaultConsistency != ReadConsistencyEventual {
		t.Errorf("expected eventual consistency, got %v", cfg.DefaultConsistency)
	}
	if !cfg.AutoFailover {
		t.Error("expected auto failover enabled")
	}
	if !cfg.SplitBrainDetection {
		t.Error("expected split brain detection enabled")
	}
}

func TestReadConsistencyString(t *testing.T) {
	tests := []struct {
		rc   ReadConsistency
		want string
	}{
		{ReadConsistencyEventual, "eventual"},
		{ReadConsistencyStrong, "strong"},
		{ReadConsistencyBoundedStaleness, "bounded_staleness"},
		{ReadConsistency(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.rc.String(); got != tt.want {
			t.Errorf("ReadConsistency(%d).String() = %q, want %q", tt.rc, got, tt.want)
		}
	}
}

func TestEmbeddedClusterStartStop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ec := NewEmbeddedCluster(db, DefaultEmbeddedClusterConfig())
	if err := ec.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	// Should auto-become leader with no peers
	if !ec.IsLeader() {
		t.Error("expected to be leader with no peers")
	}
	if err := ec.Start(); err == nil {
		t.Error("expected error on double start")
	}
	if err := ec.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestEmbeddedClusterWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ec := NewEmbeddedCluster(db, DefaultEmbeddedClusterConfig())
	if err := ec.Start(); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop()

	err = ec.Write(Point{
		Metric:    "cluster_test",
		Tags:      map[string]string{"host": "node1"},
		Value:     42.0,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	stats := ec.Stats()
	if stats.ReplicatedWrites != 1 {
		t.Errorf("expected 1 replicated write, got %d", stats.ReplicatedWrites)
	}
}

func TestEmbeddedClusterRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ec := NewEmbeddedCluster(db, DefaultEmbeddedClusterConfig())
	if err := ec.Start(); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop()

	now := time.Now()
	baseTime := now.Truncate(time.Hour) // Align to partition boundary
	ec.Write(Point{
		Metric:    "cluster_read_test",
		Tags:      map[string]string{"host": "node1"},
		Value:     99.0,
		Timestamp: baseTime.Add(time.Minute).UnixNano(),
	})
	db.Flush()

	// Eventual consistency read
	result, err := ec.Read(&Query{
		Metric: "cluster_read_test",
		Start:  baseTime.UnixNano(),
		End:    baseTime.Add(time.Hour).UnixNano(),
	}, ReadConsistencyEventual)
	if err != nil {
		t.Fatalf("eventual read failed: %v", err)
	}
	if len(result.Points) == 0 {
		t.Error("expected points from eventual read")
	}

	// Strong consistency read (should work since we're leader)
	result, err = ec.Read(&Query{
		Metric: "cluster_read_test",
		Start:  baseTime.UnixNano(),
		End:    baseTime.Add(time.Hour).UnixNano(),
	}, ReadConsistencyStrong)
	if err != nil {
		t.Fatalf("strong read failed: %v", err)
	}
	if len(result.Points) == 0 {
		t.Error("expected points from strong read")
	}

	stats := ec.Stats()
	if stats.LocalReads < 2 {
		t.Errorf("expected at least 2 local reads, got %d", stats.LocalReads)
	}
}

func TestEmbeddedClusterHealth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ec := NewEmbeddedCluster(db, DefaultEmbeddedClusterConfig())
	if err := ec.Start(); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop()

	health := ec.Health()
	if !health.Healthy {
		t.Error("expected healthy cluster")
	}
	if health.TotalNodes != 1 {
		t.Errorf("expected 1 total node, got %d", health.TotalNodes)
	}
	if health.SplitBrain {
		t.Error("should not detect split brain with single node")
	}
}

func TestEmbeddedClusterReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.ReadOnly = true
	ec := NewEmbeddedCluster(db, cfg)
	if err := ec.Start(); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop()

	err = ec.Write(Point{
		Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano(),
	})
	if err == nil {
		t.Error("expected error on write to read-only node")
	}
}

func TestEmbeddedClusterNodeID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "test-node-1"
	ec := NewEmbeddedCluster(db, cfg)
	if ec.NodeID() != "test-node-1" {
		t.Errorf("expected node ID test-node-1, got %s", ec.NodeID())
	}
}
