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

func TestEmbeddedClusterReplication(t *testing.T) {
	leaderDB := setupTestDB(t)
	defer leaderDB.Close()
	replicaDB := setupTestDB(t)
	defer replicaDB.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "leader"
	ec := NewEmbeddedCluster(leaderDB, cfg)
	if err := ec.Start(); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop()

	ec.ConnectPeer("replica-1", replicaDB)

	p := Point{Metric: "test.metric", Value: 42.0, Timestamp: 1000}
	if err := ec.WriteWithConsistency(p, WriteConsistencyQuorum); err != nil {
		t.Fatalf("WriteWithConsistency: %v", err)
	}

	// Flush both DBs to ensure data is queryable
	leaderDB.Flush()
	replicaDB.Flush()

	result, err := replicaDB.Execute(&Query{Metric: "test.metric", Start: 0, End: 2000})
	if err != nil {
		t.Fatalf("replica query: %v", err)
	}
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point on replica, got %d", len(result.Points))
	}
	if ec.Stats().ReplicatedWrites != 1 {
		t.Errorf("expected 1 replicated write, got %d", ec.Stats().ReplicatedWrites)
	}
}

func TestWriteConsistencyLevels(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	ec := NewEmbeddedCluster(db, cfg)
	ec.Start()
	defer ec.Stop()

	p := Point{Metric: "test.one", Value: 1.0, Timestamp: 1000}
	if err := ec.WriteWithConsistency(p, WriteConsistencyOne); err != nil {
		t.Fatalf("ONE write: %v", err)
	}

	if WriteConsistencyOne.String() != "ONE" {
		t.Errorf("expected ONE, got %s", WriteConsistencyOne.String())
	}
	if WriteConsistencyQuorum.String() != "QUORUM" {
		t.Errorf("expected QUORUM, got %s", WriteConsistencyQuorum.String())
	}
	if WriteConsistencyAll.String() != "ALL" {
		t.Errorf("expected ALL, got %s", WriteConsistencyAll.String())
	}
}

func TestClusterSimulation(t *testing.T) {
	db1 := setupTestDB(t)
	defer db1.Close()
	db2 := setupTestDB(t)
	defer db2.Close()

	cfg1 := DefaultEmbeddedClusterConfig()
	cfg1.NodeID = "node-1"
	ec1 := NewEmbeddedCluster(db1, cfg1)
	ec1.Start()
	defer ec1.Stop()

	cfg2 := DefaultEmbeddedClusterConfig()
	cfg2.NodeID = "node-2"
	ec2 := NewEmbeddedCluster(db2, cfg2)
	ec2.Start()
	defer ec2.Stop()

	sim := NewClusterSimulator()
	sim.AddNode("node-1", ec1)
	sim.AddNode("node-2", ec2)

	p := Point{Metric: "sim.metric", Value: 100, Timestamp: 1000}
	if err := sim.SimulateWrite("node-1", p); err != nil {
		t.Fatalf("SimulateWrite: %v", err)
	}

	sim.PartitionNode("node-2")
	if !sim.IsPartitioned("node-2") {
		t.Error("node-2 should be partitioned")
	}

	// Write to partitioned node fails but should be recorded
	_ = sim.SimulateWrite("node-2", Point{Metric: "fail", Value: 1, Timestamp: 2000})

	sim.HealPartition("node-2")
	history := sim.History()
	if len(history) < 1 {
		t.Errorf("expected at least 1 history event, got %d", len(history))
	}
	if !sim.CheckLinearizability() {
		t.Error("linearizability check failed")
	}
}

func TestLeaderLease(t *testing.T) {
	lease := &LeaderLease{duration: 100 * time.Millisecond}
	if lease.IsValid() {
		t.Error("lease should not be valid before renewal")
	}
	lease.Renew("node-1")
	if !lease.IsValid() {
		t.Error("lease should be valid after renewal")
	}
	lease.Revoke()
	if lease.IsValid() {
		t.Error("lease should not be valid after revoke")
	}
}

func TestAntiEntropyRepair(t *testing.T) {
	ae := NewAntiEntropyRepair()
	ae.ComputeChecksum("partition-1", []byte("data1"))
	ae.ComputeChecksum("partition-2", []byte("data2"))

	local := make(map[string]uint32)
	ae.mu.RLock()
	for k, v := range ae.checksums {
		local[k] = v
	}
	ae.mu.RUnlock()

	divergent := ae.CompareChecksums(local)
	if len(divergent) != 0 {
		t.Errorf("expected 0 divergent, got %d", len(divergent))
	}

	remote := map[string]uint32{
		"partition-1": 99999,
		"partition-3": 11111,
	}
	divergent = ae.CompareChecksums(remote)
	if len(divergent) < 2 {
		t.Errorf("expected at least 2 divergent, got %d", len(divergent))
	}
	ae.MarkRepaired()
}

func TestEmbeddedClusterMultiNodeAllConsistency(t *testing.T) {
	// Create leader + 2 replicas for ALL consistency test
	leaderDB := setupTestDB(t)
	defer leaderDB.Close()
	replica1DB := setupTestDB(t)
	defer replica1DB.Close()
	replica2DB := setupTestDB(t)
	defer replica2DB.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "leader"
	ec := NewEmbeddedCluster(leaderDB, cfg)
	if err := ec.Start(); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop()

	ec.ConnectPeer("replica-1", replica1DB)
	ec.ConnectPeer("replica-2", replica2DB)

	t.Run("AllConsistencyWrite", func(t *testing.T) {
		p := Point{Metric: "replicated_metric", Value: 42.0, Timestamp: 1000}
		err := ec.WriteWithConsistency(p, WriteConsistencyAll)
		if err != nil {
			t.Fatalf("WriteConsistencyAll: %v", err)
		}

		// Verify on leader
		leaderDB.Flush()
		res, _ := leaderDB.Execute(&Query{Metric: "replicated_metric", Start: 0, End: 2000})
		if len(res.Points) != 1 {
			t.Errorf("leader: expected 1 point, got %d", len(res.Points))
		}

		// Verify on both replicas
		replica1DB.Flush()
		res1, _ := replica1DB.Execute(&Query{Metric: "replicated_metric", Start: 0, End: 2000})
		if len(res1.Points) != 1 {
			t.Errorf("replica-1: expected 1 point, got %d", len(res1.Points))
		}

		replica2DB.Flush()
		res2, _ := replica2DB.Execute(&Query{Metric: "replicated_metric", Start: 0, End: 2000})
		if len(res2.Points) != 1 {
			t.Errorf("replica-2: expected 1 point, got %d", len(res2.Points))
		}
	})

	t.Run("QuorumConsistencyWithOnePeerDown", func(t *testing.T) {
		// Disconnect one peer to simulate failure
		ec.mu.Lock()
		for i := range ec.peers {
			if ec.peers[i].ID == "replica-2" {
				ec.peers[i].Healthy = false
				ec.peers[i].replicaDB = nil
			}
		}
		ec.mu.Unlock()

		// QUORUM should still succeed (2/3 nodes = majority)
		p := Point{Metric: "quorum_metric", Value: 99.0, Timestamp: 2000}
		err := ec.WriteWithConsistency(p, WriteConsistencyQuorum)
		if err != nil {
			t.Fatalf("WriteConsistencyQuorum with 1 peer down: %v", err)
		}

		// ALL should fail (only 2/3 available)
		p2 := Point{Metric: "all_metric", Value: 100.0, Timestamp: 3000}
		err = ec.WriteWithConsistency(p2, WriteConsistencyAll)
		if err == nil {
			t.Error("WriteConsistencyAll should fail with 1 peer down")
		}
	})

	t.Run("StrongReadRequiresLeader", func(t *testing.T) {
		q := &Query{Metric: "replicated_metric", Start: 0, End: 5000}

		// Leader can serve strong reads
		_, err := ec.Read(q, ReadConsistencyStrong)
		if err != nil {
			t.Fatalf("strong read on leader: %v", err)
		}

		// Eventual reads always work
		_, err = ec.Read(q, ReadConsistencyEventual)
		if err != nil {
			t.Fatalf("eventual read: %v", err)
		}
	})
}
