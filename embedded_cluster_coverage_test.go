package chronicle

import (
	"testing"
	"time"
)

// --- ClusterSimulator ---

func TestClusterSimulator_Lifecycle(t *testing.T) {
	sim := NewClusterSimulator()

	if sim == nil {
		t.Fatal("Expected non-nil simulator")
	}

	// History starts empty
	if len(sim.History()) != 0 {
		t.Error("History should start empty")
	}
}

func TestClusterSimulator_AddAndPartitionNode(t *testing.T) {
	sim := NewClusterSimulator()

	dir := t.TempDir()
	db, err := Open(dir+"/sim.db", DefaultConfig(dir+"/sim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "sim-node-1"
	cluster := NewEmbeddedCluster(db, cfg)

	sim.AddNode("sim-node-1", cluster)

	if sim.IsPartitioned("sim-node-1") {
		t.Error("Node should not be partitioned initially")
	}

	sim.PartitionNode("sim-node-1")
	if !sim.IsPartitioned("sim-node-1") {
		t.Error("Node should be partitioned")
	}

	sim.HealPartition("sim-node-1")
	if sim.IsPartitioned("sim-node-1") {
		t.Error("Node should not be partitioned after heal")
	}
}

func TestClusterSimulator_SimulateWrite(t *testing.T) {
	sim := NewClusterSimulator()

	dir := t.TempDir()
	db, err := Open(dir+"/sim.db", DefaultConfig(dir+"/sim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "writer-node"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()

	p := Point{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano()}
	err = sim.SimulateWrite("writer-node", p)
	if err == nil {
		t.Error("Expected error for unknown node before AddNode")
	}

	sim.AddNode("writer-node", cluster)

	err = sim.SimulateWrite("writer-node", p)
	if err != nil {
		t.Fatalf("SimulateWrite: %v", err)
	}

	history := sim.History()
	if len(history) == 0 {
		t.Error("Expected history entries after write")
	}
}

func TestClusterSimulator_SimulateWrite_Partitioned(t *testing.T) {
	sim := NewClusterSimulator()

	dir := t.TempDir()
	db, err := Open(dir+"/sim.db", DefaultConfig(dir+"/sim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "part-node"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()

	sim.AddNode("part-node", cluster)
	sim.PartitionNode("part-node")

	p := Point{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano()}
	err = sim.SimulateWrite("part-node", p)
	if err == nil {
		t.Error("Expected error writing to partitioned node")
	}
}

func TestClusterSimulator_SimulateWrite_UnknownNode(t *testing.T) {
	sim := NewClusterSimulator()

	p := Point{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano()}
	err := sim.SimulateWrite("nonexistent", p)
	if err == nil {
		t.Error("Expected error for unknown node")
	}
}

func TestClusterSimulator_CheckLinearizability(t *testing.T) {
	sim := NewClusterSimulator()

	linearizable := sim.CheckLinearizability()
	if !linearizable {
		t.Error("Empty simulator should be linearizable")
	}
}

func TestClusterSimulator_CheckLinearizability_WithHistory(t *testing.T) {
	sim := NewClusterSimulator()

	dir := t.TempDir()
	db, err := Open(dir+"/sim.db", DefaultConfig(dir+"/sim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "lin-node"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()

	sim.AddNode("lin-node", cluster)

	p := Point{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()}
	_ = sim.SimulateWrite("lin-node", p)

	linearizable := sim.CheckLinearizability()
	if !linearizable {
		t.Error("Should be linearizable after successful writes")
	}
}

func TestClusterSimulator_IsPartitioned_Unknown(t *testing.T) {
	sim := NewClusterSimulator()
	if sim.IsPartitioned("unknown") {
		t.Error("Unknown node should not be partitioned")
	}
}

// --- ConnectPeer ---

func TestConnectPeer(t *testing.T) {
	dir := t.TempDir()

	db1, err := Open(dir+"/node1.db", DefaultConfig(dir+"/node1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	db2, err := Open(dir+"/node2.db", DefaultConfig(dir+"/node2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	cfg1 := DefaultEmbeddedClusterConfig()
	cfg1.NodeID = "node-1"
	cluster1 := NewEmbeddedCluster(db1, cfg1)

	cluster1.ConnectPeer("node-2", db2)

	// Connect same peer again to exercise the update path
	cluster1.ConnectPeer("node-2", db2)
}

// --- WriteWithConsistency edge cases ---

func TestWriteWithConsistency_AllLevels(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/wc.db", DefaultConfig(dir+"/wc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "wc-node"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()
	cluster.isLeader.Store(true)

	p := Point{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano()}

	// WriteConsistencyOne
	err = cluster.WriteWithConsistency(p, WriteConsistencyOne)
	if err != nil {
		t.Fatalf("WriteConsistencyOne: %v", err)
	}

	// WriteConsistencyQuorum (no peers, just self)
	err = cluster.WriteWithConsistency(p, WriteConsistencyQuorum)
	if err != nil {
		t.Fatalf("WriteConsistencyQuorum: %v", err)
	}

	// WriteConsistencyAll (no peers, just self)
	err = cluster.WriteWithConsistency(p, WriteConsistencyAll)
	if err != nil {
		t.Fatalf("WriteConsistencyAll: %v", err)
	}
}

func TestWriteWithConsistency_NotRunning(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/wc2.db", DefaultConfig(dir+"/wc2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "wc-stopped"
	cluster := NewEmbeddedCluster(db, cfg)

	p := Point{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()}
	err = cluster.WriteWithConsistency(p, WriteConsistencyOne)
	if err == nil {
		t.Error("Expected error writing to stopped cluster")
	}
}

func TestWriteWithConsistency_NotLeader(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/wc3.db", DefaultConfig(dir+"/wc3.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "wc-follower"
	cfg.Peers = []string{"peer-addr:1234"}
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()
	cluster.isLeader.Store(false)

	p := Point{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()}
	err = cluster.WriteWithConsistency(p, WriteConsistencyOne)
	if err == nil {
		t.Error("Expected error writing as non-leader")
	}
}

// --- ReadWithLease ---

func TestReadWithLease_EventualConsistency(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/rl.db", DefaultConfig(dir+"/rl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_ = db.Write(Point{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano()})

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "read-node"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()

	q := &Query{Metric: "cpu"}
	result, err := cluster.ReadWithLease(q, ReadConsistencyEventual)
	if err != nil {
		t.Fatalf("ReadWithLease eventual: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestReadWithLease_StrongConsistency_AsLeader(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/rl2.db", DefaultConfig(dir+"/rl2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_ = db.Write(Point{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano()})

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "leader-read"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()
	cluster.isLeader.Store(true)
	// Set up valid lease
	cluster.lease = &LeaderLease{duration: time.Hour}
	cluster.lease.Renew("leader-read")

	q := &Query{Metric: "cpu"}
	result, err := cluster.ReadWithLease(q, ReadConsistencyStrong)
	if err != nil {
		t.Fatalf("ReadWithLease strong: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestReadWithLease_StrongConsistency_NotLeader(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/rl3.db", DefaultConfig(dir+"/rl3.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "follower-read"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()
	cluster.isLeader.Store(false)

	q := &Query{Metric: "cpu"}
	_, err = cluster.ReadWithLease(q, ReadConsistencyStrong)
	if err == nil {
		t.Error("Expected error for strong read on non-leader")
	}
}

func TestReadWithLease_BoundedStaleness(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/rl4.db", DefaultConfig(dir+"/rl4.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_ = db.Write(Point{Metric: "mem", Value: 88.0, Timestamp: time.Now().UnixNano()})

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "bounded-read"
	cluster := NewEmbeddedCluster(db, cfg)
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()

	q := &Query{Metric: "mem"}
	result, err := cluster.ReadWithLease(q, ReadConsistencyBoundedStaleness)
	if err != nil {
		t.Fatalf("ReadWithLease bounded: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestReadWithLease_NotRunning(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/rl5.db", DefaultConfig(dir+"/rl5.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultEmbeddedClusterConfig()
	cfg.NodeID = "stopped-read"
	cluster := NewEmbeddedCluster(db, cfg)

	q := &Query{Metric: "cpu"}
	_, err = cluster.ReadWithLease(q, ReadConsistencyEventual)
	if err == nil {
		t.Error("Expected error reading from stopped cluster")
	}
}
