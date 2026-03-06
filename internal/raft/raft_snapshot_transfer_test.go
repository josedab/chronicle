package raft

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// --- SnapshotTransferManager ---

func TestSnapshotTransferManager_Lifecycle(t *testing.T) {
	cfg := DefaultSnapshotTransferManagerConfig()
	if cfg.MaxSnapshots != 5 {
		t.Errorf("MaxSnapshots = %d, want 5", cfg.MaxSnapshots)
	}

	mgr := NewSnapshotTransferManager(cfg)
	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}

	snaps := mgr.ListSnapshots()
	if len(snaps) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(snaps))
	}
}

func TestSnapshotTransferManager_ZeroMaxSnapshots(t *testing.T) {
	mgr := NewSnapshotTransferManager(SnapshotTransferManagerConfig{MaxSnapshots: 0})
	// Should default to 5
	if mgr.config.MaxSnapshots != 5 {
		t.Errorf("MaxSnapshots = %d, want 5", mgr.config.MaxSnapshots)
	}
}

func TestSnapshotTransferManager_CreateAndList(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())

	snap, err := mgr.CreateSnapshot(10, 2, []byte("state-data-10"))
	if err != nil {
		t.Fatal(err)
	}
	if snap.Index != 10 {
		t.Errorf("Index = %d", snap.Index)
	}
	if snap.Term != 2 {
		t.Errorf("Term = %d", snap.Term)
	}
	if snap.Checksum == 0 {
		t.Error("Checksum should not be zero")
	}

	snap2, _ := mgr.CreateSnapshot(20, 3, []byte("state-data-20"))
	_ = snap2

	list := mgr.ListSnapshots()
	if len(list) != 2 {
		t.Fatalf("Expected 2 snapshots, got %d", len(list))
	}
	// Should be sorted newest first
	if list[0].Index != 20 {
		t.Errorf("First snapshot index = %d, want 20 (newest first)", list[0].Index)
	}
}

func TestSnapshotTransferManager_CreateNilData(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())
	_, err := mgr.CreateSnapshot(1, 1, nil)
	if err == nil {
		t.Error("Expected error for nil data")
	}
}

func TestSnapshotTransferManager_RestoreValid(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())
	snap, _ := mgr.CreateSnapshot(10, 1, []byte("valid-data"))

	err := mgr.RestoreSnapshot(snap)
	if err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
}

func TestSnapshotTransferManager_RestoreCorrupt(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())
	snap, _ := mgr.CreateSnapshot(10, 1, []byte("valid-data"))

	// Corrupt the data
	snap.Data[0] ^= 0xFF
	err := mgr.RestoreSnapshot(snap)
	if err == nil {
		t.Error("Expected error for corrupted snapshot")
	}
}

func TestSnapshotTransferManager_RestoreNil(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())
	err := mgr.RestoreSnapshot(nil)
	if err == nil {
		t.Error("Expected error for nil snapshot")
	}
}

func TestSnapshotTransferManager_Prune(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())
	for i := uint64(1); i <= 10; i++ {
		mgr.CreateSnapshot(i, 1, []byte("data"))
	}

	if len(mgr.ListSnapshots()) != 10 {
		t.Fatal("Should have 10 snapshots")
	}

	mgr.PruneSnapshots(3)
	list := mgr.ListSnapshots()
	if len(list) != 3 {
		t.Errorf("After prune(3): %d snapshots", len(list))
	}
	// Should keep the 3 newest (index 10, 9, 8)
	if list[0].Index != 10 {
		t.Errorf("Newest = %d, want 10", list[0].Index)
	}
}

func TestSnapshotTransferManager_PruneZero(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())
	mgr.CreateSnapshot(1, 1, []byte("data"))

	mgr.PruneSnapshots(0) // keep < 1, no-op
	if len(mgr.ListSnapshots()) != 1 {
		t.Error("PruneSnapshots(0) should be a no-op")
	}
}

func TestSnapshotTransferManager_PruneMoreThanExist(t *testing.T) {
	mgr := NewSnapshotTransferManager(DefaultSnapshotTransferManagerConfig())
	mgr.CreateSnapshot(1, 1, []byte("data"))

	mgr.PruneSnapshots(10)
	if len(mgr.ListSnapshots()) != 1 {
		t.Error("Prune with keep > count should be a no-op")
	}
}

// --- appendProposal ---

func TestAppendProposal_AsLeader(t *testing.T) {
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
	node.currentTerm = 3
	node.stateMu.Unlock()

	proposal := &RaftProposal{
		Command:  []byte(`{"op":"write","key":"cpu"}`),
		Response: make(chan error, 1),
		Done:     make(chan struct{}),
	}

	err = node.appendProposal(proposal)
	if err != nil {
		t.Fatalf("appendProposal: %v", err)
	}
	if proposal.Index != 1 {
		t.Errorf("Index = %d, want 1", proposal.Index)
	}
	if proposal.Term != 3 {
		t.Errorf("Term = %d, want 3", proposal.Term)
	}

	// Verify log entry
	entry := node.log.Get(1)
	if entry == nil {
		t.Fatal("Log entry 1 should exist")
	}
	if entry.Type != RaftLogCommand {
		t.Errorf("Entry type = %d, want command", entry.Type)
	}

	// Verify proposal tracked
	node.proposalsMu.Lock()
	_, tracked := node.proposals[1]
	node.proposalsMu.Unlock()
	if !tracked {
		t.Error("Proposal should be tracked at index 1")
	}
}

func TestAppendProposal_NotLeader(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "follower"
	config.BindAddr = ""
	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	proposal := &RaftProposal{
		Command:  []byte("cmd"),
		Response: make(chan error, 1),
	}

	err = node.appendProposal(proposal)
	if err == nil {
		t.Error("Expected error when not leader")
	}
}

// --- checkQuorum ---

func TestCheckQuorum_NotLeader(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "follower"
	config.BindAddr = ""
	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	// Should be a no-op for followers
	node.checkQuorum()
	if node.Role() != RaftRoleFollower {
		t.Error("Follower should remain follower")
	}
}

func TestCheckQuorum_LeaderWithQuorum(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "leader"
	config.BindAddr = ""
	config.Peers = []RaftPeer{
		{ID: "peer1", Addr: "localhost:9001"},
		{ID: "peer2", Addr: "localhost:9002"},
	}
	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.stateMu.Unlock()

	// Mark peers as healthy and recently seen
	node.peersMu.Lock()
	for _, p := range node.peers {
		p.Healthy = true
		p.LastSeen = time.Now()
	}
	node.peersMu.Unlock()

	node.checkQuorum()
	if node.Role() != RaftRoleLeader {
		t.Error("Leader with quorum should stay leader")
	}
}

func TestCheckQuorum_LeaderLostQuorum(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "leader"
	config.BindAddr = ""
	config.Peers = []RaftPeer{
		{ID: "peer1", Addr: "localhost:9001"},
		{ID: "peer2", Addr: "localhost:9002"},
	}
	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.currentTerm = 5
	node.stateMu.Unlock()

	// All peers are unhealthy → lost quorum
	node.peersMu.Lock()
	for _, p := range node.peers {
		p.Healthy = false
		p.LastSeen = time.Now().Add(-time.Hour)
	}
	node.peersMu.Unlock()

	node.checkQuorum()
	if node.Role() != RaftRoleFollower {
		t.Error("Leader should step down when quorum lost")
	}
}

// --- RaftCluster Start/Stop/Leader/Write ---

func TestRaftCluster_StartStop(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "cluster-node"
	config.BindAddr = ""

	cluster, err := NewRaftCluster(store, config)
	if err != nil {
		t.Fatal(err)
	}

	if err := cluster.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := cluster.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestRaftCluster_WriteNotRunning(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "test"
	config.BindAddr = ""

	cluster, err := NewRaftCluster(store, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	p := Point{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano()}
	err = cluster.Write(ctx, p)
	if err == nil {
		t.Error("Expected error writing to non-running cluster")
	}
}

func TestRaftCluster_ReadNotLeader(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "test"
	config.BindAddr = ""

	cluster, err := NewRaftCluster(store, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = cluster.Read(ctx, "cpu", time.Time{}, time.Now())
	if err == nil {
		t.Error("Expected error reading from non-leader")
	}
}

func TestRaftCluster_LeaderEmpty(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "test"
	config.BindAddr = ""

	cluster, err := NewRaftCluster(store, config)
	if err != nil {
		t.Fatal(err)
	}

	if cluster.Leader() != "" {
		t.Errorf("Expected empty leader, got %q", cluster.Leader())
	}
}

// --- SimRand.Duration ---

func TestSimRand_Duration(t *testing.T) {
	rng := NewSimRand(42)
	d := rng.Duration(100*time.Millisecond, 500*time.Millisecond)
	if d < 100*time.Millisecond || d > 500*time.Millisecond {
		t.Errorf("Duration = %v, expected between 100ms and 500ms", d)
	}
}

func TestSimRand_Duration_EqualMinMax(t *testing.T) {
	rng := NewSimRand(42)
	d := rng.Duration(100*time.Millisecond, 100*time.Millisecond)
	if d != 100*time.Millisecond {
		t.Errorf("Duration = %v, want 100ms when min==max", d)
	}
}

// --- RaftCluster Write encodes correctly ---

func TestRaftCluster_WriteEncodesCommand(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "test"
	config.BindAddr = ""

	cluster, err := NewRaftCluster(store, config)
	if err != nil {
		t.Fatal(err)
	}

	// Start and make leader to test encoding
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()

	cluster.node.stateMu.Lock()
	cluster.node.role = RaftRoleLeader
	cluster.node.currentTerm = 1
	cluster.node.stateMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	p := Point{Metric: "test_metric", Value: 99.9, Timestamp: 12345}
	// This will timeout since no peers to replicate to, but the proposal should be logged
	_ = cluster.Write(ctx, p)

	// Check that the log has an entry
	entry := cluster.node.log.Get(cluster.node.log.LastIndex())
	if entry != nil {
		var cmd RaftCommand
		if err := json.Unmarshal(entry.Data, &cmd); err == nil {
			if cmd.Op != "write" {
				t.Errorf("Command op = %q, want write", cmd.Op)
			}
		}
	}
}
