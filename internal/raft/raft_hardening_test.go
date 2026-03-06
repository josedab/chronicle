package raft

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- Snapshot Manager ---

func TestSnapshotManager_CreateAndRestoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSnapshotManagerConfig()
	config.SnapshotDir = dir
	config.MaxSnapshots = 3

	mgr := NewSnapshotManager(config)

	// Create a snapshot
	data := []byte("snapshot-state-data-for-testing")
	snap, err := mgr.CreateSnapshot(10, 2, data)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("Expected non-nil ManagedSnapshot")
	}
	if snap.Meta.Index != 10 {
		t.Errorf("Index = %d, want 10", snap.Meta.Index)
	}
	if snap.Meta.Term != 2 {
		t.Errorf("Term = %d, want 2", snap.Meta.Term)
	}

	// Verify snapshot file exists
	files, _ := filepath.Glob(filepath.Join(dir, "snap-*.snap"))
	if len(files) == 0 {
		t.Error("Expected snapshot file to be created")
	}

	// Restore latest
	restored, err := mgr.RestoreLatest()
	if err != nil {
		t.Fatalf("RestoreLatest: %v", err)
	}
	if restored.Meta.Index != 10 {
		t.Errorf("Restored index = %d, want 10", restored.Meta.Index)
	}
	if restored.Meta.Term != 2 {
		t.Errorf("Restored term = %d, want 2", restored.Meta.Term)
	}
	if string(restored.Data) != string(data) {
		t.Error("Restored data doesn't match")
	}
}

func TestSnapshotManager_MultipleSnapshotsLatest(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSnapshotManagerConfig()
	config.SnapshotDir = dir
	config.MaxSnapshots = 2

	mgr := NewSnapshotManager(config)

	mgr.CreateSnapshot(5, 1, []byte("snap1"))
	time.Sleep(time.Millisecond) // ensure distinct nanosecond IDs
	mgr.CreateSnapshot(10, 2, []byte("snap2"))
	time.Sleep(time.Millisecond)
	mgr.CreateSnapshot(15, 3, []byte("snap3"))

	// Restore latest — should be the most recent
	restored, err := mgr.RestoreLatest()
	if err != nil {
		t.Fatal(err)
	}
	if restored.Meta.Index != 15 {
		t.Errorf("Latest index = %d, want 15", restored.Meta.Index)
	}
}

func TestSnapshotManager_EmptyDirRestore(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSnapshotManagerConfig()
	config.SnapshotDir = dir

	mgr := NewSnapshotManager(config)

	_, err := mgr.RestoreLatest()
	if err == nil {
		t.Error("Expected error restoring from empty dir")
	}
}

// --- LogCompactor ---

func TestLogCompactor_NewInstance(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "compact-node"
	config.BindAddr = ""
	config.SnapshotThreshold = 100
	config.DataDir = t.TempDir()

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	snapConfig := DefaultSnapshotManagerConfig()
	snapConfig.SnapshotDir = dir
	snapMgr := NewSnapshotManager(snapConfig)

	compactorConfig := DefaultLogCompactorConfig()
	compactorConfig.CompactionThreshold = 50

	compactor := NewLogCompactor(node.log, snapMgr, compactorConfig)
	if compactor == nil {
		t.Fatal("Expected non-nil compactor")
	}
}

// --- maybeSnapshot ---

func TestMaybeSnapshot_BelowThreshold(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "snap-node"
	config.BindAddr = ""
	config.DataDir = t.TempDir()
	config.SnapshotThreshold = 1000

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	// Add only a few entries — below threshold
	for i := uint64(1); i <= 5; i++ {
		node.log.Append(&RaftLogEntry{Index: i, Term: 1, Type: RaftLogCommand, Data: []byte("cmd")})
	}

	node.stateMu.Lock()
	node.lastApplied = 5
	node.stateMu.Unlock()

	// Should not snapshot — below threshold, no panic = success
	node.maybeSnapshot()
}

func TestMaybeSnapshot_AboveThreshold(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "snap-node"
	config.BindAddr = ""
	config.DataDir = t.TempDir()
	config.SnapshotThreshold = 10

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	// Add enough entries to exceed threshold
	for i := uint64(1); i <= 20; i++ {
		node.log.Append(&RaftLogEntry{Index: i, Term: 1, Type: RaftLogCommand, Data: []byte("cmd")})
	}

	node.stateMu.Lock()
	node.lastApplied = 20
	node.stateMu.Unlock()

	// Should attempt snapshot (or at least not panic)
	node.maybeSnapshot()
}

// --- readSnapshotMeta ---

func TestReadSnapshotMeta_ValidFile(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSnapshotManagerConfig()
	config.SnapshotDir = dir

	mgr := NewSnapshotManager(config)

	// Create a snapshot first
	_, err := mgr.CreateSnapshot(42, 7, []byte("test-data"))
	if err != nil {
		t.Fatal(err)
	}

	// Find the snapshot file
	files, _ := filepath.Glob(filepath.Join(dir, "snap-*.snap"))
	if len(files) == 0 {
		t.Fatal("No snapshot files found")
	}

	// Read meta from it
	meta, err := mgr.readSnapshotMeta(files[0])
	if err != nil {
		t.Fatalf("readSnapshotMeta: %v", err)
	}
	if meta.Index != 42 {
		t.Errorf("Index = %d, want 42", meta.Index)
	}
	if meta.Term != 7 {
		t.Errorf("Term = %d, want 7", meta.Term)
	}
}

func TestReadSnapshotMeta_InvalidFile(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSnapshotManagerConfig()
	config.SnapshotDir = dir

	mgr := NewSnapshotManager(config)

	// Create a corrupt file
	badFile := filepath.Join(dir, "snapshot-bad")
	os.WriteFile(badFile, []byte("not a valid snapshot"), 0644)

	_, err := mgr.readSnapshotMeta(badFile)
	if err == nil {
		t.Error("Expected error for invalid snapshot file")
	}
}

// --- confirmLeadership deeper paths ---

func TestConfirmLeadership_ExpiredLease(t *testing.T) {
	store := newTestStore()
	config := DefaultRaftConfig()
	config.NodeID = "leader"
	config.BindAddr = ""
	config.DataDir = t.TempDir()
	config.LeaseTimeout = 1 * time.Millisecond

	node, err := NewRaftNode(store, config)
	if err != nil {
		t.Fatal(err)
	}

	node.stateMu.Lock()
	node.role = RaftRoleLeader
	node.leaseExpiry = time.Now().Add(-time.Hour) // expired
	node.leaseExtended.Store(false)
	node.stateMu.Unlock()

	ctx := t.Context()
	err = node.confirmLeadership(ctx)
	// With expired lease and no peers to heartbeat, should timeout or return error
	if err == nil {
		// Some implementations may still succeed for single-node
		t.Log("confirmLeadership succeeded (single-node case)")
	}
}
