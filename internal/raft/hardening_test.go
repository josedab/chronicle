package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"
)

func TestSnapshotManager_CreateAndRestore(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultSnapshotManagerConfig()
	cfg.SnapshotDir = dir

	sm := NewSnapshotManager(cfg)

	data := []byte("state-machine-data-v1")
	snap, err := sm.CreateSnapshot(10, 2, data)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.Meta.Index != 10 {
		t.Errorf("expected Index=10, got %d", snap.Meta.Index)
	}
	if snap.Meta.Term != 2 {
		t.Errorf("expected Term=2, got %d", snap.Meta.Term)
	}
	if !bytes.Equal(snap.Data, data) {
		t.Errorf("snapshot data mismatch")
	}

	t.Run("RestoreLatest", func(t *testing.T) {
		restored, err := sm.RestoreLatest()
		if err != nil {
			t.Fatalf("RestoreLatest: %v", err)
		}
		if !bytes.Equal(restored.Data, data) {
			t.Errorf("restored data mismatch")
		}
		if restored.Meta.Index != 10 {
			t.Errorf("expected restored Index=10, got %d", restored.Meta.Index)
		}
	})

	t.Run("MultipleSnapshots", func(t *testing.T) {
		_, err := sm.CreateSnapshot(20, 3, []byte("state-v2"))
		if err != nil {
			t.Fatalf("CreateSnapshot 2: %v", err)
		}
		list := sm.ListSnapshots()
		if len(list) < 2 {
			t.Fatalf("expected at least 2 snapshots, got %d", len(list))
		}
		if list[0].Index != 20 {
			t.Errorf("expected newest snapshot Index=20, got %d", list[0].Index)
		}
	})
}

func TestSnapshotManager_PruneSnapshots(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultSnapshotManagerConfig()
	cfg.SnapshotDir = dir
	cfg.MaxSnapshots = 2

	sm := NewSnapshotManager(cfg)

	for i := uint64(1); i <= 5; i++ {
		_, err := sm.CreateSnapshot(i*10, i, []byte("data"))
		if err != nil {
			t.Fatalf("CreateSnapshot %d: %v", i, err)
		}
	}

	if len(sm.ListSnapshots()) != 5 {
		t.Fatalf("expected 5 snapshots before prune, got %d", len(sm.ListSnapshots()))
	}

	sm.PruneSnapshots()

	list := sm.ListSnapshots()
	if len(list) != 2 {
		t.Fatalf("expected 2 snapshots after prune, got %d", len(list))
	}
	if list[0].Index != 50 {
		t.Errorf("expected newest snapshot Index=50, got %d", list[0].Index)
	}
}

func TestSnapshotTransfer_ChunkSerialization(t *testing.T) {
	cfg := DefaultSnapshotTransferConfig()
	cfg.ChunkSize = 16

	st := NewSnapshotTransfer(cfg)
	_ = st // verify it creates without panic

	chunk := SnapshotChunk{
		SnapshotID:  "test-snap",
		ChunkIndex:  0,
		TotalChunks: 1,
		Data:        []byte("hello-chunk"),
		Done:        true,
		Meta:        &SnapshotMeta{ID: "test-snap", Index: 5, Term: 1},
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(chunk); err != nil {
		t.Fatalf("encode chunk: %v", err)
	}

	var decoded SnapshotChunk
	if err := gob.NewDecoder(&buf).Decode(&decoded); err != nil {
		t.Fatalf("decode chunk: %v", err)
	}

	if decoded.SnapshotID != "test-snap" {
		t.Errorf("expected SnapshotID=test-snap, got %s", decoded.SnapshotID)
	}
	if !bytes.Equal(decoded.Data, []byte("hello-chunk")) {
		t.Errorf("chunk data mismatch")
	}
	if decoded.Meta == nil || decoded.Meta.Index != 5 {
		t.Errorf("expected Meta.Index=5")
	}
}

func TestLogCompactor_Compact(t *testing.T) {
	dir := t.TempDir()
	raftLog, err := NewRaftLog(dir + "/wal")
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}

	// Append entries to the log.
	for i := 0; i < 100; i++ {
		raftLog.Append(&RaftLogEntry{
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte("entry"),
		})
	}

	snapDir := t.TempDir()
	snapCfg := DefaultSnapshotManagerConfig()
	snapCfg.SnapshotDir = snapDir
	snapMgr := NewSnapshotManager(snapCfg)

	_, err = snapMgr.CreateSnapshot(50, 1, []byte("state"))
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	lcCfg := DefaultLogCompactorConfig()
	lcCfg.CompactionThreshold = 10
	lcCfg.MinRetainedEntries = 5
	lc := NewLogCompactor(raftLog, snapMgr, lcCfg)

	result, err := lc.Compact(50)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.EntriesRemoved == 0 {
		t.Errorf("expected some entries to be removed")
	}
}

func TestJointConsensus_ProposeAndCommit(t *testing.T) {
	cfg := DefaultJointConsensusConfig()
	jc := NewJointConsensus(cfg)

	transition, err := jc.ProposeChange(MembershipChange{
		Type: MembershipAddNode,
		Node: RaftPeer{ID: "node-1", Addr: "localhost:9001"},
	})
	if err != nil {
		t.Fatalf("ProposeChange: %v", err)
	}
	if transition.Phase != JointPhaseJoint {
		t.Errorf("expected JointPhaseJoint, got %d", transition.Phase)
	}
	if transition.Status != TransitionPending {
		t.Errorf("expected TransitionPending, got %d", transition.Status)
	}

	if err := jc.CommitTransition(transition.ID); err != nil {
		t.Fatalf("CommitTransition: %v", err)
	}
	if transition.Status != TransitionCommitted {
		t.Errorf("expected TransitionCommitted, got %d", transition.Status)
	}

	cfg2 := jc.GetConfiguration()
	found := false
	for _, v := range cfg2.Voters {
		if v.ID == "node-1" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("node-1 should be in voters after commit")
	}
}

func TestJointConsensus_AbortTransition(t *testing.T) {
	cfg := DefaultJointConsensusConfig()
	jc := NewJointConsensus(cfg)

	// Add a node first.
	trans, err := jc.ProposeChange(MembershipChange{
		Type: MembershipAddNode,
		Node: RaftPeer{ID: "node-A", Addr: "localhost:8001"},
	})
	if err != nil {
		t.Fatalf("ProposeChange: %v", err)
	}
	if err := jc.CommitTransition(trans.ID); err != nil {
		t.Fatalf("CommitTransition: %v", err)
	}

	// Now propose adding another node, but abort.
	trans2, err := jc.ProposeChange(MembershipChange{
		Type: MembershipAddNode,
		Node: RaftPeer{ID: "node-B", Addr: "localhost:8002"},
	})
	if err != nil {
		t.Fatalf("ProposeChange 2: %v", err)
	}

	if err := jc.AbortTransition(trans2.ID); err != nil {
		t.Fatalf("AbortTransition: %v", err)
	}
	if trans2.Status != TransitionAborted {
		t.Errorf("expected TransitionAborted, got %d", trans2.Status)
	}

	// node-B should NOT be in voters.
	cfgResult := jc.GetConfiguration()
	for _, v := range cfgResult.Voters {
		if v.ID == "node-B" {
			t.Errorf("node-B should not be in voters after abort")
		}
	}
}

func TestConsensusVerifier_ElectionSafety(t *testing.T) {
	cv := NewConsensusVerifier()

	cv.RecordEvent(ConsensusEvent{
		Type: EventElection, NodeID: "node-1", Term: 1, Timestamp: time.Now(),
	})
	cv.RecordEvent(ConsensusEvent{
		Type: EventElection, NodeID: "node-2", Term: 2, Timestamp: time.Now(),
	})

	if err := cv.VerifyElectionSafety(); err != nil {
		t.Fatalf("unexpected safety violation: %v", err)
	}

	t.Run("Violation", func(t *testing.T) {
		cv2 := NewConsensusVerifier()
		cv2.RecordEvent(ConsensusEvent{
			Type: EventElection, NodeID: "node-1", Term: 1, Timestamp: time.Now(),
		})
		cv2.RecordEvent(ConsensusEvent{
			Type: EventElection, NodeID: "node-2", Term: 1, Timestamp: time.Now(),
		})
		if err := cv2.VerifyElectionSafety(); err == nil {
			t.Errorf("expected election safety violation for two leaders in same term")
		}
	})
}

func TestConsensusVerifier_LogMatching(t *testing.T) {
	cv := NewConsensusVerifier()

	// Two nodes with matching log entries.
	for i := uint64(1); i <= 5; i++ {
		cv.RecordEvent(ConsensusEvent{
			Type: EventAppend, NodeID: "node-1", Term: 1, Index: i, Timestamp: time.Now(),
		})
		cv.RecordEvent(ConsensusEvent{
			Type: EventAppend, NodeID: "node-2", Term: 1, Index: i, Timestamp: time.Now(),
		})
	}

	if err := cv.VerifyLogMatching(); err != nil {
		t.Fatalf("unexpected log matching violation: %v", err)
	}

	t.Run("Violation", func(t *testing.T) {
		cv2 := NewConsensusVerifier()
		cv2.RecordEvent(ConsensusEvent{
			Type: EventAppend, NodeID: "node-1", Term: 1, Index: 1, Timestamp: time.Now(),
		})
		cv2.RecordEvent(ConsensusEvent{
			Type: EventAppend, NodeID: "node-2", Term: 2, Index: 1, Timestamp: time.Now(),
		})
		cv2.RecordEvent(ConsensusEvent{
			Type: EventAppend, NodeID: "node-1", Term: 1, Index: 2, Timestamp: time.Now(),
		})
		cv2.RecordEvent(ConsensusEvent{
			Type: EventAppend, NodeID: "node-2", Term: 1, Index: 2, Timestamp: time.Now(),
		})
		if err := cv2.VerifyLogMatching(); err == nil {
			t.Errorf("expected log matching violation")
		}
	})
}
