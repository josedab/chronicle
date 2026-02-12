package raft

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"time"
)

func (rn *RaftNode) snapshotLoop() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-ticker.C:
			rn.maybeSnapshot()
		}
	}
}

func (rn *RaftNode) maybeSnapshot() {
	if rn.snapshotting.Load() {
		return
	}

	rn.stateMu.RLock()
	lastApplied := rn.lastApplied
	rn.stateMu.RUnlock()

	logLen := uint64(rn.log.Len())
	if logLen < rn.config.SnapshotThreshold {
		return
	}

	rn.snapshotting.Store(true)
	defer rn.snapshotting.Store(false)

	snapshot, err := rn.createSnapshot(lastApplied)
	if err != nil {
		return
	}

	if err := rn.saveSnapshot(snapshot); err != nil {
		return
	}

	rn.log.CompactBefore(lastApplied)

	rn.stateMu.Lock()
	rn.lastSnapshotIndex = lastApplied
	rn.lastSnapshotTerm = rn.log.TermAt(lastApplied)
	rn.stateMu.Unlock()

	rn.notifySnapshot(rn.lastSnapshotIndex, rn.lastSnapshotTerm)
}

func (rn *RaftNode) createSnapshot(lastIndex uint64) (*RaftSnapshot, error) {

	return &RaftSnapshot{
		LastIndex: lastIndex,
		LastTerm:  rn.log.TermAt(lastIndex),
		Data:      []byte{},
	}, nil
}

func (rn *RaftNode) saveSnapshot(snapshot *RaftSnapshot) error {
	if rn.config.DataDir == "" {
		return nil
	}

	path := filepath.Join(rn.config.DataDir, "snapshot.dat")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(snapshot)
}
