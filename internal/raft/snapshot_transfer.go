package raft

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// SnapshotTransferManagerConfig configures the SnapshotTransferManager.
type SnapshotTransferManagerConfig struct {
	// MaxSnapshots is the maximum number of snapshots to retain in memory.
	MaxSnapshots int
}

// DefaultSnapshotTransferManagerConfig returns sensible defaults.
func DefaultSnapshotTransferManagerConfig() SnapshotTransferManagerConfig {
	return SnapshotTransferManagerConfig{
		MaxSnapshots: 5,
	}
}

// SnapshotData holds a point-in-time snapshot of the state machine.
type SnapshotData struct {
	// Index is the last log index included in the snapshot.
	Index uint64

	// Term is the term of the last log entry included.
	Term uint64

	// Config holds serialised cluster configuration at snapshot time.
	Config []byte

	// Data is the raw state-machine data.
	Data []byte

	// Checksum is the CRC-32 (IEEE) checksum of Data.
	Checksum uint32
}

// SnapshotTransferManager manages creation, storage and restoration of
// in-memory snapshots. It is safe for concurrent use.
type SnapshotTransferManager struct {
	config    SnapshotTransferManagerConfig
	snapshots []SnapshotData
	mu        sync.RWMutex
}

// NewSnapshotTransferManager creates a new SnapshotTransferManager.
func NewSnapshotTransferManager(config SnapshotTransferManagerConfig) *SnapshotTransferManager {
	if config.MaxSnapshots <= 0 {
		config.MaxSnapshots = 5
	}
	return &SnapshotTransferManager{
		config:    config,
		snapshots: make([]SnapshotData, 0),
	}
}

// CreateSnapshot creates a new snapshot with a CRC-32 checksum over the
// supplied data and stores it in the manager. Snapshots are kept sorted
// by descending index so that the most recent snapshot is always first.
func (m *SnapshotTransferManager) CreateSnapshot(index, term uint64, data []byte) (*SnapshotData, error) {
	if data == nil {
		return nil, fmt.Errorf("snapshot data must not be nil")
	}

	snap := SnapshotData{
		Index:    index,
		Term:     term,
		Data:     make([]byte, len(data)),
		Checksum: crc32.ChecksumIEEE(data),
	}
	copy(snap.Data, data)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.snapshots = append(m.snapshots, snap)

	// Keep sorted newest-first by index.
	sort.Slice(m.snapshots, func(i, j int) bool {
		return m.snapshots[i].Index > m.snapshots[j].Index
	})

	return &snap, nil
}

// RestoreSnapshot validates the checksum of the supplied snapshot. It returns
// an error if the checksum does not match, indicating data corruption.
func (m *SnapshotTransferManager) RestoreSnapshot(snap *SnapshotData) error {
	if snap == nil {
		return fmt.Errorf("snapshot must not be nil")
	}

	computed := crc32.ChecksumIEEE(snap.Data)
	if computed != snap.Checksum {
		return fmt.Errorf("snapshot checksum mismatch: expected %d, got %d", snap.Checksum, computed)
	}

	return nil
}

// ListSnapshots returns a copy of all available snapshots, ordered from
// newest to oldest by index.
func (m *SnapshotTransferManager) ListSnapshots() []SnapshotData {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]SnapshotData, len(m.snapshots))
	copy(out, m.snapshots)
	return out
}

// PruneSnapshots keeps only the keep most recent snapshots and discards the
// rest. If keep is less than 1, no pruning is performed.
func (m *SnapshotTransferManager) PruneSnapshots(keep int) {
	if keep < 1 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.snapshots) <= keep {
		return
	}

	m.snapshots = m.snapshots[:keep]
}
