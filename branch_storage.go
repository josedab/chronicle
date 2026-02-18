package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// BranchStorageConfig configures persistent branch storage.
type BranchStorageConfig struct {
	Enabled          bool          `json:"enabled"`
	MaxSnapshots     int           `json:"max_snapshots"`
	SnapshotInterval time.Duration `json:"snapshot_interval"`
	CompactAfter     int           `json:"compact_after"` // Compact after N commits
}

// DefaultBranchStorageConfig returns sensible defaults.
func DefaultBranchStorageConfig() BranchStorageConfig {
	return BranchStorageConfig{
		Enabled:          true,
		MaxSnapshots:     100,
		SnapshotInterval: 5 * time.Minute,
		CompactAfter:     50,
	}
}

// BranchStorageSnapshot represents a point-in-time snapshot of branch data.
type BranchStorageSnapshot struct {
	ID        string            `json:"id"`
	BranchID  string            `json:"branch_id"`
	CommitID  string            `json:"commit_id"`
	CreatedAt time.Time         `json:"created_at"`
	Points    []Point           `json:"points"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// BranchStorage provides persistent storage for branch data
// with copy-on-write semantics.
type BranchStorage struct {
	config    BranchStorageConfig
	snapshots map[string][]BranchStorageSnapshot // branchID -> snapshots
	data      map[string][]Point          // branchID -> points (working copy)
	mu        sync.RWMutex
}

// NewBranchStorage creates a new branch storage.
func NewBranchStorage(config BranchStorageConfig) *BranchStorage {
	return &BranchStorage{
		config:    config,
		snapshots: make(map[string][]BranchStorageSnapshot),
		data:      make(map[string][]Point),
	}
}

// CreateBranch initializes storage for a new branch, optionally copying
// data from a parent branch (copy-on-write).
func (bs *BranchStorage) CreateBranch(branchID, parentID string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if _, exists := bs.data[branchID]; exists {
		return fmt.Errorf("branch %q already exists", branchID)
	}

	if parentID != "" {
		parentData, exists := bs.data[parentID]
		if !exists {
			// Empty parent is fine - start with empty data
			bs.data[branchID] = make([]Point, 0)
		} else {
			// Copy-on-write: share reference until modified
			copied := make([]Point, len(parentData))
			copy(copied, parentData)
			bs.data[branchID] = copied
		}
	} else {
		bs.data[branchID] = make([]Point, 0)
	}

	return nil
}

// WritePoints writes points to a branch.
func (bs *BranchStorage) WritePoints(branchID string, points []Point) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	data, exists := bs.data[branchID]
	if !exists {
		return fmt.Errorf("branch %q not found", branchID)
	}

	bs.data[branchID] = append(data, points...)
	return nil
}

// ReadPoints reads points from a branch for a specific metric.
func (bs *BranchStorage) ReadPoints(branchID, metric string) []Point {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	data := bs.data[branchID]
	if metric == "" {
		result := make([]Point, len(data))
		copy(result, data)
		return result
	}

	var result []Point
	for _, p := range data {
		if p.Metric == metric {
			result = append(result, p)
		}
	}
	return result
}

// TakeSnapshot creates a snapshot of the current branch state.
func (bs *BranchStorage) TakeSnapshot(branchID, commitID string) (*BranchStorageSnapshot, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	data, exists := bs.data[branchID]
	if !exists {
		return nil, fmt.Errorf("branch %q not found", branchID)
	}

	snapshots := bs.snapshots[branchID]
	if len(snapshots) >= bs.config.MaxSnapshots {
		// Remove oldest
		snapshots = snapshots[1:]
	}

	snapshot := BranchStorageSnapshot{
		ID:        fmt.Sprintf("snap_%s_%d", branchID, time.Now().UnixNano()),
		BranchID:  branchID,
		CommitID:  commitID,
		CreatedAt: time.Now(),
		Points:    make([]Point, len(data)),
	}
	copy(snapshot.Points, data)

	bs.snapshots[branchID] = append(snapshots, snapshot)
	return &snapshot, nil
}

// RestoreSnapshot restores a branch to a specific snapshot.
func (bs *BranchStorage) RestoreSnapshot(branchID, snapshotID string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	snapshots := bs.snapshots[branchID]
	for _, snap := range snapshots {
		if snap.ID == snapshotID {
			restored := make([]Point, len(snap.Points))
			copy(restored, snap.Points)
			bs.data[branchID] = restored
			return nil
		}
	}
	return fmt.Errorf("snapshot %q not found in branch %q", snapshotID, branchID)
}

// ListSnapshots returns all snapshots for a branch.
func (bs *BranchStorage) ListSnapshots(branchID string) []BranchStorageSnapshot {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	snapshots := bs.snapshots[branchID]
	result := make([]BranchStorageSnapshot, len(snapshots))
	for i, s := range snapshots {
		result[i] = BranchStorageSnapshot{
			ID:        s.ID,
			BranchID:  s.BranchID,
			CommitID:  s.CommitID,
			CreatedAt: s.CreatedAt,
			Metadata:  s.Metadata,
		}
	}
	return result
}

// DeleteBranch removes branch data.
func (bs *BranchStorage) DeleteBranch(branchID string) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	delete(bs.data, branchID)
	delete(bs.snapshots, branchID)
}

// DiffBranches computes the data difference between two branches.
func (bs *BranchStorage) DiffBranches(branchA, branchB string) BranchDataDiff {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	dataA := bs.data[branchA]
	dataB := bs.data[branchB]

	diff := BranchDataDiff{
		BranchA: branchA,
		BranchB: branchB,
	}

	// Build index of metrics in each branch
	metricsA := make(map[string]int)
	metricsB := make(map[string]int)
	for _, p := range dataA {
		metricsA[p.Metric]++
	}
	for _, p := range dataB {
		metricsB[p.Metric]++
	}

	// Find added (in B but not A)
	for m := range metricsB {
		if _, exists := metricsA[m]; !exists {
			diff.AddedSeries = append(diff.AddedSeries, m)
		}
	}

	// Find removed (in A but not B)
	for m := range metricsA {
		if _, exists := metricsB[m]; !exists {
			diff.RemovedSeries = append(diff.RemovedSeries, m)
		}
	}

	// Find changed (in both but different counts)
	for m, countA := range metricsA {
		if countB, exists := metricsB[m]; exists && countA != countB {
			diff.ChangedSeries = append(diff.ChangedSeries, m)
		}
	}

	diff.TotalPointsA = len(dataA)
	diff.TotalPointsB = len(dataB)

	return diff
}

// BranchDataDiff represents data differences between two branches.
type BranchDataDiff struct {
	BranchA       string   `json:"branch_a"`
	BranchB       string   `json:"branch_b"`
	AddedSeries   []string `json:"added_series"`
	RemovedSeries []string `json:"removed_series"`
	ChangedSeries []string `json:"changed_series"`
	TotalPointsA  int      `json:"total_points_a"`
	TotalPointsB  int      `json:"total_points_b"`
}

// Stats returns branch storage statistics.
func (bs *BranchStorage) Stats() map[string]any {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	totalPoints := 0
	totalSnapshots := 0
	for _, data := range bs.data {
		totalPoints += len(data)
	}
	for _, snaps := range bs.snapshots {
		totalSnapshots += len(snaps)
	}

	return map[string]any{
		"branch_count":    len(bs.data),
		"total_points":    totalPoints,
		"total_snapshots": totalSnapshots,
	}
}

// RegisterHTTPHandlers registers branch storage HTTP endpoints.
func (bs *BranchStorage) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/branches/storage/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bs.Stats())
	})
}
