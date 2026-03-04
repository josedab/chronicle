// branching_merge.go contains extended branching functionality.
package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// MergeOption is a functional option that configures the behavior of a branch merge operation.
type MergeOption func(*mergeOptions)

type mergeOptions struct {
	strategy BranchMergeStrategy
	message  string
}

// WithMergeStrategy sets the merge strategy.
func WithMergeStrategy(strategy BranchMergeStrategy) MergeOption {
	return func(o *mergeOptions) {
		o.strategy = strategy
	}
}

// WithMergeMessage sets the merge commit message.
func WithMergeMessage(message string) MergeOption {
	return func(o *mergeOptions) {
		o.message = message
	}
}

func (bm *BranchManager) detectConflicts(source, target *Branch) []MergeConflict {
	var conflicts []MergeConflict

	// Build a map of target changes
	targetChanges := make(map[string]Point)
	for _, p := range target.delta.Adds {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		targetChanges[key] = p
	}

	// Check source changes against target
	for _, p := range source.delta.Adds {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		if targetPoint, exists := targetChanges[key]; exists {
			// Conflict: same metric+timestamp modified in both branches
			if p.Value != targetPoint.Value {
				conflicts = append(conflicts, MergeConflict{
					Metric:      p.Metric,
					Timestamp:   p.Timestamp,
					SourcePoint: p,
					TargetPoint: targetPoint,
				})
			}
		}
	}

	return conflicts
}

func isConflict(point Point, conflicts []MergeConflict) bool {
	for _, c := range conflicts {
		if c.Metric == point.Metric && c.Timestamp == point.Timestamp {
			return true
		}
	}
	return false
}

// CreateSnapshot creates a point-in-time snapshot of the current branch.
func (bm *BranchManager) CreateSnapshot(description string) (*BranchSnapshot, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if len(bm.snapshots) >= bm.config.MaxSnapshots {
		return nil, fmt.Errorf("maximum number of snapshots (%d) reached", bm.config.MaxSnapshots)
	}

	branch := bm.branches[bm.current]
	if branch == nil {
		return nil, errors.New("no current branch")
	}

	// Generate snapshot ID
	snapshotID := fmt.Sprintf("%s-%d", bm.current, time.Now().UnixNano())

	// Collect metrics and data
	metricsSet := make(map[string]struct{})
	var data []Point
	var totalSize int64

	for _, p := range branch.delta.Adds {
		metricsSet[p.Metric] = struct{}{}
		data = append(data, p)
		totalSize += int64(len(p.Metric) + 8 + 8) // rough estimate
	}

	metrics := make([]string, 0, len(metricsSet))
	for m := range metricsSet {
		metrics = append(metrics, m)
	}

	snapshot := &BranchSnapshot{
		ID:          snapshotID,
		Branch:      bm.current,
		Version:     branch.HeadVersion,
		CreatedAt:   time.Now(),
		Description: description,
		Metrics:     metrics,
		PointCount:  int64(len(data)),
		SizeBytes:   totalSize,
		data:        data,
	}

	bm.snapshots[snapshotID] = snapshot
	return snapshot, nil
}

// RestoreSnapshot restores a snapshot to the current branch.
func (bm *BranchManager) RestoreSnapshot(snapshotID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	snapshot, ok := bm.snapshots[snapshotID]
	if !ok {
		return fmt.Errorf("snapshot %q not found", snapshotID)
	}

	branch := bm.branches[bm.current]
	if branch == nil {
		return errors.New("no current branch")
	}

	if branch.Protected {
		return fmt.Errorf("cannot restore to protected branch %q", bm.current)
	}

	// Reset branch delta to snapshot state
	branch.delta = &BranchDelta{
		Adds: make([]Point, len(snapshot.data)),
	}
	copy(branch.delta.Adds, snapshot.data)
	branch.HeadVersion = snapshot.Version
	branch.UpdatedAt = time.Now()

	return nil
}

// ListSnapshots returns all snapshots for a branch.
func (bm *BranchManager) ListSnapshots(branch string) []*BranchSnapshot {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var snapshots []*BranchSnapshot
	for _, s := range bm.snapshots {
		if branch == "" || s.Branch == branch {
			snapshots = append(snapshots, s)
		}
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].CreatedAt.After(snapshots[j].CreatedAt)
	})

	return snapshots
}

// DeleteSnapshot deletes a snapshot.
func (bm *BranchManager) DeleteSnapshot(snapshotID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, ok := bm.snapshots[snapshotID]; !ok {
		return fmt.Errorf("snapshot %q not found", snapshotID)
	}

	delete(bm.snapshots, snapshotID)
	return nil
}

// Diff returns the differences between two branches.
func (bm *BranchManager) Diff(branchA, branchB string) (*BranchDiff, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	a, ok := bm.branches[branchA]
	if !ok {
		return nil, fmt.Errorf("branch %q not found", branchA)
	}

	b, ok := bm.branches[branchB]
	if !ok {
		return nil, fmt.Errorf("branch %q not found", branchB)
	}

	diff := &BranchDiff{
		BranchA: branchA,
		BranchB: branchB,
	}

	// Build maps of changes
	aChanges := make(map[string]Point)
	for _, p := range a.delta.Adds {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		aChanges[key] = p
	}

	bChanges := make(map[string]Point)
	for _, p := range b.delta.Adds {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		bChanges[key] = p
	}

	// Find differences
	for key, pa := range aChanges {
		if pb, exists := bChanges[key]; exists {
			if pa.Value != pb.Value {
				diff.Modified = append(diff.Modified, BranchPointDiff{
					Key:    key,
					ValueA: pa.Value,
					ValueB: pb.Value,
				})
			}
		} else {
			diff.OnlyInA = append(diff.OnlyInA, pa)
		}
	}

	for key, pb := range bChanges {
		if _, exists := aChanges[key]; !exists {
			diff.OnlyInB = append(diff.OnlyInB, pb)
		}
	}

	diff.TotalDiffs = len(diff.Modified) + len(diff.OnlyInA) + len(diff.OnlyInB)

	return diff, nil
}

// BranchDiff represents differences between two branches.
type BranchDiff struct {
	BranchA    string            `json:"branch_a"`
	BranchB    string            `json:"branch_b"`
	OnlyInA    []Point           `json:"only_in_a,omitempty"`
	OnlyInB    []Point           `json:"only_in_b,omitempty"`
	Modified   []BranchPointDiff `json:"modified,omitempty"`
	TotalDiffs int               `json:"total_diffs"`
}

// PointDiff represents a difference in a single point.
type BranchPointDiff struct {
	Key    string  `json:"key"`
	ValueA float64 `json:"value_a"`
	ValueB float64 `json:"value_b"`
}

// Commit finalizes changes on the current branch and writes to the underlying database.
func (bm *BranchManager) Commit(message string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch := bm.branches[bm.current]
	if branch == nil {
		return errors.New("no current branch")
	}

	if bm.current != "main" {
		return errors.New("can only commit on main branch")
	}

	// Write all delta adds to the database
	for _, p := range branch.delta.Adds {
		if err := bm.db.Write(p); err != nil {
			return fmt.Errorf("failed to write point: %w", err)
		}
	}

	// Clear delta
	branch.delta = &BranchDelta{}
	branch.HeadVersion++
	branch.UpdatedAt = time.Now()

	return bm.db.Flush()
}

// Export returns the branch state as JSON for persistence.
func (bm *BranchManager) Export() ([]byte, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	state := struct {
		Current   string                     `json:"current"`
		Branches  map[string]*Branch         `json:"branches"`
		Snapshots map[string]*BranchSnapshot `json:"snapshots"`
	}{
		Current:   bm.current,
		Branches:  bm.branches,
		Snapshots: bm.snapshots,
	}

	return json.Marshal(state)
}

// Import restores branch state from JSON.
func (bm *BranchManager) Import(data []byte) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	state := struct {
		Current   string                     `json:"current"`
		Branches  map[string]*Branch         `json:"branches"`
		Snapshots map[string]*BranchSnapshot `json:"snapshots"`
	}{}

	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	bm.current = state.Current
	bm.branches = state.Branches
	bm.snapshots = state.Snapshots

	// Ensure delta is initialized for all branches
	for _, b := range bm.branches {
		if b.delta == nil {
			b.delta = &BranchDelta{}
		}
	}

	return nil
}

// Rebase rebases the current branch onto another branch.
func (bm *BranchManager) Rebase(ontoBranch string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	onto, ok := bm.branches[ontoBranch]
	if !ok {
		return fmt.Errorf("branch %q not found", ontoBranch)
	}

	current := bm.branches[bm.current]
	if current == nil {
		return errors.New("no current branch")
	}

	if current.Protected {
		return fmt.Errorf("cannot rebase protected branch %q", bm.current)
	}

	// Update parent and version
	current.Parent = ontoBranch
	current.HeadVersion = onto.HeadVersion + 1
	current.UpdatedAt = time.Now()

	return nil
}

// Tag adds a tag to a branch.
func (bm *BranchManager) Tag(branchName, tag string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch, ok := bm.branches[branchName]
	if !ok {
		return fmt.Errorf("branch %q not found", branchName)
	}

	// Check for duplicate tag
	for _, t := range branch.Tags {
		if t == tag {
			return nil
		}
	}

	branch.Tags = append(branch.Tags, tag)
	return nil
}

// Untag removes a tag from a branch.
func (bm *BranchManager) Untag(branchName, tag string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch, ok := bm.branches[branchName]
	if !ok {
		return fmt.Errorf("branch %q not found", branchName)
	}

	newTags := make([]string, 0, len(branch.Tags))
	for _, t := range branch.Tags {
		if t != tag {
			newTags = append(newTags, t)
		}
	}
	branch.Tags = newTags
	return nil
}

// Log returns a history of branch operations.
func (bm *BranchManager) Log(branchName string, limit int) []BranchLogEntry {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	branch, ok := bm.branches[branchName]
	if !ok {
		return nil
	}

	// Build log from snapshots and branch metadata
	var entries []BranchLogEntry

	// Add branch creation
	entries = append(entries, BranchLogEntry{
		Type:      "create",
		Branch:    branchName,
		Timestamp: branch.CreatedAt,
		Message:   "Branch created",
	})

	// Add snapshots
	for _, s := range bm.snapshots {
		if s.Branch == branchName {
			entries = append(entries, BranchLogEntry{
				Type:      "snapshot",
				Branch:    branchName,
				Timestamp: s.CreatedAt,
				Message:   s.Description,
				Version:   s.Version,
			})
		}
	}

	// Sort by timestamp descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp.After(entries[j].Timestamp)
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries
}

// BranchLogEntry represents a log entry for branch history.
type BranchLogEntry struct {
	Type      string    `json:"type"`
	Branch    string    `json:"branch"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Version   int64     `json:"version,omitempty"`
}
