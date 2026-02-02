package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// BranchConfig configures branching behavior.
type BranchConfig struct {
	// MaxBranches limits the number of branches
	MaxBranches int `json:"max_branches"`

	// MaxSnapshots limits snapshots per branch
	MaxSnapshots int `json:"max_snapshots"`

	// AutoSnapshot enables automatic snapshots on writes
	AutoSnapshot bool `json:"auto_snapshot"`

	// SnapshotInterval is the minimum interval between auto-snapshots
	SnapshotInterval time.Duration `json:"snapshot_interval"`

	// RetainSnapshots is how long to keep old snapshots
	RetainSnapshots time.Duration `json:"retain_snapshots"`
}

// DefaultBranchConfig returns sensible defaults.
func DefaultBranchConfig() BranchConfig {
	return BranchConfig{
		MaxBranches:      100,
		MaxSnapshots:     1000,
		AutoSnapshot:     false,
		SnapshotInterval: time.Hour,
		RetainSnapshots:  7 * 24 * time.Hour,
	}
}

// BranchManager provides Git-like branching semantics for time-series data.
// This enables "what-if" analysis, safe experimentation, and CI/CD for data.
type BranchManager struct {
	db        *DB
	config    BranchConfig
	branches  map[string]*Branch
	snapshots map[string]*BranchSnapshot
	mu        sync.RWMutex
	current   string // current branch name
}

// Branch represents a named branch of data.
type Branch struct {
	Name        string    `json:"name"`
	Parent      string    `json:"parent,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	HeadVersion int64     `json:"head_version"`
	Description string    `json:"description,omitempty"`
	Protected   bool      `json:"protected"`
	Tags        []string  `json:"tags,omitempty"`

	// Delta stores changes from parent branch
	delta *BranchDelta
}

// BranchDelta stores changes made on a branch relative to its parent.
type BranchDelta struct {
	Adds    []Point `json:"adds"`
	Deletes []Point `json:"deletes"`
}

// Snapshot represents a point-in-time snapshot of data.
type BranchSnapshot struct {
	ID          string    `json:"id"`
	Branch      string    `json:"branch"`
	Version     int64     `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	Description string    `json:"description,omitempty"`
	Metrics     []string  `json:"metrics"`
	PointCount  int64     `json:"point_count"`
	SizeBytes   int64     `json:"size_bytes"`

	// data holds the actual snapshot data
	data []Point
}

// NewBranchManager creates a new branch manager.
func NewBranchManager(db *DB, config BranchConfig) *BranchManager {
	if config.MaxBranches <= 0 {
		config.MaxBranches = 100
	}
	if config.MaxSnapshots <= 0 {
		config.MaxSnapshots = 1000
	}

	bm := &BranchManager{
		db:        db,
		config:    config,
		branches:  make(map[string]*Branch),
		snapshots: make(map[string]*BranchSnapshot),
		current:   "main",
	}

	// Create default main branch
	bm.branches["main"] = &Branch{
		Name:        "main",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		HeadVersion: 0,
		Protected:   true,
		delta:       &BranchDelta{},
	}

	return bm
}

// CreateBranch creates a new branch from the current branch or a specific snapshot.
func (bm *BranchManager) CreateBranch(name string, opts ...BranchOption) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, exists := bm.branches[name]; exists {
		return fmt.Errorf("branch %q already exists", name)
	}

	if len(bm.branches) >= bm.config.MaxBranches {
		return fmt.Errorf("maximum number of branches (%d) reached", bm.config.MaxBranches)
	}

	// Apply options
	options := &branchOptions{
		parent: bm.current,
	}
	for _, opt := range opts {
		opt(options)
	}

	// Verify parent exists
	parent, ok := bm.branches[options.parent]
	if !ok {
		return fmt.Errorf("parent branch %q not found", options.parent)
	}

	branch := &Branch{
		Name:        name,
		Parent:      options.parent,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		HeadVersion: parent.HeadVersion,
		Description: options.description,
		delta:       &BranchDelta{},
	}

	bm.branches[name] = branch
	return nil
}

// BranchOption configures branch creation.
type BranchOption func(*branchOptions)

type branchOptions struct {
	parent      string
	description string
	fromVersion int64
}

// WithParent sets the parent branch.
func WithParent(parent string) BranchOption {
	return func(o *branchOptions) {
		o.parent = parent
	}
}

// WithDescription sets the branch description.
func WithDescription(desc string) BranchOption {
	return func(o *branchOptions) {
		o.description = desc
	}
}

// WithVersion creates a branch from a specific version.
func WithVersion(version int64) BranchOption {
	return func(o *branchOptions) {
		o.fromVersion = version
	}
}

// DeleteBranch deletes a branch.
func (bm *BranchManager) DeleteBranch(name string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch, ok := bm.branches[name]
	if !ok {
		return fmt.Errorf("branch %q not found", name)
	}

	if branch.Protected {
		return fmt.Errorf("cannot delete protected branch %q", name)
	}

	if bm.current == name {
		return fmt.Errorf("cannot delete current branch")
	}

	// Check if any branches have this as parent
	for _, b := range bm.branches {
		if b.Parent == name {
			return fmt.Errorf("branch %q has children, delete them first", name)
		}
	}

	delete(bm.branches, name)
	return nil
}

// Checkout switches to a different branch.
func (bm *BranchManager) Checkout(name string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, ok := bm.branches[name]; !ok {
		return fmt.Errorf("branch %q not found", name)
	}

	bm.current = name
	return nil
}

// CurrentBranch returns the current branch name.
func (bm *BranchManager) CurrentBranch() string {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.current
}

// ListBranches returns all branches.
func (bm *BranchManager) ListBranches() []*Branch {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	branches := make([]*Branch, 0, len(bm.branches))
	for _, b := range bm.branches {
		branches = append(branches, b)
	}

	sort.Slice(branches, func(i, j int) bool {
		return branches[i].Name < branches[j].Name
	})

	return branches
}

// GetBranch returns a specific branch.
func (bm *BranchManager) GetBranch(name string) (*Branch, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	b, ok := bm.branches[name]
	if !ok {
		return nil, fmt.Errorf("branch %q not found", name)
	}
	return b, nil
}

// Write writes a point to the current branch.
func (bm *BranchManager) Write(point Point) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch := bm.branches[bm.current]
	if branch == nil {
		return errors.New("no current branch")
	}

	// Add to branch delta
	branch.delta.Adds = append(branch.delta.Adds, point)
	branch.HeadVersion++
	branch.UpdatedAt = time.Now()

	return nil
}

// Query executes a query against the current branch.
func (bm *BranchManager) Query(q *Query) (*Result, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	branch := bm.branches[bm.current]
	if branch == nil {
		return nil, errors.New("no current branch")
	}

	// First query the underlying database
	result, err := bm.db.Execute(q)
	if err != nil {
		return nil, err
	}

	// Apply branch deltas (walk up the parent chain)
	result = bm.applyBranchDeltas(branch, result)

	return result, nil
}

// applyBranchDeltas applies branch changes to a query result.
func (bm *BranchManager) applyBranchDeltas(branch *Branch, result *Result) *Result {
	if branch == nil {
		return result
	}

	// Apply parent deltas first
	if branch.Parent != "" {
		if parent, ok := bm.branches[branch.Parent]; ok {
			result = bm.applyBranchDeltas(parent, result)
		}
	}

	// Apply this branch's deltas
	if branch.delta != nil {
		// Remove deleted points
		for _, del := range branch.delta.Deletes {
			result.Points = removePoint(result.Points, del)
		}

		// Add new points
		result.Points = append(result.Points, branch.delta.Adds...)

		// Re-sort by timestamp
		sort.Slice(result.Points, func(i, j int) bool {
			return result.Points[i].Timestamp < result.Points[j].Timestamp
		})
	}

	return result
}

func removePoint(points []Point, toRemove Point) []Point {
	result := make([]Point, 0, len(points))
	for _, p := range points {
		if p.Metric == toRemove.Metric && p.Timestamp == toRemove.Timestamp {
			continue
		}
		result = append(result, p)
	}
	return result
}

// Merge merges a branch into the current branch.
func (bm *BranchManager) Merge(sourceBranch string, opts ...MergeOption) (*MergeResult, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	source, ok := bm.branches[sourceBranch]
	if !ok {
		return nil, fmt.Errorf("source branch %q not found", sourceBranch)
	}

	target := bm.branches[bm.current]
	if target == nil {
		return nil, errors.New("no current branch")
	}

	if target.Protected {
		return nil, fmt.Errorf("cannot merge into protected branch %q", bm.current)
	}

	options := &mergeOptions{
		strategy: BranchMergeStrategyOurs,
	}
	for _, opt := range opts {
		opt(options)
	}

	result := &MergeResult{
		SourceBranch: sourceBranch,
		TargetBranch: bm.current,
		StartedAt:    time.Now(),
		Strategy:     options.strategy,
	}

	// Detect conflicts
	conflicts := bm.detectConflicts(source, target)
	result.Conflicts = conflicts

	if len(conflicts) > 0 && options.strategy != BranchMergeStrategyForce {
		if options.strategy == BranchMergeStrategyOurs {
			// Keep target's version (do nothing for conflicts)
		} else if options.strategy == BranchMergeStrategyTheirs {
			// Use source's version for conflicts
			for _, c := range conflicts {
				target.delta.Adds = append(target.delta.Adds, c.SourcePoint)
			}
		}
	}

	// Merge non-conflicting changes
	for _, add := range source.delta.Adds {
		if !isConflict(add, conflicts) {
			target.delta.Adds = append(target.delta.Adds, add)
			result.PointsMerged++
		}
	}

	target.HeadVersion++
	target.UpdatedAt = time.Now()
	result.CompletedAt = time.Now()

	return result, nil
}

// MergeResult contains the result of a merge operation.
type MergeResult struct {
	SourceBranch string              `json:"source_branch"`
	TargetBranch string              `json:"target_branch"`
	StartedAt    time.Time           `json:"started_at"`
	CompletedAt  time.Time           `json:"completed_at"`
	PointsMerged int64               `json:"points_merged"`
	Conflicts    []MergeConflict     `json:"conflicts,omitempty"`
	Strategy     BranchMergeStrategy `json:"strategy"`
}

// MergeConflict represents a conflict during merge.
type MergeConflict struct {
	Metric      string `json:"metric"`
	Timestamp   int64  `json:"timestamp"`
	SourcePoint Point  `json:"source_point"`
	TargetPoint Point  `json:"target_point"`
}

// MergeStrategy defines how to handle conflicts.
type BranchMergeStrategy string

const (
	BranchMergeStrategyOurs   BranchMergeStrategy = "ours"   // Keep target's changes
	BranchMergeStrategyTheirs BranchMergeStrategy = "theirs" // Take source's changes
	BranchMergeStrategyForce  BranchMergeStrategy = "force"  // Force merge, may lose data
)

// MergeOption configures merge behavior.
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
	BranchA    string      `json:"branch_a"`
	BranchB    string      `json:"branch_b"`
	OnlyInA    []Point     `json:"only_in_a,omitempty"`
	OnlyInB    []Point     `json:"only_in_b,omitempty"`
	Modified   []BranchPointDiff `json:"modified,omitempty"`
	TotalDiffs int         `json:"total_diffs"`
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
		Current   string               `json:"current"`
		Branches  map[string]*Branch   `json:"branches"`
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
		Current   string               `json:"current"`
		Branches  map[string]*Branch   `json:"branches"`
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
