package chronicle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BranchManager provides Git-like branching for time-series data
// Enables A/B testing, what-if analysis, and data versioning

// BranchConfig configures the branch manager
type TSBranchConfig struct {
	// Maximum number of branches
	MaxBranches int

	// Default branch name
	DefaultBranch string

	// Enable copy-on-write for efficiency
	CopyOnWrite bool

	// Retention period for deleted branches
	DeletedRetention time.Duration

	// Enable automatic branch cleanup
	AutoCleanup bool

	// Merge conflict strategy
	DefaultMergeStrategy TSMergeStrategy

	// Enable branch-level compression
	BranchCompression bool
}

// DefaultBranchConfig returns default configuration
func DefaultTSBranchConfig() *TSBranchConfig {
	return &TSBranchConfig{
		MaxBranches:          100,
		DefaultBranch:        "main",
		CopyOnWrite:          true,
		DeletedRetention:     7 * 24 * time.Hour,
		AutoCleanup:          true,
		DefaultMergeStrategy: TSMergeStrategyLastWrite,
		BranchCompression:    true,
	}
}

// TSMergeStrategy defines how to resolve conflicts during merge
type TSMergeStrategy string

const (
	TSMergeStrategyLastWrite  TSMergeStrategy = "last_write"  // Use most recent write
	TSMergeStrategyFirstWrite TSMergeStrategy = "first_write" // Use oldest write
	TSMergeStrategyMax        TSMergeStrategy = "max"         // Use maximum value
	TSMergeStrategyMin        TSMergeStrategy = "min"         // Use minimum value
	TSMergeStrategyAverage    TSMergeStrategy = "average"     // Average conflicting values
	TSMergeStrategySum        TSMergeStrategy = "sum"         // Sum conflicting values
	TSMergeStrategySource     TSMergeStrategy = "source"      // Prefer source branch
	TSMergeStrategyTarget     TSMergeStrategy = "target"      // Prefer target branch
	TSMergeStrategyManual     TSMergeStrategy = "manual"      // Require manual resolution
)

// Branch represents a data branch
type TSBranch struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Parent      string            `json:"parent"`       // Parent branch ID
	BaseCommit  string            `json:"base_commit"`  // Commit where branch was created
	HeadCommit  string            `json:"head_commit"`  // Current head commit
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	CreatedBy   string            `json:"created_by"`
	Metadata    map[string]string `json:"metadata"`
	Protected   bool              `json:"protected"`    // Protected branches can't be deleted
	Deleted     bool              `json:"deleted"`
	DeletedAt   *time.Time        `json:"deleted_at,omitempty"`
}

// BranchCommit represents a commit in a branch
type BranchCommit struct {
	ID        string            `json:"id"`
	BranchID  string            `json:"branch_id"`
	ParentID  string            `json:"parent_id"`
	Message   string            `json:"message"`
	Author    string            `json:"author"`
	Timestamp time.Time         `json:"timestamp"`
	Changes   []BranchChange    `json:"changes"`
	Metadata  map[string]string `json:"metadata"`
	Hash      string            `json:"hash"` // Content hash for integrity
}

// BranchChange represents a change in a commit
type BranchChange struct {
	Type      ChangeType `json:"type"`      // insert, update, delete
	Series    string     `json:"series"`
	Timestamp int64      `json:"timestamp"` // Point timestamp
	OldValue  *float64   `json:"old_value,omitempty"`
	NewValue  *float64   `json:"new_value,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// ChangeType defines the type of change
type ChangeType string

const (
	ChangeTypeInsert ChangeType = "insert"
	ChangeTypeUpdate ChangeType = "update"
	ChangeTypeDelete ChangeType = "delete"
)

// BranchDiff represents differences between two branches
type TSBranchDiff struct {
	SourceBranch string          `json:"source_branch"`
	TargetBranch string          `json:"target_branch"`
	BaseCommit   string          `json:"base_commit"`   // Common ancestor
	SourceCommit string          `json:"source_commit"` // Source head
	TargetCommit string          `json:"target_commit"` // Target head
	Additions    []Point         `json:"additions"`
	Deletions    []Point         `json:"deletions"`
	Modifications []PointModification `json:"modifications"`
	Conflicts    []TSMergeConflict `json:"conflicts"`
	Stats        DiffStats       `json:"stats"`
}

// PointModification represents a changed point
type PointModification struct {
	Series       string  `json:"series"`
	Timestamp    int64   `json:"timestamp"`
	SourceValue  float64 `json:"source_value"`
	TargetValue  float64 `json:"target_value"`
}

// MergeConflict represents a merge conflict
type TSMergeConflict struct {
	Series       string    `json:"series"`
	Timestamp    int64     `json:"timestamp"`
	SourceValue  float64   `json:"source_value"`
	TargetValue  float64   `json:"target_value"`
	BaseValue    *float64  `json:"base_value,omitempty"`
	Resolution   *float64  `json:"resolution,omitempty"`
	Strategy     TSMergeStrategy `json:"strategy,omitempty"`
}

// DiffStats contains statistics about a diff
type DiffStats struct {
	TotalChanges    int `json:"total_changes"`
	Additions       int `json:"additions"`
	Deletions       int `json:"deletions"`
	Modifications   int `json:"modifications"`
	Conflicts       int `json:"conflicts"`
	SeriesAffected  int `json:"series_affected"`
}

// MergeResult represents the result of a merge operation
type TSMergeResult struct {
	Success      bool            `json:"success"`
	MergeCommit  string          `json:"merge_commit"`
	Strategy     TSMergeStrategy   `json:"strategy"`
	Conflicts    []TSMergeConflict `json:"conflicts"`
	Stats        DiffStats       `json:"stats"`
	Error        string          `json:"error,omitempty"`
}

// BranchManager manages time-series branches
type TSBranchManager struct {
	db     *DB
	config *TSBranchConfig

	branches map[string]*TSBranch
	commits  map[string]*BranchCommit
	branchMu sync.RWMutex

	// Copy-on-write delta storage
	deltas   map[string]map[string][]Point // branchID -> series -> points
	deltaMu  sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	totalBranches int64
	totalCommits  int64
	totalMerges   int64
	mergeConflicts int64
}

// NewBranchManager creates a new branch manager
func NewTSBranchManager(db *DB, config *TSBranchConfig) (*TSBranchManager, error) {
	if config == nil {
		config = DefaultTSBranchConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	bm := &TSBranchManager{
		db:       db,
		config:   config,
		branches: make(map[string]*TSBranch),
		commits:  make(map[string]*BranchCommit),
		deltas:   make(map[string]map[string][]Point),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Create default branch
	mainBranch := &TSBranch{
		ID:        generateBranchID(),
		Name:      config.DefaultBranch,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Protected: true,
		Metadata:  make(map[string]string),
	}
	bm.branches[mainBranch.Name] = mainBranch
	bm.deltas[mainBranch.ID] = make(map[string][]Point)
	atomic.AddInt64(&bm.totalBranches, 1)

	// Start cleanup worker
	if config.AutoCleanup {
		bm.wg.Add(1)
		go bm.cleanupWorker()
	}

	return bm, nil
}

// CreateBranch creates a new branch from an existing branch
func (bm *TSBranchManager) CreateBranch(name, parent, description, author string) (*TSBranch, error) {
	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	// Check limits
	activeBranches := 0
	for _, b := range bm.branches {
		if !b.Deleted {
			activeBranches++
		}
	}
	if activeBranches >= bm.config.MaxBranches {
		return nil, fmt.Errorf("maximum number of branches reached (%d)", bm.config.MaxBranches)
	}

	// Check if branch already exists
	if _, exists := bm.branches[name]; exists {
		return nil, fmt.Errorf("branch already exists: %s", name)
	}

	// Get parent branch
	parentBranch, exists := bm.branches[parent]
	if !exists {
		return nil, fmt.Errorf("parent branch not found: %s", parent)
	}

	// Create new branch
	branch := &TSBranch{
		ID:          generateBranchID(),
		Name:        name,
		Description: description,
		Parent:      parentBranch.ID,
		BaseCommit:  parentBranch.HeadCommit,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		CreatedBy:   author,
		Metadata:    make(map[string]string),
	}

	bm.branches[name] = branch

	// Initialize copy-on-write delta storage
	bm.deltaMu.Lock()
	bm.deltas[branch.ID] = make(map[string][]Point)
	bm.deltaMu.Unlock()

	atomic.AddInt64(&bm.totalBranches, 1)

	return branch, nil
}

// GetBranch returns a branch by name
func (bm *TSBranchManager) GetBranch(name string) (*TSBranch, error) {
	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	branch, exists := bm.branches[name]
	if !exists || branch.Deleted {
		return nil, fmt.Errorf("branch not found: %s", name)
	}
	return branch, nil
}

// ListBranches returns all active branches
func (bm *TSBranchManager) ListBranches() []*TSBranch {
	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	branches := make([]*TSBranch, 0)
	for _, b := range bm.branches {
		if !b.Deleted {
			branches = append(branches, b)
		}
	}

	// Sort by name
	sort.Slice(branches, func(i, j int) bool {
		return branches[i].Name < branches[j].Name
	})

	return branches
}

// DeleteBranch marks a branch as deleted
func (bm *TSBranchManager) DeleteBranch(name string) error {
	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	branch, exists := bm.branches[name]
	if !exists {
		return fmt.Errorf("branch not found: %s", name)
	}

	if branch.Protected {
		return fmt.Errorf("cannot delete protected branch: %s", name)
	}

	if branch.Name == bm.config.DefaultBranch {
		return fmt.Errorf("cannot delete default branch")
	}

	now := time.Now()
	branch.Deleted = true
	branch.DeletedAt = &now

	return nil
}

// Write writes a point to a specific branch
func (bm *TSBranchManager) Write(branchName string, point *Point, author, message string) error {
	bm.branchMu.RLock()
	branch, exists := bm.branches[branchName]
	bm.branchMu.RUnlock()

	if !exists || branch.Deleted {
		return fmt.Errorf("branch not found: %s", branchName)
	}

	// Store in delta storage
	bm.deltaMu.Lock()
	if bm.deltas[branch.ID] == nil {
		bm.deltas[branch.ID] = make(map[string][]Point)
	}
	bm.deltas[branch.ID][point.Metric] = append(bm.deltas[branch.ID][point.Metric], *point)
	bm.deltaMu.Unlock()

	// Create commit
	commit := &BranchCommit{
		ID:        generateCommitID(),
		BranchID:  branch.ID,
		ParentID:  branch.HeadCommit,
		Message:   message,
		Author:    author,
		Timestamp: time.Now(),
		Changes: []BranchChange{
			{
				Type:      ChangeTypeInsert,
				Series:    point.Metric,
				Timestamp: point.Timestamp,
				NewValue:  &point.Value,
				Tags:      point.Tags,
			},
		},
	}
	commit.Hash = computeCommitHash(commit)

	bm.branchMu.Lock()
	bm.commits[commit.ID] = commit
	branch.HeadCommit = commit.ID
	branch.UpdatedAt = time.Now()
	bm.branchMu.Unlock()

	atomic.AddInt64(&bm.totalCommits, 1)

	return nil
}

// WriteBatch writes multiple points to a branch in a single commit
func (bm *TSBranchManager) WriteBatch(branchName string, points []Point, author, message string) error {
	bm.branchMu.RLock()
	branch, exists := bm.branches[branchName]
	bm.branchMu.RUnlock()

	if !exists || branch.Deleted {
		return fmt.Errorf("branch not found: %s", branchName)
	}

	// Store in delta storage
	bm.deltaMu.Lock()
	if bm.deltas[branch.ID] == nil {
		bm.deltas[branch.ID] = make(map[string][]Point)
	}

	changes := make([]BranchChange, 0, len(points))
	for _, p := range points {
		bm.deltas[branch.ID][p.Metric] = append(bm.deltas[branch.ID][p.Metric], p)
		val := p.Value
		changes = append(changes, BranchChange{
			Type:      ChangeTypeInsert,
			Series:    p.Metric,
			Timestamp: p.Timestamp,
			NewValue:  &val,
			Tags:      p.Tags,
		})
	}
	bm.deltaMu.Unlock()

	// Create commit
	commit := &BranchCommit{
		ID:        generateCommitID(),
		BranchID:  branch.ID,
		ParentID:  branch.HeadCommit,
		Message:   message,
		Author:    author,
		Timestamp: time.Now(),
		Changes:   changes,
	}
	commit.Hash = computeCommitHash(commit)

	bm.branchMu.Lock()
	bm.commits[commit.ID] = commit
	branch.HeadCommit = commit.ID
	branch.UpdatedAt = time.Now()
	bm.branchMu.Unlock()

	atomic.AddInt64(&bm.totalCommits, 1)

	return nil
}

// Query queries data from a specific branch
func (bm *TSBranchManager) Query(branchName string, query *Query) ([]Point, error) {
	bm.branchMu.RLock()
	branch, exists := bm.branches[branchName]
	bm.branchMu.RUnlock()

	if !exists || branch.Deleted {
		return nil, fmt.Errorf("branch not found: %s", branchName)
	}

	// Collect points from branch and ancestors
	points := make([]Point, 0)

	// Get branch chain (from current to root)
	branchChain := bm.getBranchChain(branch)

	// Query from main database first
	baseResult, err := bm.db.Execute(query)
	if err != nil {
		return nil, err
	}
	if baseResult != nil {
		points = append(points, baseResult.Points...)
	}

	// Apply deltas from each branch in the chain (oldest to newest)
	bm.deltaMu.RLock()
	for i := len(branchChain) - 1; i >= 0; i-- {
		b := branchChain[i]
		if seriesDeltas, exists := bm.deltas[b.ID]; exists {
			if query.Metric != "" {
				if deltaPoints, exists := seriesDeltas[query.Metric]; exists {
					points = append(points, deltaPoints...)
				}
			} else {
				for _, deltaPoints := range seriesDeltas {
					points = append(points, deltaPoints...)
				}
			}
		}
	}
	bm.deltaMu.RUnlock()

	// Filter by time range
	filtered := make([]Point, 0)
	for _, p := range points {
		if query.Start > 0 && p.Timestamp < query.Start {
			continue
		}
		if query.End > 0 && p.Timestamp > query.End {
			continue
		}
		filtered = append(filtered, p)
	}

	// Sort by timestamp
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp < filtered[j].Timestamp
	})

	// Apply limit
	if query.Limit > 0 && len(filtered) > query.Limit {
		filtered = filtered[:query.Limit]
	}

	return filtered, nil
}

func (bm *TSBranchManager) getBranchChain(branch *TSBranch) []*TSBranch {
	chain := []*TSBranch{branch}

	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	current := branch
	for current.Parent != "" {
		for _, b := range bm.branches {
			if b.ID == current.Parent {
				chain = append(chain, b)
				current = b
				break
			}
		}
		if current.Parent == "" || len(chain) > 100 { // Prevent infinite loops
			break
		}
	}

	return chain
}

// Diff computes the difference between two branches
func (bm *TSBranchManager) Diff(sourceBranch, targetBranch string) (*TSBranchDiff, error) {
	source, err := bm.GetBranch(sourceBranch)
	if err != nil {
		return nil, fmt.Errorf("source branch: %w", err)
	}

	target, err := bm.GetBranch(targetBranch)
	if err != nil {
		return nil, fmt.Errorf("target branch: %w", err)
	}

	diff := &TSBranchDiff{
		SourceBranch: sourceBranch,
		TargetBranch: targetBranch,
		SourceCommit: source.HeadCommit,
		TargetCommit: target.HeadCommit,
		Additions:    make([]Point, 0),
		Deletions:    make([]Point, 0),
		Modifications: make([]PointModification, 0),
		Conflicts:    make([]TSMergeConflict, 0),
	}

	// Find common ancestor
	diff.BaseCommit = bm.findCommonAncestor(source, target)

	// Get points from both branches
	sourcePoints := bm.getBranchPoints(source)
	targetPoints := bm.getBranchPoints(target)

	// Build maps for comparison
	sourceMap := make(map[string]Point)
	for _, p := range sourcePoints {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		sourceMap[key] = p
	}

	targetMap := make(map[string]Point)
	for _, p := range targetPoints {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		targetMap[key] = p
	}

	// Find additions (in source but not in target)
	seriesSet := make(map[string]bool)
	for key, p := range sourceMap {
		seriesSet[p.Metric] = true
		if _, exists := targetMap[key]; !exists {
			diff.Additions = append(diff.Additions, p)
		} else if targetMap[key].Value != p.Value {
			diff.Modifications = append(diff.Modifications, PointModification{
				Series:      p.Metric,
				Timestamp:   p.Timestamp,
				SourceValue: p.Value,
				TargetValue: targetMap[key].Value,
			})
		}
	}

	// Find deletions (in target but not in source)
	for key, p := range targetMap {
		seriesSet[p.Metric] = true
		if _, exists := sourceMap[key]; !exists {
			diff.Deletions = append(diff.Deletions, p)
		}
	}

	// Calculate stats
	diff.Stats = DiffStats{
		TotalChanges:   len(diff.Additions) + len(diff.Deletions) + len(diff.Modifications),
		Additions:      len(diff.Additions),
		Deletions:      len(diff.Deletions),
		Modifications:  len(diff.Modifications),
		Conflicts:      len(diff.Conflicts),
		SeriesAffected: len(seriesSet),
	}

	return diff, nil
}

func (bm *TSBranchManager) findCommonAncestor(source, target *TSBranch) string {
	// Simple implementation - find first shared parent
	sourceAncestors := make(map[string]bool)

	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	// Build source ancestor set
	current := source
	for current != nil {
		sourceAncestors[current.ID] = true
		if current.Parent == "" {
			break
		}
		for _, b := range bm.branches {
			if b.ID == current.Parent {
				current = b
				break
			}
		}
	}

	// Find first target ancestor in source set
	current = target
	for current != nil {
		if sourceAncestors[current.ID] {
			return current.HeadCommit
		}
		if current.Parent == "" {
			break
		}
		for _, b := range bm.branches {
			if b.ID == current.Parent {
				current = b
				break
			}
		}
	}

	return ""
}

func (bm *TSBranchManager) getBranchPoints(branch *TSBranch) []Point {
	points := make([]Point, 0)

	bm.deltaMu.RLock()
	defer bm.deltaMu.RUnlock()

	if deltas, exists := bm.deltas[branch.ID]; exists {
		for _, seriesPoints := range deltas {
			points = append(points, seriesPoints...)
		}
	}

	return points
}

// Merge merges source branch into target branch
func (bm *TSBranchManager) Merge(sourceBranch, targetBranch, author, message string, strategy TSMergeStrategy) (*TSMergeResult, error) {
	if strategy == "" {
		strategy = bm.config.DefaultMergeStrategy
	}

	// Get diff
	diff, err := bm.Diff(sourceBranch, targetBranch)
	if err != nil {
		return nil, err
	}

	result := &TSMergeResult{
		Strategy:  strategy,
		Stats:     diff.Stats,
		Conflicts: make([]TSMergeConflict, 0),
	}

	// Check for conflicts in modifications
	for _, mod := range diff.Modifications {
		conflict := TSMergeConflict{
			Series:      mod.Series,
			Timestamp:   mod.Timestamp,
			SourceValue: mod.SourceValue,
			TargetValue: mod.TargetValue,
		}

		// Resolve conflict based on strategy
		var resolved float64
		switch strategy {
		case TSMergeStrategyLastWrite:
			resolved = mod.SourceValue // Assume source is newer
		case TSMergeStrategyFirstWrite:
			resolved = mod.TargetValue
		case TSMergeStrategyMax:
			resolved = tsMax(mod.SourceValue, mod.TargetValue)
		case TSMergeStrategyMin:
			resolved = tsMin(mod.SourceValue, mod.TargetValue)
		case TSMergeStrategyAverage:
			resolved = (mod.SourceValue + mod.TargetValue) / 2
		case TSMergeStrategySum:
			resolved = mod.SourceValue + mod.TargetValue
		case TSMergeStrategySource:
			resolved = mod.SourceValue
		case TSMergeStrategyTarget:
			resolved = mod.TargetValue
		case TSMergeStrategyManual:
			result.Conflicts = append(result.Conflicts, conflict)
			atomic.AddInt64(&bm.mergeConflicts, 1)
			continue
		}

		conflict.Resolution = &resolved
		conflict.Strategy = strategy
		result.Conflicts = append(result.Conflicts, conflict)
	}

	// If manual strategy and has conflicts, return for resolution
	if strategy == TSMergeStrategyManual && len(diff.Modifications) > 0 {
		result.Success = false
		result.Error = "merge has conflicts requiring manual resolution"
		return result, nil
	}

	// Apply additions to target branch
	target, _ := bm.GetBranch(targetBranch)
	
	pointsToAdd := make([]Point, 0)
	for _, p := range diff.Additions {
		pointsToAdd = append(pointsToAdd, p)
	}

	// Apply resolved conflicts
	for _, conflict := range result.Conflicts {
		if conflict.Resolution != nil {
			pointsToAdd = append(pointsToAdd, Point{
				Metric:    conflict.Series,
				Timestamp: conflict.Timestamp,
				Value:     *conflict.Resolution,
			})
		}
	}

	if len(pointsToAdd) > 0 {
		if err := bm.WriteBatch(targetBranch, pointsToAdd, author, message); err != nil {
			result.Success = false
			result.Error = err.Error()
			return result, nil
		}
	}

	result.Success = true
	result.MergeCommit = target.HeadCommit
	result.Stats.Conflicts = len(result.Conflicts)

	atomic.AddInt64(&bm.totalMerges, 1)

	return result, nil
}

// Rebase rebases source branch onto target branch
func (bm *TSBranchManager) Rebase(sourceBranch, targetBranch string) error {
	source, err := bm.GetBranch(sourceBranch)
	if err != nil {
		return err
	}

	target, err := bm.GetBranch(targetBranch)
	if err != nil {
		return err
	}

	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	// Update source branch's parent to target
	source.Parent = target.ID
	source.BaseCommit = target.HeadCommit
	source.UpdatedAt = time.Now()

	return nil
}

// GetCommit returns a commit by ID
func (bm *TSBranchManager) GetCommit(commitID string) (*BranchCommit, error) {
	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	commit, exists := bm.commits[commitID]
	if !exists {
		return nil, fmt.Errorf("commit not found: %s", commitID)
	}
	return commit, nil
}

// GetCommitHistory returns commit history for a branch
func (bm *TSBranchManager) GetCommitHistory(branchName string, limit int) ([]*BranchCommit, error) {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return nil, err
	}

	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	history := make([]*BranchCommit, 0)
	currentID := branch.HeadCommit

	for currentID != "" && len(history) < limit {
		commit, exists := bm.commits[currentID]
		if !exists {
			break
		}
		history = append(history, commit)
		currentID = commit.ParentID
	}

	return history, nil
}

// Checkout switches the working branch context
func (bm *TSBranchManager) Checkout(branchName string) (*TSBranch, error) {
	return bm.GetBranch(branchName)
}

// Reset resets a branch to a specific commit
func (bm *TSBranchManager) Reset(branchName, commitID string) error {
	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	branch, exists := bm.branches[branchName]
	if !exists || branch.Deleted {
		return fmt.Errorf("branch not found: %s", branchName)
	}

	// Verify commit exists
	if _, exists := bm.commits[commitID]; !exists {
		return fmt.Errorf("commit not found: %s", commitID)
	}

	branch.HeadCommit = commitID
	branch.UpdatedAt = time.Now()

	return nil
}

// Tag creates a named reference to a commit
func (bm *TSBranchManager) Tag(branchName, tagName, commitID string) error {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return err
	}

	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	if commitID == "" {
		commitID = branch.HeadCommit
	}

	if branch.Metadata == nil {
		branch.Metadata = make(map[string]string)
	}
	branch.Metadata["tag:"+tagName] = commitID

	return nil
}

// GetTag returns the commit ID for a tag
func (bm *TSBranchManager) GetTag(branchName, tagName string) (string, error) {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return "", err
	}

	commitID, exists := branch.Metadata["tag:"+tagName]
	if !exists {
		return "", fmt.Errorf("tag not found: %s", tagName)
	}
	return commitID, nil
}

// Stats returns branch manager statistics
func (bm *TSBranchManager) Stats() BranchManagerStats {
	bm.branchMu.RLock()
	activeBranches := 0
	for _, b := range bm.branches {
		if !b.Deleted {
			activeBranches++
		}
	}
	bm.branchMu.RUnlock()

	return BranchManagerStats{
		TotalBranches:   atomic.LoadInt64(&bm.totalBranches),
		ActiveBranches:  int64(activeBranches),
		TotalCommits:    atomic.LoadInt64(&bm.totalCommits),
		TotalMerges:     atomic.LoadInt64(&bm.totalMerges),
		MergeConflicts:  atomic.LoadInt64(&bm.mergeConflicts),
	}
}

// BranchManagerStats contains branch manager statistics
type BranchManagerStats struct {
	TotalBranches  int64 `json:"total_branches"`
	ActiveBranches int64 `json:"active_branches"`
	TotalCommits   int64 `json:"total_commits"`
	TotalMerges    int64 `json:"total_merges"`
	MergeConflicts int64 `json:"merge_conflicts"`
}

// Close shuts down the branch manager
func (bm *TSBranchManager) Close() error {
	bm.cancel()
	bm.wg.Wait()
	return nil
}

func (bm *TSBranchManager) cleanupWorker() {
	defer bm.wg.Done()

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-bm.ctx.Done():
			return
		case <-ticker.C:
			bm.cleanupDeletedBranches()
		}
	}
}

func (bm *TSBranchManager) cleanupDeletedBranches() {
	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	cutoff := time.Now().Add(-bm.config.DeletedRetention)
	for name, branch := range bm.branches {
		if branch.Deleted && branch.DeletedAt != nil && branch.DeletedAt.Before(cutoff) {
			// Clean up delta storage
			bm.deltaMu.Lock()
			delete(bm.deltas, branch.ID)
			bm.deltaMu.Unlock()

			delete(bm.branches, name)
		}
	}
}

// Helper functions

func generateBranchID() string {
	return fmt.Sprintf("br_%d", time.Now().UnixNano())
}

func generateCommitID() string {
	return fmt.Sprintf("cm_%d", time.Now().UnixNano())
}

func computeCommitHash(commit *BranchCommit) string {
	data, _ := json.Marshal(commit.Changes)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func tsMax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func tsMin(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// JSON helpers
func (b *TSBranch) MarshalJSON() ([]byte, error) {
	type Alias TSBranch
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(b),
	})
}

// String returns a string representation of the branch
func (b *TSBranch) String() string {
	return fmt.Sprintf("TSBranch{Name: %s, ID: %s, Parent: %s}", b.Name, b.ID, b.Parent)
}

// IsDirty returns true if the branch has uncommitted changes
func (bm *TSBranchManager) IsDirty(branchName string) bool {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return false
	}

	bm.deltaMu.RLock()
	defer bm.deltaMu.RUnlock()

	if deltas, exists := bm.deltas[branch.ID]; exists {
		for _, points := range deltas {
			if len(points) > 0 {
				return true
			}
		}
	}
	return false
}

// GetBranchSeries returns all series names in a branch
func (bm *TSBranchManager) GetBranchSeries(branchName string) ([]string, error) {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return nil, err
	}

	bm.deltaMu.RLock()
	defer bm.deltaMu.RUnlock()

	series := make([]string, 0)
	if deltas, exists := bm.deltas[branch.ID]; exists {
		for s := range deltas {
			series = append(series, s)
		}
	}

	sort.Strings(series)
	return series, nil
}

// CompareBranches returns a summary comparison between two branches
func (bm *TSBranchManager) CompareBranches(branch1, branch2 string) (string, error) {
	diff, err := bm.Diff(branch1, branch2)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Comparing %s -> %s\n", branch1, branch2))
	sb.WriteString(fmt.Sprintf("  Additions: %d\n", diff.Stats.Additions))
	sb.WriteString(fmt.Sprintf("  Deletions: %d\n", diff.Stats.Deletions))
	sb.WriteString(fmt.Sprintf("  Modifications: %d\n", diff.Stats.Modifications))
	sb.WriteString(fmt.Sprintf("  Series affected: %d\n", diff.Stats.SeriesAffected))

	return sb.String(), nil
}
