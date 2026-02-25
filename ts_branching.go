package chronicle

import (
	"context"
	"fmt"
	"sort"
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
	Parent      string            `json:"parent"`      // Parent branch ID
	BaseCommit  string            `json:"base_commit"` // Commit where branch was created
	HeadCommit  string            `json:"head_commit"` // Current head commit
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	CreatedBy   string            `json:"created_by"`
	Metadata    map[string]string `json:"metadata"`
	Protected   bool              `json:"protected"` // Protected branches can't be deleted
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
	Type      ChangeType        `json:"type"` // insert, update, delete
	Series    string            `json:"series"`
	Timestamp int64             `json:"timestamp"` // Point timestamp
	OldValue  *float64          `json:"old_value,omitempty"`
	NewValue  *float64          `json:"new_value,omitempty"`
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
	SourceBranch  string              `json:"source_branch"`
	TargetBranch  string              `json:"target_branch"`
	BaseCommit    string              `json:"base_commit"`   // Common ancestor
	SourceCommit  string              `json:"source_commit"` // Source head
	TargetCommit  string              `json:"target_commit"` // Target head
	Additions     []Point             `json:"additions"`
	Deletions     []Point             `json:"deletions"`
	Modifications []PointModification `json:"modifications"`
	Conflicts     []TSMergeConflict   `json:"conflicts"`
	Stats         DiffStats           `json:"stats"`
}

// PointModification represents a changed point
type PointModification struct {
	Series      string  `json:"series"`
	Timestamp   int64   `json:"timestamp"`
	SourceValue float64 `json:"source_value"`
	TargetValue float64 `json:"target_value"`
}

// MergeConflict represents a merge conflict
type TSMergeConflict struct {
	Series      string          `json:"series"`
	Timestamp   int64           `json:"timestamp"`
	SourceValue float64         `json:"source_value"`
	TargetValue float64         `json:"target_value"`
	BaseValue   *float64        `json:"base_value,omitempty"`
	Resolution  *float64        `json:"resolution,omitempty"`
	Strategy    TSMergeStrategy `json:"strategy,omitempty"`
}

// DiffStats contains statistics about a diff
type DiffStats struct {
	TotalChanges   int `json:"total_changes"`
	Additions      int `json:"additions"`
	Deletions      int `json:"deletions"`
	Modifications  int `json:"modifications"`
	Conflicts      int `json:"conflicts"`
	SeriesAffected int `json:"series_affected"`
}

// MergeResult represents the result of a merge operation
type TSMergeResult struct {
	Success     bool              `json:"success"`
	MergeCommit string            `json:"merge_commit"`
	Strategy    TSMergeStrategy   `json:"strategy"`
	Conflicts   []TSMergeConflict `json:"conflicts"`
	Stats       DiffStats         `json:"stats"`
	Error       string            `json:"error,omitempty"`
}

// BranchManager manages time-series branches
type TSBranchManager struct {
	db     *DB
	config *TSBranchConfig

	branches map[string]*TSBranch
	commits  map[string]*BranchCommit
	branchMu sync.RWMutex

	// Copy-on-write delta storage
	deltas  map[string]map[string][]Point // branchID -> series -> points
	deltaMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	totalBranches  int64
	totalCommits   int64
	totalMerges    int64
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
