package chronicle

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// TSDiffMergeConfig configures the diff/merge engine.
type TSDiffMergeConfig struct {
	Enabled             bool
	MaxDiffSize         int
	MergeTimeout        time.Duration
	ConflictStrategy    string // "last-write-wins", "manual", "source-wins", "target-wins"
	AutoCleanupBranches bool
	BranchTTL           time.Duration
}

// DefaultTSDiffMergeConfig returns sensible defaults.
func DefaultTSDiffMergeConfig() TSDiffMergeConfig {
	return TSDiffMergeConfig{
		Enabled:             true,
		MaxDiffSize:         100000,
		MergeTimeout:        30 * time.Second,
		ConflictStrategy:    "last-write-wins",
		AutoCleanupBranches: true,
		BranchTTL:           7 * 24 * time.Hour,
	}
}

// --- Types (DM prefix avoids conflicts with branching.go / timetravel.go) ---

// DMSeriesDiff represents changes to a single series between two branches.
type DMSeriesDiff struct {
	Metric   string            `json:"metric"`
	Tags     map[string]string `json:"tags,omitempty"`
	Added    []Point           `json:"added"`
	Removed  []Point           `json:"removed"`
	Modified []DMPointDiff     `json:"modified"`
	Summary  DMDiffSummary     `json:"summary"`
}

// DMPointDiff represents a single point that differs between source and target.
type DMPointDiff struct {
	Timestamp    int64   `json:"timestamp"`
	SourceValue  float64 `json:"source_value"`
	TargetValue  float64 `json:"target_value"`
	Delta        float64 `json:"delta"`
	PercentDelta float64 `json:"percent_delta"`
}

// DMDiffSummary summarises the differences for a single series.
type DMDiffSummary struct {
	AddedCount    int     `json:"added_count"`
	RemovedCount  int     `json:"removed_count"`
	ModifiedCount int     `json:"modified_count"`
	TotalSource   int     `json:"total_source"`
	TotalTarget   int     `json:"total_target"`
	MaxDelta      float64 `json:"max_delta"`
	AvgDelta      float64 `json:"avg_delta"`
}

// DMBranchDiff represents the complete diff between two branches.
type DMBranchDiff struct {
	SourceBranch  string         `json:"source_branch"`
	TargetBranch  string         `json:"target_branch"`
	SeriesDiffs   []DMSeriesDiff `json:"series_diffs"`
	TotalAdded    int            `json:"total_added"`
	TotalRemoved  int            `json:"total_removed"`
	TotalModified int            `json:"total_modified"`
	Metrics       []string       `json:"metrics"`
	ComputedAt    time.Time      `json:"computed_at"`
	Duration      time.Duration  `json:"duration"`
}

// DMMergeConflict represents a conflict during merge.
type DMMergeConflict struct {
	Metric        string            `json:"metric"`
	Tags          map[string]string `json:"tags,omitempty"`
	Timestamp     int64             `json:"timestamp"`
	SourceValue   float64           `json:"source_value"`
	TargetValue   float64           `json:"target_value"`
	Resolution    string            `json:"resolution"` // "pending", "source", "target", "manual"
	ResolvedValue float64           `json:"resolved_value"`
}

// DMMergeRequest defines a merge operation.
type DMMergeRequest struct {
	SourceBranch string `json:"source_branch"`
	TargetBranch string `json:"target_branch"`
	Strategy     string `json:"strategy"` // "last-write-wins", "source-wins", "target-wins", "manual"
	DryRun       bool   `json:"dry_run"`
	Message      string `json:"message"`
	Author       string `json:"author"`
}

// DMMergeResult holds the outcome of a merge.
type DMMergeResult struct {
	ID           string            `json:"id"`
	SourceBranch string            `json:"source_branch"`
	TargetBranch string            `json:"target_branch"`
	Strategy     string            `json:"strategy"`
	State        string            `json:"state"` // "completed", "conflicts", "failed"
	Conflicts    []DMMergeConflict `json:"conflicts"`
	Applied      int               `json:"applied"`
	Skipped      int               `json:"skipped"`
	Duration     time.Duration     `json:"duration"`
	DryRun       bool              `json:"dry_run"`
	CommitID     string            `json:"commit_id"`
	CompletedAt  time.Time         `json:"completed_at"`
}

// DMABTest represents an A/B test using branches.
type DMABTest struct {
	ID            string           `json:"id"`
	Name          string           `json:"name"`
	Description   string           `json:"description"`
	ControlBranch string           `json:"control_branch"`
	VariantBranch string           `json:"variant_branch"`
	Metrics       []string         `json:"metrics"`
	StartTime     time.Time        `json:"start_time"`
	EndTime       time.Time        `json:"end_time"`
	State         string           `json:"state"` // "created", "running", "analyzing", "completed"
	Results       *DMABTestResults `json:"results,omitempty"`
}

// DMABTestResults holds the outcome of an A/B test analysis.
type DMABTestResults struct {
	MetricResults []DMABMetricResult `json:"metric_results"`
	Winner        string             `json:"winner"` // "control", "variant", "inconclusive"
	Confidence    float64            `json:"confidence"`
	AnalyzedAt    time.Time          `json:"analyzed_at"`
}

// DMABMetricResult holds per-metric A/B test statistics.
type DMABMetricResult struct {
	Metric        string  `json:"metric"`
	ControlMean   float64 `json:"control_mean"`
	VariantMean   float64 `json:"variant_mean"`
	ControlStddev float64 `json:"control_stddev"`
	VariantStddev float64 `json:"variant_stddev"`
	Delta         float64 `json:"delta"`
	PercentChange float64 `json:"percent_change"`
	PValue        float64 `json:"p_value"`
	Significant   bool    `json:"significant"`
}

// DMCherryPickRequest selectively applies changes from one branch to another.
type DMCherryPickRequest struct {
	SourceBranch string   `json:"source_branch"`
	TargetBranch string   `json:"target_branch"`
	Metrics      []string `json:"metrics"`
	TimeRange    [2]int64 `json:"time_range"`
	Message      string   `json:"message"`
}

// TSDiffMergeStats tracks engine-level statistics.
type TSDiffMergeStats struct {
	DiffsComputed    int64         `json:"diffs_computed"`
	MergesCompleted  int64         `json:"merges_completed"`
	MergeConflicts   int64         `json:"merge_conflicts"`
	CherryPicks      int64         `json:"cherry_picks"`
	ABTestsRun       int64         `json:"ab_tests_run"`
	AvgDiffDuration  time.Duration `json:"avg_diff_duration"`
	AvgMergeDuration time.Duration `json:"avg_merge_duration"`
}

// TSDiffMergeEngine provides Git-like diff, merge, cherry-pick and A/B testing
// operations on top of time-series branch data.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type TSDiffMergeEngine struct {
	db       *DB
	config   TSDiffMergeConfig
	mu       sync.RWMutex
	merges   map[string]*DMMergeResult
	abTests  map[string]*DMABTest
	branches map[string]map[string][]Point // branch -> metric -> points
	stats    TSDiffMergeStats

	totalDiffDuration  time.Duration
	totalMergeDuration time.Duration
}

// NewTSDiffMergeEngine creates a new diff/merge engine.
func NewTSDiffMergeEngine(db *DB, cfg TSDiffMergeConfig) *TSDiffMergeEngine {
	return &TSDiffMergeEngine{
		db:       db,
		config:   cfg,
		merges:   make(map[string]*DMMergeResult),
		abTests:  make(map[string]*DMABTest),
		branches: make(map[string]map[string][]Point),
	}
}

// ---------------------------------------------------------------------------
// Branch CRUD
// ---------------------------------------------------------------------------

// CreateBranch creates a new branch forked from baseBranch.
func (e *TSDiffMergeEngine) CreateBranch(name string, baseBranch string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if name == "" {
		return fmt.Errorf("branch name cannot be empty")
	}
	if _, exists := e.branches[name]; exists {
		return fmt.Errorf("branch already exists: %s", name)
	}

	e.branches[name] = make(map[string][]Point)
	if base, ok := e.branches[baseBranch]; ok {
		for metric, pts := range base {
			cp := make([]Point, len(pts))
			copy(cp, pts)
			e.branches[name][metric] = cp
		}
	}
	return nil
}

// DeleteBranch removes a branch.
func (e *TSDiffMergeEngine) DeleteBranch(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.branches[name]; !exists {
		return fmt.Errorf("branch not found: %s", name)
	}
	delete(e.branches, name)
	return nil
}

// ListBranches returns sorted branch names.
func (e *TSDiffMergeEngine) ListBranches() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.branches))
	for n := range e.branches {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ---------------------------------------------------------------------------
// Read / Write
// ---------------------------------------------------------------------------

// WriteToBranch appends points to a branch.
func (e *TSDiffMergeEngine) WriteToBranch(branch string, points []Point) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	bdata, ok := e.branches[branch]
	if !ok {
		return fmt.Errorf("branch not found: %s", branch)
	}
	for _, p := range points {
		bdata[p.Metric] = append(bdata[p.Metric], p)
	}
	return nil
}

// ReadFromBranch reads points for a metric in [start, end].
func (e *TSDiffMergeEngine) ReadFromBranch(branch string, metric string, start, end int64) ([]Point, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	bdata, ok := e.branches[branch]
	if !ok {
		return nil, fmt.Errorf("branch not found: %s", branch)
	}
	pts, ok := bdata[metric]
	if !ok {
		return []Point{}, nil
	}

	var result []Point
	for _, p := range pts {
		if p.Timestamp >= start && p.Timestamp <= end {
			result = append(result, p)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result, nil
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

// DiffBranches computes a full diff between source and target branches.
func (e *TSDiffMergeEngine) DiffBranches(source, target string) (*DMBranchDiff, error) {
	startTime := time.Now()

	e.mu.RLock()
	srcData, srcOK := e.branches[source]
	tgtData, tgtOK := e.branches[target]
	e.mu.RUnlock()

	if !srcOK {
		return nil, fmt.Errorf("source branch not found: %s", source)
	}
	if !tgtOK {
		return nil, fmt.Errorf("target branch not found: %s", target)
	}

	metricsSet := make(map[string]bool)
	e.mu.RLock()
	for m := range srcData {
		metricsSet[m] = true
	}
	for m := range tgtData {
		metricsSet[m] = true
	}
	e.mu.RUnlock()

	metrics := make([]string, 0, len(metricsSet))
	for m := range metricsSet {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)

	diff := &DMBranchDiff{
		SourceBranch: source,
		TargetBranch: target,
		Metrics:      metrics,
	}

	for _, metric := range metrics {
		sd, err := e.DiffMetric(source, target, metric)
		if err != nil {
			return nil, err
		}
		diff.SeriesDiffs = append(diff.SeriesDiffs, *sd)
		diff.TotalAdded += sd.Summary.AddedCount
		diff.TotalRemoved += sd.Summary.RemovedCount
		diff.TotalModified += sd.Summary.ModifiedCount
	}

	diff.ComputedAt = time.Now()
	diff.Duration = time.Since(startTime)

	e.mu.Lock()
	e.stats.DiffsComputed++
	e.totalDiffDuration += diff.Duration
	e.stats.AvgDiffDuration = e.totalDiffDuration / time.Duration(e.stats.DiffsComputed)
	e.mu.Unlock()

	return diff, nil
}

// DiffMetric computes the diff for a single metric between two branches.
func (e *TSDiffMergeEngine) DiffMetric(source, target, metric string) (*DMSeriesDiff, error) {
	e.mu.RLock()
	srcData, srcOK := e.branches[source]
	tgtData, tgtOK := e.branches[target]
	e.mu.RUnlock()

	if !srcOK {
		return nil, fmt.Errorf("source branch not found: %s", source)
	}
	if !tgtOK {
		return nil, fmt.Errorf("target branch not found: %s", target)
	}

	e.mu.RLock()
	srcPts := srcData[metric]
	tgtPts := tgtData[metric]
	e.mu.RUnlock()

	srcMap := make(map[int64]float64, len(srcPts))
	for _, p := range srcPts {
		srcMap[p.Timestamp] = p.Value
	}
	tgtMap := make(map[int64]float64, len(tgtPts))
	for _, p := range tgtPts {
		tgtMap[p.Timestamp] = p.Value
	}

	sd := &DMSeriesDiff{
		Metric:   metric,
		Added:    []Point{},
		Removed:  []Point{},
		Modified: []DMPointDiff{},
	}

	// Points in target but not in source => added.
	for _, p := range tgtPts {
		if _, ok := srcMap[p.Timestamp]; !ok {
			sd.Added = append(sd.Added, p)
		}
	}

	// Points in source but not in target => removed.
	for _, p := range srcPts {
		if _, ok := tgtMap[p.Timestamp]; !ok {
			sd.Removed = append(sd.Removed, p)
		}
	}

	// Points in both with different values => modified.
	var totalDelta float64
	var maxDelta float64
	for ts, sv := range srcMap {
		if tv, ok := tgtMap[ts]; ok && sv != tv {
			delta := tv - sv
			absDelta := math.Abs(delta)
			pctDelta := 0.0
			if sv != 0 {
				pctDelta = (delta / sv) * 100
			}
			sd.Modified = append(sd.Modified, DMPointDiff{
				Timestamp:    ts,
				SourceValue:  sv,
				TargetValue:  tv,
				Delta:        delta,
				PercentDelta: pctDelta,
			})
			totalDelta += absDelta
			if absDelta > maxDelta {
				maxDelta = absDelta
			}
		}
	}

	avgDelta := 0.0
	if len(sd.Modified) > 0 {
		avgDelta = totalDelta / float64(len(sd.Modified))
	}

	sd.Summary = DMDiffSummary{
		AddedCount:    len(sd.Added),
		RemovedCount:  len(sd.Removed),
		ModifiedCount: len(sd.Modified),
		TotalSource:   len(srcPts),
		TotalTarget:   len(tgtPts),
		MaxDelta:      maxDelta,
		AvgDelta:      avgDelta,
	}

	return sd, nil
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

// Merge merges source branch into target branch.
func (e *TSDiffMergeEngine) Merge(req DMMergeRequest) (*DMMergeResult, error) {
	startTime := time.Now()
	strategy := req.Strategy
	if strategy == "" {
		strategy = e.config.ConflictStrategy
	}

	diff, err := e.DiffBranches(req.SourceBranch, req.TargetBranch)
	if err != nil {
		return nil, err
	}

	result := &DMMergeResult{
		ID:           fmt.Sprintf("merge_%d", time.Now().UnixNano()),
		SourceBranch: req.SourceBranch,
		TargetBranch: req.TargetBranch,
		Strategy:     strategy,
		DryRun:       req.DryRun,
		Conflicts:    []DMMergeConflict{},
	}

	var pointsToApply []Point

	for _, sd := range diff.SeriesDiffs {
		// Points only in source => merge into target.
		for _, p := range sd.Removed {
			pointsToApply = append(pointsToApply, p)
		}
		// Modified points => potential conflicts.
		for _, mod := range sd.Modified {
			result.Conflicts = append(result.Conflicts, DMMergeConflict{
				Metric:      sd.Metric,
				Tags:        sd.Tags,
				Timestamp:   mod.Timestamp,
				SourceValue: mod.SourceValue,
				TargetValue: mod.TargetValue,
				Resolution:  "pending",
			})
		}
	}

	result.Conflicts = e.resolveConflicts(result.Conflicts, strategy)

	hasUnresolved := false
	for _, c := range result.Conflicts {
		if c.Resolution == "pending" {
			hasUnresolved = true
			break
		}
		pointsToApply = append(pointsToApply, Point{
			Metric:    c.Metric,
			Tags:      c.Tags,
			Timestamp: c.Timestamp,
			Value:     c.ResolvedValue,
		})
	}

	if hasUnresolved {
		result.State = "conflicts"
		result.Skipped = len(result.Conflicts)
		e.mu.Lock()
		e.stats.MergeConflicts += int64(len(result.Conflicts))
		e.mu.Unlock()
	} else if !req.DryRun {
		e.mu.Lock()
		tgtData, ok := e.branches[req.TargetBranch]
		if !ok {
			e.mu.Unlock()
			return nil, fmt.Errorf("target branch not found: %s", req.TargetBranch)
		}
		for _, p := range pointsToApply {
			tgtData[p.Metric] = append(tgtData[p.Metric], p)
		}
		e.mu.Unlock()
		result.Applied = len(pointsToApply)
		result.State = "completed"
		result.CommitID = fmt.Sprintf("mc_%d", time.Now().UnixNano())
	} else {
		result.Applied = len(pointsToApply)
		result.State = "completed"
	}

	result.CompletedAt = time.Now()
	result.Duration = time.Since(startTime)

	e.mu.Lock()
	e.merges[result.ID] = result
	e.stats.MergesCompleted++
	e.totalMergeDuration += result.Duration
	e.stats.AvgMergeDuration = e.totalMergeDuration / time.Duration(e.stats.MergesCompleted)
	e.mu.Unlock()

	return result, nil
}

func (e *TSDiffMergeEngine) resolveConflicts(conflicts []DMMergeConflict, strategy string) []DMMergeConflict {
	for i := range conflicts {
		switch strategy {
		case "source-wins":
			conflicts[i].Resolution = "source"
			conflicts[i].ResolvedValue = conflicts[i].SourceValue
		case "target-wins":
			conflicts[i].Resolution = "target"
			conflicts[i].ResolvedValue = conflicts[i].TargetValue
		case "last-write-wins":
			conflicts[i].Resolution = "source"
			conflicts[i].ResolvedValue = conflicts[i].SourceValue
		case "manual":
			conflicts[i].Resolution = "pending"
		default:
			conflicts[i].Resolution = "source"
			conflicts[i].ResolvedValue = conflicts[i].SourceValue
		}
	}
	return conflicts
}
