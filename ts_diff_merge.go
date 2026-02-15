package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
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

// ---------------------------------------------------------------------------
// Cherry-pick
// ---------------------------------------------------------------------------

// CherryPick selectively applies changes from source to target.
func (e *TSDiffMergeEngine) CherryPick(req DMCherryPickRequest) (*DMMergeResult, error) {
	startTime := time.Now()

	e.mu.RLock()
	srcData, srcOK := e.branches[req.SourceBranch]
	_, tgtOK := e.branches[req.TargetBranch]
	e.mu.RUnlock()

	if !srcOK {
		return nil, fmt.Errorf("source branch not found: %s", req.SourceBranch)
	}
	if !tgtOK {
		return nil, fmt.Errorf("target branch not found: %s", req.TargetBranch)
	}

	metricsSet := make(map[string]bool, len(req.Metrics))
	for _, m := range req.Metrics {
		metricsSet[m] = true
	}

	var pointsToApply []Point

	e.mu.RLock()
	for metric, pts := range srcData {
		if len(metricsSet) > 0 && !metricsSet[metric] {
			continue
		}
		for _, p := range pts {
			if req.TimeRange[0] > 0 && p.Timestamp < req.TimeRange[0] {
				continue
			}
			if req.TimeRange[1] > 0 && p.Timestamp > req.TimeRange[1] {
				continue
			}
			pointsToApply = append(pointsToApply, p)
		}
	}
	e.mu.RUnlock()

	e.mu.Lock()
	tgtData := e.branches[req.TargetBranch]
	for _, p := range pointsToApply {
		tgtData[p.Metric] = append(tgtData[p.Metric], p)
	}
	e.stats.CherryPicks++
	e.mu.Unlock()

	result := &DMMergeResult{
		ID:           fmt.Sprintf("cp_%d", time.Now().UnixNano()),
		SourceBranch: req.SourceBranch,
		TargetBranch: req.TargetBranch,
		Strategy:     "cherry-pick",
		State:        "completed",
		Applied:      len(pointsToApply),
		Conflicts:    []DMMergeConflict{},
		CommitID:     fmt.Sprintf("cc_%d", time.Now().UnixNano()),
		CompletedAt:  time.Now(),
		Duration:     time.Since(startTime),
	}

	e.mu.Lock()
	e.merges[result.ID] = result
	e.mu.Unlock()

	return result, nil
}

// ---------------------------------------------------------------------------
// A/B Testing
// ---------------------------------------------------------------------------

// CreateABTest registers a new A/B test.
func (e *TSDiffMergeEngine) CreateABTest(test DMABTest) (*DMABTest, error) {
	if test.ID == "" {
		test.ID = fmt.Sprintf("ab_%d", time.Now().UnixNano())
	}

	e.mu.RLock()
	_, ctlOK := e.branches[test.ControlBranch]
	_, varOK := e.branches[test.VariantBranch]
	e.mu.RUnlock()

	if !ctlOK {
		return nil, fmt.Errorf("control branch not found: %s", test.ControlBranch)
	}
	if !varOK {
		return nil, fmt.Errorf("variant branch not found: %s", test.VariantBranch)
	}

	if test.State == "" {
		test.State = "created"
	}

	e.mu.Lock()
	e.abTests[test.ID] = &test
	e.stats.ABTestsRun++
	e.mu.Unlock()

	return &test, nil
}

// AnalyzeABTest computes statistical results for the given test.
func (e *TSDiffMergeEngine) AnalyzeABTest(testID string) (*DMABTestResults, error) {
	e.mu.RLock()
	test, ok := e.abTests[testID]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("A/B test not found: %s", testID)
	}

	e.mu.Lock()
	test.State = "analyzing"
	e.mu.Unlock()

	results := &DMABTestResults{
		MetricResults: []DMABMetricResult{},
		AnalyzedAt:    time.Now(),
	}

	variantWins := 0
	controlWins := 0

	for _, metric := range test.Metrics {
		e.mu.RLock()
		ctlPts := e.branches[test.ControlBranch][metric]
		varPts := e.branches[test.VariantBranch][metric]
		e.mu.RUnlock()

		ctlVals := dmFilterValues(ctlPts, test.StartTime.UnixNano(), test.EndTime.UnixNano())
		varVals := dmFilterValues(varPts, test.StartTime.UnixNano(), test.EndTime.UnixNano())

		ctlMean, ctlStd := dmMeanStddev(ctlVals)
		varMean, varStd := dmMeanStddev(varVals)

		delta := varMean - ctlMean
		pctChange := 0.0
		if ctlMean != 0 {
			pctChange = (delta / ctlMean) * 100
		}

		pValue, _ := e.tTest(ctlVals, varVals)
		significant := pValue < 0.05

		mr := DMABMetricResult{
			Metric:        metric,
			ControlMean:   ctlMean,
			VariantMean:   varMean,
			ControlStddev: ctlStd,
			VariantStddev: varStd,
			Delta:         delta,
			PercentChange: pctChange,
			PValue:        pValue,
			Significant:   significant,
		}
		results.MetricResults = append(results.MetricResults, mr)

		if significant {
			if varMean > ctlMean {
				variantWins++
			} else {
				controlWins++
			}
		}
	}

	if variantWins > controlWins {
		results.Winner = "variant"
	} else if controlWins > variantWins {
		results.Winner = "control"
	} else {
		results.Winner = "inconclusive"
	}

	if len(results.MetricResults) > 0 {
		totalConf := 0.0
		for _, mr := range results.MetricResults {
			totalConf += 1 - mr.PValue
		}
		results.Confidence = totalConf / float64(len(results.MetricResults))
	}

	e.mu.Lock()
	test.State = "completed"
	test.Results = results
	e.mu.Unlock()

	return results, nil
}

// tTest computes a two-sample Student's t-test returning (p-value, t-statistic).
func (e *TSDiffMergeEngine) tTest(control, variant []float64) (float64, float64) {
	if len(control) < 2 || len(variant) < 2 {
		return 1.0, 0.0
	}

	cMean, cStd := dmMeanStddev(control)
	vMean, vStd := dmMeanStddev(variant)

	n1 := float64(len(control))
	n2 := float64(len(variant))

	se := math.Sqrt((cStd*cStd)/n1 + (vStd*vStd)/n2)
	if se == 0 {
		return 1.0, 0.0
	}

	tStat := (vMean - cMean) / se

	// Welch-Satterthwaite degrees of freedom.
	num := math.Pow((cStd*cStd)/n1+(vStd*vStd)/n2, 2)
	denom := math.Pow((cStd*cStd)/n1, 2)/(n1-1) + math.Pow((vStd*vStd)/n2, 2)/(n2-1)
	df := num / denom
	if df < 1 {
		df = 1
	}

	pValue := dmApproxTwoTailedP(math.Abs(tStat), df)
	return pValue, tStat
}

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

// GetMerge returns a merge result by ID.
func (e *TSDiffMergeEngine) GetMerge(id string) *DMMergeResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.merges[id]
}

// ListMerges returns all merge results.
func (e *TSDiffMergeEngine) ListMerges() []*DMMergeResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]*DMMergeResult, 0, len(e.merges))
	for _, m := range e.merges {
		out = append(out, m)
	}
	return out
}

// GetABTest returns an A/B test by ID.
func (e *TSDiffMergeEngine) GetABTest(id string) *DMABTest {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.abTests[id]
}

// ListABTests returns all A/B tests.
func (e *TSDiffMergeEngine) ListABTests() []*DMABTest {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]*DMABTest, 0, len(e.abTests))
	for _, t := range e.abTests {
		out = append(out, t)
	}
	return out
}

// Stats returns engine statistics.
func (e *TSDiffMergeEngine) Stats() TSDiffMergeStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// ---------------------------------------------------------------------------
// HTTP Handlers
// ---------------------------------------------------------------------------

// RegisterHTTPHandlers registers diff/merge HTTP endpoints.
func (e *TSDiffMergeEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/branches", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListBranches())
		case http.MethodPost:
			var req struct {
				Name       string `json:"name"`
				BaseBranch string `json:"base_branch"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if err := e.CreateBranch(req.Name, req.BaseBranch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case http.MethodDelete:
			name := r.URL.Query().Get("name")
			if name == "" {
				name = strings.TrimPrefix(r.URL.Path, "/api/v1/branches/")
			}
			if err := e.DeleteBranch(name); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/branches/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Source string `json:"source"`
			Target string `json:"target"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		diff, err := e.DiffBranches(req.Source, req.Target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(diff)
	})

	mux.HandleFunc("/api/v1/branches/merge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req DMMergeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := e.Merge(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/branches/cherry-pick", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req DMCherryPickRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := e.CherryPick(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/ab-tests", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListABTests())
		case http.MethodPost:
			var test DMABTest
			if err := json.NewDecoder(r.Body).Decode(&test); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			created, err := e.CreateABTest(test)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(created)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/ab-tests/analyze", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		results, err := e.AnalyzeABTest(req.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("/api/v1/branches/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

// ---------------------------------------------------------------------------
// internal helpers
// ---------------------------------------------------------------------------

func dmFilterValues(pts []Point, startNano, endNano int64) []float64 {
	var vals []float64
	for _, p := range pts {
		if startNano > 0 && p.Timestamp < startNano {
			continue
		}
		if endNano > 0 && p.Timestamp > endNano {
			continue
		}
		vals = append(vals, p.Value)
	}
	return vals
}

func dmMeanStddev(vals []float64) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))
	if len(vals) < 2 {
		return mean, 0
	}
	variance := 0.0
	for _, v := range vals {
		d := v - mean
		variance += d * d
	}
	variance /= float64(len(vals) - 1)
	return mean, math.Sqrt(variance)
}

func dmApproxTwoTailedP(t float64, df float64) float64 {
	x := t * math.Sqrt(df/(df+t*t))
	p := 2 * (1 - dmNormalCDF(x))
	if p > 1 {
		p = 1
	}
	if p < 0 {
		p = 0
	}
	return p
}

func dmNormalCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt2))
}
