package chronicle

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// TimeTravelDebugConfig configures the time-travel debugging engine.
type TimeTravelDebugConfig struct {
	Enabled          bool
	MaxSnapshots     int
	DiffTimeout      time.Duration
	MaxDiffResults   int
	ReplayBufferSize int
}

// DefaultTimeTravelDebugConfig returns default time-travel debug configuration.
func DefaultTimeTravelDebugConfig() TimeTravelDebugConfig {
	return TimeTravelDebugConfig{
		Enabled:          true,
		MaxSnapshots:     100,
		DiffTimeout:      30 * time.Second,
		MaxDiffResults:   10000,
		ReplayBufferSize: 1000,
	}
}

// DiffChangeType classifies the nature of a metric difference.
type DiffChangeType int

const (
	DiffUnchanged DiffChangeType = iota
	DiffAdded
	DiffRemoved
	DiffModified
)

// String returns a human-readable label for the change type.
func (d DiffChangeType) String() string {
	switch d {
	case DiffUnchanged:
		return "unchanged"
	case DiffAdded:
		return "added"
	case DiffRemoved:
		return "removed"
	case DiffModified:
		return "modified"
	default:
		return "unknown"
	}
}

// MetricDiff represents the difference between two time points for a single series.
type MetricDiff struct {
	Metric        string         `json:"metric"`
	TagSet        string         `json:"tag_set"`
	ValueBefore   float64        `json:"value_before"`
	ValueAfter    float64        `json:"value_after"`
	Delta         float64        `json:"delta"`
	PercentChange float64        `json:"percent_change"`
	TimeBefore    int64          `json:"time_before"`
	TimeAfter     int64          `json:"time_after"`
	ChangeType    DiffChangeType `json:"change_type"`
}

// DebugDiffResult holds the complete diff between two metric states.
type DebugDiffResult struct {
	FromTime   int64         `json:"from_time"`
	ToTime     int64         `json:"to_time"`
	Metric     string        `json:"metric,omitempty"`
	Added      int           `json:"added"`
	Removed    int           `json:"removed"`
	Modified   int           `json:"modified"`
	Unchanged  int           `json:"unchanged"`
	Diffs      []MetricDiff  `json:"diffs"`
	ComputedAt time.Time     `json:"computed_at"`
	Duration   time.Duration `json:"duration"`
}

// ReplayRequest defines what to replay.
type ReplayRequest struct {
	Metric    string            `json:"metric"`
	StartTime int64             `json:"start_time"`
	EndTime   int64             `json:"end_time"`
	StepSize  time.Duration     `json:"step_size"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// ReplayFrame is a single frame in a replay.
type ReplayFrame struct {
	Timestamp int64    `json:"timestamp"`
	Points    []Point  `json:"points"`
	Anomalies []string `json:"anomalies,omitempty"`
	Index     int      `json:"index"`
	Total     int      `json:"total"`
}

// ReplaySession holds state for an active replay.
type ReplaySession struct {
	ID        string        `json:"id"`
	Request   ReplayRequest `json:"request"`
	Frames    []ReplayFrame `json:"frames"`
	Current   int           `json:"current"`
	State     string        `json:"state"` // "created", "playing", "paused", "done"
	CreatedAt time.Time     `json:"created_at"`
}

// WhatIfScenario defines a hypothetical scenario.
type WhatIfScenario struct {
	ID            string               `json:"id"`
	Name          string               `json:"name"`
	Description   string               `json:"description"`
	BranchName    string               `json:"branch_name"`
	Modifications []WhatIfModification `json:"modifications"`
	CreatedAt     time.Time            `json:"created_at"`
	Result        *WhatIfResult        `json:"result,omitempty"`
}

// WhatIfModification describes a single hypothetical data transformation.
type WhatIfModification struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Operation string            `json:"operation"` // "scale", "shift", "replace", "drop"
	Factor    float64           `json:"factor"`
	Value     float64           `json:"value"`
}

// WhatIfResult holds the outcome of running a what-if scenario.
type WhatIfResult struct {
	ScenarioID    string             `json:"scenario_id"`
	OriginalStats map[string]float64 `json:"original_stats"`
	ModifiedStats map[string]float64 `json:"modified_stats"`
	Impact        map[string]float64 `json:"impact"`
	ComputedAt    time.Time          `json:"computed_at"`
}

// DebugTimeline shows metric history with annotations.
type DebugTimeline struct {
	Metric    string          `json:"metric"`
	Points    []Point         `json:"points"`
	Events    []TimelineEvent `json:"events"`
	StartTime int64           `json:"start_time"`
	EndTime   int64           `json:"end_time"`
}

// TimelineEvent represents an annotated event on the timeline.
type TimelineEvent struct {
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"type"` // "anomaly", "deploy", "config_change", "alert"
	Description string `json:"description"`
	Severity    string `json:"severity"` // "info", "warning", "critical"
}

// TimeTravelDebugStats tracks engine usage statistics.
type TimeTravelDebugStats struct {
	DiffsComputed   int64         `json:"diffs_computed"`
	ReplaysCreated  int64         `json:"replays_created"`
	ScenariosRun    int64         `json:"scenarios_run"`
	AvgDiffDuration time.Duration `json:"avg_diff_duration"`
	CacheHits       int64         `json:"cache_hits"`
	CacheMisses     int64         `json:"cache_misses"`
}

// TimeTravelDebugEngine provides debugging-focused time-travel capabilities.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type TimeTravelDebugEngine struct {
	db        *DB
	config    TimeTravelDebugConfig
	mu        sync.RWMutex
	replays   map[string]*ReplaySession
	scenarios map[string]*WhatIfScenario
	diffCache map[string]*DebugDiffResult
	stats     TimeTravelDebugStats

	// atomic counters for stats
	diffsComputed  int64
	replaysCreated int64
	scenariosRun   int64
	cacheHits      int64
	cacheMisses    int64
	totalDiffNanos int64
}

// NewTimeTravelDebugEngine creates a new time-travel debug engine.
func NewTimeTravelDebugEngine(db *DB, cfg TimeTravelDebugConfig) *TimeTravelDebugEngine {
	return &TimeTravelDebugEngine{
		db:        db,
		config:    cfg,
		replays:   make(map[string]*ReplaySession),
		scenarios: make(map[string]*WhatIfScenario),
		diffCache: make(map[string]*DebugDiffResult),
	}
}

// QueryAsOf queries metric data as it existed at a specific point in time.
func (e *TimeTravelDebugEngine) QueryAsOf(metric string, tags map[string]string, asOf time.Time, start, end int64) ([]Point, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}

	asOfNano := asOf.UnixNano()

	// Clamp end to asOf if it exceeds
	if end == 0 || end > asOfNano {
		end = asOfNano
	}

	q := &Query{
		Metric: metric,
		Tags:   tags,
		Start:  start,
		End:    end,
	}

	result, err := e.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("as-of query failed: %w", err)
	}

	// Filter to points at or before asOf
	var filtered []Point
	for _, p := range result.Points {
		if p.Timestamp <= asOfNano {
			filtered = append(filtered, p)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp < filtered[j].Timestamp
	})

	return filtered, nil
}

// diffCacheKey builds a deterministic cache key for a diff request.
func diffCacheKey(metric string, from, to int64) string {
	return fmt.Sprintf("%s:%d:%d", metric, from, to)
}

// Diff computes the difference for a specific metric between two timestamps.
func (e *TimeTravelDebugEngine) Diff(metric string, fromTime, toTime int64) (*DebugDiffResult, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}

	key := diffCacheKey(metric, fromTime, toTime)

	e.mu.RLock()
	if cached, ok := e.diffCache[key]; ok {
		e.mu.RUnlock()
		atomic.AddInt64(&e.cacheHits, 1)
		return cached, nil
	}
	e.mu.RUnlock()
	atomic.AddInt64(&e.cacheMisses, 1)

	startComp := time.Now()

	fromPoints, err := e.queryAtTime(metric, fromTime)
	if err != nil {
		return nil, fmt.Errorf("diff from-query failed: %w", err)
	}

	toPoints, err := e.queryAtTime(metric, toTime)
	if err != nil {
		return nil, fmt.Errorf("diff to-query failed: %w", err)
	}

	result := e.computeDiff(metric, fromTime, toTime, fromPoints, toPoints)
	result.ComputedAt = time.Now()
	result.Duration = time.Since(startComp)

	atomic.AddInt64(&e.diffsComputed, 1)
	atomic.AddInt64(&e.totalDiffNanos, int64(result.Duration))

	e.mu.Lock()
	e.diffCache[key] = result
	e.mu.Unlock()

	return result, nil
}

// DiffAll computes the difference across all metrics between two timestamps.
func (e *TimeTravelDebugEngine) DiffAll(fromTime, toTime int64) (*DebugDiffResult, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}

	key := diffCacheKey("", fromTime, toTime)

	e.mu.RLock()
	if cached, ok := e.diffCache[key]; ok {
		e.mu.RUnlock()
		atomic.AddInt64(&e.cacheHits, 1)
		return cached, nil
	}
	e.mu.RUnlock()
	atomic.AddInt64(&e.cacheMisses, 1)

	startComp := time.Now()

	fromPoints, err := e.queryAtTime("", fromTime)
	if err != nil {
		return nil, fmt.Errorf("diff-all from-query failed: %w", err)
	}

	toPoints, err := e.queryAtTime("", toTime)
	if err != nil {
		return nil, fmt.Errorf("diff-all to-query failed: %w", err)
	}

	result := e.computeDiff("", fromTime, toTime, fromPoints, toPoints)
	result.ComputedAt = time.Now()
	result.Duration = time.Since(startComp)

	atomic.AddInt64(&e.diffsComputed, 1)
	atomic.AddInt64(&e.totalDiffNanos, int64(result.Duration))

	e.mu.Lock()
	e.diffCache[key] = result
	e.mu.Unlock()

	return result, nil
}

// queryAtTime retrieves points for a metric (or all metrics if empty) up to the given timestamp.
func (e *TimeTravelDebugEngine) queryAtTime(metric string, ts int64) ([]Point, error) {
	if metric != "" {
		q := &Query{Metric: metric, Start: 0, End: ts}
		result, err := e.db.Execute(q)
		if err != nil {
			return nil, err
		}
		return result.Points, nil
	}

	// All metrics
	var all []Point
	for _, m := range e.db.Metrics() {
		q := &Query{Metric: m, Start: 0, End: ts}
		result, err := e.db.Execute(q)
		if err != nil {
			continue
		}
		all = append(all, result.Points...)
	}
	return all, nil
}

// seriesKeyStr builds a canonical key from a point's metric + tags.
func seriesKeyStr(p Point) string {
	sk := NewSeriesKey(p.Metric, p.Tags)
	return sk.String()
}

// computeDiff builds a DebugDiffResult from two sets of points.
func (e *TimeTravelDebugEngine) computeDiff(metric string, fromTime, toTime int64, fromPts, toPts []Point) *DebugDiffResult {
	// Group by series key, keeping the last value per series
	fromMap := make(map[string]Point)
	for _, p := range fromPts {
		k := seriesKeyStr(p)
		if existing, ok := fromMap[k]; !ok || p.Timestamp > existing.Timestamp {
			fromMap[k] = p
		}
	}

	toMap := make(map[string]Point)
	for _, p := range toPts {
		k := seriesKeyStr(p)
		if existing, ok := toMap[k]; !ok || p.Timestamp > existing.Timestamp {
			toMap[k] = p
		}
	}

	result := &DebugDiffResult{
		FromTime: fromTime,
		ToTime:   toTime,
		Metric:   metric,
		Diffs:    make([]MetricDiff, 0),
	}

	seen := make(map[string]bool)

	// Process keys in toMap (added or modified)
	for k, toP := range toMap {
		seen[k] = true
		if fromP, ok := fromMap[k]; ok {
			if fromP.Value == toP.Value {
				result.Unchanged++
				result.Diffs = append(result.Diffs, MetricDiff{
					Metric:      toP.Metric,
					TagSet:      k,
					ValueBefore: fromP.Value,
					ValueAfter:  toP.Value,
					TimeBefore:  fromP.Timestamp,
					TimeAfter:   toP.Timestamp,
					ChangeType:  DiffUnchanged,
				})
			} else {
				delta := toP.Value - fromP.Value
				pct := 0.0
				if fromP.Value != 0 {
					pct = (delta / math.Abs(fromP.Value)) * 100
				}
				result.Modified++
				result.Diffs = append(result.Diffs, MetricDiff{
					Metric:        toP.Metric,
					TagSet:        k,
					ValueBefore:   fromP.Value,
					ValueAfter:    toP.Value,
					Delta:         delta,
					PercentChange: pct,
					TimeBefore:    fromP.Timestamp,
					TimeAfter:     toP.Timestamp,
					ChangeType:    DiffModified,
				})
			}
		} else {
			result.Added++
			result.Diffs = append(result.Diffs, MetricDiff{
				Metric:     toP.Metric,
				TagSet:     k,
				ValueAfter: toP.Value,
				TimeAfter:  toP.Timestamp,
				ChangeType: DiffAdded,
			})
		}

		if e.config.MaxDiffResults > 0 && len(result.Diffs) >= e.config.MaxDiffResults {
			break
		}
	}

	// Removed: in fromMap but not in toMap
	for k, fromP := range fromMap {
		if seen[k] {
			continue
		}
		result.Removed++
		result.Diffs = append(result.Diffs, MetricDiff{
			Metric:      fromP.Metric,
			TagSet:      k,
			ValueBefore: fromP.Value,
			TimeBefore:  fromP.Timestamp,
			ChangeType:  DiffRemoved,
		})

		if e.config.MaxDiffResults > 0 && len(result.Diffs) >= e.config.MaxDiffResults {
			break
		}
	}

	return result
}

// CreateReplay creates a new replay session.
func (e *TimeTravelDebugEngine) CreateReplay(req ReplayRequest) (*ReplaySession, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}
	if req.Metric == "" {
		return nil, fmt.Errorf("replay metric is required")
	}
	if req.StartTime >= req.EndTime {
		return nil, fmt.Errorf("start time must be before end time")
	}
	if req.StepSize <= 0 {
		req.StepSize = time.Minute
	}

	id := fmt.Sprintf("replay-%d", time.Now().UnixNano())

	// Build frames
	stepNanos := req.StepSize.Nanoseconds()
	var frames []ReplayFrame
	idx := 0
	for ts := req.StartTime; ts <= req.EndTime; ts += stepNanos {
		q := &Query{
			Metric: req.Metric,
			Tags:   req.Tags,
			Start:  ts,
			End:    ts + stepNanos,
		}
		result, err := e.db.Execute(q)
		if err != nil {
			continue
		}

		var anomalies []string
		for _, p := range result.Points {
			if p.Value > 1e6 || p.Value < -1e6 {
				anomalies = append(anomalies, fmt.Sprintf("extreme value %.2f at %d", p.Value, p.Timestamp))
			}
		}

		frames = append(frames, ReplayFrame{
			Timestamp: ts,
			Points:    result.Points,
			Anomalies: anomalies,
			Index:     idx,
		})
		idx++

		if e.config.ReplayBufferSize > 0 && len(frames) >= e.config.ReplayBufferSize {
			break
		}
	}

	// Set total on all frames
	total := len(frames)
	for i := range frames {
		frames[i].Total = total
	}

	session := &ReplaySession{
		ID:        id,
		Request:   req,
		Frames:    frames,
		Current:   0,
		State:     "created",
		CreatedAt: time.Now(),
	}

	atomic.AddInt64(&e.replaysCreated, 1)

	e.mu.Lock()
	e.replays[id] = session
	e.mu.Unlock()

	return session, nil
}
