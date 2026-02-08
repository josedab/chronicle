package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
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
	FromTime   int64        `json:"from_time"`
	ToTime     int64        `json:"to_time"`
	Metric     string       `json:"metric,omitempty"`
	Added      int          `json:"added"`
	Removed    int          `json:"removed"`
	Modified   int          `json:"modified"`
	Unchanged  int          `json:"unchanged"`
	Diffs      []MetricDiff `json:"diffs"`
	ComputedAt time.Time    `json:"computed_at"`
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
	Type        string `json:"type"`     // "anomaly", "deploy", "config_change", "alert"
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

// NextFrame advances the replay and returns the next frame.
func (e *TimeTravelDebugEngine) NextFrame(replayID string) (*ReplayFrame, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session, ok := e.replays[replayID]
	if !ok {
		return nil, fmt.Errorf("replay session not found: %s", replayID)
	}

	if session.Current >= len(session.Frames) {
		session.State = "done"
		return nil, fmt.Errorf("replay complete")
	}

	frame := &session.Frames[session.Current]
	session.Current++
	session.State = "playing"

	if session.Current >= len(session.Frames) {
		session.State = "done"
	}

	return frame, nil
}

// SeekFrame seeks to a specific frame index in the replay.
func (e *TimeTravelDebugEngine) SeekFrame(replayID string, index int) (*ReplayFrame, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session, ok := e.replays[replayID]
	if !ok {
		return nil, fmt.Errorf("replay session not found: %s", replayID)
	}

	if index < 0 || index >= len(session.Frames) {
		return nil, fmt.Errorf("frame index %d out of range [0, %d)", index, len(session.Frames))
	}

	session.Current = index
	session.State = "paused"

	return &session.Frames[index], nil
}

// CloseReplay removes a replay session.
func (e *TimeTravelDebugEngine) CloseReplay(replayID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.replays[replayID]; !ok {
		return fmt.Errorf("replay session not found: %s", replayID)
	}

	delete(e.replays, replayID)
	return nil
}

// CreateWhatIf registers a what-if scenario for later execution.
func (e *TimeTravelDebugEngine) CreateWhatIf(scenario WhatIfScenario) (*WhatIfScenario, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}
	if scenario.Name == "" {
		return nil, fmt.Errorf("scenario name is required")
	}

	if scenario.ID == "" {
		scenario.ID = fmt.Sprintf("whatif-%d", time.Now().UnixNano())
	}
	scenario.CreatedAt = time.Now()

	e.mu.Lock()
	e.scenarios[scenario.ID] = &scenario
	e.mu.Unlock()

	return &scenario, nil
}

// RunWhatIf executes a what-if scenario and computes its impact.
func (e *TimeTravelDebugEngine) RunWhatIf(scenarioID string) (*WhatIfResult, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}

	e.mu.RLock()
	scenario, ok := e.scenarios[scenarioID]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("scenario not found: %s", scenarioID)
	}

	originalStats := make(map[string]float64)
	modifiedStats := make(map[string]float64)
	impact := make(map[string]float64)

	for _, mod := range scenario.Modifications {
		q := &Query{Metric: mod.Metric, Tags: mod.Tags}
		result, err := e.db.Execute(q)
		if err != nil {
			continue
		}

		// Compute original stats
		var origSum, origCount float64
		for _, p := range result.Points {
			origSum += p.Value
			origCount++
		}
		origAvg := 0.0
		if origCount > 0 {
			origAvg = origSum / origCount
		}
		originalStats[mod.Metric+"_avg"] = origAvg
		originalStats[mod.Metric+"_count"] = origCount
		originalStats[mod.Metric+"_sum"] = origSum

		// Apply modification
		var modSum, modCount float64
		for _, p := range result.Points {
			v := applyModification(p.Value, mod)
			modSum += v
			modCount++
		}
		modAvg := 0.0
		if modCount > 0 {
			modAvg = modSum / modCount
		}
		modifiedStats[mod.Metric+"_avg"] = modAvg
		modifiedStats[mod.Metric+"_count"] = modCount
		modifiedStats[mod.Metric+"_sum"] = modSum

		// Compute impact
		if origAvg != 0 {
			impact[mod.Metric+"_avg_change_pct"] = ((modAvg - origAvg) / math.Abs(origAvg)) * 100
		}
		impact[mod.Metric+"_sum_delta"] = modSum - origSum
	}

	whatIfResult := &WhatIfResult{
		ScenarioID:    scenarioID,
		OriginalStats: originalStats,
		ModifiedStats: modifiedStats,
		Impact:        impact,
		ComputedAt:    time.Now(),
	}

	atomic.AddInt64(&e.scenariosRun, 1)

	e.mu.Lock()
	scenario.Result = whatIfResult
	e.mu.Unlock()

	return whatIfResult, nil
}

// applyModification transforms a value according to a what-if modification.
func applyModification(value float64, mod WhatIfModification) float64 {
	switch mod.Operation {
	case "scale":
		return value * mod.Factor
	case "shift":
		return value + mod.Value
	case "replace":
		return mod.Value
	case "drop":
		return 0
	default:
		return value
	}
}

// GetTimeline builds an annotated timeline for a metric.
func (e *TimeTravelDebugEngine) GetTimeline(metric string, start, end int64) (*DebugTimeline, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}

	q := &Query{Metric: metric, Start: start, End: end}
	result, err := e.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("timeline query failed: %w", err)
	}

	points := result.Points
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// Detect anomalies and generate events
	var events []TimelineEvent
	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			prev := points[i-1].Value
			curr := points[i].Value
			if prev != 0 {
				changePct := math.Abs((curr - prev) / prev) * 100
				if changePct > 50 {
					severity := "warning"
					if changePct > 100 {
						severity = "critical"
					}
					events = append(events, TimelineEvent{
						Timestamp:   points[i].Timestamp,
						Type:        "anomaly",
						Description: fmt.Sprintf("%.1f%% change from %.2f to %.2f", changePct, prev, curr),
						Severity:    severity,
					})
				}
			}
		}
	}

	return &DebugTimeline{
		Metric:    metric,
		Points:    points,
		Events:    events,
		StartTime: start,
		EndTime:   end,
	}, nil
}

// ListReplays returns all active replay sessions.
func (e *TimeTravelDebugEngine) ListReplays() []*ReplaySession {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sessions := make([]*ReplaySession, 0, len(e.replays))
	for _, s := range e.replays {
		sessions = append(sessions, s)
	}

	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].CreatedAt.Before(sessions[j].CreatedAt)
	})

	return sessions
}

// ListScenarios returns all what-if scenarios.
func (e *TimeTravelDebugEngine) ListScenarios() []*WhatIfScenario {
	e.mu.RLock()
	defer e.mu.RUnlock()

	scenarios := make([]*WhatIfScenario, 0, len(e.scenarios))
	for _, s := range e.scenarios {
		scenarios = append(scenarios, s)
	}

	sort.Slice(scenarios, func(i, j int) bool {
		return scenarios[i].CreatedAt.Before(scenarios[j].CreatedAt)
	})

	return scenarios
}

// Stats returns engine usage statistics.
func (e *TimeTravelDebugEngine) Stats() TimeTravelDebugStats {
	diffs := atomic.LoadInt64(&e.diffsComputed)
	totalNanos := atomic.LoadInt64(&e.totalDiffNanos)

	var avgDiff time.Duration
	if diffs > 0 {
		avgDiff = time.Duration(totalNanos / diffs)
	}

	return TimeTravelDebugStats{
		DiffsComputed:   diffs,
		ReplaysCreated:  atomic.LoadInt64(&e.replaysCreated),
		ScenariosRun:    atomic.LoadInt64(&e.scenariosRun),
		AvgDiffDuration: avgDiff,
		CacheHits:       atomic.LoadInt64(&e.cacheHits),
		CacheMisses:     atomic.LoadInt64(&e.cacheMisses),
	}
}

// RegisterHTTPHandlers registers debug HTTP endpoints on the given mux.
func (e *TimeTravelDebugEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/debug/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric   string `json:"metric"`
			FromTime int64  `json:"from_time"`
			ToTime   int64  `json:"to_time"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		var result *DebugDiffResult
		var err error
		if req.Metric != "" {
			result, err = e.Diff(req.Metric, req.FromTime, req.ToTime)
		} else {
			result, err = e.DiffAll(req.FromTime, req.ToTime)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/debug/query-as-of", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string            `json:"metric"`
			Tags   map[string]string `json:"tags"`
			AsOf   int64             `json:"as_of"`
			Start  int64             `json:"start"`
			End    int64             `json:"end"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		points, err := e.QueryAsOf(req.Metric, req.Tags, time.Unix(0, req.AsOf), req.Start, req.End)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(points)
	})

	mux.HandleFunc("/api/v1/debug/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req ReplayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		session, err := e.CreateReplay(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(session)
	})

	mux.HandleFunc("/api/v1/debug/replay/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Handle /api/v1/debug/replay/{id}/next
		if strings.HasSuffix(path, "/next") && r.Method == http.MethodGet {
			id := strings.TrimPrefix(path, "/api/v1/debug/replay/")
			id = strings.TrimSuffix(id, "/next")
			frame, err := e.NextFrame(id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(frame)
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	mux.HandleFunc("/api/v1/debug/what-if", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var scenario WhatIfScenario
		if err := json.NewDecoder(r.Body).Decode(&scenario); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		created, err := e.CreateWhatIf(scenario)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(created)
	})

	mux.HandleFunc("/api/v1/debug/what-if/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Handle /api/v1/debug/what-if/{id}/run
		if strings.HasSuffix(path, "/run") && r.Method == http.MethodPost {
			id := strings.TrimPrefix(path, "/api/v1/debug/what-if/")
			id = strings.TrimSuffix(id, "/run")
			result, err := e.RunWhatIf(id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(result)
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	mux.HandleFunc("/api/v1/debug/timeline", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric parameter required", http.StatusBadRequest)
			return
		}
		var start, end int64
		fmt.Sscanf(r.URL.Query().Get("start"), "%d", &start)
		fmt.Sscanf(r.URL.Query().Get("end"), "%d", &end)

		timeline, err := e.GetTimeline(metric, start, end)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(timeline)
	})

	mux.HandleFunc("/api/v1/debug/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
