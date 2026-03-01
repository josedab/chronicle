package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// IncrementalRefreshMode controls how a view is refreshed.
type IncrementalRefreshMode int

const (
	RefreshFull        IncrementalRefreshMode = iota
	RefreshIncremental
	RefreshManual
)

func (m IncrementalRefreshMode) String() string {
	switch m {
	case RefreshFull:
		return "full"
	case RefreshIncremental:
		return "incremental"
	case RefreshManual:
		return "manual"
	default:
		return "unknown"
	}
}

// IncrementalViewConfig configures incremental materialized view maintenance.
type IncrementalViewConfig struct {
	RefreshMode        IncrementalRefreshMode `json:"refresh_mode"`
	AutoRefreshOnCompact bool                 `json:"auto_refresh_on_compact"`
	DeltaRetention     time.Duration          `json:"delta_retention"`
	MaxDeltaEntries    int                    `json:"max_delta_entries"`
}

// DefaultIncrementalViewConfig returns sensible defaults.
func DefaultIncrementalViewConfig() IncrementalViewConfig {
	return IncrementalViewConfig{
		RefreshMode:          RefreshIncremental,
		AutoRefreshOnCompact: true,
		DeltaRetention:       24 * time.Hour,
		MaxDeltaEntries:      100000,
	}
}

// DeltaLogEntry records a change to a partition for incremental refresh.
type DeltaLogEntry struct {
	PartitionID   uint64    `json:"partition_id"`
	LSN           uint64    `json:"lsn"`
	Metric        string    `json:"metric"`
	PointCount    int       `json:"point_count"`
	MinTimestamp   int64    `json:"min_timestamp"`
	MaxTimestamp   int64    `json:"max_timestamp"`
	Timestamp     time.Time `json:"timestamp"`
}

// ViewAggState holds algebraic aggregate decomposition state.
type ViewAggState struct {
	Sum   float64 `json:"sum"`
	Count int64   `json:"count"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

// Avg computes the average from decomposed sum/count.
func (s *ViewAggState) Avg() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

// Merge combines two aggregate states algebraically.
func (s *ViewAggState) Merge(other *ViewAggState) {
	if other == nil {
		return
	}
	s.Sum += other.Sum
	s.Count += other.Count
	if other.Min < s.Min {
		s.Min = other.Min
	}
	if other.Max > s.Max {
		s.Max = other.Max
	}
}

// IncrementalViewDefinition extends ViewV2Definition with incremental refresh support.
type IncrementalViewDefinition struct {
	Name           string                 `json:"name"`
	SourceMetric   string                 `json:"source_metric"`
	Aggregation    AggFunc                `json:"aggregation"`
	GroupBy        []string               `json:"group_by,omitempty"`
	WindowSize     time.Duration          `json:"window_size"`
	RefreshMode    IncrementalRefreshMode `json:"refresh_mode"`
	OutputMetric   string                 `json:"output_metric"`
	Enabled        bool                   `json:"enabled"`
}

// incrementalViewRuntime holds state for an incremental view.
type incrementalViewRuntime struct {
	def          IncrementalViewDefinition
	aggStates    map[string]*ViewAggState // key: groupBy values concatenated
	watermark    uint64                   // LSN watermark
	deltaLog     []DeltaLogEntry
	lastRefresh  time.Time
	mu           sync.Mutex
}

// IncrementalViewEngine manages incremental materialized view maintenance.
type IncrementalViewEngine struct {
	config IncrementalViewConfig
	db     *DB
	views  map[string]*incrementalViewRuntime
	lsn    uint64
	mu     sync.RWMutex
}

// NewIncrementalViewEngine creates a new incremental view engine.
func NewIncrementalViewEngine(db *DB, config IncrementalViewConfig) *IncrementalViewEngine {
	return &IncrementalViewEngine{
		config: config,
		db:     db,
		views:  make(map[string]*incrementalViewRuntime),
	}
}

// CreateView creates a new incremental materialized view (CREATE MATERIALIZED VIEW).
func (e *IncrementalViewEngine) CreateView(def IncrementalViewDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("incremental_view: name required")
	}
	if def.SourceMetric == "" {
		return fmt.Errorf("incremental_view: source metric required")
	}
	if def.OutputMetric == "" {
		def.OutputMetric = "matview_" + def.Name
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.views[def.Name]; exists {
		return fmt.Errorf("incremental_view: view %q already exists", def.Name)
	}

	e.views[def.Name] = &incrementalViewRuntime{
		def:         def,
		aggStates:   make(map[string]*ViewAggState),
		lastRefresh: time.Now(),
	}

	return nil
}

// DropView removes a materialized view (DROP VIEW).
func (e *IncrementalViewEngine) DropView(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.views[name]; !exists {
		return fmt.Errorf("incremental_view: view %q not found", name)
	}
	delete(e.views, name)
	return nil
}

// EnableView enables a disabled view.
func (e *IncrementalViewEngine) EnableView(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	vr, exists := e.views[name]
	if !exists {
		return fmt.Errorf("incremental_view: view %q not found", name)
	}
	vr.def.Enabled = true
	return nil
}

// DisableView disables a view without dropping it.
func (e *IncrementalViewEngine) DisableView(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	vr, exists := e.views[name]
	if !exists {
		return fmt.Errorf("incremental_view: view %q not found", name)
	}
	vr.def.Enabled = false
	return nil
}

// RefreshView manually refreshes a view (REFRESH VIEW).
func (e *IncrementalViewEngine) RefreshView(ctx context.Context, name string) error {
	e.mu.RLock()
	vr, exists := e.views[name]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("incremental_view: view %q not found", name)
	}

	if !vr.def.Enabled {
		return fmt.Errorf("incremental_view: view %q is disabled", name)
	}

	switch vr.def.RefreshMode {
	case RefreshIncremental:
		return e.incrementalRefresh(ctx, vr)
	default:
		return e.fullRefresh(ctx, vr)
	}
}

// RefreshAll refreshes all views.
func (e *IncrementalViewEngine) RefreshAll(ctx context.Context) error {
	e.mu.RLock()
	names := make([]string, 0, len(e.views))
	for name := range e.views {
		names = append(names, name)
	}
	e.mu.RUnlock()

	for _, name := range names {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := e.RefreshView(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// RecordDelta records a partition change for incremental processing.
func (e *IncrementalViewEngine) RecordDelta(partitionID uint64, metric string, pointCount int, minTs, maxTs int64) {
	e.mu.Lock()
	e.lsn++
	lsn := e.lsn

	for _, vr := range e.views {
		if vr.def.SourceMetric != "" && vr.def.SourceMetric != metric {
			continue
		}
		vr.mu.Lock()
		entry := DeltaLogEntry{
			PartitionID: partitionID,
			LSN:         lsn,
			Metric:      metric,
			PointCount:  pointCount,
			MinTimestamp: minTs,
			MaxTimestamp: maxTs,
			Timestamp:   time.Now(),
		}
		vr.deltaLog = append(vr.deltaLog, entry)

		// Trim delta log if too large
		if len(vr.deltaLog) > e.config.MaxDeltaEntries {
			vr.deltaLog = vr.deltaLog[len(vr.deltaLog)-e.config.MaxDeltaEntries:]
		}
		vr.mu.Unlock()
	}
	e.mu.Unlock()
}

// OnCompaction is called during compaction to trigger auto-refresh.
func (e *IncrementalViewEngine) OnCompaction(partitionID uint64, metric string, pointCount int, minTs, maxTs int64) {
	e.RecordDelta(partitionID, metric, pointCount, minTs, maxTs)

	if !e.config.AutoRefreshOnCompact {
		return
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, vr := range e.views {
		if !vr.def.Enabled {
			continue
		}
		if vr.def.SourceMetric != "" && vr.def.SourceMetric != metric {
			continue
		}
		go func(v *incrementalViewRuntime) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			e.incrementalRefresh(ctx, v)
		}(vr)
	}
}

// ApplyPoints incrementally applies points to matching views.
func (e *IncrementalViewEngine) ApplyPoints(points []Point) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, vr := range e.views {
		if !vr.def.Enabled {
			continue
		}
		for i := range points {
			p := &points[i]
			if vr.def.SourceMetric != "" && vr.def.SourceMetric != p.Metric {
				continue
			}
			e.applyPointToView(vr, p)
		}
	}
	return nil
}

func (e *IncrementalViewEngine) applyPointToView(vr *incrementalViewRuntime, p *Point) {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	groupKey := incrViewGroupKey(p, vr.def.GroupBy)

	state, exists := vr.aggStates[groupKey]
	if !exists {
		state = &ViewAggState{
			Min: math.Inf(1),
			Max: math.Inf(-1),
		}
		vr.aggStates[groupKey] = state
	}

	// Algebraic decomposition: update partial aggregates
	state.Sum += p.Value
	state.Count++
	if p.Value < state.Min {
		state.Min = p.Value
	}
	if p.Value > state.Max {
		state.Max = p.Value
	}
}

func incrViewGroupKey(p *Point, groupBy []string) string {
	if len(groupBy) == 0 {
		return "__global__"
	}
	key := ""
	for i, gb := range groupBy {
		if i > 0 {
			key += "|"
		}
		key += gb + "=" + p.Tags[gb]
	}
	return key
}

// incrementalRefresh applies only changed partitions since last watermark.
func (e *IncrementalViewEngine) incrementalRefresh(ctx context.Context, vr *incrementalViewRuntime) error {
	vr.mu.Lock()
	watermark := vr.watermark
	pendingDeltas := make([]DeltaLogEntry, 0)
	for _, d := range vr.deltaLog {
		if d.LSN > watermark {
			pendingDeltas = append(pendingDeltas, d)
		}
	}
	vr.mu.Unlock()

	if len(pendingDeltas) == 0 {
		return nil
	}

	// Fetch and apply data for each delta
	for _, delta := range pendingDeltas {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		q := &Query{
			Metric: delta.Metric,
			Start:  delta.MinTimestamp,
			End:    delta.MaxTimestamp,
		}

		result, err := e.db.Execute(q)
		if err != nil {
			continue
		}

		for i := range result.Points {
			e.applyPointToView(vr, &result.Points[i])
		}
	}

	// Update watermark to the highest LSN processed
	vr.mu.Lock()
	maxLSN := watermark
	for _, d := range pendingDeltas {
		if d.LSN > maxLSN {
			maxLSN = d.LSN
		}
	}
	vr.watermark = maxLSN
	vr.lastRefresh = time.Now()
	// Trim processed delta log entries
	newLog := make([]DeltaLogEntry, 0)
	for _, d := range vr.deltaLog {
		if d.LSN > maxLSN {
			newLog = append(newLog, d)
		}
	}
	vr.deltaLog = newLog
	vr.mu.Unlock()

	// Write aggregated results to output metric
	return e.writeViewOutput(vr)
}

// fullRefresh does a complete recomputation.
func (e *IncrementalViewEngine) fullRefresh(ctx context.Context, vr *incrementalViewRuntime) error {
	q := &Query{Metric: vr.def.SourceMetric}
	result, err := e.db.Execute(q)
	if err != nil {
		return fmt.Errorf("incremental_view: full refresh: %w", err)
	}

	vr.mu.Lock()
	vr.aggStates = make(map[string]*ViewAggState)
	vr.mu.Unlock()

	for i := range result.Points {
		e.applyPointToView(vr, &result.Points[i])
	}

	vr.mu.Lock()
	vr.lastRefresh = time.Now()
	vr.mu.Unlock()

	return e.writeViewOutput(vr)
}

// writeViewOutput writes aggregated results to the DB as the output metric.
func (e *IncrementalViewEngine) writeViewOutput(vr *incrementalViewRuntime) error {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	now := time.Now().UnixNano()

	for groupKey, state := range vr.aggStates {
		var value float64
		switch vr.def.Aggregation {
		case AggSum:
			value = state.Sum
		case AggCount:
			value = float64(state.Count)
		case AggMin:
			value = state.Min
		case AggMax:
			value = state.Max
		case AggMean:
			value = state.Avg()
		default:
			value = state.Sum
		}

		tags := map[string]string{
			"__view__": vr.def.Name,
			"__group__": groupKey,
		}

		p := Point{
			Metric:    vr.def.OutputMetric,
			Value:     value,
			Timestamp: now,
			Tags:      tags,
		}
		e.db.Write(p)
	}

	return nil
}

// GetViewState returns the current state of a view.
func (e *IncrementalViewEngine) GetViewState(name string) (*IncrementalViewState, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	vr, exists := e.views[name]
	if !exists {
		return nil, fmt.Errorf("incremental_view: view %q not found", name)
	}

	vr.mu.Lock()
	defer vr.mu.Unlock()

	return &IncrementalViewState{
		Name:          vr.def.Name,
		SourceMetric:  vr.def.SourceMetric,
		OutputMetric:  vr.def.OutputMetric,
		Aggregation:   vr.def.Aggregation,
		RefreshMode:   vr.def.RefreshMode,
		Enabled:       vr.def.Enabled,
		LSNWatermark:  vr.watermark,
		PendingDeltas: len(vr.deltaLog),
		GroupCount:    len(vr.aggStates),
		LastRefresh:   vr.lastRefresh,
	}, nil
}

// IncrementalViewState describes the runtime state of an incremental view.
type IncrementalViewState struct {
	Name          string                 `json:"name"`
	SourceMetric  string                 `json:"source_metric"`
	OutputMetric  string                 `json:"output_metric"`
	Aggregation   AggFunc                `json:"aggregation"`
	RefreshMode   IncrementalRefreshMode `json:"refresh_mode"`
	Enabled       bool                   `json:"enabled"`
	LSNWatermark  uint64                 `json:"lsn_watermark"`
	PendingDeltas int                    `json:"pending_deltas"`
	GroupCount    int                    `json:"group_count"`
	LastRefresh   time.Time              `json:"last_refresh"`
}

// ListViews returns all view names.
func (e *IncrementalViewEngine) ListViews() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.views))
	for name := range e.views {
		names = append(names, name)
	}
	return names
}

// ExecuteCQL processes CQL-style materialized view commands.
func (e *IncrementalViewEngine) ExecuteCQL(command string) (string, error) {
	// Simple command parser for CQL DDL
	switch {
	case len(command) > 25 && command[:25] == "CREATE MATERIALIZED VIEW ":
		return e.parseCQLCreate(command)
	case len(command) > 13 && command[:13] == "REFRESH VIEW ":
		name := command[13:]
		err := e.RefreshView(context.Background(), name)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("VIEW %s refreshed", name), nil
	case len(command) > 10 && command[:10] == "DROP VIEW ":
		name := command[10:]
		err := e.DropView(name)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("VIEW %s dropped", name), nil
	default:
		return "", fmt.Errorf("incremental_view: unknown CQL command")
	}
}

func (e *IncrementalViewEngine) parseCQLCreate(command string) (string, error) {
	// CREATE MATERIALIZED VIEW <name> AS SELECT <agg>(<metric>) GROUP BY <tags>
	def := IncrementalViewDefinition{
		RefreshMode: e.config.RefreshMode,
		Enabled:     true,
	}

	// Extract view name (after "CREATE MATERIALIZED VIEW ")
	rest := command[25:]
	spaceIdx := 0
	for i, c := range rest {
		if c == ' ' {
			spaceIdx = i
			break
		}
	}
	if spaceIdx == 0 {
		def.Name = rest
		def.SourceMetric = rest // default source metric = view name
		return "", e.CreateView(def)
	}
	def.Name = rest[:spaceIdx]
	def.SourceMetric = def.Name // default source metric = view name

	if err := e.CreateView(def); err != nil {
		return "", err
	}
	return fmt.Sprintf("MATERIALIZED VIEW %s created", def.Name), nil
}
