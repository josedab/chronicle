package chronicle

// materialized_views.go implements V1 of materialized views with basic refresh modes.
// V2 (materialized_views_v2.go) adds windowed views, incremental maintenance,
// dependency tracking, and cost-based refresh optimization.
//
// Deprecated: New callers should prefer the V2 API (ViewManagerV2) for production use.
// This file will be removed in a future major version.

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Materialized view refresh modes (extending the existing RefreshMode type).
const (
	RefreshEager    RefreshMode = iota + 10 // update on every write
	RefreshLazy                             // update on query if stale
	RefreshPeriodic                         // update on a timer
)

// MaterializedViewConfig controls the materialized view engine behaviour.
type MaterializedViewConfig struct {
	MaxViews         int
	DefaultStaleness time.Duration
	CheckInterval    time.Duration
	MaxMemoryBytes   int64
	EnableShadowMode bool
}

// DefaultMaterializedViewConfig returns sensible defaults.
func DefaultMaterializedViewConfig() MaterializedViewConfig {
	return MaterializedViewConfig{
		MaxViews:         256,
		DefaultStaleness: 1 * time.Minute,
		CheckInterval:    10 * time.Second,
		MaxMemoryBytes:   256 * 1024 * 1024,
		EnableShadowMode: false,
	}
}

// MaterializedViewDefinition describes a single materialized view.
type MaterializedViewDefinition struct {
	Name         string
	SourceMetric string
	Tags         map[string]string
	Aggregation  AggFunc
	Window       time.Duration
	GroupBy      []string
	RefreshMode  RefreshMode
	MaxStaleness time.Duration
	Enabled      bool
}

// --- Incremental Aggregator ------------------------------------------------

type windowState struct {
	count         int64
	sum, min, max float64
	mean, m2      float64 // Welford's online variance
	first, last   Point
	windowStart   int64
}

// AggregateUpdate is emitted each time a point is applied.
type AggregateUpdate struct {
	GroupKey           string
	OldValue, NewValue float64
	WindowStart        int64
	PointCount         int64
}

// AggregateState is a snapshot of one group key's current aggregate.
type AggregateState struct {
	Value       float64
	Count       int64
	WindowStart int64
	LastUpdated time.Time
}

// IncrementalAggregator maintains running aggregates per group key.
type IncrementalAggregator struct {
	function AggFunc
	window   time.Duration
	windows  map[string]*windowState
	mu       sync.RWMutex
}

// NewIncrementalAggregator creates an aggregator for the given function and window.
func NewIncrementalAggregator(function AggFunc, window time.Duration) *IncrementalAggregator {
	return &IncrementalAggregator{
		function: function, window: window,
		windows: make(map[string]*windowState),
	}
}

// Apply incrementally incorporates a single point and returns the update.
func (ia *IncrementalAggregator) Apply(point *Point) (*AggregateUpdate, error) {
	if point == nil {
		return nil, fmt.Errorf("materialized_views: nil point")
	}
	groupKey := buildGroupKey(point)
	windowStart := ia.windowStartFor(point.Timestamp)

	ia.mu.Lock()
	defer ia.mu.Unlock()

	ws, ok := ia.windows[groupKey]
	if !ok || ws.windowStart != windowStart {
		ws = &windowState{min: math.MaxFloat64, max: -math.MaxFloat64, first: *point, windowStart: windowStart}
		ia.windows[groupKey] = ws
	}
	oldValue := ia.computeValue(ws)
	ws.count++
	ws.sum += point.Value
	ws.last = *point
	if point.Value < ws.min {
		ws.min = point.Value
	}
	if point.Value > ws.max {
		ws.max = point.Value
	}
	// Welford's online algorithm for mean / variance
	delta := point.Value - ws.mean
	ws.mean += delta / float64(ws.count)
	delta2 := point.Value - ws.mean
	ws.m2 += delta * delta2

	newValue := ia.computeValue(ws)
	return &AggregateUpdate{
		GroupKey: groupKey, OldValue: oldValue, NewValue: newValue,
		WindowStart: windowStart, PointCount: ws.count,
	}, nil
}

// GetCurrent returns the current aggregate state for a group key.
func (ia *IncrementalAggregator) GetCurrent(groupKey string) *AggregateState {
	ia.mu.RLock()
	defer ia.mu.RUnlock()
	ws, ok := ia.windows[groupKey]
	if !ok {
		return nil
	}
	return &AggregateState{
		Value: ia.computeValue(ws), Count: ws.count,
		WindowStart: ws.windowStart, LastUpdated: time.Now(),
	}
}

func (ia *IncrementalAggregator) computeValue(ws *windowState) float64 {
	if ws.count == 0 {
		return 0
	}
	switch ia.function {
	case AggCount:
		return float64(ws.count)
	case AggSum:
		return ws.sum
	case AggMean:
		return ws.mean
	case AggMin:
		return ws.min
	case AggMax:
		return ws.max
	case AggFirst:
		return ws.first.Value
	case AggLast:
		return ws.last.Value
	case AggStddev:
		if ws.count < 2 {
			return 0
		}
		return math.Sqrt(ws.m2 / float64(ws.count-1))
	default:
		return ws.sum
	}
}

func (ia *IncrementalAggregator) windowStartFor(ts int64) int64 {
	if ia.window <= 0 {
		return 0
	}
	w := ia.window.Nanoseconds()
	return (ts / w) * w
}

func buildGroupKey(p *Point) string {
	if len(p.Tags) == 0 {
		return p.Metric
	}
	key := p.Metric
	for k, v := range p.Tags {
		key += "|" + k + "=" + v
	}
	return key
}

// --- Dependency Tracker ----------------------------------------------------

type viewDependency struct {
	ViewName  string
	Sources   []string
	CreatedAt time.Time
}

// DependencyTracker maps views to their source metrics and vice-versa.
type DependencyTracker struct {
	dependencies map[string]*viewDependency
	reverseDeps  map[string][]string
	mu           sync.RWMutex
}

// NewDependencyTracker creates an empty tracker.
func NewDependencyTracker() *DependencyTracker {
	return &DependencyTracker{
		dependencies: make(map[string]*viewDependency),
		reverseDeps:  make(map[string][]string),
	}
}

// AddDependency records that viewName depends on sourceMetric.
func (dt *DependencyTracker) AddDependency(viewName, sourceMetric string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dep, ok := dt.dependencies[viewName]
	if !ok {
		dep = &viewDependency{ViewName: viewName, CreatedAt: time.Now()}
		dt.dependencies[viewName] = dep
	}
	for _, s := range dep.Sources {
		if s == sourceMetric {
			return
		}
	}
	dep.Sources = append(dep.Sources, sourceMetric)
	dt.reverseDeps[sourceMetric] = append(dt.reverseDeps[sourceMetric], viewName)
}

// RemoveDependency removes all dependency entries for the given view.
func (dt *DependencyTracker) RemoveDependency(viewName string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dep, ok := dt.dependencies[viewName]
	if !ok {
		return
	}
	for _, src := range dep.Sources {
		views := dt.reverseDeps[src]
		filtered := views[:0]
		for _, v := range views {
			if v != viewName {
				filtered = append(filtered, v)
			}
		}
		if len(filtered) == 0 {
			delete(dt.reverseDeps, src)
		} else {
			dt.reverseDeps[src] = filtered
		}
	}
	delete(dt.dependencies, viewName)
}

// GetAffectedViews returns the views that must be updated when the given metric changes.
func (dt *DependencyTracker) GetAffectedViews(metric string) []string {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	views := dt.reverseDeps[metric]
	out := make([]string, len(views))
	copy(out, views)
	return out
}

// GetDependencies returns the source metrics that a view depends on.
func (dt *DependencyTracker) GetDependencies(viewName string) []string {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	dep, ok := dt.dependencies[viewName]
	if !ok {
		return nil
	}
	out := make([]string, len(dep.Sources))
	copy(out, dep.Sources)
	return out
}

// DetectCycles returns any circular dependency chains found.
func (dt *DependencyTracker) DetectCycles() [][]string {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	visited := make(map[string]bool)
	inStack := make(map[string]bool)
	var cycles [][]string

	var dfs func(node string, path []string)
	dfs = func(node string, path []string) {
		if inStack[node] {
			start := -1
			for i, n := range path {
				if n == node {
					start = i
					break
				}
			}
			if start >= 0 {
				cycle := make([]string, len(path)-start)
				copy(cycle, path[start:])
				cycles = append(cycles, cycle)
			}
			return
		}
		if visited[node] {
			return
		}
		visited[node] = true
		inStack[node] = true
		path = append(path, node)
		if dep := dt.dependencies[node]; dep != nil {
			for _, src := range dep.Sources {
				if _, ok := dt.dependencies[src]; ok {
					dfs(src, path)
				}
			}
		}
		inStack[node] = false
	}

	for name := range dt.dependencies {
		dfs(name, nil)
	}
	return cycles
}

// --- Materialized View Store -----------------------------------------------

// MaterializedResult holds one aggregate group result.
type MaterializedResult struct {
	GroupKey               string
	Value                  float64
	Count                  int64
	WindowStart, WindowEnd int64
	ComputedAt             time.Time
}

type materializedView struct {
	definition   *MaterializedViewDefinition
	aggregator   *IncrementalAggregator
	results      map[string]*MaterializedResult
	lastRefresh  time.Time
	refreshCount int64
	stale        bool
}

// MaterializedViewStore manages all cached view results.
type MaterializedViewStore struct {
	views map[string]*materializedView
	mu    sync.RWMutex
}

func newMaterializedViewStore() *MaterializedViewStore {
	return &MaterializedViewStore{views: make(map[string]*materializedView)}
}

func (s *MaterializedViewStore) get(name string) (*materializedView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.views[name]
	return v, ok
}

func (s *MaterializedViewStore) all() []*materializedView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*materializedView, 0, len(s.views))
	for _, v := range s.views {
		out = append(out, v)
	}
	return out
}

// --- Info / Stats ----------------------------------------------------------

// MaterializedViewInfo exposes view metadata.
type MaterializedViewInfo struct {
	Name, SourceMetric string
	Aggregation        AggFunc
	Window             time.Duration
	GroupBy            []string
	RefreshMode        RefreshMode
	Stale              bool
	LastRefresh        time.Time
	ResultCount        int
	PointsProcessed    int64
}

// MaterializedViewStats reports engine-wide metrics.
type MaterializedViewStats struct {
	TotalViews, ActiveViews, StaleViews int
	TotalRefreshes, IncrementalUpdates  int64
	AvgRefreshLatency                   time.Duration
	MemoryUsedBytes                     int64
}

// --- Materialized View Engine ----------------------------------------------

// MaterializedViewEngine is the main orchestrator for incremental materialized views.
type MaterializedViewEngine struct {
	config  MaterializedViewConfig
	store   *MaterializedViewStore
	tracker *DependencyTracker
	db      *DB
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	// stats
	totalRefreshes     int64
	incrementalUpdates int64
	refreshLatencySum  time.Duration
}

// NewMaterializedViewEngine creates a new engine bound to the given DB.
func NewMaterializedViewEngine(db *DB, config MaterializedViewConfig) *MaterializedViewEngine {
	ctx, cancel := context.WithCancel(context.Background())
	return &MaterializedViewEngine{
		config: config, store: newMaterializedViewStore(),
		tracker: NewDependencyTracker(), db: db,
		ctx: ctx, cancel: cancel,
	}
}

// CreateView registers a new materialized view definition.
func (e *MaterializedViewEngine) CreateView(def *MaterializedViewDefinition) error {
	if def == nil {
		return fmt.Errorf("materialized_views: nil definition")
	}
	if def.Name == "" {
		return fmt.Errorf("materialized_views: empty view name")
	}
	if def.SourceMetric == "" {
		return fmt.Errorf("materialized_views: empty source metric")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.store.views[def.Name]; exists {
		return fmt.Errorf("materialized_views: view %q already exists", def.Name)
	}
	if len(e.store.views) >= e.config.MaxViews {
		return fmt.Errorf("materialized_views: max views (%d) reached", e.config.MaxViews)
	}
	if def.MaxStaleness == 0 {
		def.MaxStaleness = e.config.DefaultStaleness
	}
	mv := &materializedView{
		definition: def,
		aggregator: NewIncrementalAggregator(def.Aggregation, def.Window),
		results:    make(map[string]*MaterializedResult),
		stale:      true,
	}
	e.store.views[def.Name] = mv
	e.tracker.AddDependency(def.Name, def.SourceMetric)
	return nil
}

// DropView removes a view by name.
func (e *MaterializedViewEngine) DropView(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.store.views[name]; !ok {
		return fmt.Errorf("materialized_views: view %q not found", name)
	}
	delete(e.store.views, name)
	e.tracker.RemoveDependency(name)
	return nil
}

// GetView returns information about a single view.
func (e *MaterializedViewEngine) GetView(name string) (*MaterializedViewInfo, error) {
	mv, ok := e.store.get(name)
	if !ok {
		return nil, fmt.Errorf("materialized_views: view %q not found", name)
	}
	return e.viewInfo(mv), nil
}

// ListViews returns info for every registered view.
func (e *MaterializedViewEngine) ListViews() []*MaterializedViewInfo {
	all := e.store.all()
	infos := make([]*MaterializedViewInfo, len(all))
	for i, mv := range all {
		infos[i] = e.viewInfo(mv)
	}
	return infos
}

func (e *MaterializedViewEngine) viewInfo(mv *materializedView) *MaterializedViewInfo {
	def := mv.definition
	mv.aggregator.mu.RLock()
	var processed int64
	for _, ws := range mv.aggregator.windows {
		processed += ws.count
	}
	mv.aggregator.mu.RUnlock()
	return &MaterializedViewInfo{
		Name: def.Name, SourceMetric: def.SourceMetric,
		Aggregation: def.Aggregation, Window: def.Window,
		GroupBy: def.GroupBy, RefreshMode: def.RefreshMode,
		Stale: mv.stale, LastRefresh: mv.lastRefresh,
		ResultCount: len(mv.results), PointsProcessed: processed,
	}
}

// Query reads materialized results for the given time range.
func (e *MaterializedViewEngine) Query(viewName string, start, end int64) (*Result, error) {
	mv, ok := e.store.get(viewName)
	if !ok {
		return nil, fmt.Errorf("materialized_views: view %q not found", viewName)
	}
	if mv.definition.RefreshMode == RefreshLazy && mv.stale {
		if err := e.RefreshView(viewName); err != nil {
			return nil, err
		}
	}
	var points []Point
	for _, r := range mv.results {
		if r.WindowStart >= start && r.WindowEnd <= end {
			points = append(points, Point{
				Metric: viewName, Value: r.Value, Timestamp: r.WindowStart,
			})
		}
	}
	return &Result{Points: points}, nil
}

// OnWrite is called when new data arrives; it incrementally updates affected views.
func (e *MaterializedViewEngine) OnWrite(point *Point) {
	if point == nil {
		return
	}
	affected := e.tracker.GetAffectedViews(point.Metric)
	for _, viewName := range affected {
		mv, ok := e.store.get(viewName)
		if !ok || !mv.definition.Enabled {
			continue
		}
		if mv.definition.RefreshMode != RefreshEager {
			mv.stale = true
			continue
		}
		update, err := mv.aggregator.Apply(point)
		if err != nil {
			continue
		}
		windowEnd := update.WindowStart + mv.definition.Window.Nanoseconds()
		mv.results[update.GroupKey] = &MaterializedResult{
			GroupKey: update.GroupKey, Value: update.NewValue,
			Count: update.PointCount, WindowStart: update.WindowStart,
			WindowEnd: windowEnd, ComputedAt: time.Now(),
		}
		e.mu.Lock()
		e.incrementalUpdates++
		e.mu.Unlock()
	}
}

// RefreshView performs a full recomputation of a view by querying the source.
func (e *MaterializedViewEngine) RefreshView(name string) error {
	mv, ok := e.store.get(name)
	if !ok {
		return fmt.Errorf("materialized_views: view %q not found", name)
	}
	start := time.Now()
	q := &Query{
		Metric: mv.definition.SourceMetric, Tags: mv.definition.Tags,
		Aggregation: &Aggregation{Function: mv.definition.Aggregation, Window: mv.definition.Window},
		GroupBy:     mv.definition.GroupBy,
	}
	res, err := e.db.Execute(q)
	if err != nil {
		return fmt.Errorf("materialized_views: refresh %q: %w", name, err)
	}
	newResults := make(map[string]*MaterializedResult, len(res.Points))
	for _, p := range res.Points {
		key := buildGroupKey(&p)
		newResults[key] = &MaterializedResult{
			GroupKey: key, Value: p.Value, Count: 1,
			WindowStart: p.Timestamp, WindowEnd: p.Timestamp + mv.definition.Window.Nanoseconds(),
			ComputedAt: time.Now(),
		}
	}
	e.store.mu.Lock()
	mv.results = newResults
	mv.lastRefresh = time.Now()
	mv.refreshCount++
	mv.stale = false
	e.store.mu.Unlock()

	elapsed := time.Since(start)
	e.mu.Lock()
	e.totalRefreshes++
	e.refreshLatencySum += elapsed
	e.mu.Unlock()
	return nil
}

// Start launches the background staleness-check loop.
func (e *MaterializedViewEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.mu.Unlock()
	e.wg.Add(1)
	go e.stalenessLoop()
}

// Stop signals the background loop to stop and waits for it.
func (e *MaterializedViewEngine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.running = false
	e.cancel()
	e.mu.Unlock()
	e.wg.Wait()
}

func (e *MaterializedViewEngine) stalenessLoop() {
	defer e.wg.Done()
	interval := e.config.CheckInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.checkStaleness()
		}
	}
}

func (e *MaterializedViewEngine) checkStaleness() {
	for _, mv := range e.store.all() {
		if !mv.definition.Enabled || mv.definition.RefreshMode != RefreshPeriodic {
			continue
		}
		if time.Since(mv.lastRefresh) > mv.definition.MaxStaleness {
			mv.stale = true
			_ = e.RefreshView(mv.definition.Name)
		}
	}
}

// Stats returns engine-wide statistics.
func (e *MaterializedViewEngine) Stats() *MaterializedViewStats {
	e.mu.RLock()
	totalRefreshes := e.totalRefreshes
	incUpdates := e.incrementalUpdates
	latSum := e.refreshLatencySum
	e.mu.RUnlock()

	all := e.store.all()
	var active, stale int
	var memEstimate int64
	for _, mv := range all {
		if mv.definition.Enabled {
			active++
		}
		if mv.stale {
			stale++
		}
		memEstimate += int64(len(mv.results)) * 128 // rough estimate per entry
	}
	var avgLatency time.Duration
	if totalRefreshes > 0 {
		avgLatency = latSum / time.Duration(totalRefreshes)
	}
	return &MaterializedViewStats{
		TotalViews: len(all), ActiveViews: active, StaleViews: stale,
		TotalRefreshes: totalRefreshes, IncrementalUpdates: incUpdates,
		AvgRefreshLatency: avgLatency, MemoryUsedBytes: memEstimate,
	}
}

// --- Shadow Verifier -------------------------------------------------------

// ShadowDiscrepancy records a mismatch between incremental and full computation.
type ShadowDiscrepancy struct {
	GroupKey                           string
	IncrementalValue, FullComputeValue float64
	Difference                         float64
}

// ShadowVerifyResult is the outcome of a shadow verification run.
type ShadowVerifyResult struct {
	ViewName                    string
	Match                       bool
	IncrementalValue, FullValue float64
	Discrepancies               []ShadowDiscrepancy
	Duration                    time.Duration
}

// ShadowVerifier compares incremental results against full recomputation.
type ShadowVerifier struct {
	engine        *MaterializedViewEngine
	discrepancies []ShadowDiscrepancy
	mu            sync.RWMutex
}

// NewShadowVerifier creates a verifier bound to the given engine.
func NewShadowVerifier(engine *MaterializedViewEngine) *ShadowVerifier {
	return &ShadowVerifier{engine: engine}
}

// Verify compares the incremental state of a view with a full recomputation.
func (sv *ShadowVerifier) Verify(viewName string) (*ShadowVerifyResult, error) {
	start := time.Now()
	mv, ok := sv.engine.store.get(viewName)
	if !ok {
		return nil, fmt.Errorf("materialized_views: view %q not found", viewName)
	}
	// Capture current incremental results.
	incrementalResults := make(map[string]float64, len(mv.results))
	for k, r := range mv.results {
		incrementalResults[k] = r.Value
	}
	// Full recomputation via the DB.
	q := &Query{
		Metric: mv.definition.SourceMetric, Tags: mv.definition.Tags,
		Aggregation: &Aggregation{Function: mv.definition.Aggregation, Window: mv.definition.Window},
		GroupBy:     mv.definition.GroupBy,
	}
	res, err := sv.engine.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("materialized_views: shadow verify %q: %w", viewName, err)
	}
	fullResults := make(map[string]float64, len(res.Points))
	for _, p := range res.Points {
		fullResults[buildGroupKey(&p)] = p.Value
	}
	// Compare incremental vs full results.
	var discs []ShadowDiscrepancy
	var incSum, fullSum float64
	for key, incVal := range incrementalResults {
		incSum += incVal
		fVal, ok := fullResults[key]
		if ok {
			fullSum += fVal
		}
		diff := incVal - fVal
		if !ok || math.Abs(diff) > 1e-9 {
			discs = append(discs, ShadowDiscrepancy{
				GroupKey: key, IncrementalValue: incVal,
				FullComputeValue: fVal, Difference: diff,
			})
		}
	}
	for key, fVal := range fullResults {
		if _, seen := incrementalResults[key]; !seen {
			fullSum += fVal
			discs = append(discs, ShadowDiscrepancy{
				GroupKey: key, IncrementalValue: 0,
				FullComputeValue: fVal, Difference: -fVal,
			})
		}
	}
	sv.mu.Lock()
	sv.discrepancies = append(sv.discrepancies, discs...)
	sv.mu.Unlock()
	return &ShadowVerifyResult{
		ViewName: viewName, Match: len(discs) == 0,
		IncrementalValue: incSum, FullValue: fullSum,
		Discrepancies: discs, Duration: time.Since(start),
	}, nil
}
