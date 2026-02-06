package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Pipeline Manager — orchestrates multiple ETL pipelines
// ---------------------------------------------------------------------------

// ETLPipelineManagerConfig configures the pipeline manager.
type ETLPipelineManagerConfig struct {
	MaxPipelines     int           `json:"max_pipelines" yaml:"max_pipelines"`
	HealthInterval   time.Duration `json:"health_interval" yaml:"health_interval"`
	MetricsRetention time.Duration `json:"metrics_retention" yaml:"metrics_retention"`
}

// DefaultETLPipelineManagerConfig returns sensible defaults.
func DefaultETLPipelineManagerConfig() ETLPipelineManagerConfig {
	return ETLPipelineManagerConfig{
		MaxPipelines:     64,
		HealthInterval:   10 * time.Second,
		MetricsRetention: 24 * time.Hour,
	}
}

// PipelineState represents the lifecycle state of a managed pipeline.
type PipelineState string

const (
	PipelineStateCreated  PipelineState = "created"
	PipelineStateRunning  PipelineState = "running"
	PipelineStateStopped  PipelineState = "stopped"
	PipelineStateFailed   PipelineState = "failed"
	PipelineStateDraining PipelineState = "draining"
)

// ManagedPipeline wraps an ETLPipeline with management metadata.
type ManagedPipeline struct {
	Name      string            `json:"name"`
	State     PipelineState     `json:"state"`
	Pipeline  *ETLPipeline      `json:"-"`
	Spec      *PipelineSpec     `json:"spec,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	StartedAt *time.Time        `json:"started_at,omitempty"`
	StoppedAt *time.Time        `json:"stopped_at,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Error     string            `json:"error,omitempty"`
	mu        sync.RWMutex
}

// ETLPipelineManager manages the lifecycle of multiple ETL pipelines.
type ETLPipelineManager struct {
	db        *DB
	config    ETLPipelineManagerConfig
	pipelines map[string]*ManagedPipeline
	mu        sync.RWMutex
	running   atomic.Bool
	cancel    context.CancelFunc

	// stats
	totalCreated  atomic.Uint64
	totalStarted  atomic.Uint64
	totalStopped  atomic.Uint64
	totalFailed   atomic.Uint64
}

// NewETLPipelineManager creates a new pipeline manager.
func NewETLPipelineManager(db *DB, config ETLPipelineManagerConfig) *ETLPipelineManager {
	return &ETLPipelineManager{
		db:        db,
		config:    config,
		pipelines: make(map[string]*ManagedPipeline),
	}
}

// Start begins background health monitoring.
func (pm *ETLPipelineManager) Start() error {
	if pm.running.Swap(true) {
		return fmt.Errorf("pipeline manager already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	pm.cancel = cancel
	go pm.healthLoop(ctx)
	return nil
}

// Stop halts all pipelines and the manager.
func (pm *ETLPipelineManager) Stop() error {
	if !pm.running.Swap(false) {
		return nil
	}
	if pm.cancel != nil {
		pm.cancel()
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, mp := range pm.pipelines {
		mp.mu.Lock()
		if mp.State == PipelineStateRunning {
			if mp.Pipeline != nil {
				_ = mp.Pipeline.Stop()
			}
			mp.State = PipelineStateStopped
			now := time.Now()
			mp.StoppedAt = &now
		}
		mp.mu.Unlock()
	}
	return nil
}

// CreatePipeline registers a new pipeline.
func (pm *ETLPipelineManager) CreatePipeline(name string, pipeline *ETLPipeline, labels map[string]string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if len(pm.pipelines) >= pm.config.MaxPipelines {
		return fmt.Errorf("max pipelines (%d) reached", pm.config.MaxPipelines)
	}
	if _, exists := pm.pipelines[name]; exists {
		return fmt.Errorf("pipeline %q already exists", name)
	}
	pm.pipelines[name] = &ManagedPipeline{
		Name:      name,
		State:     PipelineStateCreated,
		Pipeline:  pipeline,
		CreatedAt: time.Now(),
		Labels:    labels,
	}
	pm.totalCreated.Add(1)
	return nil
}

// StartPipeline starts a named pipeline.
func (pm *ETLPipelineManager) StartPipeline(name string) error {
	pm.mu.RLock()
	mp, ok := pm.pipelines[name]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.State == PipelineStateRunning {
		return fmt.Errorf("pipeline %q already running", name)
	}
	if mp.Pipeline != nil {
		if err := mp.Pipeline.Start(); err != nil {
			mp.State = PipelineStateFailed
			mp.Error = err.Error()
			pm.totalFailed.Add(1)
			return err
		}
	}
	mp.State = PipelineStateRunning
	now := time.Now()
	mp.StartedAt = &now
	mp.Error = ""
	pm.totalStarted.Add(1)
	return nil
}

// StopPipeline stops a named pipeline.
func (pm *ETLPipelineManager) StopPipeline(name string) error {
	pm.mu.RLock()
	mp, ok := pm.pipelines[name]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.State != PipelineStateRunning {
		return fmt.Errorf("pipeline %q not running", name)
	}
	if mp.Pipeline != nil {
		_ = mp.Pipeline.Stop()
	}
	mp.State = PipelineStateStopped
	now := time.Now()
	mp.StoppedAt = &now
	pm.totalStopped.Add(1)
	return nil
}

// DeletePipeline removes a pipeline (must be stopped).
func (pm *ETLPipelineManager) DeletePipeline(name string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	mp, ok := pm.pipelines[name]
	if !ok {
		return fmt.Errorf("pipeline %q not found", name)
	}
	mp.mu.RLock()
	state := mp.State
	mp.mu.RUnlock()
	if state == PipelineStateRunning {
		return fmt.Errorf("cannot delete running pipeline %q", name)
	}
	delete(pm.pipelines, name)
	return nil
}

// ListPipelines returns all managed pipelines.
func (pm *ETLPipelineManager) ListPipelines() []*ManagedPipeline {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	result := make([]*ManagedPipeline, 0, len(pm.pipelines))
	for _, mp := range pm.pipelines {
		result = append(result, mp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// GetPipeline returns a specific pipeline by name.
func (pm *ETLPipelineManager) GetPipeline(name string) (*ManagedPipeline, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	mp, ok := pm.pipelines[name]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", name)
	}
	return mp, nil
}

// ETLPipelineManagerStats holds aggregate manager stats.
type ETLPipelineManagerStats struct {
	TotalPipelines int    `json:"total_pipelines"`
	Running        int    `json:"running"`
	Stopped        int    `json:"stopped"`
	Failed         int    `json:"failed"`
	TotalCreated   uint64 `json:"total_created"`
	TotalStarted   uint64 `json:"total_started"`
	TotalStopped   uint64 `json:"total_stopped"`
	TotalFailed    uint64 `json:"total_failed"`
}

// Stats returns aggregate statistics.
func (pm *ETLPipelineManager) Stats() ETLPipelineManagerStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	stats := ETLPipelineManagerStats{
		TotalPipelines: len(pm.pipelines),
		TotalCreated:   pm.totalCreated.Load(),
		TotalStarted:   pm.totalStarted.Load(),
		TotalStopped:   pm.totalStopped.Load(),
		TotalFailed:    pm.totalFailed.Load(),
	}
	for _, mp := range pm.pipelines {
		mp.mu.RLock()
		switch mp.State {
		case PipelineStateRunning:
			stats.Running++
		case PipelineStateStopped:
			stats.Stopped++
		case PipelineStateFailed:
			stats.Failed++
		}
		mp.mu.RUnlock()
	}
	return stats
}

func (pm *ETLPipelineManager) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(pm.config.HealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pm.checkHealth()
		}
	}
}

func (pm *ETLPipelineManager) checkHealth() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, mp := range pm.pipelines {
		mp.mu.Lock()
		if mp.State == PipelineStateRunning && mp.Pipeline != nil {
			stats := mp.Pipeline.Stats()
			if stats != nil && stats.PointsErrored > 1000 {
				mp.State = PipelineStateFailed
				mp.Error = "excessive errors detected"
				pm.totalFailed.Add(1)
			}
		}
		mp.mu.Unlock()
	}
}

// RegisterHTTPHandlers registers pipeline manager HTTP endpoints.
func (pm *ETLPipelineManager) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/etl/pipelines", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			pipelines := pm.ListPipelines()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(pipelines)
		case http.MethodPost:
			var req struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			pipeline := NewETLPipeline(DefaultETLPipelineConfig())
			if err := pm.CreatePipeline(req.Name, pipeline, req.Labels); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusCreated)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/etl/pipelines/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := pm.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})
}

// ---------------------------------------------------------------------------
// Windowed Join — time-based join of two point streams
// ---------------------------------------------------------------------------

// WindowedJoinConfig configures a windowed join.
type WindowedJoinConfig struct {
	LeftMetric  string        `json:"left_metric" yaml:"left_metric"`
	RightMetric string        `json:"right_metric" yaml:"right_metric"`
	WindowSize  time.Duration `json:"window_size" yaml:"window_size"`
	OutputMetric string       `json:"output_metric" yaml:"output_metric"`
	JoinType    WindowedJoinType      `json:"join_type" yaml:"join_type"`
}

// WindowedJoinType specifies the type of join for windowed operations.
type WindowedJoinType int

const (
	WindowedJoinInner WindowedJoinType = iota
	WindowedJoinLeftOuter
	WindowedJoinRightOuter
	WindowedJoinFullOuter
)

// WindowedJoin performs time-aligned joins between two point streams.
type WindowedJoin struct {
	config     WindowedJoinConfig
	leftBuf    []*Point
	rightBuf   []*Point
	mu         sync.Mutex
	processed  atomic.Uint64
	emitted    atomic.Uint64
}

// NewWindowedJoin creates a windowed join operator.
func NewWindowedJoin(config WindowedJoinConfig) *WindowedJoin {
	if config.WindowSize == 0 {
		config.WindowSize = time.Minute
	}
	if config.OutputMetric == "" {
		config.OutputMetric = config.LeftMetric + "_joined_" + config.RightMetric
	}
	return &WindowedJoin{
		config:   config,
		leftBuf:  make([]*Point, 0, 256),
		rightBuf: make([]*Point, 0, 256),
	}
}

// AddLeft adds a point from the left stream.
func (wj *WindowedJoin) AddLeft(p *Point) {
	wj.mu.Lock()
	defer wj.mu.Unlock()
	wj.leftBuf = append(wj.leftBuf, p)
}

// AddRight adds a point from the right stream.
func (wj *WindowedJoin) AddRight(p *Point) {
	wj.mu.Lock()
	defer wj.mu.Unlock()
	wj.rightBuf = append(wj.rightBuf, p)
}

// Emit produces joined points within the window, then flushes matched entries.
func (wj *WindowedJoin) Emit() []*Point {
	wj.mu.Lock()
	defer wj.mu.Unlock()

	windowNs := wj.config.WindowSize.Nanoseconds()
	var results []*Point
	leftMatched := make(map[int]bool)
	rightMatched := make(map[int]bool)

	for li, lp := range wj.leftBuf {
		for ri, rp := range wj.rightBuf {
			diff := lp.Timestamp - rp.Timestamp
			if diff < 0 {
				diff = -diff
			}
			if diff <= windowNs {
				joined := wj.mergePoints(lp, rp)
				results = append(results, joined)
				leftMatched[li] = true
				rightMatched[ri] = true
			}
		}
	}

	// Handle outer joins
	if wj.config.JoinType == WindowedJoinLeftOuter || wj.config.JoinType == WindowedJoinFullOuter {
		for li, lp := range wj.leftBuf {
			if !leftMatched[li] {
				p := &Point{
					Metric:    wj.config.OutputMetric,
					Timestamp: lp.Timestamp,
					Value:     lp.Value,
					Tags:      etlCloneTags(lp.Tags),
				}
				results = append(results, p)
			}
		}
	}
	if wj.config.JoinType == WindowedJoinRightOuter || wj.config.JoinType == WindowedJoinFullOuter {
		for ri, rp := range wj.rightBuf {
			if !rightMatched[ri] {
				p := &Point{
					Metric:    wj.config.OutputMetric,
					Timestamp: rp.Timestamp,
					Value:     rp.Value,
					Tags:      etlCloneTags(rp.Tags),
				}
				results = append(results, p)
			}
		}
	}

	wj.processed.Add(uint64(len(wj.leftBuf) + len(wj.rightBuf)))
	wj.emitted.Add(uint64(len(results)))

	// Flush buffers
	wj.leftBuf = wj.leftBuf[:0]
	wj.rightBuf = wj.rightBuf[:0]

	return results
}

func (wj *WindowedJoin) mergePoints(left, right *Point) *Point {
	tags := etlCloneTags(left.Tags)
	for k, v := range right.Tags {
		if _, exists := tags[k]; !exists {
			tags["right_"+k] = v
		}
	}
	return &Point{
		Metric:    wj.config.OutputMetric,
		Timestamp: left.Timestamp,
		Value:     left.Value,
		Tags:      tags,
	}
}

func etlCloneTags(src map[string]string) map[string]string {
	if src == nil {
		return make(map[string]string)
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// WindowedJoinStats returns join statistics.
type WindowedJoinStats struct {
	Processed uint64 `json:"processed"`
	Emitted   uint64 `json:"emitted"`
}

// Stats returns join statistics.
func (wj *WindowedJoin) Stats() WindowedJoinStats {
	return WindowedJoinStats{
		Processed: wj.processed.Load(),
		Emitted:   wj.emitted.Load(),
	}
}

// ---------------------------------------------------------------------------
// Enrichment Lookup — enrich points from Chronicle data
// ---------------------------------------------------------------------------

// EnrichmentLookupConfig configures enrichment.
type EnrichmentLookupConfig struct {
	LookupMetric string        `json:"lookup_metric" yaml:"lookup_metric"`
	KeyTag       string        `json:"key_tag" yaml:"key_tag"`
	ValueTag     string        `json:"value_tag" yaml:"value_tag"`
	CacheTTL     time.Duration `json:"cache_ttl" yaml:"cache_ttl"`
	MaxCacheSize int           `json:"max_cache_size" yaml:"max_cache_size"`
}

// DefaultEnrichmentLookupConfig returns defaults.
func DefaultEnrichmentLookupConfig() EnrichmentLookupConfig {
	return EnrichmentLookupConfig{
		CacheTTL:     5 * time.Minute,
		MaxCacheSize: 10000,
	}
}

type enrichmentCacheEntry struct {
	value     string
	expiresAt time.Time
}

// EnrichmentLookup enriches points with data from Chronicle queries.
type EnrichmentLookup struct {
	db     *DB
	config EnrichmentLookupConfig
	cache  map[string]*enrichmentCacheEntry
	mu     sync.RWMutex
	hits   atomic.Uint64
	misses atomic.Uint64
}

// NewEnrichmentLookup creates a new enrichment lookup.
func NewEnrichmentLookup(db *DB, config EnrichmentLookupConfig) *EnrichmentLookup {
	return &EnrichmentLookup{
		db:     db,
		config: config,
		cache:  make(map[string]*enrichmentCacheEntry),
	}
}

// Enrich enriches a point using the lookup.
func (el *EnrichmentLookup) Enrich(p *Point) (*Point, error) {
	if p == nil {
		return nil, fmt.Errorf("nil point")
	}

	key := ""
	if el.config.KeyTag != "" && p.Tags != nil {
		key = p.Tags[el.config.KeyTag]
	}
	if key == "" {
		key = p.Metric
	}

	// Check cache
	el.mu.RLock()
	entry, cached := el.cache[key]
	el.mu.RUnlock()

	if cached && time.Now().Before(entry.expiresAt) {
		el.hits.Add(1)
		enriched := &Point{
			Metric:    p.Metric,
			Timestamp: p.Timestamp,
			Value:     p.Value,
			Tags:      etlCloneTags(p.Tags),
		}
		if el.config.ValueTag != "" {
			enriched.Tags[el.config.ValueTag] = entry.value
		} else {
			enriched.Tags["enriched"] = entry.value
		}
		return enriched, nil
	}

	el.misses.Add(1)

	// Lookup from DB
	lookupMetric := el.config.LookupMetric
	if lookupMetric == "" {
		lookupMetric = key
	}
	result, err := el.db.Execute(&Query{Metric: lookupMetric})
	if err != nil || result == nil || len(result.Points) == 0 {
		return p, nil // no enrichment available
	}

	// Use latest value as enrichment
	latest := result.Points[len(result.Points)-1]
	value := fmt.Sprintf("%.4f", latest.Value)

	// Cache the result
	el.mu.Lock()
	if len(el.cache) >= el.config.MaxCacheSize {
		// Evict oldest entries
		for k := range el.cache {
			delete(el.cache, k)
			if len(el.cache) < el.config.MaxCacheSize/2 {
				break
			}
		}
	}
	el.cache[key] = &enrichmentCacheEntry{
		value:     value,
		expiresAt: time.Now().Add(el.config.CacheTTL),
	}
	el.mu.Unlock()

	enriched := &Point{
		Metric:    p.Metric,
		Timestamp: p.Timestamp,
		Value:     p.Value,
		Tags:      etlCloneTags(p.Tags),
	}
	if el.config.ValueTag != "" {
		enriched.Tags[el.config.ValueTag] = value
	} else {
		enriched.Tags["enriched"] = value
	}
	return enriched, nil
}

// EnrichmentLookupStats holds lookup stats.
type EnrichmentLookupStats struct {
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	CacheSize int    `json:"cache_size"`
	HitRate   float64 `json:"hit_rate"`
}

// Stats returns enrichment lookup stats.
func (el *EnrichmentLookup) Stats() EnrichmentLookupStats {
	el.mu.RLock()
	cacheSize := len(el.cache)
	el.mu.RUnlock()
	hits := el.hits.Load()
	misses := el.misses.Load()
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	return EnrichmentLookupStats{
		Hits:      hits,
		Misses:    misses,
		CacheSize: cacheSize,
		HitRate:   math.Round(hitRate*10000) / 10000,
	}
}

// ---------------------------------------------------------------------------
// Pipeline Spec — YAML-based pipeline definition
// ---------------------------------------------------------------------------

// PipelineSpec defines a pipeline in YAML.
type PipelineSpec struct {
	Name       string            `json:"name" yaml:"name"`
	Version    string            `json:"version" yaml:"version"`
	Labels     map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Source     SourceSpec        `json:"source" yaml:"source"`
	Transforms []TransformSpec  `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	Sinks      []SinkSpec       `json:"sinks" yaml:"sinks"`
	Joins      []JoinSpec       `json:"joins,omitempty" yaml:"joins,omitempty"`
	Settings   SettingsSpec     `json:"settings,omitempty" yaml:"settings,omitempty"`
}

// SourceSpec defines a pipeline source.
type SourceSpec struct {
	Type     string            `json:"type" yaml:"type"`
	Metric   string            `json:"metric,omitempty" yaml:"metric,omitempty"`
	Interval string            `json:"interval,omitempty" yaml:"interval,omitempty"`
	Config   map[string]string `json:"config,omitempty" yaml:"config,omitempty"`
}

// TransformSpec defines a transform step.
type TransformSpec struct {
	Type      string            `json:"type" yaml:"type"`
	Name      string            `json:"name,omitempty" yaml:"name,omitempty"`
	Config    map[string]string `json:"config,omitempty" yaml:"config,omitempty"`
	Condition string            `json:"condition,omitempty" yaml:"condition,omitempty"`
}

// SinkSpec defines a pipeline sink.
type SinkSpec struct {
	Type   string            `json:"type" yaml:"type"`
	Config map[string]string `json:"config,omitempty" yaml:"config,omitempty"`
}

// JoinSpec defines a windowed join.
type JoinSpec struct {
	LeftMetric  string `json:"left_metric" yaml:"left_metric"`
	RightMetric string `json:"right_metric" yaml:"right_metric"`
	WindowSize  string `json:"window_size" yaml:"window_size"`
	OutputMetric string `json:"output_metric" yaml:"output_metric"`
	JoinType    string `json:"join_type" yaml:"join_type"`
}

// SettingsSpec holds pipeline-level settings.
type SettingsSpec struct {
	Workers        int    `json:"workers,omitempty" yaml:"workers,omitempty"`
	BufferSize     int    `json:"buffer_size,omitempty" yaml:"buffer_size,omitempty"`
	Backpressure   string `json:"backpressure,omitempty" yaml:"backpressure,omitempty"`
	CheckpointPath string `json:"checkpoint_path,omitempty" yaml:"checkpoint_path,omitempty"`
}

// ParsePipelineSpec parses a YAML pipeline specification.
func ParsePipelineSpec(data []byte) (*PipelineSpec, error) {
	var spec PipelineSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parsing pipeline spec: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &spec, nil
}

// Validate checks the pipeline spec for errors.
func (s *PipelineSpec) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("pipeline name is required")
	}
	if s.Source.Type == "" {
		return fmt.Errorf("source type is required")
	}
	if len(s.Sinks) == 0 {
		return fmt.Errorf("at least one sink is required")
	}
	for i, sink := range s.Sinks {
		if sink.Type == "" {
			return fmt.Errorf("sink[%d] type is required", i)
		}
	}
	return nil
}

// ToYAML serializes the spec to YAML.
func (s *PipelineSpec) ToYAML() ([]byte, error) {
	return yaml.Marshal(s)
}

// BuildPipeline builds an ETLPipeline from a spec.
func (s *PipelineSpec) BuildPipeline(db *DB) (*ETLPipeline, error) {
	config := DefaultETLPipelineConfig()
	config.Name = s.Name

	if s.Settings.Workers > 0 {
		config.Workers = s.Settings.Workers
	}
	if s.Settings.BufferSize > 0 {
		config.MaxBufferSize = s.Settings.BufferSize
	}
	if s.Settings.CheckpointPath != "" {
		// checkpoint path is managed externally
	}
	switch s.Settings.Backpressure {
	case "drop":
		config.BackpressureStrategy = BackpressureDrop
	case "block":
		config.BackpressureStrategy = BackpressureBlock
	case "sample":
		config.BackpressureStrategy = BackpressureSample
	}

	pipeline := NewETLPipeline(config)

	// Set source
	switch s.Source.Type {
	case "chronicle", "database":
		metric := s.Source.Metric
		if metric == "" {
			metric = "*"
		}
		interval := 10 * time.Second
		if s.Source.Interval != "" {
			if d, err := time.ParseDuration(s.Source.Interval); err == nil {
				interval = d
			}
		}
		pipeline.From(NewETLDatabaseSource(db, metric, interval))
	default:
		return nil, fmt.Errorf("unsupported source type: %s", s.Source.Type)
	}

	// Add transforms
	for _, t := range s.Transforms {
		switch t.Type {
		case "filter":
			if cond := t.Config["metric"]; cond != "" {
				metric := cond
				pipeline.Filter(func(p *Point) bool {
					return p.Metric == metric
				})
			}
		case "rename":
			from := t.Config["from"]
			to := t.Config["to"]
			if from != "" && to != "" {
				pipeline.Transform(func(p *Point) (*Point, error) {
					if p.Metric == from {
						p.Metric = to
					}
					return p, nil
				})
			}
		case "scale":
			factorStr := t.Config["factor"]
			if factorStr != "" {
				var factor float64
				if _, err := fmt.Sscanf(factorStr, "%f", &factor); err == nil {
					pipeline.Transform(func(p *Point) (*Point, error) {
						p.Value *= factor
						return p, nil
					})
				}
			}
		case "tag":
			key := t.Config["key"]
			value := t.Config["value"]
			if key != "" {
				pipeline.Transform(func(p *Point) (*Point, error) {
					if p.Tags == nil {
						p.Tags = make(map[string]string)
					}
					p.Tags[key] = value
					return p, nil
				})
			}
		}
	}

	// Set sink
	for _, sink := range s.Sinks {
		switch sink.Type {
		case "chronicle", "database":
			pipeline.To(NewETLDatabaseSink(db))
		default:
			return nil, fmt.Errorf("unsupported sink type: %s", sink.Type)
		}
	}

	return pipeline, nil
}

// sink for pipelines is defined in streaming_etl.go (ETLDatabaseSink)
