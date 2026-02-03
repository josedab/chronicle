package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// BackpressureStrategy defines how the pipeline handles slow consumers.
type BackpressureStrategy int

const (
	BackpressureDrop   BackpressureStrategy = iota
	BackpressureBlock
	BackpressureSample
)

// ETLErrorHandler is a callback invoked when a stage encounters an error.
type ETLErrorHandler func(error, *Point)

// ETLTransformFunc transforms a point, returning a modified copy or an error.
type ETLTransformFunc func(*Point) (*Point, error)

// ETLFilterFunc returns true if a point should pass through the pipeline.
type ETLFilterFunc func(*Point) bool

// ETLEnrichFunc enriches a point with additional data.
type ETLEnrichFunc func(*Point) (*Point, error)

// ETLRouteFunc returns a routing key for a point.
type ETLRouteFunc func(*Point) string

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// ETLSource produces points for a pipeline.
type ETLSource interface {
	Open(ctx context.Context) error
	Read(ctx context.Context) (*Point, error)
	Close() error
}

// ETLSink consumes points produced by a pipeline.
type ETLSink interface {
	Open(ctx context.Context) error
	Write(ctx context.Context, point *Point) error
	Flush(ctx context.Context) error
	Close() error
}

// ETLStage is a single processing step in a pipeline.
type ETLStage interface {
	Process(ctx context.Context, point *Point) ([]*Point, error)
	Name() string
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// ETLPipelineConfig holds configuration for an ETL pipeline.
type ETLPipelineConfig struct {
	Name                 string
	MaxBufferSize        int
	CheckpointInterval   time.Duration
	Workers              int
	BackpressureStrategy BackpressureStrategy
	ErrorHandler         ETLErrorHandler
}

// DefaultETLPipelineConfig returns sensible defaults for a pipeline.
func DefaultETLPipelineConfig() ETLPipelineConfig {
	return ETLPipelineConfig{
		Name:                 "default",
		MaxBufferSize:        10000,
		CheckpointInterval:   30 * time.Second,
		Workers:              1,
		BackpressureStrategy: BackpressureBlock,
	}
}

// ---------------------------------------------------------------------------
// Pipeline Stats
// ---------------------------------------------------------------------------

// ETLPipelineStats tracks runtime metrics for a pipeline.
type ETLPipelineStats struct {
	PointsRead           uint64
	PointsWritten        uint64
	PointsFiltered       uint64
	PointsErrored        uint64
	BytesProcessed       int64
	Uptime               time.Duration
	StageLatencies       map[string]time.Duration
	BackpressureEvents   uint64
	LastCheckpoint       time.Time
	startTime            time.Time
	mu                   sync.RWMutex
}

func newETLPipelineStats() *ETLPipelineStats {
	return &ETLPipelineStats{
		StageLatencies: make(map[string]time.Duration),
		startTime:      time.Now(),
	}
}

func (s *ETLPipelineStats) snapshot() *ETLPipelineStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	latencies := make(map[string]time.Duration, len(s.StageLatencies))
	for k, v := range s.StageLatencies {
		latencies[k] = v
	}
	return &ETLPipelineStats{
		PointsRead:         atomic.LoadUint64(&s.PointsRead),
		PointsWritten:      atomic.LoadUint64(&s.PointsWritten),
		PointsFiltered:     atomic.LoadUint64(&s.PointsFiltered),
		PointsErrored:      atomic.LoadUint64(&s.PointsErrored),
		BytesProcessed:     atomic.LoadInt64(&s.BytesProcessed),
		Uptime:             time.Since(s.startTime),
		StageLatencies:     latencies,
		BackpressureEvents: atomic.LoadUint64(&s.BackpressureEvents),
		LastCheckpoint:     s.LastCheckpoint,
	}
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

// ETLCheckpoint persists pipeline progress so it can resume after a restart.
type ETLCheckpoint struct {
	PipelineName    string    `json:"pipeline_name"`
	LastProcessed   int64     `json:"last_processed"`
	PointsProcessed uint64    `json:"points_processed"`
	SavedAt         time.Time `json:"saved_at"`
}

func (c *ETLCheckpoint) save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("etl checkpoint marshal: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

func loadETLCheckpoint(path string) (*ETLCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cp ETLCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("etl checkpoint unmarshal: %w", err)
	}
	return &cp, nil
}

// ---------------------------------------------------------------------------
// ETL Pipeline
// ---------------------------------------------------------------------------

// ETLPipeline orchestrates an Extract-Transform-Load flow over time-series points.
type ETLPipeline struct {
	config     ETLPipelineConfig
	source     ETLSource
	stages     []ETLStage
	sink       ETLSink
	running    bool
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	stats      *ETLPipelineStats
	checkpoint *ETLCheckpoint
}

// NewETLPipeline creates a new pipeline with the given configuration.
func NewETLPipeline(config ETLPipelineConfig) *ETLPipeline {
	if config.MaxBufferSize <= 0 {
		config.MaxBufferSize = 10000
	}
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.CheckpointInterval <= 0 {
		config.CheckpointInterval = 30 * time.Second
	}
	return &ETLPipeline{
		config: config,
		stats:  newETLPipelineStats(),
		checkpoint: &ETLCheckpoint{
			PipelineName: config.Name,
		},
	}
}

// From sets the pipeline source (fluent API).
func (p *ETLPipeline) From(source ETLSource) *ETLPipeline {
	p.source = source
	return p
}

// Transform adds a transform stage to the pipeline.
func (p *ETLPipeline) Transform(fn ETLTransformFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlTransformStage{fn: fn})
	return p
}

// Filter adds a filter stage to the pipeline.
func (p *ETLPipeline) Filter(fn ETLFilterFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlFilterStage{fn: fn})
	return p
}

// Enrich adds an enrichment stage to the pipeline.
func (p *ETLPipeline) Enrich(fn ETLEnrichFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlEnrichStage{fn: fn})
	return p
}

// Route adds a routing stage to the pipeline.
func (p *ETLPipeline) Route(fn ETLRouteFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlRouteStage{fn: fn})
	return p
}

// Aggregate adds a windowed aggregation stage to the pipeline.
func (p *ETLPipeline) Aggregate(config ETLAggregateConfig) *ETLPipeline {
	p.stages = append(p.stages, newETLAggregateStage(config))
	return p
}

// To sets the pipeline sink (fluent API).
func (p *ETLPipeline) To(sink ETLSink) *ETLPipeline {
	p.sink = sink
	return p
}

// Start begins pipeline processing.
func (p *ETLPipeline) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("etl pipeline %q already running", p.config.Name)
	}
	if p.source == nil {
		return fmt.Errorf("etl pipeline %q has no source", p.config.Name)
	}
	if p.sink == nil {
		return fmt.Errorf("etl pipeline %q has no sink", p.config.Name)
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.stats = newETLPipelineStats()

	if err := p.source.Open(p.ctx); err != nil {
		return fmt.Errorf("etl source open: %w", err)
	}
	if err := p.sink.Open(p.ctx); err != nil {
		_ = p.source.Close()
		return fmt.Errorf("etl sink open: %w", err)
	}

	p.running = true

	for i := 0; i < p.config.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	p.wg.Add(1)
	go p.checkpointLoop()

	return nil
}

// Stop gracefully shuts down the pipeline.
func (p *ETLPipeline) Stop() error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return fmt.Errorf("etl pipeline %q not running", p.config.Name)
	}
	p.cancel()
	p.mu.Unlock()

	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false

	if err := p.sink.Flush(context.Background()); err != nil {
		return fmt.Errorf("etl sink flush: %w", err)
	}
	if err := p.sink.Close(); err != nil {
		return fmt.Errorf("etl sink close: %w", err)
	}
	if err := p.source.Close(); err != nil {
		return fmt.Errorf("etl source close: %w", err)
	}
	return nil
}

// Stats returns a snapshot of the pipeline statistics.
func (p *ETLPipeline) Stats() *ETLPipelineStats {
	return p.stats.snapshot()
}

func (p *ETLPipeline) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		pt, err := p.source.Read(p.ctx)
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}
			atomic.AddUint64(&p.stats.PointsErrored, 1)
			if p.config.ErrorHandler != nil {
				p.config.ErrorHandler(err, nil)
			}
			continue
		}
		if pt == nil {
			continue
		}

		atomic.AddUint64(&p.stats.PointsRead, 1)

		points := []*Point{pt}
		filtered := false
		for _, stage := range p.stages {
			start := time.Now()
			var next []*Point
			for _, sp := range points {
				out, sErr := stage.Process(p.ctx, sp)
				if sErr != nil {
					atomic.AddUint64(&p.stats.PointsErrored, 1)
					if p.config.ErrorHandler != nil {
						p.config.ErrorHandler(sErr, sp)
					}
					continue
				}
				next = append(next, out...)
			}
			elapsed := time.Since(start)
			p.stats.mu.Lock()
			p.stats.StageLatencies[stage.Name()] = elapsed
			p.stats.mu.Unlock()

			points = next
			if len(points) == 0 {
				filtered = true
				break
			}
		}

		if filtered {
			atomic.AddUint64(&p.stats.PointsFiltered, 1)
			continue
		}

		for _, out := range points {
			if wErr := p.sink.Write(p.ctx, out); wErr != nil {
				atomic.AddUint64(&p.stats.PointsErrored, 1)
				if p.config.ErrorHandler != nil {
					p.config.ErrorHandler(wErr, out)
				}
				continue
			}
			atomic.AddUint64(&p.stats.PointsWritten, 1)
			p.checkpoint.LastProcessed = out.Timestamp
			atomic.AddUint64(&p.checkpoint.PointsProcessed, 1)
		}
	}
}

func (p *ETLPipeline) checkpointLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.config.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkpoint.SavedAt = time.Now()
			path := fmt.Sprintf("checkpoint_%s.json", p.config.Name)
			_ = p.checkpoint.save(path)
			p.stats.mu.Lock()
			p.stats.LastCheckpoint = p.checkpoint.SavedAt
			p.stats.mu.Unlock()
		}
	}
}

// ---------------------------------------------------------------------------
// Built-in Stages
// ---------------------------------------------------------------------------

type etlTransformStage struct{ fn ETLTransformFunc }

func (s *etlTransformStage) Name() string { return "transform" }
func (s *etlTransformStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	out, err := s.fn(pt)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return []*Point{out}, nil
}

type etlFilterStage struct{ fn ETLFilterFunc }

func (s *etlFilterStage) Name() string { return "filter" }
func (s *etlFilterStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	if s.fn(pt) {
		return []*Point{pt}, nil
	}
	return nil, nil
}

type etlEnrichStage struct{ fn ETLEnrichFunc }

func (s *etlEnrichStage) Name() string { return "enrich" }
func (s *etlEnrichStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	out, err := s.fn(pt)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return []*Point{out}, nil
}

type etlRouteStage struct{ fn ETLRouteFunc }

func (s *etlRouteStage) Name() string { return "route" }
func (s *etlRouteStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	key := s.fn(pt)
	if pt.Tags == nil {
		pt.Tags = make(map[string]string)
	}
	pt.Tags["_route"] = key
	return []*Point{pt}, nil
}

// ---------------------------------------------------------------------------
// Windowed Aggregation Stage
// ---------------------------------------------------------------------------

// ETLAggregateConfig configures the windowed aggregation stage.
type ETLAggregateConfig struct {
	Window   time.Duration
	Function AggFunc
	GroupBy  []string
	EmitMode string // "onClose" or "onUpdate"
}

type etlAggregateStage struct {
	config  ETLAggregateConfig
	mu      sync.Mutex
	windows map[string]*etlAggWindow
}

type etlAggWindow struct {
	sum   float64
	count int64
	min   float64
	max   float64
	start int64
}

func newETLAggregateStage(config ETLAggregateConfig) *etlAggregateStage {
	if config.EmitMode == "" {
		config.EmitMode = "onClose"
	}
	return &etlAggregateStage{
		config:  config,
		windows: make(map[string]*etlAggWindow),
	}
}

func (s *etlAggregateStage) Name() string { return "aggregate" }

func (s *etlAggregateStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.groupKey(pt)
	windowNs := s.config.Window.Nanoseconds()
	windowStart := (pt.Timestamp / windowNs) * windowNs

	wk := fmt.Sprintf("%s_%d", key, windowStart)
	w, exists := s.windows[wk]
	if !exists {
		w = &etlAggWindow{
			min:   pt.Value,
			max:   pt.Value,
			start: windowStart,
		}
		s.windows[wk] = w
	}

	w.sum += pt.Value
	w.count++
	if pt.Value < w.min {
		w.min = pt.Value
	}
	if pt.Value > w.max {
		w.max = pt.Value
	}

	var result []*Point

	// Emit closed windows
	for k, aw := range s.windows {
		if k == wk {
			continue
		}
		if windowStart-aw.start >= windowNs {
			result = append(result, s.emit(pt.Metric, key, aw))
			delete(s.windows, k)
		}
	}

	if s.config.EmitMode == "onUpdate" {
		result = append(result, s.emit(pt.Metric, key, w))
	}

	return result, nil
}

func (s *etlAggregateStage) emit(metric, group string, w *etlAggWindow) *Point {
	var val float64
	switch s.config.Function {
	case AggSum:
		val = w.sum
	case AggCount:
		val = float64(w.count)
	case AggMean:
		if w.count > 0 {
			val = w.sum / float64(w.count)
		}
	case AggMin:
		val = w.min
	case AggMax:
		val = w.max
	default:
		val = w.sum
	}
	return &Point{
		Metric:    metric + "_agg",
		Tags:      map[string]string{"group": group},
		Value:     val,
		Timestamp: w.start,
	}
}

func (s *etlAggregateStage) groupKey(pt *Point) string {
	if len(s.config.GroupBy) == 0 {
		return "_all"
	}
	key := ""
	for i, tag := range s.config.GroupBy {
		if i > 0 {
			key += "|"
		}
		if pt.Tags != nil {
			key += pt.Tags[tag]
		}
	}
	return key
}

// ---------------------------------------------------------------------------
// Built-in Sources
// ---------------------------------------------------------------------------

// ETLQueryFunc is a function that fetches points from a DB for a given time range.
type ETLQueryFunc func(metric string, start, end int64) ([]Point, error)

// ETLDatabaseSource reads points from a Chronicle DB by polling a metric at a
// fixed interval using a caller-supplied query function.
type ETLDatabaseSource struct {
	db       *DB
	metric   string
	interval time.Duration
	queryFn  ETLQueryFunc
	lastTS   int64
	ticker   *time.Ticker
	points   []*Point
	idx      int
}

// NewETLDatabaseSource creates a source that polls db for the given metric.
// If queryFn is nil the source will return an error on Open.
func NewETLDatabaseSource(db *DB, metric string, interval time.Duration) *ETLDatabaseSource {
	return &ETLDatabaseSource{
		db:       db,
		metric:   metric,
		interval: interval,
	}
}

// WithQueryFunc sets the function used to fetch points from the database.
func (s *ETLDatabaseSource) WithQueryFunc(fn ETLQueryFunc) *ETLDatabaseSource {
	s.queryFn = fn
	return s
}

func (s *ETLDatabaseSource) Open(_ context.Context) error {
	if s.queryFn == nil {
		return fmt.Errorf("etl database source: no query function configured")
	}
	s.ticker = time.NewTicker(s.interval)
	s.lastTS = 0
	return nil
}

func (s *ETLDatabaseSource) Read(ctx context.Context) (*Point, error) {
	for {
		if s.idx < len(s.points) {
			pt := s.points[s.idx]
			s.idx++
			if pt.Timestamp > s.lastTS {
				s.lastTS = pt.Timestamp
			}
			return pt, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ticker.C:
			s.points = s.fetchPoints()
			s.idx = 0
		}
	}
}

func (s *ETLDatabaseSource) fetchPoints() []*Point {
	now := time.Now().UnixNano()
	start := s.lastTS
	if start == 0 {
		start = now - s.interval.Nanoseconds()
	}
	results, err := s.queryFn(s.metric, start, now)
	if err != nil || len(results) == 0 {
		return nil
	}
	pts := make([]*Point, len(results))
	for i := range results {
		pts[i] = &results[i]
	}
	return pts
}

func (s *ETLDatabaseSource) Close() error {
	if s.ticker != nil {
		s.ticker.Stop()
	}
	return nil
}

// ETLChannelSource reads points from a Go channel.
type ETLChannelSource struct {
	ch <-chan *Point
}

// NewETLChannelSource creates a source backed by a channel.
func NewETLChannelSource(ch <-chan *Point) *ETLChannelSource {
	return &ETLChannelSource{ch: ch}
}

func (s *ETLChannelSource) Open(_ context.Context) error { return nil }

func (s *ETLChannelSource) Read(ctx context.Context) (*Point, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pt, ok := <-s.ch:
		if !ok {
			return nil, fmt.Errorf("etl channel source closed")
		}
		return pt, nil
	}
}

func (s *ETLChannelSource) Close() error { return nil }

// ---------------------------------------------------------------------------
// Built-in Sinks
// ---------------------------------------------------------------------------

// ETLDatabaseSink writes points to a Chronicle DB.
type ETLDatabaseSink struct {
	db *DB
}

// NewETLDatabaseSink creates a sink that writes to db.
func NewETLDatabaseSink(db *DB) *ETLDatabaseSink {
	return &ETLDatabaseSink{db: db}
}

func (s *ETLDatabaseSink) Open(_ context.Context) error  { return nil }
func (s *ETLDatabaseSink) Flush(_ context.Context) error { return nil }
func (s *ETLDatabaseSink) Close() error                  { return nil }

func (s *ETLDatabaseSink) Write(_ context.Context, pt *Point) error {
	return s.db.Write(*pt)
}

// ETLChannelSink writes points to a Go channel.
type ETLChannelSink struct {
	ch chan<- *Point
}

// NewETLChannelSink creates a sink backed by a channel.
func NewETLChannelSink(ch chan<- *Point) *ETLChannelSink {
	return &ETLChannelSink{ch: ch}
}

func (s *ETLChannelSink) Open(_ context.Context) error  { return nil }
func (s *ETLChannelSink) Flush(_ context.Context) error { return nil }
func (s *ETLChannelSink) Close() error                  { return nil }

func (s *ETLChannelSink) Write(ctx context.Context, pt *Point) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- pt:
		return nil
	}
}

// ETLMultiSink fans out writes to multiple sinks.
type ETLMultiSink struct {
	sinks []ETLSink
}

// NewETLMultiSink creates a sink that writes to all provided sinks.
func NewETLMultiSink(sinks ...ETLSink) *ETLMultiSink {
	return &ETLMultiSink{sinks: sinks}
}

func (s *ETLMultiSink) Open(ctx context.Context) error {
	for _, sk := range s.sinks {
		if err := sk.Open(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *ETLMultiSink) Write(ctx context.Context, pt *Point) error {
	for _, sk := range s.sinks {
		if err := sk.Write(ctx, pt); err != nil {
			return err
		}
	}
	return nil
}

func (s *ETLMultiSink) Flush(ctx context.Context) error {
	for _, sk := range s.sinks {
		if err := sk.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *ETLMultiSink) Close() error {
	var last error
	for _, sk := range s.sinks {
		if err := sk.Close(); err != nil {
			last = err
		}
	}
	return last
}

// ---------------------------------------------------------------------------
// Pipeline Registry
// ---------------------------------------------------------------------------

// ETLPipelineInfo provides a summary of a registered pipeline.
type ETLPipelineInfo struct {
	Name   string
	Status string
	Stats  *ETLPipelineStats
}

// ETLRegistry manages a collection of named ETL pipelines.
type ETLRegistry struct {
	pipelines map[string]*ETLPipeline
	mu        sync.RWMutex
}

// NewETLRegistry creates an empty pipeline registry.
func NewETLRegistry() *ETLRegistry {
	return &ETLRegistry{
		pipelines: make(map[string]*ETLPipeline),
	}
}

// Register adds a pipeline to the registry.
func (r *ETLRegistry) Register(pipeline *ETLPipeline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := pipeline.config.Name
	if _, exists := r.pipelines[name]; exists {
		return fmt.Errorf("etl pipeline %q already registered", name)
	}
	r.pipelines[name] = pipeline
	return nil
}

// Unregister removes a pipeline from the registry, stopping it if running.
func (r *ETLRegistry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, exists := r.pipelines[name]
	if !exists {
		return fmt.Errorf("etl pipeline %q not found", name)
	}
	p.mu.RLock()
	running := p.running
	p.mu.RUnlock()
	if running {
		if err := p.Stop(); err != nil {
			return err
		}
	}
	delete(r.pipelines, name)
	return nil
}

// Get returns the pipeline with the given name, or nil.
func (r *ETLRegistry) Get(name string) *ETLPipeline {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pipelines[name]
}

// List returns info for all registered pipelines.
func (r *ETLRegistry) List() []*ETLPipelineInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	infos := make([]*ETLPipelineInfo, 0, len(r.pipelines))
	for _, p := range r.pipelines {
		p.mu.RLock()
		status := "stopped"
		if p.running {
			status = "running"
		}
		p.mu.RUnlock()
		infos = append(infos, &ETLPipelineInfo{
			Name:   p.config.Name,
			Status: status,
			Stats:  p.stats.snapshot(),
		})
	}
	return infos
}

// StartAll starts every registered pipeline that is not already running.
func (r *ETLRegistry) StartAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.pipelines {
		p.mu.RLock()
		running := p.running
		p.mu.RUnlock()
		if !running {
			_ = p.Start()
		}
	}
}

// StopAll stops every running pipeline in the registry.
func (r *ETLRegistry) StopAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.pipelines {
		p.mu.RLock()
		running := p.running
		p.mu.RUnlock()
		if running {
			_ = p.Stop()
		}
	}
}
