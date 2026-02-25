// streaming_etl_stages.go contains extended streaming etl functionality.
package chronicle

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func (s *etlRouteStage) Name() string { return "route" }
func (s *etlRouteStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	key := s.fn(pt)
	pt.ensureTags()
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
			_ = p.Start() //nolint:errcheck // best-effort ETL processing
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
			stopQuietly(p)
		}
	}
}
