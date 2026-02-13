package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// DataLineageConfig configures the data lineage tracker.
type DataLineageConfig struct {
	Enabled       bool
	MaxEntries    int
	TrackWrites   bool
	TrackQueries  bool
	TrackTransforms bool
}

// DefaultDataLineageConfig returns sensible defaults.
func DefaultDataLineageConfig() DataLineageConfig {
	return DataLineageConfig{
		Enabled:       true,
		MaxEntries:    10000,
		TrackWrites:   true,
		TrackQueries:  true,
		TrackTransforms: true,
	}
}

// LineageEventType represents the type of lineage event.
type LineageEventType string

const (
	LineageWrite     LineageEventType = "write"
	LineageQuery     LineageEventType = "query"
	LineageTransform LineageEventType = "transform"
	LineageExport    LineageEventType = "export"
	LineageDelete    LineageEventType = "delete"
	LineageDownsample LineageEventType = "downsample"
)

// LineageEntry records a single data provenance event.
type LineageEntry struct {
	ID          string           `json:"id"`
	Metric      string           `json:"metric"`
	EventType   LineageEventType `json:"event_type"`
	Source      string           `json:"source"`
	Destination string           `json:"destination,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	PointCount  int64            `json:"point_count"`
	Timestamp   time.Time        `json:"timestamp"`
	Details     string           `json:"details,omitempty"`
}

// LineageGraph represents the dependency graph for a metric.
type LineageGraph struct {
	Metric      string          `json:"metric"`
	Upstream    []string        `json:"upstream"`
	Downstream  []string        `json:"downstream"`
	Transforms  []string        `json:"transforms"`
	EntryCount  int             `json:"entry_count"`
	FirstSeen   time.Time       `json:"first_seen"`
	LastSeen    time.Time       `json:"last_seen"`
}

// DataLineageStats holds engine statistics.
type DataLineageStats struct {
	TotalEntries    int   `json:"total_entries"`
	TrackedMetrics  int   `json:"tracked_metrics"`
	WriteEvents     int64 `json:"write_events"`
	QueryEvents     int64 `json:"query_events"`
	TransformEvents int64 `json:"transform_events"`
}

// DataLineageEngine tracks data provenance and lineage.
type DataLineageEngine struct {
	db     *DB
	config DataLineageConfig
	mu     sync.RWMutex
	entries []LineageEntry
	graphs  map[string]*LineageGraph
	running bool
	stopCh  chan struct{}
	stats   DataLineageStats
	entrySeq int64
}

// NewDataLineageEngine creates a new lineage tracker.
func NewDataLineageEngine(db *DB, cfg DataLineageConfig) *DataLineageEngine {
	return &DataLineageEngine{
		db:      db,
		config:  cfg,
		entries: make([]LineageEntry, 0),
		graphs:  make(map[string]*LineageGraph),
		stopCh:  make(chan struct{}),
	}
}

func (e *DataLineageEngine) Start() {
	e.mu.Lock()
	if e.running { e.mu.Unlock(); return }
	e.running = true
	e.mu.Unlock()
}

func (e *DataLineageEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running { return }
	e.running = false
	close(e.stopCh)
}

// RecordWrite records a data write event.
func (e *DataLineageEngine) RecordWrite(metric string, source string, pointCount int64, tags map[string]string) {
	if !e.config.TrackWrites { return }
	e.record(metric, LineageWrite, source, "", pointCount, tags, "")
	e.mu.Lock()
	e.stats.WriteEvents++
	e.mu.Unlock()
}

// RecordQuery records a query event.
func (e *DataLineageEngine) RecordQuery(metric string, consumer string) {
	if !e.config.TrackQueries { return }
	e.record(metric, LineageQuery, metric, consumer, 0, nil, "")
	e.mu.Lock()
	e.stats.QueryEvents++
	e.mu.Unlock()
}

// RecordTransform records a data transformation.
func (e *DataLineageEngine) RecordTransform(sourceMetric, targetMetric, transform string, pointCount int64) {
	if !e.config.TrackTransforms { return }
	e.record(sourceMetric, LineageTransform, sourceMetric, targetMetric, pointCount, nil, transform)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.stats.TransformEvents++

	// Update graph edges
	srcGraph := e.getOrCreateGraph(sourceMetric)
	dstGraph := e.getOrCreateGraph(targetMetric)
	if !lineageContains(srcGraph.Downstream, targetMetric) {
		srcGraph.Downstream = append(srcGraph.Downstream, targetMetric)
	}
	if !lineageContains(dstGraph.Upstream, sourceMetric) {
		dstGraph.Upstream = append(dstGraph.Upstream, sourceMetric)
	}
	if !lineageContains(srcGraph.Transforms, transform) {
		srcGraph.Transforms = append(srcGraph.Transforms, transform)
	}
}

func (e *DataLineageEngine) record(metric string, eventType LineageEventType, source, dest string, count int64, tags map[string]string, details string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.entrySeq++
	entry := LineageEntry{
		ID:          fmt.Sprintf("lin-%d", e.entrySeq),
		Metric:      metric,
		EventType:   eventType,
		Source:      source,
		Destination: dest,
		Tags:        tags,
		PointCount:  count,
		Timestamp:   time.Now(),
		Details:     details,
	}
	e.entries = append(e.entries, entry)
	if len(e.entries) > e.config.MaxEntries {
		e.entries = e.entries[len(e.entries)-e.config.MaxEntries:]
	}

	graph := e.getOrCreateGraph(metric)
	graph.EntryCount++
	graph.LastSeen = time.Now()
	e.stats.TotalEntries = len(e.entries)
}

func (e *DataLineageEngine) getOrCreateGraph(metric string) *LineageGraph {
	if g, ok := e.graphs[metric]; ok { return g }
	g := &LineageGraph{
		Metric:    metric,
		FirstSeen: time.Now(),
		LastSeen:  time.Now(),
	}
	e.graphs[metric] = g
	e.stats.TrackedMetrics = len(e.graphs)
	return g
}

func lineageContains(s []string, v string) bool {
	for _, item := range s { if item == v { return true } }
	return false
}

// GetGraph returns the lineage graph for a metric.
func (e *DataLineageEngine) GetGraph(metric string) *LineageGraph {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if g, ok := e.graphs[metric]; ok { cp := *g; return &cp }
	return nil
}

// GetEntries returns lineage entries for a metric, or all if metric is empty.
func (e *DataLineageEngine) GetEntries(metric string, limit int) []LineageEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []LineageEntry
	for _, entry := range e.entries {
		if metric != "" && entry.Metric != metric { continue }
		result = append(result, entry)
	}
	if limit > 0 && len(result) > limit { result = result[len(result)-limit:] }
	return result
}

// TrackedMetrics returns all tracked metric names.
func (e *DataLineageEngine) TrackedMetrics() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	metrics := make([]string, 0, len(e.graphs))
	for m := range e.graphs { metrics = append(metrics, m) }
	return metrics
}

// GetStats returns lineage stats.
func (e *DataLineageEngine) GetStats() DataLineageStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *DataLineageEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/lineage/graph", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		if metric == "" { w.Header().Set("Content-Type", "application/json"); json.NewEncoder(w).Encode(e.TrackedMetrics()); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetGraph(metric))
	})
	mux.HandleFunc("/api/v1/lineage/entries", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetEntries(metric, 100))
	})
	mux.HandleFunc("/api/v1/lineage/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
