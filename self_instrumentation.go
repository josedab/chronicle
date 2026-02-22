package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// SelfInstrumentationConfig configures Chronicle's self-monitoring.
type SelfInstrumentationConfig struct {
	Enabled          bool
	CollectInterval  time.Duration
	EmitOTLP         bool
}

// DefaultSelfInstrumentationConfig returns sensible defaults.
func DefaultSelfInstrumentationConfig() SelfInstrumentationConfig {
	return SelfInstrumentationConfig{
		Enabled:         true,
		CollectInterval: 15 * time.Second,
		EmitOTLP:        false,
	}
}

// SelfMetric represents an internal Chronicle metric.
type SelfMetric struct {
	Name        string  `json:"name"`
	Value       float64 `json:"value"`
	Unit        string  `json:"unit"`
	Type        string  `json:"type"` // counter, gauge, histogram
	Description string  `json:"description"`
}

// SelfInstrumentationEngine tracks Chronicle's own performance metrics.
type SelfInstrumentationEngine struct {
	db     *DB
	config SelfInstrumentationConfig
	mu     sync.RWMutex

	// Atomic counters for hot-path metrics
	writesTotal    atomic.Int64
	writeErrors    atomic.Int64
	queriesTotal   atomic.Int64
	queryErrors    atomic.Int64
	pointsWritten  atomic.Int64
	bytesWritten   atomic.Int64

	// Gauge values (updated periodically)
	partitionCount int
	walSizeBytes   int64
	metricCount    int
	cacheHitRate   float64

	// Latency tracking
	writeLatencySum atomic.Int64
	queryLatencySum atomic.Int64

	running bool
	stopCh  chan struct{}
}

// NewSelfInstrumentationEngine creates a new self-instrumentation engine.
func NewSelfInstrumentationEngine(db *DB, cfg SelfInstrumentationConfig) *SelfInstrumentationEngine {
	return &SelfInstrumentationEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

func (e *SelfInstrumentationEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
	go e.collectLoop()
}

func (e *SelfInstrumentationEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; close(e.stopCh)
}

// RecordWrite records a write operation.
func (e *SelfInstrumentationEngine) RecordWrite(points int, bytes int64, latencyNs int64, err bool) {
	e.writesTotal.Add(1)
	e.pointsWritten.Add(int64(points))
	e.bytesWritten.Add(bytes)
	e.writeLatencySum.Add(latencyNs)
	if err { e.writeErrors.Add(1) }
}

// RecordQuery records a query operation.
func (e *SelfInstrumentationEngine) RecordQuery(latencyNs int64, err bool) {
	e.queriesTotal.Add(1)
	e.queryLatencySum.Add(latencyNs)
	if err { e.queryErrors.Add(1) }
}

// Collect returns all current self-metrics.
func (e *SelfInstrumentationEngine) Collect() []SelfMetric {
	writes := e.writesTotal.Load()
	queries := e.queriesTotal.Load()

	var avgWriteLatency, avgQueryLatency float64
	if writes > 0 {
		avgWriteLatency = float64(e.writeLatencySum.Load()) / float64(writes) / 1e6 // ms
	}
	if queries > 0 {
		avgQueryLatency = float64(e.queryLatencySum.Load()) / float64(queries) / 1e6 // ms
	}

	e.mu.RLock()
	partitions := e.partitionCount
	walSize := e.walSizeBytes
	metricCnt := e.metricCount
	cacheHit := e.cacheHitRate
	e.mu.RUnlock()

	return []SelfMetric{
		{Name: "chronicle_writes_total", Value: float64(writes), Unit: "1", Type: "counter", Description: "Total write operations"},
		{Name: "chronicle_write_errors_total", Value: float64(e.writeErrors.Load()), Unit: "1", Type: "counter", Description: "Total write errors"},
		{Name: "chronicle_queries_total", Value: float64(queries), Unit: "1", Type: "counter", Description: "Total query operations"},
		{Name: "chronicle_query_errors_total", Value: float64(e.queryErrors.Load()), Unit: "1", Type: "counter", Description: "Total query errors"},
		{Name: "chronicle_points_written_total", Value: float64(e.pointsWritten.Load()), Unit: "1", Type: "counter", Description: "Total points written"},
		{Name: "chronicle_bytes_written_total", Value: float64(e.bytesWritten.Load()), Unit: "bytes", Type: "counter", Description: "Total bytes written"},
		{Name: "chronicle_write_latency_avg_ms", Value: avgWriteLatency, Unit: "ms", Type: "gauge", Description: "Average write latency"},
		{Name: "chronicle_query_latency_avg_ms", Value: avgQueryLatency, Unit: "ms", Type: "gauge", Description: "Average query latency"},
		{Name: "chronicle_partitions_count", Value: float64(partitions), Unit: "1", Type: "gauge", Description: "Current partition count"},
		{Name: "chronicle_wal_size_bytes", Value: float64(walSize), Unit: "bytes", Type: "gauge", Description: "Current WAL size"},
		{Name: "chronicle_metrics_count", Value: float64(metricCnt), Unit: "1", Type: "gauge", Description: "Number of unique metrics"},
		{Name: "chronicle_cache_hit_rate", Value: cacheHit, Unit: "ratio", Type: "gauge", Description: "Query cache hit rate"},
	}
}

// PrometheusExposition returns metrics in Prometheus text format.
func (e *SelfInstrumentationEngine) PrometheusExposition() string {
	var out string
	for _, m := range e.Collect() {
		out += "# HELP " + m.Name + " " + m.Description + "\n"
		out += "# TYPE " + m.Name + " " + m.Type + "\n"
		out += m.Name + " " + formatFloat(m.Value) + "\n"
	}
	return out
}

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return json.Number(json.Number(formatInt(int64(v)))).String()
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func formatInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func (e *SelfInstrumentationEngine) collectLoop() {
	ticker := time.NewTicker(e.config.CollectInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh: return
		case <-ticker.C:
			e.mu.Lock()
			if e.db != nil {
				e.metricCount = len(e.db.Metrics())
			}
			e.mu.Unlock()
		}
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *SelfInstrumentationEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/self-metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Collect())
	})
	mux.HandleFunc("/api/v1/self-metrics/prometheus", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte(e.PrometheusExposition()))
	})
}
