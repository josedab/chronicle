package chronicle

import (
	"encoding/json"
	"net/http"
	"sort"
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
	writesRejected atomic.Int64

	// Gauge values (updated periodically)
	partitionCount int
	walSizeBytes   int64
	metricCount    int
	cacheHitRate   float64

	// Latency tracking (averages)
	writeLatencySum atomic.Int64
	queryLatencySum atomic.Int64

	// Latency histograms for percentile computation
	writeLatencies *latencyHistogram
	queryLatencies *latencyHistogram

	// Optional SLO integration. When set, RecordWrite/RecordQuery
	// automatically record good/bad events to the SLO engine.
	sloEngine  *SLOEngine
	sloWriteName string // SLO name for write availability
	sloQueryName string // SLO name for query availability

	running bool
	stopCh  chan struct{}
}

// latencyHistogram is a lock-free ring buffer for recent latency samples
// that supports percentile computation.
type latencyHistogram struct {
	mu      sync.Mutex
	samples []float64 // milliseconds
	pos     int
	full    bool
}

func newLatencyHistogram(capacity int) *latencyHistogram {
	return &latencyHistogram{samples: make([]float64, capacity)}
}

func (h *latencyHistogram) record(ms float64) {
	h.mu.Lock()
	h.samples[h.pos] = ms
	h.pos++
	if h.pos >= len(h.samples) {
		h.pos = 0
		h.full = true
	}
	h.mu.Unlock()
}

// percentiles returns p50, p95, p99 from the current samples.
func (h *latencyHistogram) percentiles() (p50, p95, p99 float64) {
	h.mu.Lock()
	n := h.pos
	if h.full {
		n = len(h.samples)
	}
	if n == 0 {
		h.mu.Unlock()
		return 0, 0, 0
	}
	// Copy for sorting outside lock
	cp := make([]float64, n)
	if h.full {
		copy(cp, h.samples)
	} else {
		copy(cp, h.samples[:n])
	}
	h.mu.Unlock()

	sort.Float64s(cp)
	p50 = cp[int(float64(n)*0.5)]
	p95 = cp[int(float64(n)*0.95)]
	idx99 := int(float64(n) * 0.99)
	if idx99 >= n {
		idx99 = n - 1
	}
	p99 = cp[idx99]
	return p50, p95, p99
}

// NewSelfInstrumentationEngine creates a new self-instrumentation engine.
func NewSelfInstrumentationEngine(db *DB, cfg SelfInstrumentationConfig) *SelfInstrumentationEngine {
	return &SelfInstrumentationEngine{
		db:             db,
		config:         cfg,
		stopCh:         make(chan struct{}),
		writeLatencies: newLatencyHistogram(10000),
		queryLatencies: newLatencyHistogram(10000),
	}
}

func (e *SelfInstrumentationEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
	go e.collectLoop()
}

func (e *SelfInstrumentationEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; select { case <-e.stopCh: default: close(e.stopCh) }
}

// RecordWrite records a write operation.
func (e *SelfInstrumentationEngine) RecordWrite(points int, bytes int64, latencyNs int64, err bool) {
	e.writesTotal.Add(1)
	e.pointsWritten.Add(int64(points))
	e.bytesWritten.Add(bytes)
	e.writeLatencySum.Add(latencyNs)
	e.writeLatencies.record(float64(latencyNs) / 1e6) // ns → ms
	if err { e.writeErrors.Add(1) }
	if e.sloEngine != nil && e.sloWriteName != "" {
		e.sloEngine.RecordEvent(e.sloWriteName, !err) //nolint:errcheck
	}
}

// RecordQuery records a query operation.
func (e *SelfInstrumentationEngine) RecordQuery(latencyNs int64, err bool) {
	e.queriesTotal.Add(1)
	e.queryLatencySum.Add(latencyNs)
	e.queryLatencies.record(float64(latencyNs) / 1e6)
	if err { e.queryErrors.Add(1) }
	if e.sloEngine != nil && e.sloQueryName != "" {
		e.sloEngine.RecordEvent(e.sloQueryName, !err) //nolint:errcheck
	}
}

// RecordRejectedWrite records a write that was rejected (rate limit, validation, etc).
func (e *SelfInstrumentationEngine) RecordRejectedWrite() {
	e.writesRejected.Add(1)
}

// SetSLOEngine wires an SLO engine for automatic event recording.
// writeSLO and querySLO are the SLO names to record events for.
func (e *SelfInstrumentationEngine) SetSLOEngine(slo *SLOEngine, writeSLO, querySLO string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sloEngine = slo
	e.sloWriteName = writeSLO
	e.sloQueryName = querySLO
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

	wp50, wp95, wp99 := e.writeLatencies.percentiles()
	qp50, qp95, qp99 := e.queryLatencies.percentiles()

	e.mu.RLock()
	partitions := e.partitionCount
	walSize := e.walSizeBytes
	metricCnt := e.metricCount
	cacheHit := e.cacheHitRate
	e.mu.RUnlock()

	metrics := []SelfMetric{
		{Name: "chronicle_writes_total", Value: float64(writes), Unit: "1", Type: "counter", Description: "Total write operations"},
		{Name: "chronicle_write_errors_total", Value: float64(e.writeErrors.Load()), Unit: "1", Type: "counter", Description: "Total write errors"},
		{Name: "chronicle_writes_rejected_total", Value: float64(e.writesRejected.Load()), Unit: "1", Type: "counter", Description: "Total writes rejected by rate limiting or validation"},
		{Name: "chronicle_queries_total", Value: float64(queries), Unit: "1", Type: "counter", Description: "Total query operations"},
		{Name: "chronicle_query_errors_total", Value: float64(e.queryErrors.Load()), Unit: "1", Type: "counter", Description: "Total query errors"},
		{Name: "chronicle_points_written_total", Value: float64(e.pointsWritten.Load()), Unit: "1", Type: "counter", Description: "Total points written"},
		{Name: "chronicle_bytes_written_total", Value: float64(e.bytesWritten.Load()), Unit: "bytes", Type: "counter", Description: "Total bytes written"},
		{Name: "chronicle_write_latency_avg_ms", Value: avgWriteLatency, Unit: "ms", Type: "gauge", Description: "Average write latency"},
		{Name: "chronicle_write_latency_p50_ms", Value: wp50, Unit: "ms", Type: "gauge", Description: "Write latency 50th percentile"},
		{Name: "chronicle_write_latency_p95_ms", Value: wp95, Unit: "ms", Type: "gauge", Description: "Write latency 95th percentile"},
		{Name: "chronicle_write_latency_p99_ms", Value: wp99, Unit: "ms", Type: "gauge", Description: "Write latency 99th percentile"},
		{Name: "chronicle_query_latency_avg_ms", Value: avgQueryLatency, Unit: "ms", Type: "gauge", Description: "Average query latency"},
		{Name: "chronicle_query_latency_p50_ms", Value: qp50, Unit: "ms", Type: "gauge", Description: "Query latency 50th percentile"},
		{Name: "chronicle_query_latency_p95_ms", Value: qp95, Unit: "ms", Type: "gauge", Description: "Query latency 95th percentile"},
		{Name: "chronicle_query_latency_p99_ms", Value: qp99, Unit: "ms", Type: "gauge", Description: "Query latency 99th percentile"},
		{Name: "chronicle_partitions_count", Value: float64(partitions), Unit: "1", Type: "gauge", Description: "Current partition count"},
		{Name: "chronicle_wal_size_bytes", Value: float64(walSize), Unit: "bytes", Type: "gauge", Description: "Current WAL size"},
		{Name: "chronicle_metrics_count", Value: float64(metricCnt), Unit: "1", Type: "gauge", Description: "Number of unique metrics"},
		{Name: "chronicle_cache_hit_rate", Value: cacheHit, Unit: "ratio", Type: "gauge", Description: "Query cache hit rate"},
	}

	// Add replication metrics if replicator is active
	if e.db != nil && e.db.lifecycle != nil && e.db.lifecycle.replicator != nil {
		r := e.db.lifecycle.replicator
		metrics = append(metrics,
			SelfMetric{Name: "chronicle_replication_dropped_total", Value: float64(r.DroppedPoints()), Unit: "1", Type: "counter", Description: "Points permanently lost due to replication DLQ overflow"},
			SelfMetric{Name: "chronicle_replication_dlq_length", Value: float64(r.DLQLen()), Unit: "1", Type: "gauge", Description: "Points awaiting retry in dead-letter queue"},
		)
	}

	return metrics
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
				e.partitionCount = e.db.index.Count()
				if e.db.wal != nil {
					e.walSizeBytes = e.db.wal.Position()
				}
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
