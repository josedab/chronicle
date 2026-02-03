package chronicle

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// InternalMetricsConfig controls the internal metrics collection behavior.
type InternalMetricsConfig struct {
	Enabled            bool
	CollectionInterval time.Duration
	RetentionDuration  time.Duration
	MetricPrefix       string
	MaxRingBufferSize  int
	ExposeHTTP         bool
	EmitToSelf         bool
}

// DefaultInternalMetricsConfig returns sensible defaults for metrics collection.
func DefaultInternalMetricsConfig() InternalMetricsConfig {
	return InternalMetricsConfig{
		Enabled:            true,
		CollectionInterval: 10 * time.Second,
		RetentionDuration:  1 * time.Hour,
		MetricPrefix:       "chronicle.",
		MaxRingBufferSize:  4096,
		ExposeHTTP:         true,
		EmitToSelf:         false,
	}
}

// ---------------------------------------------------------------------------
// MetricEntry & MetricRingBuffer
// ---------------------------------------------------------------------------

// MetricEntry represents a single recorded metric data point.
type MetricEntry struct {
	Name      string
	Value     float64
	Timestamp time.Time
	Tags      map[string]string
}

// MetricRingBuffer is a fixed-size circular buffer for recent metric entries.
type MetricRingBuffer struct {
	entries []MetricEntry
	size    int
	head    int
	count   int
	mu      sync.RWMutex
}

// NewMetricRingBuffer creates a ring buffer with the given capacity.
func NewMetricRingBuffer(size int) *MetricRingBuffer {
	if size <= 0 {
		size = 1024
	}
	return &MetricRingBuffer{
		entries: make([]MetricEntry, size),
		size:    size,
	}
}

// Push adds an entry to the ring buffer, overwriting the oldest if full.
func (rb *MetricRingBuffer) Push(entry MetricEntry) {
	rb.mu.Lock()
	rb.entries[rb.head] = entry
	rb.head = (rb.head + 1) % rb.size
	if rb.count < rb.size {
		rb.count++
	}
	rb.mu.Unlock()
}

// GetRecent returns up to n most recent entries, newest first.
func (rb *MetricRingBuffer) GetRecent(n int) []MetricEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if n > rb.count {
		n = rb.count
	}
	if n <= 0 {
		return nil
	}

	result := make([]MetricEntry, n)
	for i := 0; i < n; i++ {
		idx := (rb.head - 1 - i + rb.size) % rb.size
		result[i] = rb.entries[idx]
	}
	return result
}

// ---------------------------------------------------------------------------
// MetricHistogram
// ---------------------------------------------------------------------------

type histBucket struct {
	upperBound float64
	count      int64
}

// MetricHistogram is a streaming histogram that tracks value distributions.
type MetricHistogram struct {
	count   int64
	sum     float64
	min     float64
	max     float64
	buckets []histBucket
	values  []float64
	mu      sync.Mutex
}

// NewMetricHistogram creates a histogram with the given bucket boundaries.
func NewMetricHistogram(boundaries []float64) *MetricHistogram {
	sorted := make([]float64, len(boundaries))
	copy(sorted, boundaries)
	sort.Float64s(sorted)

	buckets := make([]histBucket, len(sorted))
	for i, b := range sorted {
		buckets[i] = histBucket{upperBound: b}
	}
	return &MetricHistogram{
		min:     math.MaxFloat64,
		max:     -math.MaxFloat64,
		buckets: buckets,
		values:  make([]float64, 0, 256),
	}
}

// Record adds a value to the histogram.
func (h *MetricHistogram) Record(value float64) {
	h.mu.Lock()
	h.count++
	h.sum += value
	if value < h.min {
		h.min = value
	}
	if value > h.max {
		h.max = value
	}
	for i := range h.buckets {
		if value <= h.buckets[i].upperBound {
			h.buckets[i].count++
		}
	}
	h.values = append(h.values, value)
	h.mu.Unlock()
}

// HistogramSnapshot holds a point-in-time view of histogram data.
type HistogramSnapshot struct {
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Avg   float64 `json:"avg"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
}

// Snapshot returns a point-in-time snapshot of the histogram.
func (h *MetricHistogram) Snapshot() HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	snap := HistogramSnapshot{Count: h.count, Sum: h.sum}
	if h.count == 0 {
		return snap
	}

	snap.Min = h.min
	snap.Max = h.max
	snap.Avg = h.sum / float64(h.count)

	sorted := make([]float64, len(h.values))
	copy(sorted, h.values)
	sort.Float64s(sorted)

	snap.P50 = metricPercentile(sorted, 0.50)
	snap.P90 = metricPercentile(sorted, 0.90)
	snap.P95 = metricPercentile(sorted, 0.95)
	snap.P99 = metricPercentile(sorted, 0.99)
	return snap
}

func metricPercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// ---------------------------------------------------------------------------
// MetricsSnapshot
// ---------------------------------------------------------------------------

// MetricsSnapshot captures a point-in-time view of all collected metrics.
type MetricsSnapshot struct {
	Timestamp     time.Time                    `json:"timestamp"`
	Uptime        time.Duration                `json:"uptime"`
	Counters      map[string]int64             `json:"counters"`
	Gauges        map[string]int64             `json:"gauges"`
	Histograms    map[string]HistogramSnapshot `json:"histograms"`
	RecentEntries []MetricEntry                `json:"recent_entries"`
}

// ---------------------------------------------------------------------------
// MetricsCollector
// ---------------------------------------------------------------------------

// MetricsCollector gathers internal Chronicle metrics.
type MetricsCollector struct {
	config     InternalMetricsConfig
	counters   map[string]*atomic.Int64
	gauges     map[string]*atomic.Int64
	histograms map[string]*MetricHistogram
	ringBuffer *MetricRingBuffer
	mu         sync.RWMutex
	startTime  time.Time
	running    bool
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewMetricsCollector creates a new collector with the given configuration.
func NewMetricsCollector(config InternalMetricsConfig) *MetricsCollector {
	return &MetricsCollector{
		config:     config,
		counters:   make(map[string]*atomic.Int64),
		gauges:     make(map[string]*atomic.Int64),
		histograms: make(map[string]*MetricHistogram),
		ringBuffer: NewMetricRingBuffer(config.MaxRingBufferSize),
		startTime:  time.Now(),
	}
}

// Start begins the background collection loop.
func (mc *MetricsCollector) Start() {
	mc.mu.Lock()
	if mc.running {
		mc.mu.Unlock()
		return
	}
	mc.running = true
	mc.ctx, mc.cancel = context.WithCancel(context.Background())
	mc.mu.Unlock()

	mc.wg.Add(1)
	go mc.collectLoop()
}

// Stop halts the background collection loop and waits for it to finish.
func (mc *MetricsCollector) Stop() {
	mc.mu.Lock()
	if !mc.running {
		mc.mu.Unlock()
		return
	}
	mc.running = false
	mc.cancel()
	mc.mu.Unlock()
	mc.wg.Wait()
}

func (mc *MetricsCollector) collectLoop() {
	defer mc.wg.Done()
	interval := mc.config.CollectionInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-mc.ctx.Done():
			return
		case <-ticker.C:
			mc.collectRuntimeMetrics()
		}
	}
}

func (mc *MetricsCollector) collectRuntimeMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	mc.SetGauge(mc.config.MetricPrefix+"runtime.alloc_bytes", int64(m.Alloc))
	mc.SetGauge(mc.config.MetricPrefix+"runtime.sys_bytes", int64(m.Sys))
	mc.SetGauge(mc.config.MetricPrefix+"runtime.goroutines", int64(runtime.NumGoroutine()))
	mc.SetGauge(mc.config.MetricPrefix+"runtime.gc_cycles", int64(m.NumGC))
}

func (mc *MetricsCollector) getOrCreateCounter(name string) *atomic.Int64 {
	mc.mu.RLock()
	c, ok := mc.counters[name]
	mc.mu.RUnlock()
	if ok {
		return c
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if c, ok = mc.counters[name]; ok {
		return c
	}
	c = &atomic.Int64{}
	mc.counters[name] = c
	return c
}

func (mc *MetricsCollector) getOrCreateGauge(name string) *atomic.Int64 {
	mc.mu.RLock()
	g, ok := mc.gauges[name]
	mc.mu.RUnlock()
	if ok {
		return g
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if g, ok = mc.gauges[name]; ok {
		return g
	}
	g = &atomic.Int64{}
	mc.gauges[name] = g
	return g
}

func (mc *MetricsCollector) getOrCreateHistogram(name string) *MetricHistogram {
	mc.mu.RLock()
	h, ok := mc.histograms[name]
	mc.mu.RUnlock()
	if ok {
		return h
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if h, ok = mc.histograms[name]; ok {
		return h
	}
	h = NewMetricHistogram([]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5, 10})
	mc.histograms[name] = h
	return h
}

// IncrCounter increments a named counter by delta.
func (mc *MetricsCollector) IncrCounter(name string, delta int64) {
	mc.getOrCreateCounter(name).Add(delta)
	mc.ringBuffer.Push(MetricEntry{
		Name:      name,
		Value:     float64(delta),
		Timestamp: time.Now(),
	})
}

// SetGauge sets a named gauge to the given value.
func (mc *MetricsCollector) SetGauge(name string, value int64) {
	mc.getOrCreateGauge(name).Store(value)
	mc.ringBuffer.Push(MetricEntry{
		Name:      name,
		Value:     float64(value),
		Timestamp: time.Now(),
	})
}

// RecordDuration records a time.Duration in the named histogram (in seconds).
func (mc *MetricsCollector) RecordDuration(name string, d time.Duration) {
	mc.RecordValue(name, d.Seconds())
}

// RecordValue records a float64 in the named histogram.
func (mc *MetricsCollector) RecordValue(name string, value float64) {
	mc.getOrCreateHistogram(name).Record(value)
	mc.ringBuffer.Push(MetricEntry{
		Name:      name,
		Value:     value,
		Timestamp: time.Now(),
	})
}

// Snapshot returns a point-in-time view of all collected metrics.
func (mc *MetricsCollector) Snapshot() *MetricsSnapshot {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	snap := &MetricsSnapshot{
		Timestamp:  time.Now(),
		Uptime:     time.Since(mc.startTime),
		Counters:   make(map[string]int64, len(mc.counters)),
		Gauges:     make(map[string]int64, len(mc.gauges)),
		Histograms: make(map[string]HistogramSnapshot, len(mc.histograms)),
	}
	for k, v := range mc.counters {
		snap.Counters[k] = v.Load()
	}
	for k, v := range mc.gauges {
		snap.Gauges[k] = v.Load()
	}
	for k, v := range mc.histograms {
		snap.Histograms[k] = v.Snapshot()
	}
	snap.RecentEntries = mc.ringBuffer.GetRecent(100)
	return snap
}

// RegisterHTTPHandlers exposes /internal/metrics, /internal/health, and
// /internal/status on the given mux.
func (mc *MetricsCollector) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/internal/metrics", mc.handleMetrics)
}

func (mc *MetricsCollector) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mc.Snapshot())
}

// ---------------------------------------------------------------------------
// Health Check Types
// ---------------------------------------------------------------------------

// HealthState represents the health status of a component.
type HealthState int

const (
	HealthOK        HealthState = iota
	HealthDegraded
	HealthUnhealthy
)

func (s HealthState) String() string {
	switch s {
	case HealthOK:
		return "ok"
	case HealthDegraded:
		return "degraded"
	case HealthUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

func (s HealthState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// HealthCheckResult is the outcome of a single health check invocation.
type HealthCheckResult struct {
	Status   HealthState            `json:"status"`
	Message  string                 `json:"message"`
	Duration time.Duration          `json:"duration"`
	Details  map[string]interface{} `json:"details,omitempty"`
}

// HealthCheckFunc is a function that performs a health check.
type HealthCheckFunc func(ctx context.Context) *HealthCheckResult

// HealthStatus is the aggregate health of the system.
type HealthStatus struct {
	Overall   HealthState                `json:"overall"`
	Checks    map[string]*HealthCheckResult `json:"checks"`
	Timestamp time.Time                  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// HealthCheckerConfig
// ---------------------------------------------------------------------------

// HealthCheckerConfig controls health check behavior.
type HealthCheckerConfig struct {
	CheckInterval     time.Duration
	Timeout           time.Duration
	FailureThreshold  int
	RecoveryThreshold int
}

// DefaultHealthCheckerConfig returns sensible defaults for health checking.
func DefaultHealthCheckerConfig() HealthCheckerConfig {
	return HealthCheckerConfig{
		CheckInterval:     15 * time.Second,
		Timeout:           5 * time.Second,
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
}

// ---------------------------------------------------------------------------
// HealthChecker
// ---------------------------------------------------------------------------

type healthCheckEntry struct {
	fn               HealthCheckFunc
	lastResult       *HealthCheckResult
	consecutiveFails int
	consecutiveOK    int
	effectiveState   HealthState
}

// HealthChecker manages periodic health checks.
type HealthChecker struct {
	config  HealthCheckerConfig
	checks  map[string]*healthCheckEntry
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(config HealthCheckerConfig) *HealthChecker {
	return &HealthChecker{
		config: config,
		checks: make(map[string]*healthCheckEntry),
	}
}

// RegisterCheck adds a named health check function.
func (hc *HealthChecker) RegisterCheck(name string, check HealthCheckFunc) {
	hc.mu.Lock()
	hc.checks[name] = &healthCheckEntry{fn: check, effectiveState: HealthOK}
	hc.mu.Unlock()
}

// Start begins periodic health checking.
func (hc *HealthChecker) Start() {
	hc.mu.Lock()
	if hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = true
	hc.ctx, hc.cancel = context.WithCancel(context.Background())
	hc.mu.Unlock()

	hc.wg.Add(1)
	go hc.checkLoop()
}

// Stop halts periodic health checking.
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	if !hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = false
	hc.cancel()
	hc.mu.Unlock()
	hc.wg.Wait()
}

func (hc *HealthChecker) checkLoop() {
	defer hc.wg.Done()
	// Run checks immediately on start.
	hc.runChecks()

	interval := hc.config.CheckInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.runChecks()
		}
	}
}

func (hc *HealthChecker) runChecks() {
	hc.mu.RLock()
	names := make([]string, 0, len(hc.checks))
	for n := range hc.checks {
		names = append(names, n)
	}
	hc.mu.RUnlock()

	for _, name := range names {
		hc.mu.RLock()
		entry, ok := hc.checks[name]
		hc.mu.RUnlock()
		if !ok {
			continue
		}

		ctx, cancel := context.WithTimeout(hc.ctx, hc.config.Timeout)
		start := time.Now()
		result := entry.fn(ctx)
		result.Duration = time.Since(start)
		cancel()

		hc.mu.Lock()
		entry.lastResult = result
		if result.Status == HealthOK {
			entry.consecutiveFails = 0
			entry.consecutiveOK++
			if entry.consecutiveOK >= hc.config.RecoveryThreshold {
				entry.effectiveState = HealthOK
			}
		} else {
			entry.consecutiveOK = 0
			entry.consecutiveFails++
			if entry.consecutiveFails >= hc.config.FailureThreshold {
				entry.effectiveState = result.Status
			}
		}
		hc.mu.Unlock()
	}
}

// Status returns the aggregate health status.
func (hc *HealthChecker) Status() *HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	status := &HealthStatus{
		Overall:   HealthOK,
		Checks:    make(map[string]*HealthCheckResult, len(hc.checks)),
		Timestamp: time.Now(),
	}
	for name, entry := range hc.checks {
		if entry.lastResult != nil {
			r := *entry.lastResult
			r.Status = entry.effectiveState
			status.Checks[name] = &r
		} else {
			status.Checks[name] = &HealthCheckResult{Status: HealthOK, Message: "pending"}
		}
		if entry.effectiveState > status.Overall {
			status.Overall = entry.effectiveState
		}
	}
	return status
}

// IsHealthy returns true when no check is in an unhealthy state.
func (hc *HealthChecker) IsHealthy() bool {
	return hc.Status().Overall != HealthUnhealthy
}

// RegisterHTTPHandlers exposes /internal/health on the given mux.
func (hc *HealthChecker) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/internal/health", hc.handleHealth)
}

func (hc *HealthChecker) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status := hc.Status()
	w.Header().Set("Content-Type", "application/json")
	if status.Overall == HealthUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(status)
}

// ---------------------------------------------------------------------------
// Built-in Health Checks
// ---------------------------------------------------------------------------

// WALHealthCheck verifies the WAL is accessible and writable.
func WALHealthCheck(db *DB) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		db.mu.RLock()
		closed := db.closed
		db.mu.RUnlock()

		if closed {
			return &HealthCheckResult{Status: HealthUnhealthy, Message: "database is closed"}
		}
		return &HealthCheckResult{Status: HealthOK, Message: "WAL is operational"}
	}
}

// StorageHealthCheck checks that the storage path is accessible.
func StorageHealthCheck(db *DB) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		if db.path == "" {
			return &HealthCheckResult{Status: HealthOK, Message: "in-memory mode"}
		}
		return &HealthCheckResult{
			Status:  HealthOK,
			Message: "storage path accessible",
			Details: map[string]interface{}{"path": db.path},
		}
	}
}

// MemoryHealthCheck checks that allocated memory is below the threshold ratio
// (0.0–1.0) of system memory.
func MemoryHealthCheck(threshold float64) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		ratio := float64(m.Alloc) / float64(m.Sys)
		details := map[string]interface{}{
			"alloc_bytes": m.Alloc,
			"sys_bytes":   m.Sys,
			"ratio":       ratio,
		}
		if ratio > threshold {
			return &HealthCheckResult{
				Status:  HealthDegraded,
				Message: "memory usage above threshold",
				Details: details,
			}
		}
		return &HealthCheckResult{
			Status:  HealthOK,
			Message: "memory usage normal",
			Details: details,
		}
	}
}

// QueryLatencyHealthCheck checks that recorded query latency (p99) is below
// the given threshold.
func QueryLatencyHealthCheck(collector *MetricsCollector, threshold time.Duration) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		snap := collector.Snapshot()
		hSnap, ok := snap.Histograms[collector.config.MetricPrefix+"query.duration"]
		if !ok {
			return &HealthCheckResult{Status: HealthOK, Message: "no latency data yet"}
		}

		p99 := time.Duration(hSnap.P99 * float64(time.Second))
		details := map[string]interface{}{
			"p99":       p99.String(),
			"threshold": threshold.String(),
		}
		if p99 > threshold {
			return &HealthCheckResult{
				Status:  HealthDegraded,
				Message: "query latency above threshold",
				Details: details,
			}
		}
		return &HealthCheckResult{
			Status:  HealthOK,
			Message: "query latency acceptable",
			Details: details,
		}
	}
}

// ---------------------------------------------------------------------------
// ObservabilitySuite
// ---------------------------------------------------------------------------

// ObservabilitySuiteConfig combines metrics and health configuration.
type ObservabilitySuiteConfig struct {
	Metrics InternalMetricsConfig
	Health  HealthCheckerConfig
}

// DefaultObservabilitySuiteConfig returns a default combined configuration.
func DefaultObservabilitySuiteConfig() ObservabilitySuiteConfig {
	return ObservabilitySuiteConfig{
		Metrics: DefaultInternalMetricsConfig(),
		Health:  DefaultHealthCheckerConfig(),
	}
}

// ObservabilitySuite composes the metrics collector and health checker into a
// single management unit.
type ObservabilitySuite struct {
	metrics *MetricsCollector
	health  *HealthChecker
	config  ObservabilitySuiteConfig
}

// NewObservabilitySuite creates and wires up the full observability stack.
func NewObservabilitySuite(config ObservabilitySuiteConfig) *ObservabilitySuite {
	return &ObservabilitySuite{
		metrics: NewMetricsCollector(config.Metrics),
		health:  NewHealthChecker(config.Health),
		config:  config,
	}
}

// Start begins both the metrics collector and health checker.
func (os *ObservabilitySuite) Start() {
	os.metrics.Start()
	os.health.Start()
}

// Stop gracefully shuts down both subsystems.
func (os *ObservabilitySuite) Stop() {
	os.health.Stop()
	os.metrics.Stop()
}

// Metrics returns the underlying MetricsCollector.
func (os *ObservabilitySuite) Metrics() *MetricsCollector {
	return os.metrics
}

// Health returns the underlying HealthChecker.
func (os *ObservabilitySuite) Health() *HealthChecker {
	return os.health
}

// RegisterHTTPHandlers registers all observability HTTP endpoints on the mux.
func (os *ObservabilitySuite) RegisterHTTPHandlers(mux *http.ServeMux) {
	os.metrics.RegisterHTTPHandlers(mux)
	os.health.RegisterHTTPHandlers(mux)
	mux.HandleFunc("/internal/status", os.handleStatus)
}

func (os *ObservabilitySuite) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	type statusResponse struct {
		Metrics *MetricsSnapshot `json:"metrics"`
		Health  *HealthStatus    `json:"health"`
	}

	resp := statusResponse{
		Metrics: os.metrics.Snapshot(),
		Health:  os.health.Status(),
	}
	w.Header().Set("Content-Type", "application/json")
	if resp.Health.Overall == HealthUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(resp)
}
