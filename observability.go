package chronicle

import (
	"context"
	"math"
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
