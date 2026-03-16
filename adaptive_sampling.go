package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// AdaptiveSamplingConfig configures the adaptive sampling controller.
type AdaptiveSamplingConfig struct {
	Enabled           bool
	DefaultRate       float64 // 0.0-1.0
	MinRate           float64
	MaxRate           float64
	VolatilityWindow  int
	StorageBudgetMB   int64
	AdjustInterval    time.Duration
}

// DefaultAdaptiveSamplingConfig returns sensible defaults.
func DefaultAdaptiveSamplingConfig() AdaptiveSamplingConfig {
	return AdaptiveSamplingConfig{
		Enabled:          true,
		DefaultRate:      1.0,
		MinRate:          0.01,
		MaxRate:          1.0,
		VolatilityWindow: 100,
		StorageBudgetMB:  1024,
		AdjustInterval:   30 * time.Second,
	}
}

// SamplingRule defines per-metric sampling behavior.
type SamplingRule struct {
	MetricPattern string  `json:"metric_pattern"`
	Rate          float64 `json:"rate"`
	Priority      int     `json:"priority"` // 1=critical (never drop), 10=low priority
	Adaptive      bool    `json:"adaptive"`
}

// MetricVolatility tracks the data volatility for a metric.
type MetricVolatility struct {
	Metric     string  `json:"metric"`
	Volatility float64 `json:"volatility"` // coefficient of variation
	Rate       float64 `json:"current_rate"`
	PointsIn   int64   `json:"points_in"`
	PointsKept int64   `json:"points_kept"`
	Dropped    int64   `json:"dropped"`
}

// AdaptiveSamplingStats holds engine statistics.
type AdaptiveSamplingStats struct {
	MetricsTracked int     `json:"metrics_tracked"`
	TotalIn        int64   `json:"total_points_in"`
	TotalKept      int64   `json:"total_points_kept"`
	TotalDropped   int64   `json:"total_dropped"`
	OverallRate    float64 `json:"overall_rate"`
	AvgVolatility  float64 `json:"avg_volatility"`
}

// AdaptiveSamplingEngine dynamically adjusts ingestion sampling rates.
type AdaptiveSamplingEngine struct {
	db     *DB
	config AdaptiveSamplingConfig
	mu     sync.RWMutex
	rules  []SamplingRule
	metrics map[string]*MetricVolatility
	values  map[string][]float64 // recent values for volatility
	running bool
	stopCh  chan struct{}
	stats   AdaptiveSamplingStats
}

// NewAdaptiveSamplingEngine creates a new adaptive sampling engine.
func NewAdaptiveSamplingEngine(db *DB, cfg AdaptiveSamplingConfig) *AdaptiveSamplingEngine {
	return &AdaptiveSamplingEngine{
		db:      db,
		config:  cfg,
		rules:   make([]SamplingRule, 0),
		metrics: make(map[string]*MetricVolatility),
		values:  make(map[string][]float64),
		stopCh:  make(chan struct{}),
	}
}

func (e *AdaptiveSamplingEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
	go e.adjustLoop()
}

func (e *AdaptiveSamplingEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; select { case <-e.stopCh: default: close(e.stopCh) }
}

// AddRule adds a sampling rule.
func (e *AdaptiveSamplingEngine) AddRule(rule SamplingRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, rule)
}

// ShouldSample determines if a point should be kept.
func (e *AdaptiveSamplingEngine) ShouldSample(p Point) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	mv := e.getOrCreateMetric(p.Metric)
	mv.PointsIn++
	e.stats.TotalIn++

	// Track values for volatility
	e.values[p.Metric] = append(e.values[p.Metric], p.Value)
	if len(e.values[p.Metric]) > e.config.VolatilityWindow {
		e.values[p.Metric] = e.values[p.Metric][1:]
	}

	// Compute volatility
	if len(e.values[p.Metric]) >= 10 {
		mv.Volatility = coefficientOfVariation(e.values[p.Metric])
	}

	// Determine rate
	rate := mv.Rate
	if rate <= 0 { rate = e.config.DefaultRate }

	// Adaptive: high volatility → higher rate (keep more)
	if mv.Volatility > 0.5 { rate = math.Min(e.config.MaxRate, rate*1.5) }
	if mv.Volatility < 0.1 { rate = math.Max(e.config.MinRate, rate*0.8) }
	mv.Rate = rate

	// Deterministic sampling
	hash := fnv1a64SamplingHash([]byte(fmt.Sprintf("%s-%d", p.Metric, p.Timestamp)))
	threshold := uint64(rate * float64(^uint64(0)))
	keep := hash <= threshold

	if keep {
		mv.PointsKept++
		e.stats.TotalKept++
	} else {
		mv.Dropped++
		e.stats.TotalDropped++
	}

	return keep
}

func coefficientOfVariation(values []float64) float64 {
	if len(values) == 0 { return 0 }
	var sum float64
	for _, v := range values { sum += v }
	mean := sum / float64(len(values))
	if mean == 0 { return 0 }

	var variance float64
	for _, v := range values {
		d := v - mean
		variance += d * d
	}
	stddev := math.Sqrt(variance / float64(len(values)))
	return stddev / math.Abs(mean)
}

// Use a differently named hash to avoid collision with cluster_engine
func fnv1a64SamplingHash(data []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, b := range data { h ^= uint64(b); h *= 1099511628211 }
	return h
}

func (e *AdaptiveSamplingEngine) getOrCreateMetric(metric string) *MetricVolatility {
	if mv, ok := e.metrics[metric]; ok { return mv }
	mv := &MetricVolatility{Metric: metric, Rate: e.config.DefaultRate}
	e.metrics[metric] = mv
	e.stats.MetricsTracked = len(e.metrics)
	return mv
}

// GetMetricVolatility returns volatility info for a metric.
func (e *AdaptiveSamplingEngine) GetMetricVolatility(metric string) *MetricVolatility {
	e.mu.RLock(); defer e.mu.RUnlock()
	if mv, ok := e.metrics[metric]; ok { cp := *mv; return &cp }
	return nil
}

// ListMetrics returns all tracked metrics.
func (e *AdaptiveSamplingEngine) ListMetrics() []MetricVolatility {
	e.mu.RLock(); defer e.mu.RUnlock()
	result := make([]MetricVolatility, 0, len(e.metrics))
	for _, mv := range e.metrics { result = append(result, *mv) }
	return result
}

// GetStats returns engine stats.
func (e *AdaptiveSamplingEngine) GetStats() AdaptiveSamplingStats {
	e.mu.RLock(); defer e.mu.RUnlock()
	stats := e.stats
	if stats.TotalIn > 0 { stats.OverallRate = float64(stats.TotalKept) / float64(stats.TotalIn) }
	return stats
}

func (e *AdaptiveSamplingEngine) adjustLoop() {
	ticker := time.NewTicker(e.config.AdjustInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh: return
		case <-ticker.C: // periodic rate adjustment
		}
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *AdaptiveSamplingEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sampling/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListMetrics())
	})
	mux.HandleFunc("/api/v1/sampling/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}


