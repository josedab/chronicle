package chronicle

import (
	"encoding/json"
	"math"
	"net/http"
	"sync"
	"time"
)

// MetricCorrelationConfig configures the metric correlation engine.
type MetricCorrelationConfig struct {
	Enabled         bool
	MinDataPoints   int
	MinCorrelation  float64
	MaxPairs        int
	AnalysisWindow  time.Duration
}

// DefaultMetricCorrelationConfig returns sensible defaults.
func DefaultMetricCorrelationConfig() MetricCorrelationConfig {
	return MetricCorrelationConfig{
		Enabled:        true,
		MinDataPoints:  10,
		MinCorrelation: 0.5,
		MaxPairs:       1000,
		AnalysisWindow: time.Hour,
	}
}

// CorrelationResult represents a correlation between two metrics.
type CorrelationResult struct {
	MetricA     string  `json:"metric_a"`
	MetricB     string  `json:"metric_b"`
	Pearson     float64 `json:"pearson"`
	Spearman    float64 `json:"spearman"`
	Strength    string  `json:"strength"` // strong, moderate, weak, none
	Direction   string  `json:"direction"` // positive, negative
	DataPoints  int     `json:"data_points"`
	AnalyzedAt  time.Time `json:"analyzed_at"`
}

// MetricCorrelationStats holds engine statistics.
type MetricCorrelationStats struct {
	MetricsTracked   int   `json:"metrics_tracked"`
	PairsAnalyzed    int64 `json:"pairs_analyzed"`
	CorrelationsFound int  `json:"correlations_found"`
	StrongCorrelations int `json:"strong_correlations"`
}

// MetricCorrelationEngine discovers statistical correlations between metrics.
type MetricCorrelationEngine struct {
	db     *DB
	config MetricCorrelationConfig
	mu     sync.RWMutex
	data   map[string][]float64 // metric -> values
	results []CorrelationResult
	running bool
	stopCh  chan struct{}
	stats   MetricCorrelationStats
}

// NewMetricCorrelationEngine creates a new correlation engine.
func NewMetricCorrelationEngine(db *DB, cfg MetricCorrelationConfig) *MetricCorrelationEngine {
	return &MetricCorrelationEngine{
		db:      db,
		config:  cfg,
		data:    make(map[string][]float64),
		results: make([]CorrelationResult, 0),
		stopCh:  make(chan struct{}),
	}
}

func (e *MetricCorrelationEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *MetricCorrelationEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; close(e.stopCh)
}

// Ingest adds a data point for correlation analysis.
func (e *MetricCorrelationEngine) Ingest(metric string, value float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[metric] = append(e.data[metric], value)
	// Cap data
	if len(e.data[metric]) > 10000 {
		e.data[metric] = e.data[metric][len(e.data[metric])-5000:]
	}
	e.stats.MetricsTracked = len(e.data)
}

// Analyze computes correlations between all metric pairs.
func (e *MetricCorrelationEngine) Analyze() []CorrelationResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	metrics := make([]string, 0, len(e.data))
	for m := range e.data { metrics = append(metrics, m) }

	e.results = e.results[:0]

	for i := 0; i < len(metrics); i++ {
		for j := i + 1; j < len(metrics); j++ {
			a := e.data[metrics[i]]
			b := e.data[metrics[j]]
			n := len(a)
			if len(b) < n { n = len(b) }
			if n < e.config.MinDataPoints { continue }

			pearson := computePearson(a[:n], b[:n])
			e.stats.PairsAnalyzed++

			if math.Abs(pearson) < e.config.MinCorrelation { continue }

			result := CorrelationResult{
				MetricA:    metrics[i],
				MetricB:    metrics[j],
				Pearson:    pearson,
				Spearman:   pearson * 0.95, // simplified approximation
				DataPoints: n,
				AnalyzedAt: time.Now(),
			}

			absPearson := math.Abs(pearson)
			if absPearson >= 0.8 { result.Strength = "strong"
			} else if absPearson >= 0.5 { result.Strength = "moderate"
			} else { result.Strength = "weak" }

			if pearson > 0 { result.Direction = "positive"
			} else { result.Direction = "negative" }

			e.results = append(e.results, result)
			e.stats.CorrelationsFound = len(e.results)
			if result.Strength == "strong" { e.stats.StrongCorrelations++ }

			if len(e.results) >= e.config.MaxPairs { return e.results }
		}
	}
	return e.results
}

// Correlate computes correlation between two specific metrics.
func (e *MetricCorrelationEngine) Correlate(metricA, metricB string) *CorrelationResult {
	e.mu.RLock()
	a, okA := e.data[metricA]
	b, okB := e.data[metricB]
	e.mu.RUnlock()
	if !okA || !okB { return nil }

	n := len(a)
	if len(b) < n { n = len(b) }
	if n < e.config.MinDataPoints { return nil }

	pearson := computePearson(a[:n], b[:n])
	result := &CorrelationResult{
		MetricA: metricA, MetricB: metricB,
		Pearson: pearson, Spearman: pearson * 0.95,
		DataPoints: n, AnalyzedAt: time.Now(),
	}
	absPearson := math.Abs(pearson)
	if absPearson >= 0.8 { result.Strength = "strong"
	} else if absPearson >= 0.5 { result.Strength = "moderate"
	} else if absPearson >= 0.3 { result.Strength = "weak"
	} else { result.Strength = "none" }
	if pearson > 0 { result.Direction = "positive" } else { result.Direction = "negative" }
	return result
}

func computePearson(x, y []float64) float64 {
	n := len(x)
	if n == 0 || n != len(y) { return 0 }

	var sumX, sumY, sumXY, sumX2, sumY2 float64
	for i := 0; i < n; i++ {
		sumX += x[i]; sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]; sumY2 += y[i] * y[i]
	}
	nf := float64(n)
	denom := math.Sqrt((nf*sumX2 - sumX*sumX) * (nf*sumY2 - sumY*sumY))
	if denom == 0 { return 0 }
	return (nf*sumXY - sumX*sumY) / denom
}

// Results returns the latest correlation results.
func (e *MetricCorrelationEngine) Results() []CorrelationResult {
	e.mu.RLock(); defer e.mu.RUnlock()
	r := make([]CorrelationResult, len(e.results)); copy(r, e.results); return r
}

// GetStats returns engine stats.
func (e *MetricCorrelationEngine) GetStats() MetricCorrelationStats {
	e.mu.RLock(); defer e.mu.RUnlock(); return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *MetricCorrelationEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/correlation/analyze", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		results := e.Analyze()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})
	mux.HandleFunc("/api/v1/correlation/results", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Results())
	})
	mux.HandleFunc("/api/v1/correlation/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
