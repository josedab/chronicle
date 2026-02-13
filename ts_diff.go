package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// TSDiffConfig configures the time-series diff engine.
type TSDiffConfig struct {
	Enabled         bool
	MaxComparePoints int
	SignificanceLevel float64
}

// DefaultTSDiffConfig returns sensible defaults.
func DefaultTSDiffConfig() TSDiffConfig {
	return TSDiffConfig{
		Enabled:          true,
		MaxComparePoints: 100000,
		SignificanceLevel: 0.05,
	}
}

// TSDiffRequest specifies what to compare.
type TSDiffRequest struct {
	Metric  string `json:"metric"`
	WindowA [2]int64 `json:"window_a"` // [start, end] nanoseconds
	WindowB [2]int64 `json:"window_b"`
	TagsA   map[string]string `json:"tags_a,omitempty"`
	TagsB   map[string]string `json:"tags_b,omitempty"`
}

// TSDiffResult holds the comparison results.
type TSDiffResult struct {
	Metric         string  `json:"metric"`
	MeanA          float64 `json:"mean_a"`
	MeanB          float64 `json:"mean_b"`
	StddevA        float64 `json:"stddev_a"`
	StddevB        float64 `json:"stddev_b"`
	MinA           float64 `json:"min_a"`
	MinB           float64 `json:"min_b"`
	MaxA           float64 `json:"max_a"`
	MaxB           float64 `json:"max_b"`
	CountA         int     `json:"count_a"`
	CountB         int     `json:"count_b"`
	MeanDiff       float64 `json:"mean_diff"`
	MeanDiffPct    float64 `json:"mean_diff_pct"`
	Significant    bool    `json:"significant"`
	TStatistic     float64 `json:"t_statistic"`
	EffectSize     float64 `json:"effect_size"` // Cohen's d
	Verdict        string  `json:"verdict"` // "no_change", "increase", "decrease", "significant_change"
	AnalyzedAt     time.Time `json:"analyzed_at"`
}

// TSDiffStats holds engine statistics.
type TSDiffStats struct {
	TotalComparisons int64 `json:"total_comparisons"`
	SignificantDiffs int64 `json:"significant_diffs"`
}

// TSDiffEngine compares metric behavior across time windows.
type TSDiffEngine struct {
	db     *DB
	config TSDiffConfig
	mu     sync.RWMutex
	history []TSDiffResult
	running bool
	stopCh  chan struct{}
	stats   TSDiffStats
}

// NewTSDiffEngine creates a new diff engine.
func NewTSDiffEngine(db *DB, cfg TSDiffConfig) *TSDiffEngine {
	return &TSDiffEngine{
		db:      db,
		config:  cfg,
		history: make([]TSDiffResult, 0),
		stopCh:  make(chan struct{}),
	}
}

func (e *TSDiffEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *TSDiffEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; close(e.stopCh)
}

// Compare compares a metric across two time windows.
func (e *TSDiffEngine) Compare(req TSDiffRequest) (*TSDiffResult, error) {
	if req.Metric == "" { return nil, fmt.Errorf("metric required") }

	// Fetch data for window A
	qA := &Query{Metric: req.Metric, Start: req.WindowA[0], End: req.WindowA[1], Tags: req.TagsA}
	resultA, err := e.db.Execute(qA)
	if err != nil { return nil, fmt.Errorf("window A query: %w", err) }

	// Fetch data for window B
	qB := &Query{Metric: req.Metric, Start: req.WindowB[0], End: req.WindowB[1], Tags: req.TagsB}
	resultB, err := e.db.Execute(qB)
	if err != nil { return nil, fmt.Errorf("window B query: %w", err) }

	var valuesA, valuesB []float64
	if resultA != nil {
		for _, p := range resultA.Points { valuesA = append(valuesA, p.Value) }
	}
	if resultB != nil {
		for _, p := range resultB.Points { valuesB = append(valuesB, p.Value) }
	}

	return e.computeDiff(req.Metric, valuesA, valuesB), nil
}

// CompareValues compares two slices of values directly.
func (e *TSDiffEngine) CompareValues(metric string, valuesA, valuesB []float64) *TSDiffResult {
	return e.computeDiff(metric, valuesA, valuesB)
}

func (e *TSDiffEngine) computeDiff(metric string, a, b []float64) *TSDiffResult {
	statsA := computeTSDescriptiveStats(a)
	statsB := computeTSDescriptiveStats(b)

	result := &TSDiffResult{
		Metric:     metric,
		MeanA:      statsA.mean, MeanB: statsB.mean,
		StddevA:    statsA.stddev, StddevB: statsB.stddev,
		MinA:       statsA.min, MinB: statsB.min,
		MaxA:       statsA.max, MaxB: statsB.max,
		CountA:     len(a), CountB: len(b),
		MeanDiff:   statsB.mean - statsA.mean,
		AnalyzedAt: time.Now(),
	}

	if statsA.mean != 0 {
		result.MeanDiffPct = (result.MeanDiff / math.Abs(statsA.mean)) * 100
	}

	// Welch's t-test
	if len(a) > 1 && len(b) > 1 && (statsA.stddev > 0 || statsB.stddev > 0) {
		nA, nB := float64(len(a)), float64(len(b))
		seA := statsA.stddev / math.Sqrt(nA)
		seB := statsB.stddev / math.Sqrt(nB)
		seDiff := math.Sqrt(seA*seA + seB*seB)
		if seDiff > 0 {
			result.TStatistic = result.MeanDiff / seDiff
		}
	}

	// Cohen's d effect size
	pooledStd := math.Sqrt((statsA.stddev*statsA.stddev + statsB.stddev*statsB.stddev) / 2)
	if pooledStd > 0 {
		result.EffectSize = math.Abs(result.MeanDiff) / pooledStd
	}

	// Significance (simplified: |t| > 2 roughly corresponds to p < 0.05)
	result.Significant = math.Abs(result.TStatistic) > 2.0

	// Verdict
	if !result.Significant {
		result.Verdict = "no_change"
	} else if result.MeanDiff > 0 {
		result.Verdict = "increase"
	} else {
		result.Verdict = "decrease"
	}

	e.mu.Lock()
	e.stats.TotalComparisons++
	if result.Significant { e.stats.SignificantDiffs++ }
	e.history = append(e.history, *result)
	if len(e.history) > 1000 { e.history = e.history[len(e.history)-500:] }
	e.mu.Unlock()

	return result
}

type tsDiffStats struct {
	mean, stddev, min, max float64
}

func computeTSDescriptiveStats(values []float64) tsDiffStats {
	if len(values) == 0 { return tsDiffStats{} }
	s := tsDiffStats{min: values[0], max: values[0]}
	var sum float64
	for _, v := range values {
		sum += v
		if v < s.min { s.min = v }
		if v > s.max { s.max = v }
	}
	s.mean = sum / float64(len(values))
	if len(values) > 1 {
		var variance float64
		for _, v := range values { d := v - s.mean; variance += d * d }
		s.stddev = math.Sqrt(variance / float64(len(values)-1))
	}
	return s
}

// History returns recent diff results.
func (e *TSDiffEngine) History(limit int) []TSDiffResult {
	e.mu.RLock(); defer e.mu.RUnlock()
	if limit <= 0 || limit > len(e.history) { limit = len(e.history) }
	result := make([]TSDiffResult, limit)
	copy(result, e.history[len(e.history)-limit:])
	return result
}

// GetStats returns engine stats.
func (e *TSDiffEngine) GetStats() TSDiffStats {
	e.mu.RLock(); defer e.mu.RUnlock(); return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *TSDiffEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/diff/compare", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var req TSDiffRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		result, err := e.Compare(req)
		if err != nil { http.Error(w, err.Error(), http.StatusInternalServerError); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/api/v1/diff/history", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.History(50))
	})
	mux.HandleFunc("/api/v1/diff/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
