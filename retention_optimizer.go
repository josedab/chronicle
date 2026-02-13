package chronicle

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// RetentionOptimizerConfig configures the retention policy optimizer.
type RetentionOptimizerConfig struct {
	Enabled        bool          `json:"enabled"`
	StorageBudgetMB int64        `json:"storage_budget_mb"`
	CheckInterval  time.Duration `json:"check_interval"`
	MinRetention   time.Duration `json:"min_retention"`
	MaxRetention   time.Duration `json:"max_retention"`
}

// DefaultRetentionOptimizerConfig returns sensible defaults.
func DefaultRetentionOptimizerConfig() RetentionOptimizerConfig {
	return RetentionOptimizerConfig{
		Enabled:        true,
		StorageBudgetMB: 10240,
		CheckInterval:  10 * time.Minute,
		MinRetention:   24 * time.Hour,
		MaxRetention:   90 * 24 * time.Hour,
	}
}

// RetOptRecommendation represents a retention policy recommendation.
type RetOptRecommendation struct {
	Metric               string        `json:"metric"`
	CurrentRetention     time.Duration `json:"current_retention"`
	RecommendedRetention time.Duration `json:"recommended_retention"`
	Reason               string        `json:"reason"`
	Priority             int           `json:"priority"` // 1 = highest
}

// MetricImportance tracks access patterns for a metric.
type MetricImportance struct {
	Metric          string  `json:"metric"`
	AccessFrequency int64   `json:"access_frequency"`
	QueryCount      int64   `json:"query_count"`
	Importance      float64 `json:"importance"` // 0.0 to 1.0
}

// RetentionOptimizerStats holds optimizer statistics.
type RetentionOptimizerStats struct {
	MetricsTracked       int   `json:"metrics_tracked"`
	TotalAccesses        int64 `json:"total_accesses"`
	RecommendationsCount int   `json:"recommendations_count"`
}

// RetentionOptimizerEngine analyzes metric access patterns to recommend retention policies.
type RetentionOptimizerEngine struct {
	db      *DB
	config  RetentionOptimizerConfig
	mu      sync.RWMutex
	metrics map[string]*MetricImportance
	stats   RetentionOptimizerStats
	stopCh  chan struct{}
	running bool
}

// NewRetentionOptimizerEngine creates a new RetentionOptimizerEngine.
func NewRetentionOptimizerEngine(db *DB, cfg RetentionOptimizerConfig) *RetentionOptimizerEngine {
	return &RetentionOptimizerEngine{
		db:      db,
		config:  cfg,
		metrics: make(map[string]*MetricImportance),
		stopCh:  make(chan struct{}),
	}
}

// Start begins the retention optimizer engine.
func (e *RetentionOptimizerEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop halts the retention optimizer engine.
func (e *RetentionOptimizerEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// RecordAccess records a metric access event.
func (e *RetentionOptimizerEngine) RecordAccess(metric string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.metrics[metric]
	if !ok {
		m = &MetricImportance{Metric: metric}
		e.metrics[metric] = m
	}
	m.AccessFrequency++
	m.QueryCount++
	e.stats.TotalAccesses++
	e.stats.MetricsTracked = len(e.metrics)
	e.recalcImportance()
}

// GetImportance returns the importance score for a metric.
func (e *RetentionOptimizerEngine) GetImportance(metric string) (MetricImportance, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	m, ok := e.metrics[metric]
	if !ok {
		return MetricImportance{}, false
	}
	return *m, true
}

// Analyze produces retention recommendations based on access patterns.
func (e *RetentionOptimizerEngine) Analyze() []RetOptRecommendation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var recs []RetOptRecommendation
	for _, m := range e.metrics {
		rec := RetOptRecommendation{
			Metric:           m.Metric,
			CurrentRetention: e.config.MaxRetention,
		}
		if m.Importance >= 0.7 {
			rec.RecommendedRetention = e.config.MaxRetention
			rec.Reason = "high importance: frequently accessed metric"
			rec.Priority = 3
		} else if m.Importance >= 0.3 {
			rec.RecommendedRetention = (e.config.MaxRetention + e.config.MinRetention) / 2
			rec.Reason = "medium importance: moderate access frequency"
			rec.Priority = 2
		} else {
			rec.RecommendedRetention = e.config.MinRetention
			rec.Reason = "low importance: rarely accessed metric"
			rec.Priority = 1
		}
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Priority < recs[j].Priority
	})
	e.stats.RecommendationsCount = len(recs)
	return recs
}

// GetStats returns optimizer statistics.
func (e *RetentionOptimizerEngine) GetStats() RetentionOptimizerStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *RetentionOptimizerEngine) recalcImportance() {
	var maxFreq int64
	for _, m := range e.metrics {
		if m.AccessFrequency > maxFreq {
			maxFreq = m.AccessFrequency
		}
	}
	if maxFreq == 0 {
		return
	}
	for _, m := range e.metrics {
		m.Importance = float64(m.AccessFrequency) / float64(maxFreq)
	}
}

// RegisterHTTPHandlers registers retention optimizer HTTP endpoints.
func (e *RetentionOptimizerEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/retention-optimizer/analyze", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Analyze())
	})
	mux.HandleFunc("/api/v1/retention-optimizer/importance", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
			return
		}
		imp, ok := e.GetImportance(metric)
		if !ok {
			http.Error(w, "metric not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(imp)
	})
	mux.HandleFunc("/api/v1/retention-optimizer/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/retention-optimizer/record", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
			return
		}
		e.RecordAccess(metric)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "recorded"})
	})
}
