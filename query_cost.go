package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// QueryCostConfig configures the query cost estimator.
type QueryCostConfig struct {
	Enabled         bool
	MaxCostBudget   float64
	WarnThreshold   float64
	RejectThreshold float64
	CostPerPartition float64
	CostPerPoint    float64
	CostPerByte     float64
}

// DefaultQueryCostConfig returns sensible defaults.
func DefaultQueryCostConfig() QueryCostConfig {
	return QueryCostConfig{
		Enabled:         true,
		MaxCostBudget:   10000,
		WarnThreshold:   5000,
		RejectThreshold: 50000,
		CostPerPartition: 10.0,
		CostPerPoint:    0.001,
		CostPerByte:     0.0001,
	}
}

// CostEstimate describes the estimated cost of a query.
type CostEstimate struct {
	QueryMetric      string  `json:"query_metric"`
	EstimatedCost    float64 `json:"estimated_cost"`
	PartitionsCost   float64 `json:"partitions_cost"`
	PointsCost       float64 `json:"points_cost"`
	IOCost           float64 `json:"io_cost"`
	EstPartitions    int     `json:"estimated_partitions"`
	EstPoints        int64   `json:"estimated_points"`
	EstBytes         int64   `json:"estimated_bytes"`
	Verdict          string  `json:"verdict"` // allow, warn, reject
	EstDuration      time.Duration `json:"estimated_duration"`
}

// QueryCostStats holds engine statistics.
type QueryCostStats struct {
	TotalEstimates int64   `json:"total_estimates"`
	Allowed        int64   `json:"allowed"`
	Warned         int64   `json:"warned"`
	Rejected       int64   `json:"rejected"`
	AvgCost        float64 `json:"avg_cost"`
}

// QueryCostEstimator estimates query cost before execution.
type QueryCostEstimator struct {
	db     *DB
	config QueryCostConfig
	mu     sync.RWMutex
	running bool
	stopCh  chan struct{}
	stats   QueryCostStats
}

// NewQueryCostEstimator creates a new cost estimator.
func NewQueryCostEstimator(db *DB, cfg QueryCostConfig) *QueryCostEstimator {
	return &QueryCostEstimator{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

func (e *QueryCostEstimator) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *QueryCostEstimator) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; close(e.stopCh)
}

// Estimate produces a cost estimate for a query without executing it.
func (e *QueryCostEstimator) Estimate(q *Query) (*CostEstimate, error) {
	if q == nil { return nil, fmt.Errorf("nil query") }
	if q.Metric == "" { return nil, fmt.Errorf("metric required") }

	est := &CostEstimate{QueryMetric: q.Metric}

	// Estimate partitions based on time range
	if q.End > 0 && q.Start > 0 {
		rangeDuration := time.Duration(q.End - q.Start)
		partitionDuration := time.Hour // default partition size
		est.EstPartitions = int(rangeDuration/partitionDuration) + 1
	} else {
		est.EstPartitions = 24 // default: assume 24 hours of data
	}

	// Estimate points per partition
	pointsPerPartition := int64(3600) // 1 point/sec for 1 hour
	est.EstPoints = int64(est.EstPartitions) * pointsPerPartition
	est.EstBytes = est.EstPoints * 24 // ~24 bytes per point

	if q.Limit > 0 && int64(q.Limit) < est.EstPoints {
		est.EstPoints = int64(q.Limit)
	}

	// Compute cost components
	est.PartitionsCost = float64(est.EstPartitions) * e.config.CostPerPartition
	est.PointsCost = float64(est.EstPoints) * e.config.CostPerPoint
	est.IOCost = float64(est.EstBytes) * e.config.CostPerByte
	est.EstimatedCost = est.PartitionsCost + est.PointsCost + est.IOCost

	// Estimate duration (rough heuristic)
	est.EstDuration = time.Duration(est.EstimatedCost * float64(time.Microsecond))

	// Verdict
	if est.EstimatedCost >= e.config.RejectThreshold {
		est.Verdict = "reject"
	} else if est.EstimatedCost >= e.config.WarnThreshold {
		est.Verdict = "warn"
	} else {
		est.Verdict = "allow"
	}

	// Update stats
	e.mu.Lock()
	e.stats.TotalEstimates++
	switch est.Verdict {
	case "allow": e.stats.Allowed++
	case "warn": e.stats.Warned++
	case "reject": e.stats.Rejected++
	}
	if e.stats.AvgCost == 0 {
		e.stats.AvgCost = est.EstimatedCost
	} else {
		e.stats.AvgCost = (e.stats.AvgCost + est.EstimatedCost) / 2
	}
	e.mu.Unlock()

	return est, nil
}

// ShouldExecute returns true if the query is within budget.
func (e *QueryCostEstimator) ShouldExecute(q *Query) (bool, *CostEstimate, error) {
	est, err := e.Estimate(q)
	if err != nil { return false, nil, err }
	return est.Verdict != "reject", est, nil
}

// GetStats returns engine stats.
func (e *QueryCostEstimator) GetStats() QueryCostStats {
	e.mu.RLock(); defer e.mu.RUnlock(); return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *QueryCostEstimator) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/cost/estimate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var q Query
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		est, err := e.Estimate(&q)
		if err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(est)
	})
	mux.HandleFunc("/api/v1/cost/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
