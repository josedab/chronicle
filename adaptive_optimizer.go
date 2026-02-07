package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// AdaptiveOptimizerConfig configures the ML-powered adaptive query optimizer.
type AdaptiveOptimizerConfig struct {
	Enabled                bool          `json:"enabled"`
	LearningRate           float64       `json:"learning_rate"`
	MinSamplesForRecommend int           `json:"min_samples_for_recommend"`
	PlanCacheSize          int           `json:"plan_cache_size"`
	PlanCacheTTL           time.Duration `json:"plan_cache_ttl"`
	StabilityThreshold     float64       `json:"stability_threshold"`
	MaxHistorySize         int           `json:"max_history_size"`
	IndexRecommendEnabled  bool          `json:"index_recommend_enabled"`
	CostModelUpdateFreq    time.Duration `json:"cost_model_update_freq"`
}

// DefaultAdaptiveOptimizerConfig returns sensible defaults.
func DefaultAdaptiveOptimizerConfig() AdaptiveOptimizerConfig {
	return AdaptiveOptimizerConfig{
		Enabled:                true,
		LearningRate:           0.1,
		MinSamplesForRecommend: 50,
		PlanCacheSize:          500,
		PlanCacheTTL:           10 * time.Minute,
		StabilityThreshold:     0.15,
		MaxHistorySize:         10000,
		IndexRecommendEnabled:  true,
		CostModelUpdateFreq:    time.Minute,
	}
}

// QueryExecutionRecord stores statistics about a query execution.
type QueryExecutionRecord struct {
	QueryHash     string            `json:"query_hash"`
	Metric        string            `json:"metric"`
	Duration      time.Duration     `json:"duration"`
	PointsScanned int64             `json:"points_scanned"`
	PointsReturned int64            `json:"points_returned"`
	PlanUsed      string            `json:"plan_used"`
	Timestamp     time.Time         `json:"timestamp"`
	Tags          map[string]string `json:"tags,omitempty"`
}

// IndexRecommendation suggests an index to improve query performance.
type IndexRecommendation struct {
	Metric          string        `json:"metric"`
	TagKeys         []string      `json:"tag_keys"`
	Reason          string        `json:"reason"`
	EstimatedSpeedup float64      `json:"estimated_speedup"`
	QueryCount      int           `json:"affected_query_count"`
	Confidence      float64       `json:"confidence"`
	CreatedAt       time.Time     `json:"created_at"`
}

// QueryPlanCandidate is a possible execution plan.
type QueryPlanCandidate struct {
	ID            string        `json:"id"`
	Strategy      string        `json:"strategy"` // "full_scan", "index_scan", "partition_prune", "cached"
	EstimatedCost float64       `json:"estimated_cost"`
	ActualCost    float64       `json:"actual_cost"`
	Confidence    float64       `json:"confidence"`
	UsageCount    int64         `json:"usage_count"`
}

// CostModelMetrics tracks cost model accuracy.
type CostModelMetrics struct {
	TotalPredictions   int64   `json:"total_predictions"`
	AccuratePredictions int64  `json:"accurate_predictions"`
	MeanAbsoluteError  float64 `json:"mean_absolute_error"`
	LastUpdated        time.Time `json:"last_updated"`
}

// AdaptiveOptimizerStats contains optimizer statistics.
type AdaptiveOptimizerStats struct {
	TotalRecords        int64        `json:"total_records"`
	UniqueQueries       int          `json:"unique_queries"`
	PlanCacheEntries    int          `json:"plan_cache_entries"`
	Recommendations     int          `json:"recommendations"`
	CostModel           CostModelMetrics `json:"cost_model"`
	AvgQueryDuration    time.Duration `json:"avg_query_duration"`
	P95QueryDuration    time.Duration `json:"p95_query_duration"`
	OptimizationSavings float64       `json:"optimization_savings_pct"`
}

// AdaptiveOptimizer is an ML-powered query optimizer that learns from execution history.
type AdaptiveOptimizer struct {
	db     *DB
	config AdaptiveOptimizerConfig

	// Execution history
	history    []QueryExecutionRecord
	queryStats map[string]*queryStatsBucket

	// Plan cache
	planCache map[string]*QueryPlanCandidate

	// Cost model
	costModel CostModelMetrics

	// Index recommendations
	recommendations []IndexRecommendation

	mu sync.RWMutex
}

type queryStatsBucket struct {
	metric        string
	count         int64
	totalDuration time.Duration
	minDuration   time.Duration
	maxDuration   time.Duration
	totalScanned  int64
	totalReturned int64
	durations     []time.Duration
	tagFrequency  map[string]int
}

// NewAdaptiveOptimizer creates a new adaptive query optimizer.
func NewAdaptiveOptimizer(db *DB, cfg AdaptiveOptimizerConfig) *AdaptiveOptimizer {
	return &AdaptiveOptimizer{
		db:              db,
		config:          cfg,
		history:         make([]QueryExecutionRecord, 0),
		queryStats:      make(map[string]*queryStatsBucket),
		planCache:       make(map[string]*QueryPlanCandidate),
		recommendations: make([]IndexRecommendation, 0),
	}
}

// RecordExecution records a query execution for learning.
func (ao *AdaptiveOptimizer) RecordExecution(rec QueryExecutionRecord) {
	ao.mu.Lock()
	defer ao.mu.Unlock()

	rec.Timestamp = time.Now()
	ao.history = append(ao.history, rec)
	if len(ao.history) > ao.config.MaxHistorySize {
		ao.history = ao.history[len(ao.history)-ao.config.MaxHistorySize:]
	}

	// Update per-query stats
	bucket, ok := ao.queryStats[rec.QueryHash]
	if !ok {
		bucket = &queryStatsBucket{
			metric:       rec.Metric,
			minDuration:  rec.Duration,
			tagFrequency: make(map[string]int),
		}
		ao.queryStats[rec.QueryHash] = bucket
	}

	bucket.count++
	bucket.totalDuration += rec.Duration
	if rec.Duration < bucket.minDuration {
		bucket.minDuration = rec.Duration
	}
	if rec.Duration > bucket.maxDuration {
		bucket.maxDuration = rec.Duration
	}
	bucket.totalScanned += rec.PointsScanned
	bucket.totalReturned += rec.PointsReturned
	bucket.durations = append(bucket.durations, rec.Duration)
	if len(bucket.durations) > 100 {
		bucket.durations = bucket.durations[len(bucket.durations)-100:]
	}

	for k := range rec.Tags {
		bucket.tagFrequency[k]++
	}

	// Update cost model
	ao.costModel.TotalPredictions++
	if plan, ok := ao.planCache[rec.QueryHash]; ok {
		error := math.Abs(plan.EstimatedCost - float64(rec.Duration.Microseconds()))
		ao.costModel.MeanAbsoluteError = ao.costModel.MeanAbsoluteError*(1-ao.config.LearningRate) + error*ao.config.LearningRate
		plan.ActualCost = float64(rec.Duration.Microseconds())
		plan.UsageCount++

		if error < plan.EstimatedCost*ao.config.StabilityThreshold {
			ao.costModel.AccuratePredictions++
		}
	}
	ao.costModel.LastUpdated = time.Now()
}

// SelectPlan chooses the best execution plan for a query.
func (ao *AdaptiveOptimizer) SelectPlan(queryHash, metric string, tags map[string]string) *QueryPlanCandidate {
	ao.mu.RLock()
	defer ao.mu.RUnlock()

	if cached, ok := ao.planCache[queryHash]; ok {
		return cached
	}

	// Analyze query stats to decide plan
	bucket := ao.queryStats[queryHash]
	plan := &QueryPlanCandidate{
		ID:       fmt.Sprintf("plan-%s", queryHash[:8]),
		Strategy: "full_scan",
	}

	if bucket != nil && bucket.count >= int64(ao.config.MinSamplesForRecommend) {
		selectivity := float64(bucket.totalReturned) / float64(bucket.totalScanned+1)

		if selectivity < 0.01 && len(tags) > 0 {
			plan.Strategy = "index_scan"
			plan.EstimatedCost = float64(bucket.totalDuration/time.Duration(bucket.count)) * selectivity * 10
		} else if selectivity < 0.1 {
			plan.Strategy = "partition_prune"
			plan.EstimatedCost = float64(bucket.totalDuration / time.Duration(bucket.count))
		}

		plan.Confidence = math.Min(float64(bucket.count)/100.0, 1.0)
	}

	return plan
}

// GenerateRecommendations analyzes query patterns and generates index recommendations.
func (ao *AdaptiveOptimizer) GenerateRecommendations() []IndexRecommendation {
	ao.mu.Lock()
	defer ao.mu.Unlock()

	ao.recommendations = ao.recommendations[:0]

	for queryHash, bucket := range ao.queryStats {
		if bucket.count < int64(ao.config.MinSamplesForRecommend) {
			continue
		}

		avgDuration := bucket.totalDuration / time.Duration(bucket.count)
		selectivity := float64(bucket.totalReturned) / float64(bucket.totalScanned+1)

		// Recommend index if high selectivity and significant scan overhead
		if selectivity < 0.05 && avgDuration > 10*time.Millisecond {
			var tagKeys []string
			for k, freq := range bucket.tagFrequency {
				if freq > int(bucket.count/2) {
					tagKeys = append(tagKeys, k)
				}
			}
			sort.Strings(tagKeys)

			if len(tagKeys) > 0 {
				speedup := 1.0 / (selectivity + 0.01)
				ao.recommendations = append(ao.recommendations, IndexRecommendation{
					Metric:           bucket.metric,
					TagKeys:          tagKeys,
					Reason:           fmt.Sprintf("query %s scans %.0f%% of data but returns only %.2f%%", queryHash[:8], (1-selectivity)*100, selectivity*100),
					EstimatedSpeedup: speedup,
					QueryCount:       int(bucket.count),
					Confidence:       math.Min(float64(bucket.count)/200.0, 0.95),
					CreatedAt:        time.Now(),
				})
			}
		}
	}

	sort.Slice(ao.recommendations, func(i, j int) bool {
		return ao.recommendations[i].EstimatedSpeedup > ao.recommendations[j].EstimatedSpeedup
	})

	return ao.recommendations
}

// GetRecommendations returns current index recommendations.
func (ao *AdaptiveOptimizer) GetRecommendations() []IndexRecommendation {
	ao.mu.RLock()
	defer ao.mu.RUnlock()

	result := make([]IndexRecommendation, len(ao.recommendations))
	copy(result, ao.recommendations)
	return result
}

// Stats returns optimizer statistics.
func (ao *AdaptiveOptimizer) Stats() AdaptiveOptimizerStats {
	ao.mu.RLock()
	defer ao.mu.RUnlock()

	var totalDuration time.Duration
	durations := make([]time.Duration, 0, len(ao.history))
	for _, rec := range ao.history {
		totalDuration += rec.Duration
		durations = append(durations, rec.Duration)
	}

	var avgDuration, p95Duration time.Duration
	if len(durations) > 0 {
		avgDuration = totalDuration / time.Duration(len(durations))
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		p95Duration = durations[int(float64(len(durations))*0.95)]
	}

	var savings float64
	for _, bucket := range ao.queryStats {
		if bucket.count >= 2 && bucket.maxDuration > 0 {
			avg := bucket.totalDuration / time.Duration(bucket.count)
			improvement := 1.0 - float64(avg)/float64(bucket.maxDuration)
			if improvement > 0 {
				savings += improvement
			}
		}
	}
	if len(ao.queryStats) > 0 {
		savings /= float64(len(ao.queryStats))
	}

	return AdaptiveOptimizerStats{
		TotalRecords:        int64(len(ao.history)),
		UniqueQueries:       len(ao.queryStats),
		PlanCacheEntries:    len(ao.planCache),
		Recommendations:     len(ao.recommendations),
		CostModel:           ao.costModel,
		AvgQueryDuration:    avgDuration,
		P95QueryDuration:    p95Duration,
		OptimizationSavings: savings * 100,
	}
}

// RegisterHTTPHandlers registers adaptive optimizer HTTP endpoints.
func (ao *AdaptiveOptimizer) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/optimizer/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ao.Stats())
	})
	mux.HandleFunc("/api/v1/optimizer/recommendations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			recs := ao.GenerateRecommendations()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(recs)
		} else {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ao.GetRecommendations())
		}
	})
}
