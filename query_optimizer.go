package chronicle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// QueryOptimizer provides adaptive query optimization for Chronicle
// Uses workload profiling, cost models, and feedback loops for auto-tuning

// QueryOptimizerConfig configures the query optimizer
type QueryOptimizerConfig struct {
	// Enable query optimization
	Enabled bool

	// Enable workload profiling
	ProfilingEnabled bool

	// Profiling window for statistics
	ProfilingWindow time.Duration

	// Enable cost-based optimization
	CostBasedEnabled bool

	// Enable automatic index recommendations
	IndexRecommendations bool

	// Enable query plan caching
	PlanCacheEnabled bool
	PlanCacheTTL     time.Duration
	PlanCacheMaxSize int

	// Enable feedback loop
	FeedbackEnabled bool

	// Minimum samples before making recommendations
	MinSamples int

	// Learning rate for cost model updates
	LearningRate float64
}

// DefaultQueryOptimizerConfig returns default configuration
func DefaultQueryOptimizerConfig() *QueryOptimizerConfig {
	return &QueryOptimizerConfig{
		Enabled:              true,
		ProfilingEnabled:     true,
		ProfilingWindow:      time.Hour,
		CostBasedEnabled:     true,
		IndexRecommendations: true,
		PlanCacheEnabled:     true,
		PlanCacheTTL:         5 * time.Minute,
		PlanCacheMaxSize:     1000,
		FeedbackEnabled:      true,
		MinSamples:           100,
		LearningRate:         0.1,
	}
}

// QueryOptimizer manages query optimization
type AdaptiveQueryOptimizer struct {
	db     *DB
	config *QueryOptimizerConfig

	// Workload profiler
	profiler *QueryWorkloadProfiler

	// Cost model
	costModel *CostModel

	// Query plan cache
	planCache   map[string]*CachedPlan
	planCacheMu sync.RWMutex

	// Feedback collector
	feedback   []QueryFeedback
	feedbackMu sync.RWMutex

	// Recommendations
	recommendations   []Recommendation
	recommendationsMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	queriesOptimized int64
	planCacheHits    int64
	planCacheMisses  int64
	costEstimates    int64
}

// NewQueryOptimizer creates a new query optimizer
func NewAdaptiveQueryOptimizer(db *DB, config *QueryOptimizerConfig) (*AdaptiveQueryOptimizer, error) {
	if config == nil {
		config = DefaultQueryOptimizerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	qo := &AdaptiveQueryOptimizer{
		db:              db,
		config:          config,
		planCache:       make(map[string]*CachedPlan),
		feedback:        make([]QueryFeedback, 0),
		recommendations: make([]Recommendation, 0),
		ctx:             ctx,
		cancel:          cancel,
	}

	// Initialize profiler
	qo.profiler = newWorkloadProfiler(config.ProfilingWindow)

	// Initialize cost model
	qo.costModel = newCostModel()

	// Start background workers
	if config.FeedbackEnabled {
		qo.wg.Add(1)
		go qo.feedbackWorker()
	}

	if config.IndexRecommendations {
		qo.wg.Add(1)
		go qo.recommendationWorker()
	}

	return qo, nil
}

// Optimize optimizes a query and returns an execution plan
func (qo *AdaptiveQueryOptimizer) Optimize(query *Query) (*OptimizedQueryPlan, error) {
	if !qo.config.Enabled {
		return qo.createDefaultPlan(query), nil
	}

	atomic.AddInt64(&qo.queriesOptimized, 1)

	// Check plan cache
	if qo.config.PlanCacheEnabled {
		cacheKey := qo.computePlanCacheKey(query)
		if plan := qo.getCachedPlan(cacheKey); plan != nil {
			atomic.AddInt64(&qo.planCacheHits, 1)
			return plan, nil
		}
		atomic.AddInt64(&qo.planCacheMisses, 1)
	}

	// Profile the query
	if qo.config.ProfilingEnabled {
		qo.profiler.recordQuery(query)
	}

	// Generate candidate plans
	candidates := qo.generateCandidatePlans(query)

	// Estimate costs for each plan
	for _, plan := range candidates {
		plan.EstimatedCost = qo.costModel.estimateCost(plan)
		atomic.AddInt64(&qo.costEstimates, 1)
	}

	// Select best plan
	bestPlan := qo.selectBestPlan(candidates)

	// Cache the plan
	if qo.config.PlanCacheEnabled {
		cacheKey := qo.computePlanCacheKey(query)
		qo.cachePlan(cacheKey, bestPlan)
	}

	return bestPlan, nil
}

// RecordFeedback records query execution feedback for learning
func (qo *AdaptiveQueryOptimizer) RecordFeedback(feedback *QueryFeedback) {
	if !qo.config.FeedbackEnabled {
		return
	}

	qo.feedbackMu.Lock()
	qo.feedback = append(qo.feedback, *feedback)

	// Keep last 10000 entries
	if len(qo.feedback) > 10000 {
		qo.feedback = qo.feedback[1000:]
	}
	qo.feedbackMu.Unlock()

	// Update cost model with actual results
	qo.costModel.updateFromFeedback(feedback)
}

// GetRecommendations returns current optimization recommendations
func (qo *AdaptiveQueryOptimizer) GetRecommendations() []Recommendation {
	qo.recommendationsMu.RLock()
	defer qo.recommendationsMu.RUnlock()

	result := make([]Recommendation, len(qo.recommendations))
	copy(result, qo.recommendations)
	return result
}

// GetWorkloadProfile returns the current workload profile
func (qo *AdaptiveQueryOptimizer) GetWorkloadProfile() *QueryWorkloadProfile {
	return qo.profiler.getProfile()
}

// QueryPlan represents an optimized query execution plan
type OptimizedQueryPlan struct {
	ID             string            `json:"id"`
	Query          *Query            `json:"query"`
	Strategy       ExecutionStrategy `json:"strategy"`
	EstimatedCost  float64           `json:"estimated_cost"`
	EstimatedRows  int64             `json:"estimated_rows"`
	EstimatedTime  time.Duration     `json:"estimated_time"`
	IndexUsed      string            `json:"index_used,omitempty"`
	Parallelism    int               `json:"parallelism"`
	Steps          []PlanStep        `json:"steps"`
	Hints          []string          `json:"hints"`
	CreatedAt      time.Time         `json:"created_at"`
}

// ExecutionStrategy defines how a query will be executed
type ExecutionStrategy string

const (
	StrategyFullScan       ExecutionStrategy = "full_scan"
	StrategyIndexScan      ExecutionStrategy = "index_scan"
	StrategyRangeScan      ExecutionStrategy = "range_scan"
	StrategyIndexSeek      ExecutionStrategy = "index_seek"
	StrategyParallelScan   ExecutionStrategy = "parallel_scan"
	StrategyHashAggregate  ExecutionStrategy = "hash_aggregate"
	StrategySortAggregate  ExecutionStrategy = "sort_aggregate"
	StrategyMerge          ExecutionStrategy = "merge"
)

// PlanStep represents a step in the execution plan
type PlanStep struct {
	Name          string            `json:"name"`
	Type          string            `json:"type"`
	Description   string            `json:"description"`
	EstimatedCost float64           `json:"estimated_cost"`
	OutputRows    int64             `json:"output_rows"`
	Properties    map[string]string `json:"properties,omitempty"`
}

// CachedPlan represents a cached query plan
type CachedPlan struct {
	Plan      *OptimizedQueryPlan
	CachedAt  time.Time
	HitCount  int64
}

// QueryFeedback contains feedback about query execution
type QueryFeedback struct {
	PlanID        string        `json:"plan_id"`
	Query         *Query        `json:"query"`
	Strategy      ExecutionStrategy `json:"strategy"`
	ActualCost    float64       `json:"actual_cost"`
	ActualRows    int64         `json:"actual_rows"`
	ActualTime    time.Duration `json:"actual_time"`
	MemoryUsed    int64         `json:"memory_used"`
	Success       bool          `json:"success"`
	ErrorMessage  string        `json:"error_message,omitempty"`
	Timestamp     time.Time     `json:"timestamp"`
}

// Recommendation represents an optimization recommendation
type Recommendation struct {
	ID          string            `json:"id"`
	Type        RecommendationType `json:"type"`
	Priority    int               `json:"priority"` // 1-10, higher = more important
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Impact      string            `json:"impact"`
	Action      string            `json:"action"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

// RecommendationType defines the type of recommendation
type RecommendationType string

const (
	RecommendationCreateIndex    RecommendationType = "create_index"
	RecommendationDropIndex      RecommendationType = "drop_index"
	RecommendationPartition      RecommendationType = "partition"
	RecommendationCompression    RecommendationType = "compression"
	RecommendationDownsample     RecommendationType = "downsample"
	RecommendationParallelism    RecommendationType = "parallelism"
	RecommendationRetention      RecommendationType = "retention"
	RecommendationCaching        RecommendationType = "caching"
	RecommendationSchema         RecommendationType = "schema"
)

func (qo *AdaptiveQueryOptimizer) createDefaultPlan(query *Query) *OptimizedQueryPlan {
	return &OptimizedQueryPlan{
		ID:            fmt.Sprintf("plan_%d", time.Now().UnixNano()),
		Query:         query,
		Strategy:      StrategyFullScan,
		EstimatedCost: 100.0,
		Parallelism:   1,
		Steps: []PlanStep{
			{Name: "scan", Type: "table_scan", Description: "Full table scan"},
			{Name: "filter", Type: "filter", Description: "Apply time range filter"},
		},
		CreatedAt: time.Now(),
	}
}

func (qo *AdaptiveQueryOptimizer) computePlanCacheKey(query *Query) string {
	aggStr := ""
	if query.Aggregation != nil {
		aggStr = fmt.Sprintf("%d", query.Aggregation.Function)
	}
	return fmt.Sprintf("%s:%d:%d:%s:%d",
		query.Metric,
		query.Start,
		query.End,
		aggStr,
		query.Limit)
}

func (qo *AdaptiveQueryOptimizer) getCachedPlan(key string) *OptimizedQueryPlan {
	qo.planCacheMu.RLock()
	cached, exists := qo.planCache[key]
	qo.planCacheMu.RUnlock()

	if !exists {
		return nil
	}

	if time.Since(cached.CachedAt) > qo.config.PlanCacheTTL {
		return nil
	}

	atomic.AddInt64(&cached.HitCount, 1)
	return cached.Plan
}

func (qo *AdaptiveQueryOptimizer) cachePlan(key string, plan *OptimizedQueryPlan) {
	qo.planCacheMu.Lock()
	defer qo.planCacheMu.Unlock()

	// Evict if cache is full
	if len(qo.planCache) >= qo.config.PlanCacheMaxSize {
		qo.evictLRUPlan()
	}

	qo.planCache[key] = &CachedPlan{
		Plan:     plan,
		CachedAt: time.Now(),
	}
}

func (qo *AdaptiveQueryOptimizer) evictLRUPlan() {
	var lruKey string
	var minHits int64 = math.MaxInt64

	for key, cached := range qo.planCache {
		if cached.HitCount < minHits {
			minHits = cached.HitCount
			lruKey = key
		}
	}

	if lruKey != "" {
		delete(qo.planCache, lruKey)
	}
}

func (qo *AdaptiveQueryOptimizer) generateCandidatePlans(query *Query) []*OptimizedQueryPlan {
	candidates := make([]*OptimizedQueryPlan, 0)

	// Full scan plan (baseline)
	fullScan := &OptimizedQueryPlan{
		ID:       fmt.Sprintf("plan_%d_fullscan", time.Now().UnixNano()),
		Query:    query,
		Strategy: StrategyFullScan,
		Steps: []PlanStep{
			{Name: "scan", Type: "full_scan", Description: "Scan all data"},
			{Name: "filter", Type: "filter", Description: "Apply filters"},
		},
		Parallelism: 1,
		CreatedAt:   time.Now(),
	}
	candidates = append(candidates, fullScan)

	// Index scan if series is specified
	if query.Metric != "" {
		indexScan := &OptimizedQueryPlan{
			ID:        fmt.Sprintf("plan_%d_indexscan", time.Now().UnixNano()),
			Query:     query,
			Strategy:  StrategyIndexScan,
			IndexUsed: "series_idx",
			Steps: []PlanStep{
				{Name: "index_lookup", Type: "index_scan", Description: "Index lookup on series"},
				{Name: "filter", Type: "filter", Description: "Apply time filter"},
			},
			Parallelism: 1,
			CreatedAt:   time.Now(),
		}
		candidates = append(candidates, indexScan)
	}

	// Range scan if time range is specified
	if query.Start > 0 && query.End > 0 {
		rangeScan := &OptimizedQueryPlan{
			ID:       fmt.Sprintf("plan_%d_rangescan", time.Now().UnixNano()),
			Query:    query,
			Strategy: StrategyRangeScan,
			Steps: []PlanStep{
				{Name: "range_scan", Type: "range_scan", Description: "Scan time range"},
				{Name: "filter", Type: "filter", Description: "Apply additional filters"},
			},
			Parallelism: 1,
			CreatedAt:   time.Now(),
		}
		candidates = append(candidates, rangeScan)
	}

	// Parallel scan for large queries
	if query.End-query.Start > int64(24*time.Hour) {
		parallelScan := &OptimizedQueryPlan{
			ID:       fmt.Sprintf("plan_%d_parallel", time.Now().UnixNano()),
			Query:    query,
			Strategy: StrategyParallelScan,
			Steps: []PlanStep{
				{Name: "partition", Type: "partition", Description: "Partition data by time"},
				{Name: "parallel_scan", Type: "parallel_scan", Description: "Scan partitions in parallel"},
				{Name: "merge", Type: "merge", Description: "Merge results"},
			},
			Parallelism: 4,
			CreatedAt:   time.Now(),
		}
		candidates = append(candidates, parallelScan)
	}

	// Aggregation-specific plans
	if query.Aggregation != nil {
		hashAgg := &OptimizedQueryPlan{
			ID:       fmt.Sprintf("plan_%d_hashagg", time.Now().UnixNano()),
			Query:    query,
			Strategy: StrategyHashAggregate,
			Steps: []PlanStep{
				{Name: "scan", Type: "scan", Description: "Scan data"},
				{Name: "hash_aggregate", Type: "aggregate", Description: "Hash-based aggregation"},
			},
			Parallelism: 1,
			CreatedAt:   time.Now(),
		}
		candidates = append(candidates, hashAgg)
	}

	return candidates
}

func (qo *AdaptiveQueryOptimizer) selectBestPlan(candidates []*OptimizedQueryPlan) *OptimizedQueryPlan {
	if len(candidates) == 0 {
		return nil
	}

	// Sort by estimated cost
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].EstimatedCost < candidates[j].EstimatedCost
	})

	return candidates[0]
}

func (qo *AdaptiveQueryOptimizer) feedbackWorker() {
	defer qo.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-qo.ctx.Done():
			return
		case <-ticker.C:
			qo.processFeedback()
		}
	}
}

func (qo *AdaptiveQueryOptimizer) processFeedback() {
	qo.feedbackMu.RLock()
	feedbackCopy := make([]QueryFeedback, len(qo.feedback))
	copy(feedbackCopy, qo.feedback)
	qo.feedbackMu.RUnlock()

	if len(feedbackCopy) < qo.config.MinSamples {
		return
	}

	// Analyze feedback and update cost model
	var totalEstError float64
	for _, fb := range feedbackCopy {
		if fb.ActualCost > 0 {
			// Compare estimated vs actual
			// This would feed into cost model training
			totalEstError += math.Abs(fb.ActualCost - float64(fb.ActualRows))
		}
	}

	// Update cost model coefficients
	avgError := totalEstError / float64(len(feedbackCopy))
	qo.costModel.adjustCoefficients(avgError, qo.config.LearningRate)
}

func (qo *AdaptiveQueryOptimizer) recommendationWorker() {
	defer qo.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-qo.ctx.Done():
			return
		case <-ticker.C:
			qo.generateRecommendations()
		}
	}
}

func (qo *AdaptiveQueryOptimizer) generateRecommendations() {
	profile := qo.profiler.getProfile()
	recommendations := make([]Recommendation, 0)

	// Analyze hot series
	for series, stats := range profile.SeriesStats {
		// Recommend index for frequently queried series
		if stats.QueryCount > 100 && stats.AvgDuration > time.Millisecond*100 {
			recommendations = append(recommendations, Recommendation{
				ID:          fmt.Sprintf("rec_idx_%s", series),
				Type:        RecommendationCreateIndex,
				Priority:    8,
				Title:       fmt.Sprintf("Create index on series: %s", series),
				Description: fmt.Sprintf("Series %s has %d queries with avg duration %v", series, stats.QueryCount, stats.AvgDuration),
				Impact:      "Could reduce query time by 50-80%",
				Action:      fmt.Sprintf("CREATE INDEX ON series(%s)", series),
				CreatedAt:   time.Now(),
			})
		}

		// Recommend downsampling for historical data
		if stats.TimeRangeSpan > 30*24*time.Hour && stats.DataPoints > 1000000 {
			recommendations = append(recommendations, Recommendation{
				ID:          fmt.Sprintf("rec_ds_%s", series),
				Type:        RecommendationDownsample,
				Priority:    6,
				Title:       fmt.Sprintf("Downsample historical data for: %s", series),
				Description: fmt.Sprintf("Series has %.2fM data points spanning %v", float64(stats.DataPoints)/1e6, stats.TimeRangeSpan),
				Impact:      "Could reduce storage by 60-80% for older data",
				Action:      "Apply hourly downsampling for data older than 7 days",
				CreatedAt:   time.Now(),
			})
		}
	}

	// Analyze query patterns
	if profile.AvgQueryDuration > time.Second {
		recommendations = append(recommendations, Recommendation{
			ID:          "rec_cache",
			Type:        RecommendationCaching,
			Priority:    7,
			Title:       "Enable query result caching",
			Description: fmt.Sprintf("Average query duration is %v, above recommended threshold", profile.AvgQueryDuration),
			Impact:      "Could reduce repeat query latency by 90%",
			Action:      "Enable query result cache with 5 minute TTL",
			CreatedAt:   time.Now(),
		})
	}

	// Analyze parallelism opportunities
	if profile.LargeQueryRatio > 0.2 {
		recommendations = append(recommendations, Recommendation{
			ID:          "rec_parallel",
			Type:        RecommendationParallelism,
			Priority:    5,
			Title:       "Increase query parallelism",
			Description: fmt.Sprintf("%.0f%% of queries span large time ranges", profile.LargeQueryRatio*100),
			Impact:      "Could improve large query performance by 2-4x",
			Action:      "Increase parallel workers from 1 to 4",
			CreatedAt:   time.Now(),
		})
	}

	// Sort by priority
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Priority > recommendations[j].Priority
	})

	qo.recommendationsMu.Lock()
	qo.recommendations = recommendations
	qo.recommendationsMu.Unlock()
}

// Stats returns optimizer statistics
func (qo *AdaptiveQueryOptimizer) Stats() QueryOptimizerStats {
	qo.planCacheMu.RLock()
	cacheSize := len(qo.planCache)
	qo.planCacheMu.RUnlock()

	qo.feedbackMu.RLock()
	feedbackCount := len(qo.feedback)
	qo.feedbackMu.RUnlock()

	return QueryOptimizerStats{
		QueriesOptimized: atomic.LoadInt64(&qo.queriesOptimized),
		PlanCacheHits:    atomic.LoadInt64(&qo.planCacheHits),
		PlanCacheMisses:  atomic.LoadInt64(&qo.planCacheMisses),
		CostEstimates:    atomic.LoadInt64(&qo.costEstimates),
		PlanCacheSize:    cacheSize,
		FeedbackCount:    feedbackCount,
	}
}

// QueryOptimizerStats contains optimizer statistics
type QueryOptimizerStats struct {
	QueriesOptimized int64 `json:"queries_optimized"`
	PlanCacheHits    int64 `json:"plan_cache_hits"`
	PlanCacheMisses  int64 `json:"plan_cache_misses"`
	CostEstimates    int64 `json:"cost_estimates"`
	PlanCacheSize    int   `json:"plan_cache_size"`
	FeedbackCount    int   `json:"feedback_count"`
}

// Close shuts down the optimizer
func (qo *AdaptiveQueryOptimizer) Close() error {
	qo.cancel()
	qo.wg.Wait()
	return nil
}

// WorkloadProfiler profiles query workloads
type QueryWorkloadProfiler struct {
	window     time.Duration
	queries    []queryRecord
	seriesStats map[string]*SeriesQueryStats
	mu         sync.RWMutex
}

type queryRecord struct {
	Query     *Query
	Timestamp time.Time
	Duration  time.Duration
}

// SeriesQueryStats contains statistics for a series
type SeriesQueryStats struct {
	QueryCount    int           `json:"query_count"`
	AvgDuration   time.Duration `json:"avg_duration"`
	MaxDuration   time.Duration `json:"max_duration"`
	DataPoints    int64         `json:"data_points"`
	TimeRangeSpan time.Duration `json:"time_range_span"`
	LastQueried   time.Time     `json:"last_queried"`
}

// WorkloadProfile represents the current workload profile
type QueryWorkloadProfile struct {
	TotalQueries     int64                       `json:"total_queries"`
	AvgQueryDuration time.Duration               `json:"avg_query_duration"`
	SeriesStats      map[string]*SeriesQueryStats `json:"series_stats"`
	LargeQueryRatio  float64                     `json:"large_query_ratio"`
	PeakQPS          float64                     `json:"peak_qps"`
	TimeRangeHist    map[string]int              `json:"time_range_histogram"`
}

func newWorkloadProfiler(window time.Duration) *QueryWorkloadProfiler {
	return &QueryWorkloadProfiler{
		window:      window,
		queries:     make([]queryRecord, 0),
		seriesStats: make(map[string]*SeriesQueryStats),
	}
}

func (wp *QueryWorkloadProfiler) recordQuery(query *Query) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	now := time.Now()

	// Add record
	wp.queries = append(wp.queries, queryRecord{
		Query:     query,
		Timestamp: now,
	})

	// Clean old records
	cutoff := now.Add(-wp.window)
	newQueries := make([]queryRecord, 0)
	for _, q := range wp.queries {
		if q.Timestamp.After(cutoff) {
			newQueries = append(newQueries, q)
		}
	}
	wp.queries = newQueries

	// Update series stats
	if query.Metric != "" {
		stats, exists := wp.seriesStats[query.Metric]
		if !exists {
			stats = &SeriesQueryStats{}
			wp.seriesStats[query.Metric] = stats
		}
		stats.QueryCount++
		stats.LastQueried = now

		if query.End > 0 && query.Start > 0 {
			span := time.Duration(query.End - query.Start)
			if span > stats.TimeRangeSpan {
				stats.TimeRangeSpan = span
			}
		}
	}
}

func (wp *QueryWorkloadProfiler) recordDuration(query *Query, duration time.Duration) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if query.Metric == "" {
		return
	}

	stats, exists := wp.seriesStats[query.Metric]
	if !exists {
		return
	}

	// Update avg duration (exponential moving average)
	if stats.AvgDuration == 0 {
		stats.AvgDuration = duration
	} else {
		stats.AvgDuration = time.Duration(float64(stats.AvgDuration)*0.9 + float64(duration)*0.1)
	}

	if duration > stats.MaxDuration {
		stats.MaxDuration = duration
	}
}

func (wp *QueryWorkloadProfiler) getProfile() *QueryWorkloadProfile {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	profile := &QueryWorkloadProfile{
		TotalQueries:  int64(len(wp.queries)),
		SeriesStats:   make(map[string]*SeriesQueryStats),
		TimeRangeHist: make(map[string]int),
	}

	// Copy series stats
	for k, v := range wp.seriesStats {
		stats := *v
		profile.SeriesStats[k] = &stats
	}

	// Calculate avg duration
	var totalDuration time.Duration
	largeQueries := 0
	for _, q := range wp.queries {
		totalDuration += q.Duration
		if q.Query.End-q.Query.Start > int64(24*time.Hour) {
			largeQueries++
		}

		// Time range histogram
		rangeHours := (q.Query.End - q.Query.Start) / int64(time.Hour)
		bucket := "< 1h"
		if rangeHours >= 168 {
			bucket = "> 1w"
		} else if rangeHours >= 24 {
			bucket = "1d - 1w"
		} else if rangeHours >= 1 {
			bucket = "1h - 1d"
		}
		profile.TimeRangeHist[bucket]++
	}

	if len(wp.queries) > 0 {
		profile.AvgQueryDuration = totalDuration / time.Duration(len(wp.queries))
		profile.LargeQueryRatio = float64(largeQueries) / float64(len(wp.queries))
		profile.PeakQPS = float64(len(wp.queries)) / wp.window.Seconds()
	}

	return profile
}

// CostModel estimates query execution costs
type CostModel struct {
	// Cost coefficients
	scanCostPerRow      float64
	indexLookupCost     float64
	filterCostPerRow    float64
	aggregateCostPerRow float64
	parallelOverhead    float64
	memoryCostPerMB     float64

	mu sync.RWMutex
}

func newCostModel() *CostModel {
	return &CostModel{
		scanCostPerRow:      0.001,
		indexLookupCost:     0.1,
		filterCostPerRow:    0.0001,
		aggregateCostPerRow: 0.0005,
		parallelOverhead:    0.5,
		memoryCostPerMB:     0.01,
	}
}

func (cm *CostModel) estimateCost(plan *OptimizedQueryPlan) float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var cost float64

	// Estimate row count (simplified - would use statistics in production)
	estimatedRows := int64(10000)
	if plan.Query != nil {
		if plan.Query.Limit > 0 {
			estimatedRows = int64(plan.Query.Limit)
		}
		// Estimate based on time range
		if plan.Query.End > 0 && plan.Query.Start > 0 {
			hours := float64(plan.Query.End-plan.Query.Start) / float64(time.Hour)
			estimatedRows = int64(hours * 60) // ~1 point per minute
		}
	}

	plan.EstimatedRows = estimatedRows

	// Calculate cost based on strategy
	switch plan.Strategy {
	case StrategyFullScan:
		cost = float64(estimatedRows) * cm.scanCostPerRow

	case StrategyIndexScan, StrategyIndexSeek:
		cost = cm.indexLookupCost + float64(estimatedRows)*cm.scanCostPerRow*0.1

	case StrategyRangeScan:
		cost = float64(estimatedRows) * cm.scanCostPerRow * 0.5

	case StrategyParallelScan:
		baseCost := float64(estimatedRows) * cm.scanCostPerRow
		cost = baseCost/float64(plan.Parallelism) + cm.parallelOverhead

	case StrategyHashAggregate, StrategySortAggregate:
		cost = float64(estimatedRows) * (cm.scanCostPerRow + cm.aggregateCostPerRow)
	}

	// Add filter cost
	cost += float64(estimatedRows) * cm.filterCostPerRow

	// Estimate time
	plan.EstimatedTime = time.Duration(cost * float64(time.Millisecond))

	return cost
}

func (cm *CostModel) updateFromFeedback(feedback *QueryFeedback) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Adjust coefficients based on feedback
	// This is a simplified version - production would use gradient descent
	if feedback.ActualRows > 0 {
		actualCostPerRow := feedback.ActualCost / float64(feedback.ActualRows)
		// Blend with existing estimate
		cm.scanCostPerRow = cm.scanCostPerRow*0.95 + actualCostPerRow*0.05
	}
}

func (cm *CostModel) adjustCoefficients(avgError float64, learningRate float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Adjust coefficients to reduce error
	adjustment := 1.0 + (avgError * learningRate * 0.01)
	cm.scanCostPerRow *= adjustment
	cm.filterCostPerRow *= adjustment
}

// ExplainPlan returns a human-readable explanation of a query plan
func (qo *AdaptiveQueryOptimizer) ExplainPlan(plan *OptimizedQueryPlan) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Query Plan: %s\n", plan.ID))
	sb.WriteString(fmt.Sprintf("Strategy: %s\n", plan.Strategy))
	sb.WriteString(fmt.Sprintf("Estimated Cost: %.2f\n", plan.EstimatedCost))
	sb.WriteString(fmt.Sprintf("Estimated Rows: %d\n", plan.EstimatedRows))
	sb.WriteString(fmt.Sprintf("Estimated Time: %v\n", plan.EstimatedTime))
	sb.WriteString(fmt.Sprintf("Parallelism: %d\n", plan.Parallelism))

	if plan.IndexUsed != "" {
		sb.WriteString(fmt.Sprintf("Index Used: %s\n", plan.IndexUsed))
	}

	sb.WriteString("\nExecution Steps:\n")
	for i, step := range plan.Steps {
		sb.WriteString(fmt.Sprintf("  %d. %s (%s): %s\n", i+1, step.Name, step.Type, step.Description))
	}

	if len(plan.Hints) > 0 {
		sb.WriteString("\nHints:\n")
		for _, hint := range plan.Hints {
			sb.WriteString(fmt.Sprintf("  - %s\n", hint))
		}
	}

	return sb.String()
}

// AnalyzeQuery provides detailed analysis of a query
func (qo *AdaptiveQueryOptimizer) AnalyzeQuery(query *Query) *QueryAnalysis {
	analysis := &QueryAnalysis{
		Query:      query,
		Timestamp:  time.Now(),
		Issues:     make([]string, 0),
		Suggestions: make([]string, 0),
	}

	// Check for unbounded query
	if query.Start == 0 && query.End == 0 {
		analysis.Issues = append(analysis.Issues, "Query has no time bounds - may scan all data")
		analysis.Suggestions = append(analysis.Suggestions, "Add time range to limit data scanned")
	}

	// Check for missing series
	if query.Metric == "" {
		analysis.Issues = append(analysis.Issues, "No series specified - will scan all series")
		analysis.Suggestions = append(analysis.Suggestions, "Specify series name to use index")
	}

	// Check time range size
	if query.End > 0 && query.Start > 0 {
		rangeHours := float64(query.End-query.Start) / float64(time.Hour)
		if rangeHours > 168 {
			analysis.Issues = append(analysis.Issues, fmt.Sprintf("Large time range (%.0f hours) may be slow", rangeHours))
			analysis.Suggestions = append(analysis.Suggestions, "Consider using downsampled data for long ranges")
		}
	}

	// Check for missing aggregation on large ranges
	if query.Aggregation == nil && query.End-query.Start > int64(24*time.Hour) {
		analysis.Suggestions = append(analysis.Suggestions, "Consider using aggregation for large time ranges")
	}

	// Estimate complexity
	analysis.Complexity = "LOW"
	if len(analysis.Issues) > 0 {
		analysis.Complexity = "MEDIUM"
	}
	if len(analysis.Issues) > 2 {
		analysis.Complexity = "HIGH"
	}

	return analysis
}

// QueryAnalysis contains query analysis results
type QueryAnalysis struct {
	Query       *Query    `json:"query"`
	Timestamp   time.Time `json:"timestamp"`
	Complexity  string    `json:"complexity"`
	Issues      []string  `json:"issues"`
	Suggestions []string  `json:"suggestions"`
}
