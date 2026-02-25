package chronicle

import (
	"context"
	"fmt"
	"math"
	"sort"
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
	ID            string            `json:"id"`
	Query         *Query            `json:"query"`
	Strategy      ExecutionStrategy `json:"strategy"`
	EstimatedCost float64           `json:"estimated_cost"`
	EstimatedRows int64             `json:"estimated_rows"`
	EstimatedTime time.Duration     `json:"estimated_time"`
	IndexUsed     string            `json:"index_used,omitempty"`
	Parallelism   int               `json:"parallelism"`
	Steps         []PlanStep        `json:"steps"`
	Hints         []string          `json:"hints"`
	CreatedAt     time.Time         `json:"created_at"`
}

// ExecutionStrategy defines how a query will be executed
type ExecutionStrategy string

const (
	StrategyFullScan      ExecutionStrategy = "full_scan"
	StrategyIndexScan     ExecutionStrategy = "index_scan"
	StrategyRangeScan     ExecutionStrategy = "range_scan"
	StrategyIndexSeek     ExecutionStrategy = "index_seek"
	StrategyParallelScan  ExecutionStrategy = "parallel_scan"
	StrategyHashAggregate ExecutionStrategy = "hash_aggregate"
	StrategySortAggregate ExecutionStrategy = "sort_aggregate"
	StrategyMerge         ExecutionStrategy = "merge"
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
	Plan     *OptimizedQueryPlan
	CachedAt time.Time
	HitCount int64
}

// QueryFeedback contains feedback about query execution
type QueryFeedback struct {
	PlanID       string            `json:"plan_id"`
	Query        *Query            `json:"query"`
	Strategy     ExecutionStrategy `json:"strategy"`
	ActualCost   float64           `json:"actual_cost"`
	ActualRows   int64             `json:"actual_rows"`
	ActualTime   time.Duration     `json:"actual_time"`
	MemoryUsed   int64             `json:"memory_used"`
	Success      bool              `json:"success"`
	ErrorMessage string            `json:"error_message,omitempty"`
	Timestamp    time.Time         `json:"timestamp"`
}

// Recommendation represents an optimization recommendation
type Recommendation struct {
	ID          string             `json:"id"`
	Type        RecommendationType `json:"type"`
	Priority    int                `json:"priority"` // 1-10, higher = more important
	Title       string             `json:"title"`
	Description string             `json:"description"`
	Impact      string             `json:"impact"`
	Action      string             `json:"action"`
	Metadata    map[string]string  `json:"metadata,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
}

// RecommendationType defines the type of recommendation
type RecommendationType string

const (
	RecommendationCreateIndex RecommendationType = "create_index"
	RecommendationDropIndex   RecommendationType = "drop_index"
	RecommendationPartition   RecommendationType = "partition"
	RecommendationCompression RecommendationType = "compression"
	RecommendationDownsample  RecommendationType = "downsample"
	RecommendationParallelism RecommendationType = "parallelism"
	RecommendationRetention   RecommendationType = "retention"
	RecommendationCaching     RecommendationType = "caching"
	RecommendationSchema      RecommendationType = "schema"
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
