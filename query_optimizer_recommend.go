package chronicle

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Recommendation generation and statistics for the adaptive query optimizer.

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
	window      time.Duration
	queries     []queryRecord
	seriesStats map[string]*SeriesQueryStats
	mu          sync.RWMutex
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
	TotalQueries     int64                        `json:"total_queries"`
	AvgQueryDuration time.Duration                `json:"avg_query_duration"`
	SeriesStats      map[string]*SeriesQueryStats `json:"series_stats"`
	LargeQueryRatio  float64                      `json:"large_query_ratio"`
	PeakQPS          float64                      `json:"peak_qps"`
	TimeRangeHist    map[string]int               `json:"time_range_histogram"`
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
		Query:       query,
		Timestamp:   time.Now(),
		Issues:      make([]string, 0),
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
