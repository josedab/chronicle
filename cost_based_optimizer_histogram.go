// cost_based_optimizer_histogram.go contains extended cost based optimizer functionality.
package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"
)

// buildHistogram creates an equi-depth histogram from point values.
func (o *CostBasedOptimizer) buildHistogram(points []Point) []HistogramBucket {
	if len(points) == 0 {
		return nil
	}

	numBuckets := o.config.HistogramBuckets
	if numBuckets <= 0 {
		numBuckets = 64
	}
	if int64(numBuckets) > int64(len(points)) {
		numBuckets = len(points)
	}

	// Sort values for equi-depth partitioning.
	values := make([]float64, len(points))
	for i := range points {
		values[i] = points[i].Value
	}
	sort.Float64s(values)

	buckets := make([]HistogramBucket, 0, numBuckets)
	bucketSize := len(values) / numBuckets

	for i := 0; i < numBuckets; i++ {
		start := i * bucketSize
		end := start + bucketSize
		if i == numBuckets-1 {
			end = len(values) // Last bucket gets remainder.
		}
		if start >= len(values) {
			break
		}

		segment := values[start:end]
		distinct := make(map[float64]struct{})
		for _, v := range segment {
			distinct[v] = struct{}{}
		}

		buckets = append(buckets, HistogramBucket{
			LowerBound:     segment[0],
			UpperBound:     segment[len(segment)-1],
			Count:          int64(len(segment)),
			DistinctValues: int64(len(distinct)),
		})
	}

	return buckets
}

// AnalyzeAll collects statistics for every known metric.
func (o *CostBasedOptimizer) AnalyzeAll(ctx context.Context) (map[string]*TableStatistics, error) {
	o.mu.RLock()
	if !o.running {
		o.mu.RUnlock()
		return nil, fmt.Errorf("cost-based optimizer: %w", ErrClosed)
	}
	o.mu.RUnlock()

	metrics := o.db.Metrics()
	results := make(map[string]*TableStatistics, len(metrics))

	for _, m := range metrics {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}
		ts, err := o.Analyze(ctx, m)
		if err != nil {
			return results, fmt.Errorf("cost-based optimizer: analyze all: %w", err)
		}
		results[m] = ts
	}

	return results, nil
}

// GetStatistics returns the current statistics for a metric, or nil if not analyzed.
func (o *CostBasedOptimizer) GetStatistics(metric string) *TableStatistics {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.tableStats[metric]
}

// GetAllStatistics returns a snapshot of all table statistics.
func (o *CostBasedOptimizer) GetAllStatistics() map[string]*TableStatistics {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make(map[string]*TableStatistics, len(o.tableStats))
	for k, v := range o.tableStats {
		out[k] = v
	}
	return out
}

// GetStats returns engine-level statistics.
func (o *CostBasedOptimizer) GetStats() CostBasedOptimizerStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	s := o.stats
	s.MetricsTracked = len(o.tableStats)
	now := time.Now()
	for _, ts := range o.tableStats {
		if now.Sub(ts.LastAnalyzed) > o.config.StaleStatsThreshold {
			s.StaleMetrics++
		}
	}
	return s
}

// EstimateCost returns a cost estimate for a query using collected statistics.
func (o *CostBasedOptimizer) EstimateCost(q *Query) (*CBOPlanCandidate, error) {
	if q == nil {
		return nil, fmt.Errorf("cost-based optimizer: %w: nil query", ErrInvalidQuery)
	}
	if q.Metric == "" {
		return nil, fmt.Errorf("cost-based optimizer: %w: empty metric", ErrInvalidQuery)
	}

	o.mu.RLock()
	stats := o.tableStats[q.Metric]
	o.stats.TotalEstimates++
	o.mu.RUnlock()

	return o.estimatePlanCost(stats, q, PlanSeqScan), nil
}

// estimatePlanCost computes the cost for a specific plan type.
func (o *CostBasedOptimizer) estimatePlanCost(stats *TableStatistics, q *Query, plan CBOPlanType) *CBOPlanCandidate {
	candidate := &CBOPlanCandidate{Plan: plan}

	rowCount := int64(100_000) // Fallback if no stats.
	if stats != nil {
		rowCount = stats.RowCount
	}

	sel := o.config.DefaultSelectivity
	if stats != nil {
		sel = o.estimator.CombinedSelectivity(stats, q)
	}
	candidate.Selectivity = sel

	estimatedRows := int64(math.Ceil(float64(rowCount) * sel))
	if q.Limit > 0 && int64(q.Limit) < estimatedRows {
		estimatedRows = int64(q.Limit)
	}
	candidate.EstimatedRows = estimatedRows

	// Scan cost depends on plan type.
	switch plan {
	case PlanSeqScan:
		candidate.ScanCost = float64(rowCount) * o.config.SeqScanCostPerRow
		candidate.Notes = "full sequential scan"
	case PlanIndexScan:
		// Index scan reads only matching rows from disk.
		candidate.ScanCost = float64(estimatedRows) * o.config.IndexScanCostPerRow
		// Add index lookup overhead: logarithmic in total rows.
		if rowCount > 0 {
			candidate.ScanCost += math.Log2(float64(rowCount)) * o.config.IndexScanCostPerRow
		}
		candidate.Notes = "index scan with row lookup"
	case PlanIndexOnly:
		candidate.ScanCost = float64(estimatedRows) * o.config.IndexScanCostPerRow * 0.8
		candidate.Notes = "index-only scan"
	}

	// Sort cost (only if aggregation with grouping is needed).
	if q.Aggregation != nil && len(q.GroupBy) > 0 {
		candidate.SortCost = float64(estimatedRows) * math.Log2(math.Max(float64(estimatedRows), 2)) * o.config.SortCostPerRow
	}

	// Aggregation cost.
	if q.Aggregation != nil {
		candidate.AggCost = float64(estimatedRows) * o.config.AggCostPerRow
	}

	// Network cost based on output rows.
	candidate.NetworkCost = float64(estimatedRows) * o.config.NetworkCostPerRow

	candidate.EstimatedCost = candidate.ScanCost + candidate.SortCost + candidate.AggCost + candidate.NetworkCost
	return candidate
}

// ChooseBestPlan generates candidate plans and returns the one with the lowest cost.
func (o *CostBasedOptimizer) ChooseBestPlan(q *Query) (*CBOPlanCandidate, error) {
	if q == nil {
		return nil, fmt.Errorf("cost-based optimizer: %w: nil query", ErrInvalidQuery)
	}
	if q.Metric == "" {
		return nil, fmt.Errorf("cost-based optimizer: %w: empty metric", ErrInvalidQuery)
	}

	o.mu.RLock()
	stats := o.tableStats[q.Metric]
	o.mu.RUnlock()

	candidates := o.generateCandidates(stats, q)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("cost-based optimizer: no candidate plans generated")
	}

	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.EstimatedCost < best.EstimatedCost {
			best = c
		}
	}

	o.mu.Lock()
	o.stats.TotalPlans++
	switch best.Plan {
	case PlanIndexScan, PlanIndexOnly:
		o.stats.IndexPlansChosen++
	case PlanSeqScan:
		o.stats.SeqPlansChosen++
	}
	o.mu.Unlock()

	return best, nil
}

// generateCandidates produces all viable execution plans.
func (o *CostBasedOptimizer) generateCandidates(stats *TableStatistics, q *Query) []*CBOPlanCandidate {
	candidates := make([]*CBOPlanCandidate, 0, 3)

	// Sequential scan is always an option.
	candidates = append(candidates, o.estimatePlanCost(stats, q, PlanSeqScan))

	// Index scan is viable when there are filters.
	hasFilters := len(q.Tags) > 0 || len(q.TagFilters) > 0 || q.Start > 0 || q.End > 0
	if hasFilters {
		candidates = append(candidates, o.estimatePlanCost(stats, q, PlanIndexScan))
	}

	// Index-only scan when querying just tags (no value aggregation).
	if hasFilters && q.Aggregation == nil {
		candidates = append(candidates, o.estimatePlanCost(stats, q, PlanIndexOnly))
	}

	return candidates
}

// ExplainQuery returns all candidate plans with costs for diagnostic purposes.
func (o *CostBasedOptimizer) ExplainQuery(q *Query) ([]*CBOPlanCandidate, error) {
	if q == nil {
		return nil, fmt.Errorf("cost-based optimizer: %w: nil query", ErrInvalidQuery)
	}
	if q.Metric == "" {
		return nil, fmt.Errorf("cost-based optimizer: %w: empty metric", ErrInvalidQuery)
	}

	o.mu.RLock()
	stats := o.tableStats[q.Metric]
	o.mu.RUnlock()

	candidates := o.generateCandidates(stats, q)

	// Mark the best plan.
	if len(candidates) > 0 {
		best := 0
		for i, c := range candidates {
			if c.EstimatedCost < candidates[best].EstimatedCost {
				best = i
			}
		}
		candidates[best].Notes = candidates[best].Notes + " [chosen]"
	}

	return candidates, nil
}

// RegisterHTTPHandlers registers HTTP endpoints for the cost-based optimizer.
func (o *CostBasedOptimizer) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/analyze", o.handleAnalyze)
	mux.HandleFunc("/api/v1/optimizer/stats", o.handleStats)
	mux.HandleFunc("/api/v1/optimizer/explain", o.handleExplain)
}

func (o *CostBasedOptimizer) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Metric string `json:"metric"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	if req.Metric == "" {
		// Analyze all metrics.
		allStats, err := o.AnalyzeAll(ctx)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(allStats)
		return
	}

	ts, err := o.Analyze(ctx, req.Metric)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ts)
}

func (o *CostBasedOptimizer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metric := r.URL.Query().Get("metric")
	if metric != "" {
		ts := o.GetStatistics(metric)
		if ts == nil {
			http.Error(w, "no statistics for metric: "+metric, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ts)
		return
	}

	resp := struct {
		EngineStats CostBasedOptimizerStats     `json:"engine_stats"`
		Tables      map[string]*TableStatistics `json:"tables"`
	}{
		EngineStats: o.GetStats(),
		Tables:      o.GetAllStatistics(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (o *CostBasedOptimizer) handleExplain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter required", http.StatusBadRequest)
		return
	}

	q := &Query{Metric: metric}

	// Parse optional tag filters from query params.
	for key, values := range r.URL.Query() {
		if key == "metric" || key == "start" || key == "end" {
			continue
		}
		if len(values) > 0 && key != "" {
			if q.Tags == nil {
				q.Tags = make(map[string]string)
			}
			q.Tags[key] = values[0]
		}
	}

	if startStr := r.URL.Query().Get("start"); startStr != "" {
		var start int64
		if _, err := fmt.Sscanf(startStr, "%d", &start); err == nil {
			q.Start = start
		}
	}
	if endStr := r.URL.Query().Get("end"); endStr != "" {
		var end int64
		if _, err := fmt.Sscanf(endStr, "%d", &end); err == nil {
			q.End = end
		}
	}

	plans, err := o.ExplainQuery(q)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(plans)
}
