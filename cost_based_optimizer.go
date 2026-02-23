package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// CostBasedOptimizerConfig configures the cost-based query optimizer.
type CostBasedOptimizerConfig struct {
	Enabled              bool          `json:"enabled"`
	SeqScanCostPerRow    float64       `json:"seq_scan_cost_per_row"`
	IndexScanCostPerRow  float64       `json:"index_scan_cost_per_row"`
	SortCostPerRow       float64       `json:"sort_cost_per_row"`
	AggCostPerRow        float64       `json:"agg_cost_per_row"`
	NetworkCostPerRow    float64       `json:"network_cost_per_row"`
	DefaultSelectivity   float64       `json:"default_selectivity"`
	RegexSelectivity     float64       `json:"regex_selectivity"`
	HistogramBuckets     int           `json:"histogram_buckets"`
	StaleStatsThreshold  time.Duration `json:"stale_stats_threshold"`
	MaxAnalyzeSampleSize int           `json:"max_analyze_sample_size"`
}

// DefaultCostBasedOptimizerConfig returns sensible defaults.
func DefaultCostBasedOptimizerConfig() CostBasedOptimizerConfig {
	return CostBasedOptimizerConfig{
		Enabled:              true,
		SeqScanCostPerRow:    1.0,
		IndexScanCostPerRow:  0.5,
		SortCostPerRow:       2.0,
		AggCostPerRow:        0.1,
		NetworkCostPerRow:    0.05,
		DefaultSelectivity:   0.5,
		RegexSelectivity:     0.25,
		HistogramBuckets:     64,
		StaleStatsThreshold:  1 * time.Hour,
		MaxAnalyzeSampleSize: 1_000_000,
	}
}

// HistogramBucket represents a single bucket in an equi-depth histogram.
type HistogramBucket struct {
	LowerBound     float64 `json:"lower_bound"`
	UpperBound     float64 `json:"upper_bound"`
	Count          int64   `json:"count"`
	DistinctValues int64   `json:"distinct_values"`
}

// CBOColumnStats holds per-column statistics with histogram support.
type CBOColumnStats struct {
	Name           string            `json:"name"`
	NullCount      int64             `json:"null_count"`
	DistinctCount  int64             `json:"distinct_count"`
	MinValue       float64           `json:"min_value"`
	MaxValue       float64           `json:"max_value"`
	AvgValue       float64           `json:"avg_value"`
	Histogram      []HistogramBucket `json:"histogram,omitempty"`
}

// TableStatistics holds per-metric cardinality statistics.
type TableStatistics struct {
	Metric           string                    `json:"metric"`
	RowCount         int64                     `json:"row_count"`
	DistinctTagCount map[string]int64          `json:"distinct_tag_count"`
	MinTimestamp      int64                    `json:"min_timestamp"`
	MaxTimestamp      int64                    `json:"max_timestamp"`
	Columns          map[string]*CBOColumnStats `json:"columns"`
	LastAnalyzed     time.Time                 `json:"last_analyzed"`
}

// SelectivityEstimator estimates filter selectivity using table statistics.
type SelectivityEstimator struct {
	config CostBasedOptimizerConfig
}

// EstimateTimeRangeSelectivity computes selectivity for a time range filter.
func (se *SelectivityEstimator) EstimateTimeRangeSelectivity(stats *TableStatistics, start, end int64) float64 {
	if stats == nil || stats.RowCount == 0 {
		return se.config.DefaultSelectivity
	}
	totalRange := float64(stats.MaxTimestamp - stats.MinTimestamp)
	if totalRange <= 0 {
		return 1.0
	}

	qStart := start
	qEnd := end
	if qStart < stats.MinTimestamp {
		qStart = stats.MinTimestamp
	}
	if qEnd <= 0 || qEnd > stats.MaxTimestamp {
		qEnd = stats.MaxTimestamp
	}
	if qEnd <= qStart {
		return 0.0
	}

	sel := float64(qEnd-qStart) / totalRange
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

// EstimateTagEqualitySelectivity estimates selectivity for tag = 'value' filters.
func (se *SelectivityEstimator) EstimateTagEqualitySelectivity(stats *TableStatistics, tagKey string) float64 {
	if stats == nil || stats.RowCount == 0 {
		return se.config.DefaultSelectivity
	}
	distinct, ok := stats.DistinctTagCount[tagKey]
	if !ok || distinct <= 0 {
		return se.config.DefaultSelectivity
	}
	return 1.0 / float64(distinct)
}

// EstimateTagInSelectivity estimates selectivity for tag IN (...) filters.
func (se *SelectivityEstimator) EstimateTagInSelectivity(stats *TableStatistics, tagKey string, numValues int) float64 {
	eqSel := se.EstimateTagEqualitySelectivity(stats, tagKey)
	sel := eqSel * float64(numValues)
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

// EstimateTagRegexSelectivity returns a conservative selectivity for regex filters.
func (se *SelectivityEstimator) EstimateTagRegexSelectivity(stats *TableStatistics, tagKey, pattern string) float64 {
	if stats == nil || stats.RowCount == 0 {
		return se.config.RegexSelectivity
	}
	distinct, ok := stats.DistinctTagCount[tagKey]
	if !ok || distinct <= 0 {
		return se.config.RegexSelectivity
	}
	// Anchored prefixes are more selective
	if len(pattern) > 0 && pattern[0] == '^' {
		return math.Max(1.0/float64(distinct), se.config.RegexSelectivity*0.5)
	}
	return se.config.RegexSelectivity
}

// EstimateValueRangeSelectivity estimates selectivity using histogram data.
func (se *SelectivityEstimator) EstimateValueRangeSelectivity(col *CBOColumnStats, low, high float64) float64 {
	if col == nil || len(col.Histogram) == 0 {
		return se.config.DefaultSelectivity
	}
	var matchCount int64
	var totalCount int64
	for _, b := range col.Histogram {
		totalCount += b.Count
		if b.UpperBound < low || b.LowerBound > high {
			continue
		}
		// Partial overlap: estimate proportionally
		bRange := b.UpperBound - b.LowerBound
		if bRange <= 0 {
			matchCount += b.Count
			continue
		}
		overlapLow := math.Max(b.LowerBound, low)
		overlapHigh := math.Min(b.UpperBound, high)
		fraction := (overlapHigh - overlapLow) / bRange
		matchCount += int64(float64(b.Count) * fraction)
	}
	if totalCount == 0 {
		return se.config.DefaultSelectivity
	}
	return float64(matchCount) / float64(totalCount)
}

// CombinedSelectivity computes combined selectivity for a query using table stats.
func (se *SelectivityEstimator) CombinedSelectivity(stats *TableStatistics, q *Query) float64 {
	sel := 1.0

	// Time range selectivity
	if q.Start > 0 || q.End > 0 {
		timeSel := se.EstimateTimeRangeSelectivity(stats, q.Start, q.End)
		sel *= timeSel
	}

	// Exact tag match selectivity
	for tagKey := range q.Tags {
		tagSel := se.EstimateTagEqualitySelectivity(stats, tagKey)
		sel *= tagSel
	}

	// TagFilter selectivity
	for _, tf := range q.TagFilters {
		switch tf.Op {
		case TagOpEq:
			sel *= se.EstimateTagEqualitySelectivity(stats, tf.Key)
		case TagOpNotEq:
			eqSel := se.EstimateTagEqualitySelectivity(stats, tf.Key)
			sel *= (1.0 - eqSel)
		case TagOpIn:
			sel *= se.EstimateTagInSelectivity(stats, tf.Key, len(tf.Values))
		case TagOpRegex:
			pattern := ""
			if len(tf.Values) > 0 {
				pattern = tf.Values[0]
			}
			sel *= se.EstimateTagRegexSelectivity(stats, tf.Key, pattern)
		case TagOpNotRegex:
			pattern := ""
			if len(tf.Values) > 0 {
				pattern = tf.Values[0]
			}
			regSel := se.EstimateTagRegexSelectivity(stats, tf.Key, pattern)
			sel *= (1.0 - regSel)
		}
	}

	if sel < 0 {
		sel = 0
	}
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

// CBOPlanType indicates the query execution strategy.
type CBOPlanType string

const (
	PlanSeqScan   CBOPlanType = "seq_scan"
	PlanIndexScan CBOPlanType = "index_scan"
	PlanIndexOnly CBOPlanType = "index_only"
)

// CBOPlanCandidate represents a candidate execution plan with its estimated cost.
type CBOPlanCandidate struct {
	Plan            CBOPlanType `json:"plan"`
	EstimatedCost   float64  `json:"estimated_cost"`
	EstimatedRows   int64    `json:"estimated_rows"`
	Selectivity     float64  `json:"selectivity"`
	ScanCost        float64  `json:"scan_cost"`
	SortCost        float64  `json:"sort_cost"`
	AggCost         float64  `json:"agg_cost"`
	NetworkCost     float64  `json:"network_cost"`
	Notes           string   `json:"notes,omitempty"`
}

// CostBasedOptimizerStats holds engine-level statistics.
type CostBasedOptimizerStats struct {
	TotalAnalyzes   int64 `json:"total_analyzes"`
	TotalEstimates  int64 `json:"total_estimates"`
	TotalPlans      int64 `json:"total_plans"`
	MetricsTracked  int   `json:"metrics_tracked"`
	StaleMetrics    int   `json:"stale_metrics"`
	IndexPlansChosen int64 `json:"index_plans_chosen"`
	SeqPlansChosen   int64 `json:"seq_plans_chosen"`
}

// CostBasedOptimizer collects real statistics and uses them to choose query plans.
type CostBasedOptimizer struct {
	db     *DB
	config CostBasedOptimizerConfig
	mu     sync.RWMutex
	running bool
	stopCh  chan struct{}

	stats      CostBasedOptimizerStats
	tableStats map[string]*TableStatistics
	estimator  *SelectivityEstimator
}

// NewCostBasedOptimizer creates a new cost-based query optimizer.
func NewCostBasedOptimizer(db *DB, cfg CostBasedOptimizerConfig) *CostBasedOptimizer {
	return &CostBasedOptimizer{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
		tableStats: make(map[string]*TableStatistics),
		estimator: &SelectivityEstimator{config: cfg},
	}
}

// Start starts the cost-based optimizer engine.
func (o *CostBasedOptimizer) Start() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.running {
		return nil
	}
	o.running = true
	o.stopCh = make(chan struct{})
	return nil
}

// Stop stops the cost-based optimizer engine.
func (o *CostBasedOptimizer) Stop() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.running {
		return nil
	}
	o.running = false
	close(o.stopCh)
	return nil
}

// Analyze collects real statistics for a single metric by scanning its data.
func (o *CostBasedOptimizer) Analyze(ctx context.Context, metric string) (*TableStatistics, error) {
	if metric == "" {
		return nil, fmt.Errorf("cost-based optimizer: %w: empty metric name", ErrInvalidQuery)
	}

	o.mu.RLock()
	if !o.running {
		o.mu.RUnlock()
		return nil, fmt.Errorf("cost-based optimizer: %w", ErrClosed)
	}
	o.mu.RUnlock()

	// Query all points for the metric (bounded by sample size).
	q := &Query{
		Metric: metric,
		Limit:  o.config.MaxAnalyzeSampleSize,
	}

	result, err := o.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("cost-based optimizer: analyze %q: %w", metric, err)
	}

	points := result.Points
	if len(points) == 0 {
		ts := &TableStatistics{
			Metric:           metric,
			RowCount:         0,
			DistinctTagCount: make(map[string]int64),
			Columns:          make(map[string]*CBOColumnStats),
			LastAnalyzed:     time.Now(),
		}
		o.mu.Lock()
		o.tableStats[metric] = ts
		o.stats.TotalAnalyzes++
		o.mu.Unlock()
		return ts, nil
	}

	ts := o.computeStatistics(metric, points)

	o.mu.Lock()
	o.tableStats[metric] = ts
	o.stats.TotalAnalyzes++
	o.mu.Unlock()

	return ts, nil
}

// computeStatistics builds TableStatistics from a slice of points.
func (o *CostBasedOptimizer) computeStatistics(metric string, points []Point) *TableStatistics {
	ts := &TableStatistics{
		Metric:           metric,
		RowCount:         int64(len(points)),
		DistinctTagCount: make(map[string]int64),
		MinTimestamp:      points[0].Timestamp,
		MaxTimestamp:      points[0].Timestamp,
		Columns:          make(map[string]*CBOColumnStats),
		LastAnalyzed:     time.Now(),
	}

	// Track distinct tag values per key.
	tagValueSets := make(map[string]map[string]struct{})

	var valueSum float64
	minVal := math.Inf(1)
	maxVal := math.Inf(-1)
	distinctVals := make(map[float64]struct{})

	for i := range points {
		p := &points[i]

		// Timestamp bounds
		if p.Timestamp < ts.MinTimestamp {
			ts.MinTimestamp = p.Timestamp
		}
		if p.Timestamp > ts.MaxTimestamp {
			ts.MaxTimestamp = p.Timestamp
		}

		// Value stats
		valueSum += p.Value
		if p.Value < minVal {
			minVal = p.Value
		}
		if p.Value > maxVal {
			maxVal = p.Value
		}
		distinctVals[p.Value] = struct{}{}

		// Tag cardinality
		for k, v := range p.Tags {
			if tagValueSets[k] == nil {
				tagValueSets[k] = make(map[string]struct{})
			}
			tagValueSets[k][v] = struct{}{}
		}
	}

	for k, vs := range tagValueSets {
		ts.DistinctTagCount[k] = int64(len(vs))
	}

	// Build column stats for the value column.
	valCol := &CBOColumnStats{
		Name:          "value",
		NullCount:     0,
		DistinctCount: int64(len(distinctVals)),
		MinValue:      minVal,
		MaxValue:      maxVal,
		AvgValue:      valueSum / float64(len(points)),
	}
	valCol.Histogram = o.buildHistogram(points)
	ts.Columns["value"] = valCol

	// Build column stats for the timestamp column.
	tsCol := &CBOColumnStats{
		Name:          "timestamp",
		NullCount:     0,
		DistinctCount: int64(len(points)), // Approximate: each point typically has a unique timestamp.
		MinValue:      float64(ts.MinTimestamp),
		MaxValue:      float64(ts.MaxTimestamp),
		AvgValue:      float64(ts.MinTimestamp+ts.MaxTimestamp) / 2.0,
	}
	ts.Columns["timestamp"] = tsCol

	return ts
}

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
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
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
		EngineStats CostBasedOptimizerStats        `json:"engine_stats"`
		Tables      map[string]*TableStatistics     `json:"tables"`
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

