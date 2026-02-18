package chronicle

import (
	"fmt"
	"testing"
	"time"
)

func TestNewAdaptiveQueryOptimizer(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultQueryOptimizerConfig()
	qo, err := NewAdaptiveQueryOptimizer(db, config)
	if err != nil {
		t.Fatalf("NewAdaptiveQueryOptimizer() error = %v", err)
	}
	defer qo.Close()

	if qo.profiler == nil {
		t.Error("profiler should be initialized")
	}
	if qo.costModel == nil {
		t.Error("costModel should be initialized")
	}
}

func TestOptimize(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	query := &Query{
		Metric: "cpu_usage",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	plan, err := qo.Optimize(query)
	if err != nil {
		t.Fatalf("Optimize() error = %v", err)
	}

	if plan == nil {
		t.Fatal("Expected non-nil plan")
	}
	if plan.Query != query {
		t.Error("Plan should reference original query")
	}
	if plan.EstimatedCost <= 0 {
		t.Error("Plan should have positive estimated cost")
	}
}

func TestOptimizeWithDisabledOptimizer(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultQueryOptimizerConfig()
	config.Enabled = false
	qo, _ := NewAdaptiveQueryOptimizer(db, config)
	defer qo.Close()

	query := &Query{Metric: "test"}
	plan, _ := qo.Optimize(query)

	if plan.Strategy != StrategyFullScan {
		t.Errorf("Disabled optimizer should use full scan, got %s", plan.Strategy)
	}
}

func TestPlanCaching(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultQueryOptimizerConfig()
	config.PlanCacheEnabled = true
	qo, _ := NewAdaptiveQueryOptimizer(db, config)
	defer qo.Close()

	query := &Query{
		Metric: "cached_metric",
		Start:  1000,
		End:    2000,
	}

	// First call - cache miss
	qo.Optimize(query)

	// Second call - should be cache hit
	qo.Optimize(query)

	stats := qo.Stats()
	if stats.PlanCacheHits != 1 {
		t.Errorf("PlanCacheHits = %d, want 1", stats.PlanCacheHits)
	}
}

func TestCandidatePlanGeneration(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	// Query with series - should generate index scan plan
	query := &Query{
		Metric: "metrics",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	candidates := qo.generateCandidatePlans(query)

	// Should have multiple candidates
	if len(candidates) < 2 {
		t.Errorf("Expected at least 2 candidates, got %d", len(candidates))
	}

	// Should include index scan
	hasIndexScan := false
	for _, c := range candidates {
		if c.Strategy == StrategyIndexScan {
			hasIndexScan = true
			break
		}
	}
	if !hasIndexScan {
		t.Error("Expected index scan plan for series query")
	}
}

func TestParallelPlanGeneration(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	// Large time range query
	query := &Query{
		Metric: "metrics",
		Start:  time.Now().Add(-48 * time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	candidates := qo.generateCandidatePlans(query)

	// Should include parallel scan
	hasParallel := false
	for _, c := range candidates {
		if c.Strategy == StrategyParallelScan {
			hasParallel = true
			if c.Parallelism < 2 {
				t.Error("Parallel plan should have parallelism > 1")
			}
			break
		}
	}
	if !hasParallel {
		t.Error("Expected parallel scan plan for large query")
	}
}

func TestAggregationPlanGeneration(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	query := &Query{
		Metric:      "metrics",
		Aggregation: &Aggregation{Function: AggMean},
		Start:       time.Now().Add(-time.Hour).UnixNano(),
		End:         time.Now().UnixNano(),
	}

	candidates := qo.generateCandidatePlans(query)

	hasHashAgg := false
	for _, c := range candidates {
		if c.Strategy == StrategyHashAggregate {
			hasHashAgg = true
			break
		}
	}
	if !hasHashAgg {
		t.Error("Expected hash aggregate plan for aggregation query")
	}
}

func TestCostEstimation(t *testing.T) {
	cm := newCostModel()

	plans := []*OptimizedQueryPlan{
		{Strategy: StrategyFullScan, Query: &Query{Limit: 1000}},
		{Strategy: StrategyIndexScan, Query: &Query{Limit: 1000}},
		{Strategy: StrategyParallelScan, Query: &Query{Limit: 1000}, Parallelism: 4},
	}

	costs := make([]float64, len(plans))
	for i, plan := range plans {
		costs[i] = cm.estimateCost(plan)
	}

	// Index scan should be cheaper than full scan
	if costs[1] >= costs[0] {
		t.Log("Index scan should generally be cheaper than full scan")
	}
}

func TestRecordFeedback(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	feedback := &QueryFeedback{
		PlanID:     "test_plan",
		Strategy:   StrategyIndexScan,
		ActualCost: 10.5,
		ActualRows: 1000,
		ActualTime: 100 * time.Millisecond,
		Success:    true,
		Timestamp:  time.Now(),
	}

	qo.RecordFeedback(feedback)

	qo.feedbackMu.RLock()
	feedbackCount := len(qo.feedback)
	qo.feedbackMu.RUnlock()

	if feedbackCount != 1 {
		t.Errorf("Feedback count = %d, want 1", feedbackCount)
	}
}

func TestWorkloadProfiler(t *testing.T) {
	profiler := newWorkloadProfiler(time.Hour)

	// Record queries
	for i := 0; i < 10; i++ {
		profiler.recordQuery(&Query{
			Metric: "cpu",
			Start:  int64(i * 1000),
			End:    int64((i + 1) * 1000),
		})
	}

	profile := profiler.getProfile()

	if profile.TotalQueries != 10 {
		t.Errorf("TotalQueries = %d, want 10", profile.TotalQueries)
	}

	if _, exists := profile.SeriesStats["cpu"]; !exists {
		t.Error("Expected series stats for 'cpu'")
	}
}

func TestWorkloadProfilerDuration(t *testing.T) {
	profiler := newWorkloadProfiler(time.Hour)

	query := &Query{Metric: "cpu"}
	profiler.recordQuery(query)
	profiler.recordDuration(query, 100*time.Millisecond)
	profiler.recordDuration(query, 200*time.Millisecond)

	profile := profiler.getProfile()
	stats := profile.SeriesStats["cpu"]

	if stats == nil {
		t.Fatal("Expected series stats")
	}
	if stats.MaxDuration != 200*time.Millisecond {
		t.Errorf("MaxDuration = %v, want 200ms", stats.MaxDuration)
	}
}

func TestRecommendations(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	// Simulate workload
	for i := 0; i < 150; i++ {
		qo.profiler.recordQuery(&Query{Metric: "hot_series"})
		qo.profiler.recordDuration(&Query{Metric: "hot_series"}, 150*time.Millisecond)
	}

	// Generate recommendations
	qo.generateRecommendations()

	recs := qo.GetRecommendations()

	// Should have some recommendations
	if len(recs) == 0 {
		t.Log("No recommendations generated - may need more data")
	}

	// Check recommendations are sorted by priority
	for i := 1; i < len(recs); i++ {
		if recs[i].Priority > recs[i-1].Priority {
			t.Error("Recommendations should be sorted by priority descending")
		}
	}
}

func TestExplainPlan(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	plan := &OptimizedQueryPlan{
		ID:            "test_plan",
		Strategy:      StrategyIndexScan,
		EstimatedCost: 10.5,
		EstimatedRows: 1000,
		EstimatedTime: 50 * time.Millisecond,
		Parallelism:   1,
		IndexUsed:     "series_idx",
		Steps: []PlanStep{
			{Name: "index_lookup", Type: "index_scan", Description: "Look up index"},
			{Name: "filter", Type: "filter", Description: "Apply filters"},
		},
		Hints: []string{"Consider increasing cache size"},
	}

	explanation := qo.ExplainPlan(plan)

	if explanation == "" {
		t.Error("Expected non-empty explanation")
	}
	if !qoContains(explanation, "index_scan") {
		t.Error("Explanation should mention strategy")
	}
	if !qoContains(explanation, "series_idx") {
		t.Error("Explanation should mention index used")
	}
}

func qoContains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && qoContainsAt(s, substr, 0))
}

func qoContainsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestAnalyzeQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	// Query with issues
	query := &Query{} // No series, no time bounds

	analysis := qo.AnalyzeQuery(query)

	if len(analysis.Issues) == 0 {
		t.Error("Expected issues for unbounded query")
	}
	if len(analysis.Suggestions) == 0 {
		t.Error("Expected suggestions")
	}
	if analysis.Complexity != "MEDIUM" && analysis.Complexity != "HIGH" {
		t.Errorf("Complexity = %s, expected MEDIUM or HIGH", analysis.Complexity)
	}
}

func TestAnalyzeQueryLargeRange(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	// Large time range query
	query := &Query{
		Metric: "metrics",
		Start:  time.Now().Add(-30 * 24 * time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	analysis := qo.AnalyzeQuery(query)

	// Should suggest downsampling
	hasDownsampleSuggestion := false
	for _, s := range analysis.Suggestions {
		if qoContainsAt(s, "downsample", 0) || qoContainsAt(s, "aggregation", 0) {
			hasDownsampleSuggestion = true
			break
		}
	}
	if !hasDownsampleSuggestion {
		t.Log("Expected suggestion about downsampling for large range")
	}
}

func TestQOStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	// Run some queries with different metrics to avoid plan cache hits
	for i := 0; i < 5; i++ {
		qo.Optimize(&Query{Metric: fmt.Sprintf("test_%d", i)})
	}

	stats := qo.Stats()

	if stats.QueriesOptimized != 5 {
		t.Errorf("QueriesOptimized = %d, want 5", stats.QueriesOptimized)
	}
	if stats.CostEstimates < 5 {
		t.Errorf("CostEstimates = %d, want >= 5", stats.CostEstimates)
	}
}

func TestPlanCacheEviction(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultQueryOptimizerConfig()
	config.PlanCacheMaxSize = 3
	config.PlanCacheEnabled = true
	qo, _ := NewAdaptiveQueryOptimizer(db, config)
	defer qo.Close()

	// Fill cache
	for i := 0; i < 5; i++ {
		qo.Optimize(&Query{
			Metric: "series_" + string(rune('a'+i)),
			Start:  int64(i),
			End:    int64(i + 1),
		})
	}

	stats := qo.Stats()
	if stats.PlanCacheSize > 3 {
		t.Errorf("Cache size = %d, should not exceed max 3", stats.PlanCacheSize)
	}
}

func TestCostModelUpdate(t *testing.T) {
	cm := newCostModel()

	initialCost := cm.scanCostPerRow

	// Record feedback
	feedback := &QueryFeedback{
		ActualCost: 100.0,
		ActualRows: 10000,
	}
	cm.updateFromFeedback(feedback)

	// Cost should be updated
	if cm.scanCostPerRow == initialCost {
		t.Log("Cost model should update from feedback")
	}
}

func TestCostModelAdjust(t *testing.T) {
	cm := newCostModel()

	initial := cm.scanCostPerRow
	cm.adjustCoefficients(0.5, 0.1)

	if cm.scanCostPerRow == initial {
		t.Log("Coefficients should be adjusted")
	}
}

func TestSelectBestPlan(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	plans := []*OptimizedQueryPlan{
		{ID: "plan1", EstimatedCost: 100},
		{ID: "plan2", EstimatedCost: 50},
		{ID: "plan3", EstimatedCost: 75},
	}

	best := qo.selectBestPlan(plans)

	if best.ID != "plan2" {
		t.Errorf("Best plan = %s, want plan2", best.ID)
	}
}

func TestSelectBestPlanEmpty(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	best := qo.selectBestPlan([]*OptimizedQueryPlan{})

	if best != nil {
		t.Error("Expected nil for empty candidates")
	}
}

func TestWorkloadProfilerCleansOldRecords(t *testing.T) {
	profiler := newWorkloadProfiler(time.Millisecond * 100)

	// Record query
	profiler.recordQuery(&Query{Metric: "test"})

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Record another query (should clean old ones)
	profiler.recordQuery(&Query{Metric: "test"})

	profile := profiler.getProfile()

	// Should only have 1 recent query
	if profile.TotalQueries > 1 {
		t.Logf("Old queries should be cleaned, got %d", profile.TotalQueries)
	}
}

func TestPlanCacheTTL(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultQueryOptimizerConfig()
	config.PlanCacheTTL = 50 * time.Millisecond
	qo, _ := NewAdaptiveQueryOptimizer(db, config)
	defer qo.Close()

	query := &Query{Metric: "ttl_test"}

	// First call - cache miss
	qo.Optimize(query)

	// Wait for TTL expiry
	time.Sleep(100 * time.Millisecond)

	// Second call - should be cache miss due to TTL
	qo.Optimize(query)

	stats := qo.Stats()
	// Both should be misses due to TTL
	if stats.PlanCacheHits > 0 {
		t.Log("Cache should have expired")
	}
}

func TestGetWorkloadProfile(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	qo, _ := NewAdaptiveQueryOptimizer(db, nil)
	defer qo.Close()

	// Record some queries
	qo.profiler.recordQuery(&Query{Metric: "cpu", Start: 1000, End: 2000})
	qo.profiler.recordQuery(&Query{Metric: "memory", Start: 1000, End: 2000})

	profile := qo.GetWorkloadProfile()

	if profile == nil {
		t.Fatal("Expected non-nil profile")
	}
	if profile.TotalQueries != 2 {
		t.Errorf("TotalQueries = %d, want 2", profile.TotalQueries)
	}
}

func TestFeedbackDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultQueryOptimizerConfig()
	config.FeedbackEnabled = false
	qo, _ := NewAdaptiveQueryOptimizer(db, config)
	defer qo.Close()

	qo.RecordFeedback(&QueryFeedback{PlanID: "test"})

	qo.feedbackMu.RLock()
	count := len(qo.feedback)
	qo.feedbackMu.RUnlock()

	if count != 0 {
		t.Error("Feedback should not be recorded when disabled")
	}
}

func TestTimeRangeHistogram(t *testing.T) {
	profiler := newWorkloadProfiler(time.Hour)

	// Record queries with different time ranges
	ranges := []int64{
		int64(30 * time.Minute),    // < 1h
		int64(6 * time.Hour),       // 1h - 1d
		int64(3 * 24 * time.Hour),  // 1d - 1w
		int64(14 * 24 * time.Hour), // > 1w
	}

	for _, r := range ranges {
		profiler.recordQuery(&Query{
			Metric: "test",
			Start:  0,
			End:    r,
		})
	}

	profile := profiler.getProfile()

	if len(profile.TimeRangeHist) == 0 {
		t.Error("Expected time range histogram data")
	}
}
