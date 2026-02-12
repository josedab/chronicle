package chronicle

import (
	"testing"
	"time"
)

func TestAdaptiveOptimizerRecordExecution(t *testing.T) {
	ao := NewAdaptiveOptimizer(nil, DefaultAdaptiveOptimizerConfig())

	ao.RecordExecution(QueryExecutionRecord{
		QueryHash:      "abc123def456",
		Metric:         "cpu",
		Duration:       10 * time.Millisecond,
		PointsScanned:  1000,
		PointsReturned: 50,
		Tags:           map[string]string{"host": "web-01"},
	})

	stats := ao.Stats()
	if stats.TotalRecords != 1 {
		t.Errorf("expected 1 record, got %d", stats.TotalRecords)
	}
	if stats.UniqueQueries != 1 {
		t.Errorf("expected 1 unique query, got %d", stats.UniqueQueries)
	}
}

func TestAdaptiveOptimizerSelectPlan(t *testing.T) {
	ao := NewAdaptiveOptimizer(nil, DefaultAdaptiveOptimizerConfig())

	// Without history, should get full_scan
	plan := ao.SelectPlan("abc123def456", "cpu", nil)
	if plan.Strategy != "full_scan" {
		t.Errorf("expected full_scan strategy, got %s", plan.Strategy)
	}
}

func TestAdaptiveOptimizerSelectPlanWithHistory(t *testing.T) {
	cfg := DefaultAdaptiveOptimizerConfig()
	cfg.MinSamplesForRecommend = 5
	ao := NewAdaptiveOptimizer(nil, cfg)

	// Record enough executions with high selectivity
	for i := 0; i < 10; i++ {
		ao.RecordExecution(QueryExecutionRecord{
			QueryHash:      "abc12345",
			Metric:         "cpu",
			Duration:       50 * time.Millisecond,
			PointsScanned:  10000,
			PointsReturned: 10, // very selective
			Tags:           map[string]string{"host": "web-01"},
		})
	}

	plan := ao.SelectPlan("abc12345", "cpu", map[string]string{"host": "web-01"})
	if plan.Strategy != "index_scan" {
		t.Errorf("expected index_scan for highly selective query, got %s", plan.Strategy)
	}
	if plan.Confidence <= 0 {
		t.Error("expected positive confidence")
	}
}

func TestAdaptiveOptimizerRecommendations(t *testing.T) {
	cfg := DefaultAdaptiveOptimizerConfig()
	cfg.MinSamplesForRecommend = 5
	ao := NewAdaptiveOptimizer(nil, cfg)

	for i := 0; i < 10; i++ {
		ao.RecordExecution(QueryExecutionRecord{
			QueryHash:      "slow-query1",
			Metric:         "latency",
			Duration:       100 * time.Millisecond,
			PointsScanned:  100000,
			PointsReturned: 10,
			Tags:           map[string]string{"service": "api", "region": "us-east"},
		})
	}

	recs := ao.GenerateRecommendations()
	if len(recs) == 0 {
		t.Fatal("expected at least 1 recommendation")
	}
	if recs[0].Metric != "latency" {
		t.Errorf("expected recommendation for latency metric, got %s", recs[0].Metric)
	}
	if recs[0].EstimatedSpeedup <= 1.0 {
		t.Errorf("expected speedup > 1.0, got %f", recs[0].EstimatedSpeedup)
	}
}

func TestAdaptiveOptimizerMaxHistory(t *testing.T) {
	cfg := DefaultAdaptiveOptimizerConfig()
	cfg.MaxHistorySize = 10
	ao := NewAdaptiveOptimizer(nil, cfg)

	for i := 0; i < 20; i++ {
		ao.RecordExecution(QueryExecutionRecord{
			QueryHash: "q1",
			Metric:    "cpu",
			Duration:  time.Millisecond,
		})
	}

	stats := ao.Stats()
	if stats.TotalRecords > 10 {
		t.Errorf("expected max 10 records, got %d", stats.TotalRecords)
	}
}

func TestAdaptiveOptimizerStats(t *testing.T) {
	ao := NewAdaptiveOptimizer(nil, DefaultAdaptiveOptimizerConfig())

	ao.RecordExecution(QueryExecutionRecord{QueryHash: "q1", Duration: 10 * time.Millisecond})
	ao.RecordExecution(QueryExecutionRecord{QueryHash: "q2", Duration: 20 * time.Millisecond})

	stats := ao.Stats()
	if stats.AvgQueryDuration != 15*time.Millisecond {
		t.Errorf("expected avg 15ms, got %v", stats.AvgQueryDuration)
	}
}

func TestAdaptiveOptimizerCostModel(t *testing.T) {
	ao := NewAdaptiveOptimizer(nil, DefaultAdaptiveOptimizerConfig())

	// Record executions
	for i := 0; i < 5; i++ {
		ao.RecordExecution(QueryExecutionRecord{
			QueryHash: "q1",
			Duration:  10 * time.Millisecond,
		})
	}

	stats := ao.Stats()
	if stats.CostModel.TotalPredictions != 5 {
		t.Errorf("expected 5 predictions, got %d", stats.CostModel.TotalPredictions)
	}
}
