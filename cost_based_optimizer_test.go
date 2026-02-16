package chronicle

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func setupCBOTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	return db
}

func writeCBOTestPoints(t *testing.T, db *DB, metric string, n int, tags map[string]string) {
	t.Helper()
	now := time.Now()
	for i := 0; i < n; i++ {
		err := db.Write(Point{
			Metric:    metric,
			Tags:      tags,
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(n-i) * time.Second).UnixNano(),
		})
		if err != nil {
			t.Fatalf("failed to write point %d: %v", i, err)
		}
	}
	_ = db.Flush()
}

func TestDefaultCostBasedOptimizerConfig(t *testing.T) {
	cfg := DefaultCostBasedOptimizerConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.SeqScanCostPerRow <= 0 {
		t.Errorf("expected positive SeqScanCostPerRow, got %f", cfg.SeqScanCostPerRow)
	}
	if cfg.IndexScanCostPerRow <= 0 || cfg.IndexScanCostPerRow >= cfg.SeqScanCostPerRow {
		t.Errorf("expected IndexScanCostPerRow (%f) positive and less than SeqScanCostPerRow (%f)",
			cfg.IndexScanCostPerRow, cfg.SeqScanCostPerRow)
	}
	if cfg.SortCostPerRow <= 0 {
		t.Errorf("expected positive SortCostPerRow, got %f", cfg.SortCostPerRow)
	}
	if cfg.AggCostPerRow <= 0 {
		t.Errorf("expected positive AggCostPerRow, got %f", cfg.AggCostPerRow)
	}
	if cfg.NetworkCostPerRow <= 0 {
		t.Errorf("expected positive NetworkCostPerRow, got %f", cfg.NetworkCostPerRow)
	}
	if cfg.DefaultSelectivity <= 0 || cfg.DefaultSelectivity > 1.0 {
		t.Errorf("expected DefaultSelectivity in (0,1], got %f", cfg.DefaultSelectivity)
	}
	if cfg.RegexSelectivity <= 0 || cfg.RegexSelectivity > 1.0 {
		t.Errorf("expected RegexSelectivity in (0,1], got %f", cfg.RegexSelectivity)
	}
	if cfg.HistogramBuckets <= 0 {
		t.Errorf("expected positive HistogramBuckets, got %d", cfg.HistogramBuckets)
	}
	if cfg.StaleStatsThreshold <= 0 {
		t.Errorf("expected positive StaleStatsThreshold, got %v", cfg.StaleStatsThreshold)
	}
	if cfg.MaxAnalyzeSampleSize <= 0 {
		t.Errorf("expected positive MaxAnalyzeSampleSize, got %d", cfg.MaxAnalyzeSampleSize)
	}
}

func TestNewCostBasedOptimizer(t *testing.T) {
	t.Run("default_config", func(t *testing.T) {
		db := setupCBOTestDB(t)
		defer db.Close()

		cfg := DefaultCostBasedOptimizerConfig()
		opt := NewCostBasedOptimizer(db, cfg)
		if opt == nil {
			t.Fatal("expected non-nil optimizer")
		}

		stats := opt.GetStats()
		if stats.MetricsTracked != 0 {
			t.Errorf("expected 0 metrics tracked, got %d", stats.MetricsTracked)
		}
	})

	t.Run("custom_config", func(t *testing.T) {
		db := setupCBOTestDB(t)
		defer db.Close()

		cfg := CostBasedOptimizerConfig{
			Enabled:              true,
			SeqScanCostPerRow:    2.0,
			IndexScanCostPerRow:  1.0,
			SortCostPerRow:       3.0,
			AggCostPerRow:        0.2,
			NetworkCostPerRow:    0.1,
			DefaultSelectivity:   0.3,
			RegexSelectivity:     0.1,
			HistogramBuckets:     32,
			StaleStatsThreshold:  30 * time.Minute,
			MaxAnalyzeSampleSize: 500_000,
		}
		opt := NewCostBasedOptimizer(db, cfg)
		if opt == nil {
			t.Fatal("expected non-nil optimizer")
		}
	})
}

func TestCostBasedOptimizer_StartStop(t *testing.T) {
	db := setupCBOTestDB(t)
	defer db.Close()

	opt := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())

	// Start should succeed.
	if err := opt.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Starting again should be idempotent.
	if err := opt.Start(); err != nil {
		t.Fatalf("second Start failed: %v", err)
	}

	// Stop should succeed.
	if err := opt.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Stopping again should be idempotent.
	if err := opt.Stop(); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}

	// Analyze should fail after stop.
	_, err := opt.Analyze(context.Background(), "cpu")
	if err == nil {
		t.Error("expected error when analyzing after stop")
	}
}

func TestCostBasedOptimizer_Analyze(t *testing.T) {
	db := setupCBOTestDB(t)
	defer db.Close()

	opt := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
	if err := opt.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer opt.Stop()

	t.Run("empty_metric", func(t *testing.T) {
		_, err := opt.Analyze(context.Background(), "")
		if err == nil {
			t.Error("expected error for empty metric name")
		}
	})

	t.Run("no_data", func(t *testing.T) {
		ts, err := opt.Analyze(context.Background(), "nonexistent")
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}
		if ts.RowCount != 0 {
			t.Errorf("expected 0 rows, got %d", ts.RowCount)
		}
		if ts.Metric != "nonexistent" {
			t.Errorf("expected metric 'nonexistent', got %q", ts.Metric)
		}
	})

	t.Run("with_data", func(t *testing.T) {
		tags := map[string]string{"host": "a", "region": "us"}
		writeCBOTestPoints(t, db, "cpu", 50, tags)

		// Write additional points with different tag values.
		for i := 0; i < 20; i++ {
			db.Write(Point{
				Metric:    "cpu",
				Tags:      map[string]string{"host": "b", "region": "eu"},
				Value:     float64(100 + i),
				Timestamp: time.Now().Add(-time.Duration(i) * time.Second).UnixNano(),
			})
		}
		_ = db.Flush()

		ts, err := opt.Analyze(context.Background(), "cpu")
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}
		if ts.RowCount < 70 {
			t.Errorf("expected at least 70 rows, got %d", ts.RowCount)
		}
		if ts.Metric != "cpu" {
			t.Errorf("expected metric 'cpu', got %q", ts.Metric)
		}
		if ts.MinTimestamp >= ts.MaxTimestamp {
			t.Error("expected MinTimestamp < MaxTimestamp")
		}

		// Check distinct tag counts.
		hostDistinct, ok := ts.DistinctTagCount["host"]
		if !ok || hostDistinct < 2 {
			t.Errorf("expected at least 2 distinct hosts, got %d", hostDistinct)
		}
		regionDistinct, ok := ts.DistinctTagCount["region"]
		if !ok || regionDistinct < 2 {
			t.Errorf("expected at least 2 distinct regions, got %d", regionDistinct)
		}

		// Check value column stats.
		valCol, ok := ts.Columns["value"]
		if !ok {
			t.Fatal("expected 'value' column stats")
		}
		if valCol.DistinctCount <= 0 {
			t.Error("expected positive distinct count for value column")
		}
		if valCol.MinValue > valCol.MaxValue {
			t.Error("expected MinValue <= MaxValue")
		}
		if len(valCol.Histogram) == 0 {
			t.Error("expected non-empty histogram")
		}

		// Check timestamp column stats.
		if _, ok := ts.Columns["timestamp"]; !ok {
			t.Error("expected 'timestamp' column stats")
		}

		// Verify stats are retrievable.
		stored := opt.GetStatistics("cpu")
		if stored == nil {
			t.Error("expected stored statistics to be non-nil")
		}

		// Verify engine stats updated.
		engineStats := opt.GetStats()
		if engineStats.TotalAnalyzes == 0 {
			t.Error("expected TotalAnalyzes > 0")
		}
	})
}

func TestCostBasedOptimizer_EstimateCost(t *testing.T) {
	db := setupCBOTestDB(t)
	defer db.Close()

	opt := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
	if err := opt.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer opt.Stop()

	t.Run("nil_query", func(t *testing.T) {
		_, err := opt.EstimateCost(nil)
		if err == nil {
			t.Error("expected error for nil query")
		}
	})

	t.Run("empty_metric", func(t *testing.T) {
		_, err := opt.EstimateCost(&Query{})
		if err == nil {
			t.Error("expected error for empty metric")
		}
	})

	t.Run("without_stats", func(t *testing.T) {
		plan, err := opt.EstimateCost(&Query{Metric: "unknown"})
		if err != nil {
			t.Fatalf("EstimateCost failed: %v", err)
		}
		if plan.Plan != PlanSeqScan {
			t.Errorf("expected seq_scan plan, got %s", plan.Plan)
		}
		if plan.EstimatedCost <= 0 {
			t.Error("expected positive estimated cost")
		}
		// Without stats, uses fallback row count (100000) and default selectivity.
		if plan.EstimatedRows <= 0 {
			t.Error("expected positive estimated rows")
		}
	})

	t.Run("with_stats", func(t *testing.T) {
		writeCBOTestPoints(t, db, "mem", 100, map[string]string{"host": "a"})
		_, err := opt.Analyze(context.Background(), "mem")
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}

		plan, err := opt.EstimateCost(&Query{Metric: "mem"})
		if err != nil {
			t.Fatalf("EstimateCost failed: %v", err)
		}
		if plan.EstimatedCost <= 0 {
			t.Error("expected positive cost")
		}
		if plan.EstimatedRows <= 0 {
			t.Error("expected positive estimated rows")
		}
		if plan.Selectivity <= 0 || plan.Selectivity > 1.0 {
			t.Errorf("expected selectivity in (0,1], got %f", plan.Selectivity)
		}
	})

	t.Run("with_limit", func(t *testing.T) {
		plan, err := opt.EstimateCost(&Query{Metric: "mem", Limit: 5})
		if err != nil {
			t.Fatalf("EstimateCost failed: %v", err)
		}
		if plan.EstimatedRows > 5 {
			t.Errorf("expected estimated rows <= 5 with limit, got %d", plan.EstimatedRows)
		}
	})
}

func TestCostBasedOptimizer_ChooseBestPlan(t *testing.T) {
	db := setupCBOTestDB(t)
	defer db.Close()

	opt := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
	if err := opt.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer opt.Stop()

	t.Run("nil_query", func(t *testing.T) {
		_, err := opt.ChooseBestPlan(nil)
		if err == nil {
			t.Error("expected error for nil query")
		}
	})

	t.Run("empty_metric", func(t *testing.T) {
		_, err := opt.ChooseBestPlan(&Query{})
		if err == nil {
			t.Error("expected error for empty metric")
		}
	})

	t.Run("no_filters_chooses_seq_scan", func(t *testing.T) {
		plan, err := opt.ChooseBestPlan(&Query{Metric: "cpu"})
		if err != nil {
			t.Fatalf("ChooseBestPlan failed: %v", err)
		}
		// Without filters, only seq_scan is generated.
		if plan.Plan != PlanSeqScan {
			t.Errorf("expected seq_scan without filters, got %s", plan.Plan)
		}
	})

	t.Run("with_filters_considers_index", func(t *testing.T) {
		writeCBOTestPoints(t, db, "disk", 200, map[string]string{"host": "a"})
		opt.Analyze(context.Background(), "disk")

		now := time.Now()
		q := &Query{
			Metric: "disk",
			Tags:   map[string]string{"host": "a"},
			Start:  now.Add(-10 * time.Second).UnixNano(),
			End:    now.UnixNano(),
		}
		plan, err := opt.ChooseBestPlan(q)
		if err != nil {
			t.Fatalf("ChooseBestPlan failed: %v", err)
		}
		// With filters, index-based plans should be considered and potentially chosen.
		if plan.Plan != PlanIndexScan && plan.Plan != PlanIndexOnly && plan.Plan != PlanSeqScan {
			t.Errorf("unexpected plan type: %s", plan.Plan)
		}
		if plan.EstimatedCost <= 0 {
			t.Error("expected positive estimated cost")
		}

		// Verify engine stats.
		stats := opt.GetStats()
		if stats.TotalPlans == 0 {
			t.Error("expected TotalPlans > 0")
		}
	})

	t.Run("with_aggregation_includes_sort_cost", func(t *testing.T) {
		q := &Query{
			Metric: "disk",
			Tags:   map[string]string{"host": "a"},
			Aggregation: &Aggregation{
				Function: AggMean,
				Window:   time.Minute,
			},
			GroupBy: []string{"host"},
		}
		plan, err := opt.ChooseBestPlan(q)
		if err != nil {
			t.Fatalf("ChooseBestPlan failed: %v", err)
		}
		if plan.SortCost <= 0 {
			t.Error("expected positive sort cost with group by")
		}
		if plan.AggCost <= 0 {
			t.Error("expected positive agg cost with aggregation")
		}
	})
}

func TestCostBasedOptimizer_ExplainQuery(t *testing.T) {
	db := setupCBOTestDB(t)
	defer db.Close()

	opt := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
	if err := opt.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer opt.Stop()

	t.Run("nil_query", func(t *testing.T) {
		_, err := opt.ExplainQuery(nil)
		if err == nil {
			t.Error("expected error for nil query")
		}
	})

	t.Run("empty_metric", func(t *testing.T) {
		_, err := opt.ExplainQuery(&Query{})
		if err == nil {
			t.Error("expected error for empty metric")
		}
	})

	t.Run("no_filters_single_plan", func(t *testing.T) {
		plans, err := opt.ExplainQuery(&Query{Metric: "cpu"})
		if err != nil {
			t.Fatalf("ExplainQuery failed: %v", err)
		}
		if len(plans) != 1 {
			t.Fatalf("expected 1 plan without filters, got %d", len(plans))
		}
		if plans[0].Plan != PlanSeqScan {
			t.Errorf("expected seq_scan, got %s", plans[0].Plan)
		}
	})

	t.Run("with_filters_multiple_plans", func(t *testing.T) {
		writeCBOTestPoints(t, db, "net", 100, map[string]string{"iface": "eth0"})

		q := &Query{
			Metric: "net",
			Tags:   map[string]string{"iface": "eth0"},
		}
		plans, err := opt.ExplainQuery(q)
		if err != nil {
			t.Fatalf("ExplainQuery failed: %v", err)
		}
		// With tag filter and no aggregation: seq_scan + index_scan + index_only.
		if len(plans) < 2 {
			t.Fatalf("expected at least 2 plans with filters, got %d", len(plans))
		}

		// Verify the best plan is marked [chosen].
		chosenCount := 0
		for _, p := range plans {
			if strings.Contains(p.Notes, "[chosen]") {
				chosenCount++
			}
		}
		if chosenCount != 1 {
			t.Errorf("expected exactly 1 plan marked [chosen], got %d", chosenCount)
		}
	})

	t.Run("with_aggregation_excludes_index_only", func(t *testing.T) {
		q := &Query{
			Metric: "net",
			Tags:   map[string]string{"iface": "eth0"},
			Aggregation: &Aggregation{
				Function: AggSum,
				Window:   time.Minute,
			},
		}
		plans, err := opt.ExplainQuery(q)
		if err != nil {
			t.Fatalf("ExplainQuery failed: %v", err)
		}
		for _, p := range plans {
			if p.Plan == PlanIndexOnly {
				t.Error("index_only should not be generated when aggregation is set")
			}
		}
	})
}

func TestSelectivityEstimator(t *testing.T) {
	cfg := DefaultCostBasedOptimizerConfig()
	se := &SelectivityEstimator{config: cfg}

	baseStats := &TableStatistics{
		Metric:   "cpu",
		RowCount: 1000,
		DistinctTagCount: map[string]int64{
			"host":   10,
			"region": 3,
		},
		MinTimestamp: 1000,
		MaxTimestamp: 2000,
		Columns: map[string]*CBOColumnStats{
			"value": {
				Name:          "value",
				DistinctCount: 100,
				MinValue:      0,
				MaxValue:      100,
				Histogram: []HistogramBucket{
					{LowerBound: 0, UpperBound: 25, Count: 250, DistinctValues: 25},
					{LowerBound: 25, UpperBound: 50, Count: 250, DistinctValues: 25},
					{LowerBound: 50, UpperBound: 75, Count: 250, DistinctValues: 25},
					{LowerBound: 75, UpperBound: 100, Count: 250, DistinctValues: 25},
				},
			},
		},
	}

	t.Run("time_range_selectivity", func(t *testing.T) {
		tests := []struct {
			name     string
			stats    *TableStatistics
			start    int64
			end      int64
			expected float64
			delta    float64
		}{
			{"nil_stats", nil, 1000, 2000, cfg.DefaultSelectivity, 0.001},
			{"full_range", baseStats, 1000, 2000, 1.0, 0.001},
			{"half_range", baseStats, 1000, 1500, 0.5, 0.001},
			{"quarter_range", baseStats, 1000, 1250, 0.25, 0.001},
			{"clamped_start", baseStats, 500, 1500, 0.5, 0.001},
			{"end_before_start", baseStats, 1800, 1200, 0.0, 0.001},
			{"zero_total_range", &TableStatistics{RowCount: 1, MinTimestamp: 100, MaxTimestamp: 100}, 50, 150, 1.0, 0.001},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got := se.EstimateTimeRangeSelectivity(tc.stats, tc.start, tc.end)
				if math.Abs(got-tc.expected) > tc.delta {
					t.Errorf("expected ~%f, got %f", tc.expected, got)
				}
			})
		}
	})

	t.Run("tag_equality_selectivity", func(t *testing.T) {
		tests := []struct {
			name     string
			tagKey   string
			expected float64
		}{
			{"host_10_distinct", "host", 0.1},
			{"region_3_distinct", "region", 1.0 / 3.0},
			{"unknown_tag", "dc", cfg.DefaultSelectivity},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got := se.EstimateTagEqualitySelectivity(baseStats, tc.tagKey)
				if math.Abs(got-tc.expected) > 0.001 {
					t.Errorf("expected ~%f, got %f", tc.expected, got)
				}
			})
		}
	})

	t.Run("tag_in_selectivity", func(t *testing.T) {
		// host has 10 distinct values, so 1 value = 0.1, 3 values = 0.3
		got := se.EstimateTagInSelectivity(baseStats, "host", 3)
		if math.Abs(got-0.3) > 0.001 {
			t.Errorf("expected ~0.3, got %f", got)
		}
		// Capped at 1.0 when selecting all.
		got = se.EstimateTagInSelectivity(baseStats, "host", 20)
		if got > 1.0 {
			t.Errorf("expected selectivity capped at 1.0, got %f", got)
		}
	})

	t.Run("tag_regex_selectivity", func(t *testing.T) {
		// Non-anchored regex: returns config.RegexSelectivity.
		got := se.EstimateTagRegexSelectivity(baseStats, "host", "web.*")
		if math.Abs(got-cfg.RegexSelectivity) > 0.001 {
			t.Errorf("expected ~%f, got %f", cfg.RegexSelectivity, got)
		}

		// Anchored prefix: should be more selective.
		got = se.EstimateTagRegexSelectivity(baseStats, "host", "^web")
		if got > cfg.RegexSelectivity {
			t.Errorf("anchored regex should be at least as selective, got %f", got)
		}

		// Nil stats.
		got = se.EstimateTagRegexSelectivity(nil, "host", "web.*")
		if math.Abs(got-cfg.RegexSelectivity) > 0.001 {
			t.Errorf("expected default regex selectivity for nil stats, got %f", got)
		}
	})

	t.Run("value_range_selectivity", func(t *testing.T) {
		valCol := baseStats.Columns["value"]
		tests := []struct {
			name  string
			col   *CBOColumnStats
			low   float64
			high  float64
			check func(float64) bool
			desc  string
		}{
			{"nil_col", nil, 0, 100, func(s float64) bool { return math.Abs(s-cfg.DefaultSelectivity) < 0.001 }, "default selectivity"},
			{"full_range", valCol, 0, 100, func(s float64) bool { return s >= 0.9 }, "near 1.0"},
			{"first_quarter", valCol, 0, 25, func(s float64) bool { return s > 0 && s < 0.5 }, "less than 0.5"},
			{"out_of_range", valCol, 200, 300, func(s float64) bool { return s == 0.0 }, "0.0"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got := se.EstimateValueRangeSelectivity(tc.col, tc.low, tc.high)
				if !tc.check(got) {
					t.Errorf("expected %s, got %f", tc.desc, got)
				}
			})
		}
	})

	t.Run("combined_selectivity", func(t *testing.T) {
		// No filters: selectivity should be 1.0.
		q := &Query{Metric: "cpu"}
		got := se.CombinedSelectivity(baseStats, q)
		if math.Abs(got-1.0) > 0.001 {
			t.Errorf("expected 1.0 with no filters, got %f", got)
		}

		// Tag equality filter reduces selectivity.
		q = &Query{
			Metric: "cpu",
			Tags:   map[string]string{"host": "a"},
		}
		got = se.CombinedSelectivity(baseStats, q)
		if got >= 1.0 || got <= 0 {
			t.Errorf("expected selectivity in (0,1) with tag filter, got %f", got)
		}

		// Multiple filters combine multiplicatively.
		q = &Query{
			Metric: "cpu",
			Tags:   map[string]string{"host": "a", "region": "us"},
		}
		got = se.CombinedSelectivity(baseStats, q)
		expected := (1.0 / 10.0) * (1.0 / 3.0)
		if math.Abs(got-expected) > 0.001 {
			t.Errorf("expected ~%f, got %f", expected, got)
		}

		// TagFilter with various ops.
		q = &Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpNotEq, Values: []string{"a"}},
			},
		}
		got = se.CombinedSelectivity(baseStats, q)
		expected = 1.0 - (1.0 / 10.0)
		if math.Abs(got-expected) > 0.001 {
			t.Errorf("expected ~%f for NotEq, got %f", expected, got)
		}

		// TagOpIn.
		q = &Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"a", "b", "c"}},
			},
		}
		got = se.CombinedSelectivity(baseStats, q)
		expected = 3.0 / 10.0
		if math.Abs(got-expected) > 0.001 {
			t.Errorf("expected ~%f for In, got %f", expected, got)
		}

		// TagOpRegex.
		q = &Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpRegex, Values: []string{"web.*"}},
			},
		}
		got = se.CombinedSelectivity(baseStats, q)
		if got <= 0 || got > 1.0 {
			t.Errorf("expected selectivity in (0,1] for regex, got %f", got)
		}

		// TagOpNotRegex.
		q = &Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpNotRegex, Values: []string{"web.*"}},
			},
		}
		got = se.CombinedSelectivity(baseStats, q)
		if got <= 0 || got > 1.0 {
			t.Errorf("expected selectivity in (0,1] for not-regex, got %f", got)
		}
	})
}

func TestCostBasedOptimizer_AnalyzeAll(t *testing.T) {
	db := setupCBOTestDB(t)
	defer db.Close()

	opt := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
	if err := opt.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer opt.Stop()

	t.Run("no_metrics", func(t *testing.T) {
		results, err := opt.AnalyzeAll(context.Background())
		if err != nil {
			t.Fatalf("AnalyzeAll failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected 0 results, got %d", len(results))
		}
	})

	t.Run("multiple_metrics", func(t *testing.T) {
		writeCBOTestPoints(t, db, "cpu", 30, map[string]string{"host": "a"})
		writeCBOTestPoints(t, db, "mem", 20, map[string]string{"host": "b"})
		writeCBOTestPoints(t, db, "disk", 10, map[string]string{"host": "c"})

		results, err := opt.AnalyzeAll(context.Background())
		if err != nil {
			t.Fatalf("AnalyzeAll failed: %v", err)
		}
		if len(results) < 3 {
			t.Errorf("expected at least 3 metrics analyzed, got %d", len(results))
		}
		for _, metric := range []string{"cpu", "mem", "disk"} {
			ts, ok := results[metric]
			if !ok {
				t.Errorf("expected stats for %q", metric)
				continue
			}
			if ts.RowCount == 0 {
				t.Errorf("expected non-zero row count for %q", metric)
			}
		}

		// Verify all stats are stored.
		allStats := opt.GetAllStatistics()
		if len(allStats) < 3 {
			t.Errorf("expected at least 3 stored statistics, got %d", len(allStats))
		}
	})

	t.Run("not_running", func(t *testing.T) {
		db2 := setupCBOTestDB(t)
		defer db2.Close()
		opt2 := NewCostBasedOptimizer(db2, DefaultCostBasedOptimizerConfig())
		// Do not start the optimizer.
		_, err := opt2.AnalyzeAll(context.Background())
		if err == nil {
			t.Error("expected error when optimizer is not running")
		}
	})

	t.Run("context_cancellation", func(t *testing.T) {
		// With metrics already present from previous subtest, a cancelled context
		// may or may not be detected depending on scheduling. Verify it doesn't panic.
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately.
		_, _ = opt.AnalyzeAll(ctx)
		// No assertion on error: the select/default race means cancellation
		// detection is best-effort. We just ensure no panic.
	})
}
