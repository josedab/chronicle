package chronicle

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestHyperLogLogSketch(t *testing.T) {
	t.Run("basic cardinality", func(t *testing.T) {
		hll := NewHyperLogLogSketch(14)
		for i := 0; i < 10000; i++ {
			hll.AddString(fmt.Sprintf("value-%d", i))
		}
		est := hll.Estimate()
		// HLL should be within ~2% for precision 14
		if est < 9000 || est > 11000 {
			t.Errorf("expected ~10000, got %d", est)
		}
	})

	t.Run("precision bounds", func(t *testing.T) {
		hll := NewHyperLogLogSketch(2) // should clamp to 4
		if hll.precision != 4 {
			t.Errorf("expected precision 4, got %d", hll.precision)
		}
		hll2 := NewHyperLogLogSketch(20) // should clamp to 18
		if hll2.precision != 18 {
			t.Errorf("expected precision 18, got %d", hll2.precision)
		}
	})

	t.Run("merge", func(t *testing.T) {
		a := NewHyperLogLogSketch(14)
		b := NewHyperLogLogSketch(14)
		for i := 0; i < 5000; i++ {
			a.AddString(fmt.Sprintf("a-%d", i))
			b.AddString(fmt.Sprintf("b-%d", i))
		}
		a.Merge(b)
		est := a.Estimate()
		if est < 8000 || est > 12000 {
			t.Errorf("expected ~10000, got %d", est)
		}
	})

	t.Run("merge nil", func(t *testing.T) {
		a := NewHyperLogLogSketch(14)
		a.AddString("test")
		a.Merge(nil) // should not panic
	})

	t.Run("empty sketch", func(t *testing.T) {
		hll := NewHyperLogLogSketch(14)
		est := hll.Estimate()
		if est != 0 {
			t.Errorf("expected 0, got %d", est)
		}
	})
}

func TestEquiDepthHistogram(t *testing.T) {
	t.Run("basic histogram", func(t *testing.T) {
		values := make([]float64, 1000)
		for i := range values {
			values[i] = float64(i)
		}
		h := NewEquiDepthHistogram(values, 10)
		if h == nil {
			t.Fatal("expected non-nil histogram")
		}
		if len(h.Buckets) != 10 {
			t.Errorf("expected 10 buckets, got %d", len(h.Buckets))
		}
		if h.TotalCount != 1000 {
			t.Errorf("expected total 1000, got %d", h.TotalCount)
		}
	})

	t.Run("range estimation", func(t *testing.T) {
		values := make([]float64, 1000)
		for i := range values {
			values[i] = float64(i)
		}
		h := NewEquiDepthHistogram(values, 10)
		count := h.EstimateRangeCount(0, 100)
		if count < 80 || count > 120 {
			t.Errorf("expected ~100, got %d", count)
		}
	})

	t.Run("selectivity", func(t *testing.T) {
		values := make([]float64, 1000)
		for i := range values {
			values[i] = float64(i)
		}
		h := NewEquiDepthHistogram(values, 10)
		sel := h.EstimateSelectivity(0, 500)
		if sel < 0.4 || sel > 0.6 {
			t.Errorf("expected ~0.5, got %f", sel)
		}
	})

	t.Run("empty histogram", func(t *testing.T) {
		h := NewEquiDepthHistogram(nil, 10)
		if len(h.Buckets) != 0 {
			t.Errorf("expected empty buckets")
		}
		count := h.EstimateRangeCount(0, 100)
		if count != 0 {
			t.Errorf("expected 0, got %d", count)
		}
	})
}

func TestCardinalityEstimator(t *testing.T) {
	db := setupTestDB(t)

	// Write test data with UnixNano timestamps and flush
	now := time.Now()
	for i := 0; i < 100; i++ {
		p := Point{
			Metric:    "test_metric",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(100-i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": fmt.Sprintf("host-%d", i%10), "region": fmt.Sprintf("region-%d", i%3)},
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	_ = db.Flush()

	ce := NewCardinalityEstimator(db, DefaultCardinalityEstimatorConfig())

	t.Run("collect stats", func(t *testing.T) {
		stats, err := ce.CollectStats(context.Background(), "test_metric")
		if err != nil {
			t.Fatalf("collect failed: %v", err)
		}
		if stats.RowCount == 0 {
			t.Error("expected non-zero row count")
		}
	})

	t.Run("collect empty metric", func(t *testing.T) {
		_, err := ce.CollectStats(context.Background(), "")
		if err == nil {
			t.Error("expected error for empty metric")
		}
	})

	t.Run("estimate cardinality", func(t *testing.T) {
		est := ce.EstimateCardinality(&Query{Metric: "test_metric"})
		if est <= 0 {
			t.Errorf("expected positive estimate, got %d", est)
		}
	})

	t.Run("estimate with tags", func(t *testing.T) {
		fullEst := ce.EstimateCardinality(&Query{Metric: "test_metric"})
		tagEst := ce.EstimateCardinality(&Query{
			Metric: "test_metric",
			Tags:   map[string]string{"host": "host-0"},
		})
		if fullEst > 0 && tagEst > fullEst {
			t.Errorf("tag filter should not increase estimate: full=%d tag=%d", fullEst, tagEst)
		}
	})

	t.Run("estimate nil query", func(t *testing.T) {
		est := ce.EstimateCardinality(nil)
		if est != 0 {
			t.Errorf("expected 0, got %d", est)
		}
	})

	t.Run("feed to optimizer", func(t *testing.T) {
		opt := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
		if err := opt.Start(); err != nil {
			t.Fatal(err)
		}
		defer opt.Stop()
		ce.FeedEstimateToOptimizer(opt)
		ts := opt.GetStatistics("test_metric")
		if ts == nil {
			t.Fatal("expected optimizer to have stats")
		}
		if ts.RowCount == 0 {
			t.Error("expected non-zero rows in optimizer stats")
		}
	})

	t.Run("record feedback", func(t *testing.T) {
		ce.RecordFeedback("test_metric", 100, 95)
		ce.RecordFeedback("test_metric", 100, 110)
		summary := ce.GetFeedbackSummary()
		if summary["feedback_entries"].(int) != 2 {
			t.Errorf("expected 2 feedback entries")
		}
	})

	t.Run("get all stats", func(t *testing.T) {
		all := ce.GetAllStats()
		if len(all) == 0 {
			t.Error("expected non-empty stats")
		}
	})

	t.Run("collect from partition", func(t *testing.T) {
		p := &Partition{
			series: map[string]*SeriesData{
				"test": {
					Series: Series{Metric: "partition_metric", Tags: map[string]string{"env": "prod"}},
					Timestamps: []int64{1, 2, 3},
					Values:     []float64{1.0, 2.0, 3.0},
					MinTime:    1,
					MaxTime:    3,
				},
			},
		}
		ce.CollectFromPartition(p)
		stats := ce.GetStats("partition_metric")
		if stats == nil {
			t.Fatal("expected stats for partition_metric")
		}
		if stats.RowCount != 3 {
			t.Errorf("expected 3 rows, got %d", stats.RowCount)
		}
	})
}

func TestCardinalityEstimatorCollectAll(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now()
	for _, metric := range []string{"cpu", "mem"} {
		for i := 0; i < 10; i++ {
			db.Write(Point{Metric: metric, Value: float64(i), Timestamp: now.Add(-time.Duration(10-i) * time.Second).UnixNano()})
		}
	}
	_ = db.Flush()

	ce := NewCardinalityEstimator(db, DefaultCardinalityEstimatorConfig())
	err := ce.CollectAllStats(context.Background())
	if err != nil {
		t.Fatalf("collect all failed: %v", err)
	}
}

func TestCardinalityEstimatorTagFilterSelectivity(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now()
	for i := 0; i < 100; i++ {
		db.Write(Point{
			Metric:    "tf_metric",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(100-i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": fmt.Sprintf("host-%d", i%10)},
		})
	}
	_ = db.Flush()

	ce := NewCardinalityEstimator(db, DefaultCardinalityEstimatorConfig())
	ce.CollectStats(context.Background(), "tf_metric")

	fullEst := ce.EstimateCardinality(&Query{Metric: "tf_metric"})

	// TagOpEq should reduce estimate
	eqEst := ce.EstimateCardinality(&Query{
		Metric:     "tf_metric",
		TagFilters: []TagFilter{{Key: "host", Op: TagOpEq, Values: []string{"host-0"}}},
	})
	if fullEst > 0 && eqEst > fullEst {
		t.Errorf("TagOpEq should reduce estimate: full=%d eq=%d", fullEst, eqEst)
	}

	// TagOpIn with 3 values should be between eq and full
	inEst := ce.EstimateCardinality(&Query{
		Metric:     "tf_metric",
		TagFilters: []TagFilter{{Key: "host", Op: TagOpIn, Values: []string{"a", "b", "c"}}},
	})
	if fullEst > 0 && inEst > fullEst {
		t.Errorf("TagOpIn should not exceed full: full=%d in=%d", fullEst, inEst)
	}
}

func TestCardinalityEstimatorIsStale(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultCardinalityEstimatorConfig()
	cfg.StaleThreshold = 1 * time.Millisecond
	ce := NewCardinalityEstimator(db, cfg)

	if !ce.IsStale("nonexistent") {
		t.Error("nonexistent metric should be stale")
	}

	ce.CollectStats(context.Background(), "any")
	time.Sleep(5 * time.Millisecond)
	if !ce.IsStale("any") {
		t.Error("expected stale after threshold")
	}
}
