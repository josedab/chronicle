package chronicle

import (
	"math"
	"testing"
)

func TestColumnBatchStats(t *testing.T) {
	cb := NewColumnBatchFromSlices(
		[]int64{100, 200, 300, 400, 500},
		[]float64{1.0, 5.0, 3.0, 2.0, 4.0},
	)

	stats := cb.ComputeStats()
	if stats.MinValue != 1.0 {
		t.Errorf("expected min 1.0, got %f", stats.MinValue)
	}
	if stats.MaxValue != 5.0 {
		t.Errorf("expected max 5.0, got %f", stats.MaxValue)
	}
	if stats.MinTime != 100 {
		t.Errorf("expected min time 100, got %d", stats.MinTime)
	}
	if stats.MaxTime != 500 {
		t.Errorf("expected max time 500, got %d", stats.MaxTime)
	}
	if stats.TotalCount != 5 {
		t.Errorf("expected count 5, got %d", stats.TotalCount)
	}
}

func TestColumnBatchStatsEmpty(t *testing.T) {
	cb := &ColumnBatch{}
	stats := cb.ComputeStats()
	if stats.TotalCount != 0 {
		t.Errorf("expected 0 count for empty batch")
	}
}

func TestColumnBatchStatsPruning(t *testing.T) {
	stats := ColumnBatchStats{MinValue: 10, MaxValue: 20, MinTime: 100, MaxTime: 200}

	if !stats.CanPruneByValue(25, 30) {
		t.Error("should prune when range is above max")
	}
	if !stats.CanPruneByValue(1, 5) {
		t.Error("should prune when range is below min")
	}
	if stats.CanPruneByValue(15, 25) {
		t.Error("should not prune overlapping range")
	}
	if !stats.CanPruneByTime(300, 400) {
		t.Error("should prune when time is after max")
	}
	if stats.CanPruneByTime(150, 250) {
		t.Error("should not prune overlapping time")
	}
}

func TestProjectFromPoints(t *testing.T) {
	points := []Point{
		{Metric: "cpu", Timestamp: 100, Value: 1.0, Tags: map[string]string{"host": "a", "dc": "us"}},
		{Metric: "cpu", Timestamp: 200, Value: 2.0, Tags: map[string]string{"host": "b", "dc": "eu"}},
	}

	proj := ProjectFromPoints(points, []string{"host"})
	if proj.Size != 2 {
		t.Fatalf("expected size 2, got %d", proj.Size)
	}
	if proj.Metric != "cpu" {
		t.Errorf("expected metric 'cpu', got %q", proj.Metric)
	}
	if proj.TagValues["host"][0] != "a" || proj.TagValues["host"][1] != "b" {
		t.Errorf("tag projection incorrect: %v", proj.TagValues["host"])
	}
	if _, ok := proj.TagValues["dc"]; ok {
		t.Error("dc should not be projected")
	}

	cb := proj.ToColumnBatch()
	if cb.Size != 2 {
		t.Errorf("expected batch size 2, got %d", cb.Size)
	}
}

func TestColumnarVectorMedian(t *testing.T) {
	tests := []struct {
		values []float64
		want   float64
	}{
		{[]float64{1, 2, 3, 4, 5}, 3},
		{[]float64{1, 2, 3, 4}, 2.5},
		{[]float64{42}, 42},
		{nil, math.NaN()},
	}
	for _, tt := range tests {
		got := ColumnarVectorMedian(tt.values)
		if math.IsNaN(tt.want) {
			if !math.IsNaN(got) {
				t.Errorf("expected NaN, got %f", got)
			}
		} else if got != tt.want {
			t.Errorf("median(%v) = %f, want %f", tt.values, got, tt.want)
		}
	}
}

func TestColumnarVectorPercentile(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	p50 := ColumnarVectorPercentile(values, 50)
	if math.Abs(p50-5.5) > 0.01 {
		t.Errorf("p50 = %f, want ~5.5", p50)
	}

	p0 := ColumnarVectorPercentile(values, 0)
	if p0 != 1 {
		t.Errorf("p0 = %f, want 1", p0)
	}

	p100 := ColumnarVectorPercentile(values, 100)
	if p100 != 10 {
		t.Errorf("p100 = %f, want 10", p100)
	}
}

func TestBatchPipeline(t *testing.T) {
	cb := NewColumnBatchFromSlices(
		[]int64{100, 200, 300, 400, 500},
		[]float64{1.0, 5.0, 3.0, 2.0, 4.0},
	)

	pipeline := NewBatchPipeline().
		AddFilter(func(v float64) bool { return v >= 3.0 }).
		AddTimeRange(100, 500)

	result := pipeline.Execute(cb)
	if result.Size != 2 {
		t.Errorf("expected 2 values >= 3.0 in time range, got %d", result.Size)
	}
}

func TestMultiColumnBatch(t *testing.T) {
	mcb := NewMultiColumnBatch(
		[]int64{100, 200, 300},
		map[string][]float64{
			"value": {1.0, 2.0, 3.0},
			"rate":  {0.1, 0.2, 0.3},
		},
	)
	if mcb.Size != 3 {
		t.Errorf("expected size 3, got %d", mcb.Size)
	}
	if len(mcb.Columns["rate"]) != 3 {
		t.Errorf("expected 3 rate values, got %d", len(mcb.Columns["rate"]))
	}
}

func TestParallelBatchAggregator(t *testing.T) {
	points := make([]Point, 10000)
	for i := range points {
		points[i] = Point{Metric: "test", Timestamp: int64(i), Value: 1.0}
	}

	pa := NewParallelBatchAggregator(4, 1024)
	sum := pa.AggregateBatches(points, AggSum)
	if math.Abs(sum-10000) > 0.01 {
		t.Errorf("expected sum 10000, got %f", sum)
	}
}

func TestAggregateWithStats(t *testing.T) {
	points := make([]Point, 5000)
	for i := range points {
		points[i] = Point{Metric: "test", Timestamp: int64(i), Value: float64(i)}
	}

	pa := NewParallelBatchAggregator(2, 1024)
	_, stats := pa.AggregateWithStats(points, AggSum)
	if len(stats) == 0 {
		t.Fatal("expected stats from aggregation")
	}
	if stats[0].TotalCount == 0 {
		t.Error("expected non-zero total count in stats")
	}
}

func TestNewQueryEngineWithCBO(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	cbo := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
	qe := NewQueryEngineWithCBO(db, cbo)
	if qe.cbo == nil {
		t.Error("expected CBO to be set")
	}
}
