package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestColumnBatch_NewFromPoints(t *testing.T) {
	points := []Point{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 3000, Value: 3.0},
	}
	cb := NewColumnBatch(points)
	if cb.Size != 3 {
		t.Fatalf("expected size 3, got %d", cb.Size)
	}
	if cb.Values[1] != 2.0 {
		t.Fatalf("expected value 2.0, got %f", cb.Values[1])
	}
}

func TestColumnBatch_Slice(t *testing.T) {
	cb := NewColumnBatchFromSlices(
		[]int64{1, 2, 3, 4, 5},
		[]float64{10, 20, 30, 40, 50},
	)
	sub := cb.Slice(1, 4)
	if sub.Size != 3 {
		t.Fatalf("expected size 3, got %d", sub.Size)
	}
	if sub.Values[0] != 20 {
		t.Fatalf("expected 20, got %f", sub.Values[0])
	}
}

func TestColumnBatch_FilterByTimeRange(t *testing.T) {
	cb := NewColumnBatchFromSlices(
		[]int64{100, 200, 300, 400, 500},
		[]float64{1, 2, 3, 4, 5},
	)
	filtered := cb.FilterByTimeRange(200, 400)
	if filtered.Size != 2 {
		t.Fatalf("expected 2 points, got %d", filtered.Size)
	}
	if filtered.Values[0] != 2 || filtered.Values[1] != 3 {
		t.Fatalf("unexpected values: %v", filtered.Values)
	}
}

func TestVectorSum(t *testing.T) {
	vals := make([]float64, 10000)
	for i := range vals {
		vals[i] = 1.0
	}
	sum := ColumnarVectorSum(vals)
	if sum != 10000.0 {
		t.Fatalf("expected 10000, got %f", sum)
	}
}

func TestVectorSum_Empty(t *testing.T) {
	if ColumnarVectorSum(nil) != 0 {
		t.Fatal("expected 0 for nil slice")
	}
}

func TestVectorMinMax(t *testing.T) {
	vals := []float64{5, 3, 8, 1, 9, 2}
	if ColumnarVectorMin(vals) != 1 {
		t.Fatalf("expected min 1, got %f", ColumnarVectorMin(vals))
	}
	if ColumnarVectorMax(vals) != 9 {
		t.Fatalf("expected max 9, got %f", ColumnarVectorMax(vals))
	}
}

func TestVectorMinMax_Empty(t *testing.T) {
	if !math.IsNaN(ColumnarVectorMin(nil)) {
		t.Fatal("expected NaN for empty min")
	}
	if !math.IsNaN(ColumnarVectorMax(nil)) {
		t.Fatal("expected NaN for empty max")
	}
}

func TestVectorCount(t *testing.T) {
	vals := []float64{1, math.NaN(), 3, math.NaN(), 5}
	if ColumnarVectorCount(vals) != 3 {
		t.Fatalf("expected 3, got %f", ColumnarVectorCount(vals))
	}
}

func TestVectorMean(t *testing.T) {
	vals := []float64{2, 4, 6}
	mean := ColumnarVectorMean(vals)
	if mean != 4.0 {
		t.Fatalf("expected 4.0, got %f", mean)
	}
}

func TestVectorStddev(t *testing.T) {
	vals := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	sd := ColumnarVectorStddev(vals)
	if sd < 1.9 || sd > 2.1 {
		t.Fatalf("expected stddev ~2.0, got %f", sd)
	}
}

func TestColumnarAggregator_AggregateBatch(t *testing.T) {
	agg := NewColumnarAggregator(0)
	batch := NewColumnBatchFromSlices(
		[]int64{1, 2, 3},
		[]float64{10, 20, 30},
	)

	if agg.AggregateBatch(batch, AggSum) != 60 {
		t.Fatal("sum failed")
	}
	if agg.AggregateBatch(batch, AggMin) != 10 {
		t.Fatal("min failed")
	}
	if agg.AggregateBatch(batch, AggMax) != 30 {
		t.Fatal("max failed")
	}
	if agg.AggregateBatch(batch, AggCount) != 3 {
		t.Fatal("count failed")
	}
}

func TestColumnarAggregator_Windowed(t *testing.T) {
	agg := NewColumnarAggregator(0)
	batch := NewColumnBatchFromSlices(
		[]int64{
			int64(0 * time.Second),
			int64(1 * time.Second),
			int64(5 * time.Second),
			int64(6 * time.Second),
			int64(10 * time.Second),
		},
		[]float64{1, 2, 3, 4, 5},
	)
	results := agg.AggregateWindowed(batch, AggSum, 5*time.Second)
	if len(results) < 2 {
		t.Fatalf("expected at least 2 windows, got %d", len(results))
	}
}

func TestParallelAggregate(t *testing.T) {
	batches := make([]*ColumnBatch, 4)
	for i := range batches {
		vals := make([]float64, 1000)
		ts := make([]int64, 1000)
		for j := range vals {
			vals[j] = 1.0
			ts[j] = int64(j)
		}
		batches[i] = NewColumnBatchFromSlices(ts, vals)
	}
	sum := ParallelAggregate(batches, AggSum, 2)
	if sum != 4000 {
		t.Fatalf("expected 4000, got %f", sum)
	}
}

func TestParallelAggregate_MeanWeighted(t *testing.T) {
	// Batch 1: 10 values of 10.0, mean=10
	// Batch 2: 90 values of 20.0, mean=20
	// Correct weighted mean = (10*10 + 90*20)/100 = 19.0
	batch1 := NewColumnBatchFromSlices(make([]int64, 10), make([]float64, 10))
	for i := range batch1.Values {
		batch1.Values[i] = 10.0
	}
	batch2 := NewColumnBatchFromSlices(make([]int64, 90), make([]float64, 90))
	for i := range batch2.Values {
		batch2.Values[i] = 20.0
	}
	mean := ParallelAggregate([]*ColumnBatch{batch1, batch2}, AggMean, 2)
	if mean != 19.0 {
		t.Fatalf("expected weighted mean 19.0, got %f", mean)
	}
}

func TestColumnarVectorMean_WithNaN(t *testing.T) {
	vals := []float64{2, math.NaN(), 4, math.NaN(), 6}
	mean := ColumnarVectorMean(vals)
	if mean != 4.0 {
		t.Fatalf("expected 4.0, got %f", mean)
	}
}

func TestVectorSumWithNulls(t *testing.T) {
	vals := []float64{1, 2, 3, 4, 5}
	nulls := []bool{false, true, false, true, false}
	sum := ColumnarVectorSumWithNulls(vals, nulls)
	if sum != 9 {
		t.Fatalf("expected 9, got %f", sum)
	}
}

func BenchmarkColumnarVectorSum_1M(b *testing.B) {
	vals := make([]float64, 1_000_000)
	for i := range vals {
		vals[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ColumnarVectorSum(vals)
	}
}

func BenchmarkColumnarVectorMin_1M(b *testing.B) {
	vals := make([]float64, 1_000_000)
	for i := range vals {
		vals[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ColumnarVectorMin(vals)
	}
}
