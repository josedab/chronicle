package chronicle

import (
	"math"
	"testing"
)

func BenchmarkColumnarVectorSum_1K(b *testing.B)  { benchVectorSum(b, 1_000) }
func BenchmarkColumnarVectorSum_10K(b *testing.B) { benchVectorSum(b, 10_000) }
func BenchmarkColumnarVectorSum_100K(b *testing.B) { benchVectorSum(b, 100_000) }
func BenchmarkColumnarVectorSum_1M_Batch(b *testing.B)  { benchVectorSum(b, 1_000_000) }

func BenchmarkColumnarVectorMin_1M_Batch(b *testing.B) { benchVectorOp(b, 1_000_000, ColumnarVectorMin) }
func BenchmarkColumnarVectorMax_1M(b *testing.B) { benchVectorOp(b, 1_000_000, ColumnarVectorMax) }
func BenchmarkColumnarVectorMean_1M(b *testing.B) { benchVectorOp(b, 1_000_000, ColumnarVectorMean) }

func BenchmarkParallelAggregate_1M_2w(b *testing.B)  { benchParallelAgg(b, 1_000_000, 2) }
func BenchmarkParallelAggregate_1M_4w(b *testing.B)  { benchParallelAgg(b, 1_000_000, 4) }
func BenchmarkParallelAggregate_1M_8w(b *testing.B)  { benchParallelAgg(b, 1_000_000, 8) }

func BenchmarkRowAtATimeSum_1M(b *testing.B)     { benchRowSum(b, 1_000_000) }

func BenchmarkBatchPipeline_FilterAndAgg(b *testing.B) {
	values := generateValues(1_000_000)
	timestamps := generateTimestamps(1_000_000)
	cb := NewColumnBatchFromSlices(timestamps, values)

	pipeline := NewBatchPipeline().
		AddFilter(func(v float64) bool { return v > 0.5 }).
		AddTimeRange(timestamps[100], timestamps[len(timestamps)-100])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filtered := pipeline.Execute(cb)
		_ = ColumnarVectorSum(filtered.Values)
	}
}

func BenchmarkColumnBatchStats_1M(b *testing.B) {
	values := generateValues(1_000_000)
	timestamps := generateTimestamps(1_000_000)
	cb := NewColumnBatchFromSlices(timestamps, values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.ComputeStats()
	}
}

func benchVectorSum(b *testing.B, n int) {
	values := generateValues(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ColumnarVectorSum(values)
	}
}

func benchVectorOp(b *testing.B, n int, op func([]float64) float64) {
	values := generateValues(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = op(values)
	}
}

func benchParallelAgg(b *testing.B, n, workers int) {
	values := generateValues(n)
	batchSize := DefaultBatchSize
	var batches []*ColumnBatch
	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}
		ts := make([]int64, end-i)
		for j := range ts {
			ts[j] = int64(i + j)
		}
		batches = append(batches, NewColumnBatchFromSlices(ts, values[i:end]))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ParallelAggregate(batches, AggSum, workers)
	}
}

// benchRowSum measures row-at-a-time baseline for comparison.
func benchRowSum(b *testing.B, n int) {
	values := generateValues(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum float64
		for _, v := range values {
			sum += v
		}
		_ = sum
	}
}

func generateValues(n int) []float64 {
	values := make([]float64, n)
	for i := range values {
		values[i] = math.Sin(float64(i)*0.001) * 100
	}
	return values
}

func generateTimestamps(n int) []int64 {
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = int64(i) * 1_000_000_000 // 1s intervals
	}
	return ts
}
