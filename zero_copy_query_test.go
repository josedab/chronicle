package chronicle

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestVectorizedAggregator_Sum(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	got := va.Sum(data)
	if got != 55 {
		t.Errorf("sum = %f, want 55", got)
	}
}

func TestVectorizedAggregator_SumEmpty(t *testing.T) {
	va := NewVectorizedAggregator()
	if va.Sum(nil) != 0 {
		t.Error("expected 0 for empty")
	}
}

func TestVectorizedAggregator_Min(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{5, 3, 7, 1, 9}
	got := va.Min(data)
	if got != 1 {
		t.Errorf("min = %f, want 1", got)
	}
}

func TestVectorizedAggregator_MinEmpty(t *testing.T) {
	va := NewVectorizedAggregator()
	got := va.Min(nil)
	if !math.IsNaN(got) {
		t.Error("expected NaN for empty")
	}
}

func TestVectorizedAggregator_Max(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{5, 3, 7, 1, 9}
	got := va.Max(data)
	if got != 9 {
		t.Errorf("max = %f, want 9", got)
	}
}

func TestVectorizedAggregator_Avg(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{10, 20, 30}
	got := va.Avg(data)
	if got != 20 {
		t.Errorf("avg = %f, want 20", got)
	}
}

func TestVectorizedAggregator_Aggregate(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{1, 2, 3, 4, 5}

	tests := []struct {
		op   VectorAggOp
		want float64
	}{
		{VectorSum, 15},
		{VectorMin, 1},
		{VectorMax, 5},
		{VectorAvg, 3},
		{VectorCount, 5},
	}

	for _, tt := range tests {
		got, err := va.Aggregate(tt.op, data)
		if err != nil {
			t.Errorf("%s: %v", tt.op, err)
		}
		if got != tt.want {
			t.Errorf("%s = %f, want %f", tt.op, got, tt.want)
		}
	}
}

func TestVectorizedAggregator_AggregateEmpty(t *testing.T) {
	va := NewVectorizedAggregator()
	_, err := va.Aggregate(VectorSum, nil)
	if err == nil {
		t.Error("expected error for empty slice")
	}
}

func TestVectorAggOp_String(t *testing.T) {
	tests := []struct {
		op   VectorAggOp
		want string
	}{
		{VectorSum, "SUM"},
		{VectorMin, "MIN"},
		{VectorMax, "MAX"},
		{VectorAvg, "AVG"},
		{VectorCount, "COUNT"},
		{VectorAggOp(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.op.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}

func TestMmapPartition_Load(t *testing.T) {
	mp := NewMmapPartition("cpu.usage", 1000, 2000)
	ts := []int64{1000, 1100, 1200, 1300, 1400}
	vals := []float64{10, 20, 30, 40, 50}

	if err := mp.Load(ts, vals); err != nil {
		t.Fatal(err)
	}

	if mp.PointCount() != 5 {
		t.Errorf("points = %d", mp.PointCount())
	}
	if mp.SizeBytes() <= 0 {
		t.Error("expected positive size")
	}
	if mp.Metric() != "cpu.usage" {
		t.Errorf("metric = %q", mp.Metric())
	}
}

func TestMmapPartition_LoadMismatch(t *testing.T) {
	mp := NewMmapPartition("cpu", 0, 100)
	err := mp.Load([]int64{1, 2}, []float64{1})
	if err == nil {
		t.Error("expected error for mismatched lengths")
	}
}

func TestMmapPartition_ValuesSlice(t *testing.T) {
	mp := NewMmapPartition("cpu", 0, 5000)
	mp.Load([]int64{1000, 2000, 3000, 4000, 5000}, []float64{10, 20, 30, 40, 50})

	vals := mp.ValuesSlice(1500, 3500)
	if len(vals) != 2 {
		t.Errorf("vals = %d, want 2", len(vals))
	}
	if vals[0] != 20 || vals[1] != 30 {
		t.Errorf("vals = %v, want [20,30]", vals)
	}
}

func TestMmapPartition_ValuesSlice_All(t *testing.T) {
	mp := NewMmapPartition("m", 0, 100)
	mp.Load([]int64{10, 20, 30}, []float64{1, 2, 3})

	vals := mp.ValuesSlice(0, 100)
	if len(vals) != 3 {
		t.Errorf("vals = %d, want 3", len(vals))
	}
}

func TestMmapPartition_ValuesSlice_Empty(t *testing.T) {
	mp := NewMmapPartition("m", 0, 100)
	mp.Load([]int64{10, 20, 30}, []float64{1, 2, 3})

	vals := mp.ValuesSlice(50, 100)
	if vals != nil {
		t.Errorf("expected nil, got %v", vals)
	}
}

func TestMmapPartition_TimestampsSlice(t *testing.T) {
	mp := NewMmapPartition("m", 0, 100)
	mp.Load([]int64{10, 20, 30, 40}, []float64{1, 2, 3, 4})

	ts := mp.TimestampsSlice(15, 35)
	if len(ts) != 2 {
		t.Errorf("ts = %d, want 2", len(ts))
	}
}

func TestZeroCopyQueryPlanner_Execute(t *testing.T) {
	planner := NewZeroCopyQueryPlanner()

	mp1 := NewMmapPartition("cpu", 0, 100)
	mp1.Load([]int64{10, 20, 30}, []float64{1, 2, 3})

	mp2 := NewMmapPartition("cpu", 100, 200)
	mp2.Load([]int64{110, 120, 130}, []float64{4, 5, 6})

	planner.AddPartition(mp1)
	planner.AddPartition(mp2)

	if planner.PartitionCount() != 2 {
		t.Errorf("partitions = %d", planner.PartitionCount())
	}

	// Sum across partitions
	sum, err := planner.Execute("cpu", 0, 200, VectorSum)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 21 {
		t.Errorf("sum = %f, want 21", sum)
	}

	// Avg
	avg, err := planner.Execute("cpu", 0, 200, VectorAvg)
	if err != nil {
		t.Fatal(err)
	}
	if avg != 3.5 {
		t.Errorf("avg = %f, want 3.5", avg)
	}
}

func TestZeroCopyQueryPlanner_NoData(t *testing.T) {
	planner := NewZeroCopyQueryPlanner()
	_, err := planner.Execute("nonexistent", 0, 100, VectorSum)
	if err == nil {
		t.Error("expected error for no data")
	}
}

func TestZeroCopyQueryPlanner_Plan(t *testing.T) {
	planner := NewZeroCopyQueryPlanner()

	mp := NewMmapPartition("cpu", 0, 100)
	mp.Load([]int64{10, 20, 30}, []float64{1, 2, 3})
	planner.AddPartition(mp)

	plan := planner.Plan("cpu", 0, 100, VectorSum)
	if plan.Metric != "cpu" {
		t.Errorf("metric = %q", plan.Metric)
	}
	if plan.Partitions != 1 {
		t.Errorf("partitions = %d", plan.Partitions)
	}
	if plan.EstimatedPoints != 3 {
		t.Errorf("estimated = %d", plan.EstimatedPoints)
	}
	if !plan.Vectorized {
		t.Error("expected vectorized")
	}
}

func TestVectorizedAggregator_SumLargeData(t *testing.T) {
	va := NewVectorizedAggregator()
	n := 10000
	data := make([]float64, n)
	for i := range data {
		data[i] = 1.0
	}
	got := va.Sum(data)
	if got != float64(n) {
		t.Errorf("sum = %f, want %f", got, float64(n))
	}
}

func TestMin4Way(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{5, 3, 8, 1, 9, 2, 7, 4, 6, 0}
	got := va.Min4Way(data)
	if got != 0 {
		t.Errorf("Min4Way = %f, want 0", got)
	}
}

func TestMax4Way(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{5, 3, 8, 1, 9, 2, 7, 4, 6, 0}
	got := va.Max4Way(data)
	if got != 9 {
		t.Errorf("Max4Way = %f, want 9", got)
	}
}

func TestVariance(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	v := va.Variance(data)
	if math.Abs(v-4.0) > 0.01 {
		t.Errorf("Variance = %f, want ~4.0", v)
	}
}

func TestParallelVectorizedExecutor(t *testing.T) {
	pe := NewParallelVectorizedExecutor(2)
	chunks := [][]float64{
		{1, 2, 3, 4, 5},
		{6, 7, 8, 9, 10},
	}

	sum, err := pe.ExecuteParallel(chunks, VectorSum)
	if err != nil {
		t.Fatalf("ExecuteParallel sum: %v", err)
	}
	if sum != 55 {
		t.Errorf("sum = %f, want 55", sum)
	}

	min, err := pe.ExecuteParallel(chunks, VectorMin)
	if err != nil {
		t.Fatalf("ExecuteParallel min: %v", err)
	}
	if min != 1 {
		t.Errorf("min = %f, want 1", min)
	}

	max, err := pe.ExecuteParallel(chunks, VectorMax)
	if err != nil {
		t.Fatalf("ExecuteParallel max: %v", err)
	}
	if max != 10 {
		t.Errorf("max = %f, want 10", max)
	}

	avg, err := pe.ExecuteParallel(chunks, VectorAvg)
	if err != nil {
		t.Fatalf("ExecuteParallel avg: %v", err)
	}
	if math.Abs(avg-5.5) > 0.01 {
		t.Errorf("avg = %f, want 5.5", avg)
	}
}

func TestAdaptiveExecutorPathSelection(t *testing.T) {
	planner := NewZeroCopyQueryPlanner()
	ae := NewAdaptiveExecutor(planner)

	if ae.SelectPath(500) != PathRowOriented {
		t.Error("expected RowOriented for 500 points")
	}
	if ae.SelectPath(5000) != PathVectorized {
		t.Error("expected Vectorized for 5000 points")
	}
	if ae.SelectPath(200000) != PathParallelScan {
		t.Error("expected ParallelScan for 200000 points")
	}
}

func TestZeroCopyScanWithPredicate(t *testing.T) {
	planner := NewZeroCopyQueryPlanner()
	mp := NewMmapPartition("test_metric", 0, 100000)
	ts := []int64{1000, 2000, 3000, 4000, 5000}
	vals := []float64{1.0, math.NaN(), 3.0, 4.0, 5.0}
	if err := mp.Load(ts, vals); err != nil {
		t.Fatal(err)
	}
	planner.AddPartition(mp)

	minVal := 2.0
	scan := &ZeroCopyScan{
		Metric:    "test_metric",
		From:      0,
		To:        100000,
		Op:        VectorSum,
		MinValue:  &minVal,
		FilterNaN: true,
	}
	result, err := planner.ExecuteScan(scan)
	if err != nil {
		t.Fatalf("ExecuteScan: %v", err)
	}
	// 3.0 + 4.0 + 5.0 = 12.0 (NaN and 1.0 filtered)
	if math.Abs(result.Value-12.0) > 0.01 {
		t.Errorf("scan result = %f, want 12.0", result.Value)
	}
	if result.PointCount != 3 {
		t.Errorf("point count = %d, want 3", result.PointCount)
	}
}

func BenchmarkVectorizedSum1M(b *testing.B) {
	va := NewVectorizedAggregator()
	data := make([]float64, 1_000_000)
	for i := range data {
		data[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		va.Sum(data)
	}
}

func BenchmarkVectorizedMin4Way1M(b *testing.B) {
	va := NewVectorizedAggregator()
	data := make([]float64, 1_000_000)
	for i := range data {
		data[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		va.Min4Way(data)
	}
}

func BenchmarkParallelSum1M(b *testing.B) {
	pe := NewParallelVectorizedExecutor(4)
	data := make([]float64, 1_000_000)
	for i := range data {
		data[i] = float64(i)
	}
	chunkSize := len(data) / 4
	chunks := make([][]float64, 4)
	for i := 0; i < 4; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks[i] = data[start:end]
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pe.ExecuteParallel(chunks, VectorSum)
	}
}

func TestExecuteVectorizedEndToEnd(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write 100 data points
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	var expectedSum float64
	for i := 0; i < 100; i++ {
		v := float64(i + 1)
		expectedSum += v
		_ = db.Write(Point{
			Metric:    "vec_test_metric",
			Value:     v,
			Timestamp: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		})
	}
	db.Flush()

	qe := NewQueryEngine(db)
	ctx := context.Background()

	t.Run("VectorizedSum", func(t *testing.T) {
		q := &Query{
			Metric: "vec_test_metric",
			Start:  baseTime.UnixNano(),
			End:    baseTime.Add(100 * time.Second).UnixNano(),
			Aggregation: &Aggregation{
				Function: AggSum,
				Window:   100 * time.Second,
			},
		}
		result, path, err := qe.ExecuteVectorized(ctx, q)
		if err != nil {
			t.Fatalf("ExecuteVectorized: %v", err)
		}
		if len(result.Points) == 0 {
			t.Fatal("expected non-empty result")
		}
		t.Logf("path=%s, result value=%f, expected sum=%f", path, result.Points[0].Value, expectedSum)
	})

	t.Run("VectorizedMin", func(t *testing.T) {
		q := &Query{
			Metric: "vec_test_metric",
			Start:  baseTime.UnixNano(),
			End:    baseTime.Add(100 * time.Second).UnixNano(),
			Aggregation: &Aggregation{
				Function: AggMin,
				Window:   100 * time.Second,
			},
		}
		result, _, err := qe.ExecuteVectorized(ctx, q)
		if err != nil {
			t.Fatalf("ExecuteVectorized: %v", err)
		}
		if len(result.Points) == 0 {
			t.Fatal("expected non-empty result")
		}
	})

	t.Run("NonAggFallsBackToRowOriented", func(t *testing.T) {
		q := &Query{
			Metric: "vec_test_metric",
			Start:  baseTime.UnixNano(),
			End:    baseTime.Add(100 * time.Second).UnixNano(),
		}
		result, path, err := qe.ExecuteVectorized(ctx, q)
		if err != nil {
			t.Fatalf("ExecuteVectorized: %v", err)
		}
		if path != PathRowOriented {
			t.Errorf("expected RowOriented for non-agg, got %s", path)
		}
		if len(result.Points) == 0 {
			t.Fatal("expected non-empty result")
		}
	})
}
