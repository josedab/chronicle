package zerocopy

import (
	"math"
	"testing"
)

func TestVectorizedAggregator_Sum(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	got := va.Sum(data)
	if got != 55 {
		t.Errorf("Sum = %f, want 55", got)
	}
	if va.Sum(nil) != 0 {
		t.Error("Sum(nil) should be 0")
	}
}

func TestVectorizedAggregator_MinMax(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{3, 1, 4, 1, 5, 9, 2, 6}
	if got := va.Min(data); got != 1 {
		t.Errorf("Min = %f, want 1", got)
	}
	if got := va.Max(data); got != 9 {
		t.Errorf("Max = %f, want 9", got)
	}
	if !math.IsNaN(va.Min(nil)) {
		t.Error("Min(nil) should be NaN")
	}
}

func TestVectorizedAggregator_Avg(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{2, 4, 6}
	if got := va.Avg(data); got != 4 {
		t.Errorf("Avg = %f, want 4", got)
	}
}

func TestVectorizedAggregator_Aggregate(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{10, 20, 30}

	val, err := va.Aggregate(VectorSum, data)
	if err != nil || val != 60 {
		t.Errorf("Aggregate(Sum) = %f, err=%v", val, err)
	}

	val, err = va.Aggregate(VectorCount, data)
	if err != nil || val != 3 {
		t.Errorf("Aggregate(Count) = %f, err=%v", val, err)
	}

	_, err = va.Aggregate(VectorSum, nil)
	if err == nil {
		t.Error("expected error for empty slice")
	}
}

func TestMmapPartition(t *testing.T) {
	mp := NewMmapPartition("cpu", 100, 200)
	if mp.Metric() != "cpu" {
		t.Errorf("Metric = %s, want cpu", mp.Metric())
	}

	ts := []int64{100, 120, 140, 160, 180, 200}
	vals := []float64{1, 2, 3, 4, 5, 6}
	if err := mp.Load(ts, vals); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if mp.PointCount() != 6 {
		t.Errorf("PointCount = %d, want 6", mp.PointCount())
	}
	if mp.SizeBytes() <= 0 {
		t.Error("expected positive SizeBytes")
	}

	slice := mp.ValuesSlice(120, 170)
	if len(slice) != 3 {
		t.Errorf("ValuesSlice got %d values, want 3", len(slice))
	}

	tslice := mp.TimestampsSlice(120, 170)
	if len(tslice) != 3 {
		t.Errorf("TimestampsSlice got %d values, want 3", len(tslice))
	}

	if mp.ValuesSlice(300, 400) != nil {
		t.Error("expected nil for out-of-range")
	}
}

func TestMmapPartition_LoadMismatch(t *testing.T) {
	mp := NewMmapPartition("cpu", 0, 100)
	err := mp.Load([]int64{1, 2}, []float64{1.0})
	if err == nil {
		t.Error("expected error for mismatched lengths")
	}
}

func TestZeroCopyQueryPlanner(t *testing.T) {
	planner := NewZeroCopyQueryPlanner()

	mp := NewMmapPartition("cpu", 0, 1000)
	mp.Load([]int64{100, 200, 300}, []float64{10, 20, 30})
	planner.AddPartition(mp)

	if planner.PartitionCount() != 1 {
		t.Errorf("expected 1 partition, got %d", planner.PartitionCount())
	}

	plan := planner.Plan("cpu", 0, 1000, VectorSum)
	if plan.Partitions != 1 {
		t.Errorf("expected 1 partition in plan, got %d", plan.Partitions)
	}

	val, err := planner.Execute("cpu", 0, 1000, VectorSum)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if val != 60 {
		t.Errorf("expected 60, got %f", val)
	}

	_, err = planner.Execute("nonexistent", 0, 1000, VectorSum)
	if err == nil {
		t.Error("expected error for nonexistent metric")
	}
}

func TestVectorAggOpString(t *testing.T) {
	if VectorSum.String() != "SUM" {
		t.Errorf("expected SUM, got %s", VectorSum.String())
	}
	if VectorAggOp(99).String() != "UNKNOWN" {
		t.Errorf("expected UNKNOWN, got %s", VectorAggOp(99).String())
	}
}

func TestExecutionPathString(t *testing.T) {
	if PathRowOriented.String() != "row-oriented" {
		t.Errorf("expected row-oriented, got %s", PathRowOriented.String())
	}
	if ExecutionPath(99).String() != "unknown" {
		t.Errorf("expected unknown, got %s", ExecutionPath(99).String())
	}
}

func TestVarianceAndStdDev(t *testing.T) {
	va := NewVectorizedAggregator()
	data := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	variance := va.Variance(data)
	if variance < 3.9 || variance > 4.1 {
		t.Errorf("Variance = %f, expected ~4.0", variance)
	}
	stddev := va.StdDev(data)
	if stddev < 1.9 || stddev > 2.1 {
		t.Errorf("StdDev = %f, expected ~2.0", stddev)
	}
}
