package chronicle

import (
	"math"
	"testing"
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
