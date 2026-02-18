package chronicle

import (
	"fmt"
	"math"
	"sync"
	"unsafe"
)

// VectorAggOp defines a vectorized aggregation operation.
type VectorAggOp int

const (
	VectorSum VectorAggOp = iota
	VectorMin
	VectorMax
	VectorAvg
	VectorCount
)

func (op VectorAggOp) String() string {
	switch op {
	case VectorSum:
		return "SUM"
	case VectorMin:
		return "MIN"
	case VectorMax:
		return "MAX"
	case VectorAvg:
		return "AVG"
	case VectorCount:
		return "COUNT"
	default:
		return "UNKNOWN"
	}
}

// VectorizedAggregator performs optimized float64 aggregations on contiguous slices.
type VectorizedAggregator struct{}

// NewVectorizedAggregator creates a new vectorized aggregator.
func NewVectorizedAggregator() *VectorizedAggregator {
	return &VectorizedAggregator{}
}

// Aggregate performs a vectorized aggregation on a float64 slice.
func (va *VectorizedAggregator) Aggregate(op VectorAggOp, data []float64) (float64, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("zero-copy: cannot aggregate empty slice")
	}

	switch op {
	case VectorSum:
		return va.Sum(data), nil
	case VectorMin:
		return va.Min(data), nil
	case VectorMax:
		return va.Max(data), nil
	case VectorAvg:
		return va.Avg(data), nil
	case VectorCount:
		return float64(len(data)), nil
	default:
		return 0, fmt.Errorf("zero-copy: unknown op %d", op)
	}
}

// Sum computes the sum using 4-way unrolled accumulation.
func (va *VectorizedAggregator) Sum(data []float64) float64 {
	n := len(data)
	if n == 0 {
		return 0
	}

	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < n; i += 4 {
		s0 += data[i]
		s1 += data[i+1]
		s2 += data[i+2]
		s3 += data[i+3]
	}
	total := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		total += data[i]
	}
	return total
}

// Min returns the minimum value.
func (va *VectorizedAggregator) Min(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	min := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] < min {
			min = data[i]
		}
	}
	return min
}

// Max returns the maximum value.
func (va *VectorizedAggregator) Max(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	max := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] > max {
			max = data[i]
		}
	}
	return max
}

// Avg returns the average value.
func (va *VectorizedAggregator) Avg(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	return va.Sum(data) / float64(len(data))
}

// MmapPartition represents a memory-mapped partition for zero-copy reads.
type MmapPartition struct {
	mu         sync.RWMutex
	metric     string
	startTime  int64
	endTime    int64
	timestamps []int64
	values     []float64
	pointCount int
	sizeBytes  int64
}

// NewMmapPartition creates a new memory-mapped-style partition.
func NewMmapPartition(metric string, startTime, endTime int64) *MmapPartition {
	return &MmapPartition{
		metric:    metric,
		startTime: startTime,
		endTime:   endTime,
	}
}

// Load loads data into the partition.
func (mp *MmapPartition) Load(timestamps []int64, values []float64) error {
	if len(timestamps) != len(values) {
		return fmt.Errorf("zero-copy: timestamps (%d) and values (%d) length mismatch", len(timestamps), len(values))
	}

	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.timestamps = timestamps
	mp.values = values
	mp.pointCount = len(timestamps)
	mp.sizeBytes = int64(len(timestamps))*int64(unsafe.Sizeof(int64(0))) +
		int64(len(values))*int64(unsafe.Sizeof(float64(0)))
	return nil
}

// ValuesSlice returns a zero-copy slice of values in the given time range.
func (mp *MmapPartition) ValuesSlice(from, to int64) []float64 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	start := -1
	end := len(mp.timestamps)

	for i, ts := range mp.timestamps {
		if ts >= from && start == -1 {
			start = i
		}
		if ts > to {
			end = i
			break
		}
	}

	if start == -1 {
		return nil
	}
	return mp.values[start:end]
}

// TimestampsSlice returns a zero-copy slice of timestamps in the given time range.
func (mp *MmapPartition) TimestampsSlice(from, to int64) []int64 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	start := -1
	end := len(mp.timestamps)

	for i, ts := range mp.timestamps {
		if ts >= from && start == -1 {
			start = i
		}
		if ts > to {
			end = i
			break
		}
	}

	if start == -1 {
		return nil
	}
	return mp.timestamps[start:end]
}

// PointCount returns the number of points.
func (mp *MmapPartition) PointCount() int {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.pointCount
}

// SizeBytes returns the approximate memory size.
func (mp *MmapPartition) SizeBytes() int64 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.sizeBytes
}

// Metric returns the partition's metric name.
func (mp *MmapPartition) Metric() string {
	return mp.metric
}

// ZeroCopyQueryPlan describes an optimized query execution plan.
type ZeroCopyQueryPlan struct {
	Metric          string      `json:"metric"`
	TimeRange       [2]int64    `json:"time_range"`
	AggOp           VectorAggOp `json:"agg_op"`
	Partitions      int         `json:"partitions"`
	Vectorized      bool        `json:"vectorized"`
	EstimatedPoints int         `json:"estimated_points"`
}

// ZeroCopyQueryPlanner creates optimized query plans.
type ZeroCopyQueryPlanner struct {
	mu         sync.RWMutex
	partitions []*MmapPartition
	aggregator *VectorizedAggregator
}

// NewZeroCopyQueryPlanner creates a new query planner.
func NewZeroCopyQueryPlanner() *ZeroCopyQueryPlanner {
	return &ZeroCopyQueryPlanner{
		partitions: make([]*MmapPartition, 0),
		aggregator: NewVectorizedAggregator(),
	}
}

// AddPartition registers a partition with the planner.
func (p *ZeroCopyQueryPlanner) AddPartition(mp *MmapPartition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitions = append(p.partitions, mp)
}

// Plan creates an optimized query plan.
func (p *ZeroCopyQueryPlanner) Plan(metric string, from, to int64, op VectorAggOp) ZeroCopyQueryPlan {
	p.mu.RLock()
	defer p.mu.RUnlock()

	plan := ZeroCopyQueryPlan{
		Metric:     metric,
		TimeRange:  [2]int64{from, to},
		AggOp:      op,
		Vectorized: true,
	}

	for _, part := range p.partitions {
		if part.metric == metric {
			values := part.ValuesSlice(from, to)
			if len(values) > 0 {
				plan.Partitions++
				plan.EstimatedPoints += len(values)
			}
		}
	}

	return plan
}

// Execute runs a query plan and returns the aggregated result.
func (p *ZeroCopyQueryPlanner) Execute(metric string, from, to int64, op VectorAggOp) (float64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var allValues []float64
	for _, part := range p.partitions {
		if part.metric == metric {
			values := part.ValuesSlice(from, to)
			allValues = append(allValues, values...)
		}
	}

	if len(allValues) == 0 {
		return 0, fmt.Errorf("zero-copy: no data for metric %q in range", metric)
	}

	return p.aggregator.Aggregate(op, allValues)
}

// PartitionCount returns the number of registered partitions.
func (p *ZeroCopyQueryPlanner) PartitionCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.partitions)
}
