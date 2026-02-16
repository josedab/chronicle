package chronicle

import (
	"fmt"
	"math"
	"sync"
	"time"
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

// --- Vectorized Scan with Predicate Pushdown ---

// ZeroCopyScan defines a scan request with optional predicate pushdown.
type ZeroCopyScan struct {
	Metric     string
	From       int64
	To         int64
	Op         VectorAggOp
	MinValue   *float64 // optional: filter values >= MinValue
	MaxValue   *float64 // optional: filter values <= MaxValue
	FilterNaN  bool     // optional: filter out NaN values
}

// ZeroCopyScanResult holds the results of a zero-copy scan.
type ZeroCopyScanResult struct {
	Value      float64 `json:"value"`
	PointCount int     `json:"point_count"`
	ScanTimeNs int64   `json:"scan_time_ns"`
}

// ExecuteScan runs a scan with predicate pushdown and vectorized aggregation.
func (p *ZeroCopyQueryPlanner) ExecuteScan(scan *ZeroCopyScan) (*ZeroCopyScanResult, error) {
	start := time.Now()
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := &ZeroCopyScanResult{}
	var allValues []float64

	for _, part := range p.partitions {
		if part.metric != scan.Metric {
			continue
		}
		values := part.ValuesSlice(scan.From, scan.To)
		allValues = append(allValues, values...)
	}

	// Apply predicate pushdown filters
	if scan.MinValue != nil || scan.MaxValue != nil || scan.FilterNaN {
		filtered := make([]float64, 0, len(allValues))
		for _, v := range allValues {
			if scan.FilterNaN && math.IsNaN(v) {
				continue
			}
			if scan.MinValue != nil && v < *scan.MinValue {
				continue
			}
			if scan.MaxValue != nil && v > *scan.MaxValue {
				continue
			}
			filtered = append(filtered, v)
		}
		allValues = filtered
	}

	if len(allValues) == 0 {
		return nil, fmt.Errorf("zero-copy: no data for metric %q", scan.Metric)
	}

	result.PointCount = len(allValues)
	agg := NewVectorizedAggregator()
	val, err := agg.Aggregate(scan.Op, allValues)
	if err != nil {
		return nil, err
	}
	result.Value = val
	result.ScanTimeNs = time.Since(start).Nanoseconds()
	return result, nil
}

// --- Vectorized Min/Max with 4-way unrolling ---

// Min4Way computes minimum using 4-way unrolled comparison.
func (va *VectorizedAggregator) Min4Way(data []float64) float64 {
	n := len(data)
	if n == 0 {
		return math.NaN()
	}
	if n < 4 {
		return va.Min(data)
	}

	m0, m1, m2, m3 := data[0], data[1], data[2], data[3]
	i := 4
	for ; i+3 < n; i += 4 {
		if data[i] < m0 {
			m0 = data[i]
		}
		if data[i+1] < m1 {
			m1 = data[i+1]
		}
		if data[i+2] < m2 {
			m2 = data[i+2]
		}
		if data[i+3] < m3 {
			m3 = data[i+3]
		}
	}

	if m1 < m0 {
		m0 = m1
	}
	if m2 < m0 {
		m0 = m2
	}
	if m3 < m0 {
		m0 = m3
	}
	for ; i < n; i++ {
		if data[i] < m0 {
			m0 = data[i]
		}
	}
	return m0
}

// Max4Way computes maximum using 4-way unrolled comparison.
func (va *VectorizedAggregator) Max4Way(data []float64) float64 {
	n := len(data)
	if n == 0 {
		return math.NaN()
	}
	if n < 4 {
		return va.Max(data)
	}

	m0, m1, m2, m3 := data[0], data[1], data[2], data[3]
	i := 4
	for ; i+3 < n; i += 4 {
		if data[i] > m0 {
			m0 = data[i]
		}
		if data[i+1] > m1 {
			m1 = data[i+1]
		}
		if data[i+2] > m2 {
			m2 = data[i+2]
		}
		if data[i+3] > m3 {
			m3 = data[i+3]
		}
	}

	if m1 > m0 {
		m0 = m1
	}
	if m2 > m0 {
		m0 = m2
	}
	if m3 > m0 {
		m0 = m3
	}
	for ; i < n; i++ {
		if data[i] > m0 {
			m0 = data[i]
		}
	}
	return m0
}

// Variance computes population variance with single-pass Welford's algorithm.
func (va *VectorizedAggregator) Variance(data []float64) float64 {
	n := len(data)
	if n < 2 {
		return 0
	}
	var mean, m2 float64
	for i, v := range data {
		delta := v - mean
		mean += delta / float64(i+1)
		delta2 := v - mean
		m2 += delta * delta2
	}
	return m2 / float64(n)
}

// StdDev computes population standard deviation.
func (va *VectorizedAggregator) StdDev(data []float64) float64 {
	return math.Sqrt(va.Variance(data))
}

// --- Parallel Vectorized Execution ---

// ParallelVectorizedExecutor runs aggregations across partitions in parallel.
type ParallelVectorizedExecutor struct {
	workers    int
	aggregator *VectorizedAggregator
}

// NewParallelVectorizedExecutor creates an executor with the given parallelism.
func NewParallelVectorizedExecutor(workers int) *ParallelVectorizedExecutor {
	if workers <= 0 {
		workers = 4
	}
	return &ParallelVectorizedExecutor{
		workers:    workers,
		aggregator: NewVectorizedAggregator(),
	}
}

type partialResult struct {
	sum   float64
	min   float64
	max   float64
	count int
}

// ExecuteParallel runs a vectorized aggregation across multiple data chunks in parallel.
func (pe *ParallelVectorizedExecutor) ExecuteParallel(chunks [][]float64, op VectorAggOp) (float64, error) {
	if len(chunks) == 0 {
		return 0, fmt.Errorf("no data chunks")
	}

	results := make([]partialResult, len(chunks))
	var wg sync.WaitGroup

	sem := make(chan struct{}, pe.workers)
	for idx, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, data []float64) {
			defer func() { <-sem; wg.Done() }()
			r := &results[i]
			r.count = len(data)
			r.sum = pe.aggregator.Sum(data)
			r.min = pe.aggregator.Min4Way(data)
			r.max = pe.aggregator.Max4Way(data)
		}(idx, chunk)
	}
	wg.Wait()

	return mergePartialResults(results, op)
}

func mergePartialResults(results []partialResult, op VectorAggOp) (float64, error) {
	var totalSum float64
	totalMin := math.Inf(1)
	totalMax := math.Inf(-1)
	totalCount := 0

	for _, r := range results {
		if r.count == 0 {
			continue
		}
		totalSum += r.sum
		totalCount += r.count
		if r.min < totalMin {
			totalMin = r.min
		}
		if r.max > totalMax {
			totalMax = r.max
		}
	}

	if totalCount == 0 {
		return 0, fmt.Errorf("no data points")
	}

	switch op {
	case VectorSum:
		return totalSum, nil
	case VectorMin:
		return totalMin, nil
	case VectorMax:
		return totalMax, nil
	case VectorAvg:
		return totalSum / float64(totalCount), nil
	case VectorCount:
		return float64(totalCount), nil
	default:
		return 0, fmt.Errorf("unsupported op: %v", op)
	}
}

// --- Adaptive Execution ---

// AdaptiveExecutor selects the optimal execution path based on data size.
type AdaptiveExecutor struct {
	mu         sync.RWMutex
	planner    *ZeroCopyQueryPlanner
	parallel   *ParallelVectorizedExecutor
	stats      AdaptiveExecutorStats

	VectorizeThreshold int // minimum points to use vectorized path
	ParallelThreshold  int // minimum points to use parallel path
	ChunkSize          int // size of chunks for parallel execution
}

// AdaptiveExecutorStats tracks execution path usage.
type AdaptiveExecutorStats struct {
	RowOrientedCount   int64 `json:"row_oriented_count"`
	VectorizedCount    int64 `json:"vectorized_count"`
	ParallelCount      int64 `json:"parallel_count"`
	TotalExecutions    int64 `json:"total_executions"`
	AvgPointsProcessed int64 `json:"avg_points_processed"`
}

// NewAdaptiveExecutor creates an adaptive executor with sensible defaults.
func NewAdaptiveExecutor(planner *ZeroCopyQueryPlanner) *AdaptiveExecutor {
	return &AdaptiveExecutor{
		planner:            planner,
		parallel:           NewParallelVectorizedExecutor(4),
		VectorizeThreshold: 1000,
		ParallelThreshold:  100000,
		ChunkSize:          10000,
	}
}

// SelectPath determines the optimal execution path based on estimated point count.
func (ae *AdaptiveExecutor) SelectPath(estimatedPoints int) ExecutionPath {
	if estimatedPoints >= ae.ParallelThreshold {
		return PathParallelScan
	}
	if estimatedPoints >= ae.VectorizeThreshold {
		return PathVectorized
	}
	return PathRowOriented
}

// Execute runs a query using the optimal execution path.
func (ae *AdaptiveExecutor) Execute(metric string, from, to int64, op VectorAggOp) (float64, ExecutionPath, error) {
	plan := ae.planner.Plan(metric, from, to, op)
	path := ae.SelectPath(plan.EstimatedPoints)

	ae.mu.Lock()
	ae.stats.TotalExecutions++
	if ae.stats.TotalExecutions > 1 {
		ae.stats.AvgPointsProcessed = (ae.stats.AvgPointsProcessed*
			(ae.stats.TotalExecutions-1) + int64(plan.EstimatedPoints)) / ae.stats.TotalExecutions
	} else {
		ae.stats.AvgPointsProcessed = int64(plan.EstimatedPoints)
	}
	ae.mu.Unlock()

	switch path {
	case PathParallelScan:
		result, err := ae.executeParallel(metric, from, to, op)
		ae.mu.Lock()
		ae.stats.ParallelCount++
		ae.mu.Unlock()
		return result, path, err

	case PathVectorized:
		result, err := ae.planner.Execute(metric, from, to, op)
		ae.mu.Lock()
		ae.stats.VectorizedCount++
		ae.mu.Unlock()
		return result, path, err

	default:
		result, err := ae.planner.Execute(metric, from, to, op)
		ae.mu.Lock()
		ae.stats.RowOrientedCount++
		ae.mu.Unlock()
		return result, path, err
	}
}

func (ae *AdaptiveExecutor) executeParallel(metric string, from, to int64, op VectorAggOp) (float64, error) {
	ae.planner.mu.RLock()
	defer ae.planner.mu.RUnlock()

	var allValues []float64
	for _, part := range ae.planner.partitions {
		if part.metric == metric {
			values := part.ValuesSlice(from, to)
			allValues = append(allValues, values...)
		}
	}

	if len(allValues) == 0 {
		return 0, fmt.Errorf("no data for metric %q", metric)
	}

	var chunks [][]float64
	for i := 0; i < len(allValues); i += ae.ChunkSize {
		end := i + ae.ChunkSize
		if end > len(allValues) {
			end = len(allValues)
		}
		chunks = append(chunks, allValues[i:end])
	}

	return ae.parallel.ExecuteParallel(chunks, op)
}

// Stats returns executor statistics.
func (ae *AdaptiveExecutor) Stats() AdaptiveExecutorStats {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return ae.stats
}
