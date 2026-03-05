package chronicle

import (
	"math"
	"sort"
	"sync"
)

// ColumnBatchStats holds per-batch statistics for cost-based decisions.
type ColumnBatchStats struct {
	MinValue   float64 `json:"min_value"`
	MaxValue   float64 `json:"max_value"`
	NullCount  int     `json:"null_count"`
	TotalCount int     `json:"total_count"`
	MinTime    int64   `json:"min_time"`
	MaxTime    int64   `json:"max_time"`
}

// ComputeStats calculates batch-level statistics (zone map) for pruning.
func (cb *ColumnBatch) ComputeStats() ColumnBatchStats {
	if cb.Size == 0 {
		return ColumnBatchStats{}
	}
	s := ColumnBatchStats{
		MinValue:   math.Inf(1),
		MaxValue:   math.Inf(-1),
		TotalCount: cb.Size,
		MinTime:    cb.Timestamps[0],
		MaxTime:    cb.Timestamps[0],
	}
	for i := 0; i < cb.Size; i++ {
		if i < len(cb.Nulls) && cb.Nulls[i] {
			s.NullCount++
			continue
		}
		if cb.Values[i] < s.MinValue {
			s.MinValue = cb.Values[i]
		}
		if cb.Values[i] > s.MaxValue {
			s.MaxValue = cb.Values[i]
		}
		if cb.Timestamps[i] < s.MinTime {
			s.MinTime = cb.Timestamps[i]
		}
		if cb.Timestamps[i] > s.MaxTime {
			s.MaxTime = cb.Timestamps[i]
		}
	}
	return s
}

// CanPruneByValue returns true if the batch can be skipped for a value range filter.
func (s *ColumnBatchStats) CanPruneByValue(minVal, maxVal float64) bool {
	return s.MaxValue < minVal || s.MinValue > maxVal
}

// CanPruneByTime returns true if the batch can be skipped for a time range filter.
func (s *ColumnBatchStats) CanPruneByTime(start, end int64) bool {
	return s.MaxTime < start || s.MinTime > end
}

// ColumnBatchProjection extracts specific columns from points into a batch,
// enabling column projection pushdown to avoid materializing unneeded data.
type ColumnBatchProjection struct {
	Metric     string
	Timestamps []int64
	Values     []float64
	TagValues  map[string][]string // tag key -> values per row
	Size       int
}

// ProjectFromPoints creates a projection from points with only requested tag keys.
func ProjectFromPoints(points []Point, tagKeys []string) *ColumnBatchProjection {
	n := len(points)
	proj := &ColumnBatchProjection{
		Timestamps: make([]int64, n),
		Values:     make([]float64, n),
		TagValues:  make(map[string][]string, len(tagKeys)),
		Size:       n,
	}
	for _, k := range tagKeys {
		proj.TagValues[k] = make([]string, n)
	}
	for i, p := range points {
		if i == 0 {
			proj.Metric = p.Metric
		}
		proj.Timestamps[i] = p.Timestamp
		proj.Values[i] = p.Value
		for _, k := range tagKeys {
			if v, ok := p.Tags[k]; ok {
				proj.TagValues[k][i] = v
			}
		}
	}
	return proj
}

// ToColumnBatch converts a projection to a ColumnBatch (timestamps + values only).
func (p *ColumnBatchProjection) ToColumnBatch() *ColumnBatch {
	return NewColumnBatchFromSlices(p.Timestamps, p.Values)
}

// ColumnarVectorMedian computes the median value using partial sort.
func ColumnarVectorMedian(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

// ColumnarVectorPercentile computes the p-th percentile (0-100) of values.
func ColumnarVectorPercentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	if p < 0 {
		p = 0
	}
	if p > 100 {
		p = 100
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	rank := (p / 100) * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := rank - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// MultiColumnBatch holds multiple named columns for multi-column vectorized ops.
type MultiColumnBatch struct {
	Columns    map[string][]float64
	Timestamps []int64
	Size       int
}

// NewMultiColumnBatch creates a batch with named float64 columns.
func NewMultiColumnBatch(timestamps []int64, columns map[string][]float64) *MultiColumnBatch {
	size := len(timestamps)
	cols := make(map[string][]float64, len(columns))
	for k, v := range columns {
		if len(v) < size {
			size = len(v)
		}
		cols[k] = v[:size]
	}
	return &MultiColumnBatch{
		Columns:    cols,
		Timestamps: timestamps[:size],
		Size:       size,
	}
}

// BatchPipeline chains multiple operations on a ColumnBatch.
type BatchPipeline struct {
	steps []func(*ColumnBatch) *ColumnBatch
}

// NewBatchPipeline creates an empty processing pipeline.
func NewBatchPipeline() *BatchPipeline {
	return &BatchPipeline{}
}

// AddFilter adds a value filter step to the pipeline.
func (bp *BatchPipeline) AddFilter(predicate func(float64) bool) *BatchPipeline {
	bp.steps = append(bp.steps, func(cb *ColumnBatch) *ColumnBatch {
		var ts []int64
		var vals []float64
		var nulls []bool
		for i := 0; i < cb.Size; i++ {
			if predicate(cb.Values[i]) {
				ts = append(ts, cb.Timestamps[i])
				vals = append(vals, cb.Values[i])
				if i < len(cb.Nulls) {
					nulls = append(nulls, cb.Nulls[i])
				}
			}
		}
		return &ColumnBatch{
			Timestamps: ts,
			Values:     vals,
			Nulls:      nulls,
			Size:       len(vals),
		}
	})
	return bp
}

// AddTimeRange adds a time range filter step.
func (bp *BatchPipeline) AddTimeRange(start, end int64) *BatchPipeline {
	bp.steps = append(bp.steps, func(cb *ColumnBatch) *ColumnBatch {
		return cb.FilterByTimeRange(start, end)
	})
	return bp
}

// Execute runs all pipeline steps on the input batch.
func (bp *BatchPipeline) Execute(cb *ColumnBatch) *ColumnBatch {
	result := cb
	for _, step := range bp.steps {
		result = step(result)
		if result.Size == 0 {
			return result
		}
	}
	return result
}

// ParallelBatchAggregator provides CBO-aware parallel aggregation.
type ParallelBatchAggregator struct {
	workers   int
	batchSize int
}

// NewParallelBatchAggregator creates a parallel aggregator with CBO integration.
func NewParallelBatchAggregator(workers, batchSize int) *ParallelBatchAggregator {
	if workers <= 0 {
		workers = 4
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &ParallelBatchAggregator{workers: workers, batchSize: batchSize}
}

// AggregateBatches splits points into batches and aggregates in parallel.
func (pa *ParallelBatchAggregator) AggregateBatches(points []Point, fn AggFunc) float64 {
	if len(points) == 0 {
		return math.NaN()
	}

	var batches []*ColumnBatch
	for i := 0; i < len(points); i += pa.batchSize {
		end := i + pa.batchSize
		if end > len(points) {
			end = len(points)
		}
		batches = append(batches, NewColumnBatch(points[i:end]))
	}

	return ParallelAggregate(batches, fn, pa.workers)
}

// AggregateWithStats aggregates and returns per-batch statistics for zone-map pruning.
func (pa *ParallelBatchAggregator) AggregateWithStats(points []Point, fn AggFunc) (float64, []ColumnBatchStats) {
	if len(points) == 0 {
		return math.NaN(), nil
	}

	var batches []*ColumnBatch
	for i := 0; i < len(points); i += pa.batchSize {
		end := i + pa.batchSize
		if end > len(points) {
			end = len(points)
		}
		batches = append(batches, NewColumnBatch(points[i:end]))
	}

	stats := make([]ColumnBatchStats, len(batches))
	var wg sync.WaitGroup
	for i, b := range batches {
		wg.Add(1)
		go func(idx int, batch *ColumnBatch) {
			defer wg.Done()
			stats[idx] = batch.ComputeStats()
		}(i, b)
	}
	wg.Wait()

	result := ParallelAggregate(batches, fn, pa.workers)
	return result, stats
}
