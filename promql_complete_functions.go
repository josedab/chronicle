package chronicle

import (
	"math"
	"sort"
)

// --- Holt-Winters Double Exponential Smoothing ---

// RangeFuncHoltWinters is the range function ID for Holt-Winters smoothing.
const RangeFuncHoltWinters PromQLRangeFunc = 18

// Additional range function IDs for newly implemented functions.
const (
	RangeFuncAbsentOverTime PromQLRangeFunc = 19
	RangeFuncSgn            PromQLRangeFunc = 20
	RangeFuncClamp          PromQLRangeFunc = 21
	RangeFuncClampMin       PromQLRangeFunc = 22
	RangeFuncClampMax       PromQLRangeFunc = 23
	RangeFuncPresentOverTime PromQLRangeFunc = 24
)

// EvalHoltWinters applies double exponential smoothing to predict a future value.
// sf is the smoothing factor (0 < sf < 1), tf is the trend factor (0 < tf < 1).
func EvalHoltWinters(samples []PromQLSample, sf, tf float64) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	if sf <= 0 || sf >= 1 || tf <= 0 || tf >= 1 {
		return math.NaN()
	}

	// Initialize level and trend
	level := samples[0].Value
	trend := samples[1].Value - samples[0].Value

	for i := 1; i < len(samples); i++ {
		val := samples[i].Value
		if math.IsNaN(val) {
			continue
		}
		prevLevel := level
		level = sf*val + (1-sf)*(level+trend)
		trend = tf*(level-prevLevel) + (1-tf)*trend
	}

	// Predict one step ahead
	return level + trend
}

// --- PromQL Scalar Functions ---

// EvalSgn returns the sign of a value: 1 for positive, -1 for negative, 0 for zero.
func EvalSgn(v float64) float64 {
	if math.IsNaN(v) {
		return math.NaN()
	}
	if v > 0 {
		return 1
	}
	if v < 0 {
		return -1
	}
	return 0
}

// EvalClamp clamps a value between min and max bounds (PromQL clamp).
func EvalClamp(v, minVal, maxVal float64) float64 {
	if math.IsNaN(v) || math.IsNaN(minVal) || math.IsNaN(maxVal) {
		return math.NaN()
	}
	if minVal > maxVal {
		return math.NaN()
	}
	if v < minVal {
		return minVal
	}
	if v > maxVal {
		return maxVal
	}
	return v
}

// EvalClampMin clamps a value to a minimum bound (PromQL clamp_min).
func EvalClampMin(v, minVal float64) float64 {
	if math.IsNaN(v) || math.IsNaN(minVal) {
		return math.NaN()
	}
	if v < minVal {
		return minVal
	}
	return v
}

// EvalClampMax clamps a value to a maximum bound (PromQL clamp_max).
func EvalClampMax(v, maxVal float64) float64 {
	if math.IsNaN(v) || math.IsNaN(maxVal) {
		return math.NaN()
	}
	if v > maxVal {
		return maxVal
	}
	return v
}

// EvalAbsentOverTime returns 1 if no samples exist in the range, NaN otherwise.
func EvalAbsentOverTime(samples []PromQLSample) float64 {
	valid := 0
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			valid++
		}
	}
	if valid == 0 {
		return 1
	}
	return math.NaN()
}

// EvalPresentOverTime returns 1 if any sample exists in the range, NaN otherwise.
func EvalPresentOverTime(samples []PromQLSample) float64 {
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			return 1
		}
	}
	return math.NaN()
}

// --- Subquery Evaluation with Step Alignment ---

// SubqueryConfig defines parameters for subquery evaluation.
type SubqueryConfig struct {
	InnerFn func(ts int64) float64
	EndMs   int64
	RangeMs int64
	StepMs  int64
	// AlignToStep aligns evaluation timestamps to step boundaries.
	AlignToStep bool
}

// EvalSubqueryAligned evaluates a subquery with optional step alignment.
// Step alignment ensures evaluation timestamps fall on multiples of stepMs,
// matching Prometheus subquery semantics.
func EvalSubqueryAligned(cfg SubqueryConfig) []PromQLSample {
	if cfg.StepMs <= 0 || cfg.RangeMs <= 0 {
		return nil
	}

	startMs := cfg.EndMs - cfg.RangeMs

	if cfg.AlignToStep {
		startMs = (startMs / cfg.StepMs) * cfg.StepMs
	}

	var results []PromQLSample
	for ts := startMs; ts <= cfg.EndMs; ts += cfg.StepMs {
		val := cfg.InnerFn(ts)
		results = append(results, PromQLSample{Timestamp: ts, Value: val})
	}
	return results
}

// --- Native Histogram PromQL Functions ---
// These complement the existing NativeHistogram type in native_histogram.go.

// NativeHistogramSum returns the sum of observations (PromQL histogram_sum).
func NativeHistogramSum(h *NativeHistogram) float64 {
	if h == nil {
		return math.NaN()
	}
	return h.Sum
}

// NativeHistogramCount returns the count of observations (PromQL histogram_count).
func NativeHistogramCount(h *NativeHistogram) float64 {
	if h == nil {
		return math.NaN()
	}
	return float64(h.Count)
}

// NativeHistogramAvg returns the average observation value (PromQL histogram_avg).
func NativeHistogramAvg(h *NativeHistogram) float64 {
	if h == nil || h.Count == 0 {
		return math.NaN()
	}
	return h.Sum / float64(h.Count)
}

// NativeHistogramFraction returns the fraction of observations between lower and upper.
func NativeHistogramFraction(lower, upper float64, h *NativeHistogram) float64 {
	if h == nil || h.Count == 0 {
		return math.NaN()
	}
	if math.IsNaN(lower) || math.IsNaN(upper) {
		return math.NaN()
	}

	var inRange uint64

	// Count zero bucket if it falls in range
	if h.ZeroCount > 0 && lower <= h.ZeroThreshold && upper >= -h.ZeroThreshold {
		inRange += h.ZeroCount
	}

	// Count positive spans/buckets that fall within [lower, upper]
	if len(h.PositiveBuckets) > 0 {
		offset := 0
		for _, span := range h.PositiveSpans {
			for j := 0; j < int(span.Length) && offset+j < len(h.PositiveBuckets); j++ {
				idx := int(span.Offset) + j
				bucketLower := math.Ldexp(1, idx)
				bucketUpper := math.Ldexp(1, idx+1)
				if bucketLower >= lower && bucketUpper <= upper {
					if offset+j < len(h.PositiveBuckets) {
						inRange += uint64(h.PositiveBuckets[offset+j])
					}
				}
			}
			offset += int(span.Length)
		}
	}

	// Count negative spans/buckets that fall within [lower, upper]
	if len(h.NegativeBuckets) > 0 {
		offset := 0
		for _, span := range h.NegativeSpans {
			for j := 0; j < int(span.Length) && offset+j < len(h.NegativeBuckets); j++ {
				idx := int(span.Offset) + j
				bucketUpper := -math.Ldexp(1, idx)
				bucketLower := -math.Ldexp(1, idx+1)
				if bucketLower >= lower && bucketUpper <= upper {
					if offset+j < len(h.NegativeBuckets) {
						inRange += uint64(h.NegativeBuckets[offset+j])
					}
				}
			}
			offset += int(span.Length)
		}
	}

	return float64(inRange) / float64(h.Count)
}

// NativeHistogramBucketBounds returns the sorted upper bounds and counts of a native histogram.
func NativeHistogramBucketBounds(h *NativeHistogram) []PromQLHistogramBucket {
	if h == nil {
		return nil
	}

	var buckets []PromQLHistogramBucket

	// Add negative buckets
	if len(h.NegativeBuckets) > 0 {
		offset := 0
		for _, span := range h.NegativeSpans {
			for j := 0; j < int(span.Length) && offset+j < len(h.NegativeBuckets); j++ {
				idx := int(span.Offset) + j
				bound := -math.Ldexp(1, idx)
				buckets = append(buckets, PromQLHistogramBucket{
					UpperBound: bound,
					Count:      int64(h.NegativeBuckets[offset+j]),
				})
			}
			offset += int(span.Length)
		}
	}

	// Add zero bucket
	if h.ZeroCount > 0 {
		buckets = append(buckets, PromQLHistogramBucket{
			UpperBound: h.ZeroThreshold,
			Count:      int64(h.ZeroCount),
		})
	}

	// Add positive buckets
	if len(h.PositiveBuckets) > 0 {
		offset := 0
		for _, span := range h.PositiveSpans {
			for j := 0; j < int(span.Length) && offset+j < len(h.PositiveBuckets); j++ {
				idx := int(span.Offset) + j
				bound := math.Ldexp(1, idx+1)
				buckets = append(buckets, PromQLHistogramBucket{
					UpperBound: bound,
					Count:      int64(h.PositiveBuckets[offset+j]),
				})
			}
			offset += int(span.Length)
		}
	}

	// Sort by upper bound
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].UpperBound < buckets[j].UpperBound
	})

	// Make counts cumulative
	for i := 1; i < len(buckets); i++ {
		buckets[i].Count += buckets[i-1].Count
	}

	// Add +Inf bucket
	buckets = append(buckets, PromQLHistogramBucket{
		UpperBound: math.Inf(1),
		Count:      int64(h.Count),
	})

	return buckets
}

// --- PromQL Batch Math Functions ---

// EvalSgnSeries applies sgn() to a series of samples.
func EvalSgnSeries(samples []PromQLSample) []PromQLSample {
	result := make([]PromQLSample, len(samples))
	for i, s := range samples {
		result[i] = PromQLSample{Timestamp: s.Timestamp, Value: EvalSgn(s.Value)}
	}
	return result
}

// EvalClampSeries applies clamp() to a series of samples.
func EvalClampSeries(samples []PromQLSample, minVal, maxVal float64) []PromQLSample {
	result := make([]PromQLSample, len(samples))
	for i, s := range samples {
		result[i] = PromQLSample{Timestamp: s.Timestamp, Value: EvalClamp(s.Value, minVal, maxVal)}
	}
	return result
}

// EvalClampMinSeries applies clamp_min() to a series of samples.
func EvalClampMinSeries(samples []PromQLSample, minVal float64) []PromQLSample {
	result := make([]PromQLSample, len(samples))
	for i, s := range samples {
		result[i] = PromQLSample{Timestamp: s.Timestamp, Value: EvalClampMin(s.Value, minVal)}
	}
	return result
}

// EvalClampMaxSeries applies clamp_max() to a series of samples.
func EvalClampMaxSeries(samples []PromQLSample, maxVal float64) []PromQLSample {
	result := make([]PromQLSample, len(samples))
	for i, s := range samples {
		result[i] = PromQLSample{Timestamp: s.Timestamp, Value: EvalClampMax(s.Value, maxVal)}
	}
	return result
}
