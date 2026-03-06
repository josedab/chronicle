package chronicle

import (
	"math"
	"sort"
	"time"
)

// PromQLHistogramBucket represents a histogram bucket for quantile calculation.
type PromQLHistogramBucket struct {
	UpperBound float64 `json:"upper_bound"`
	Count      int64   `json:"count"`
}

// --- Range Function Engine ---

// StalenessWindow is the default Prometheus staleness period (5 minutes).
const StalenessWindow = 5 * time.Minute

// PromQLSample represents a timestamped sample value.
type PromQLSample struct {
	Timestamp int64   // unix milliseconds
	Value     float64
}

// PromQLRangeFunc identifies which range-vector function to apply.
type PromQLRangeFunc int

const (
	RangeFuncRate PromQLRangeFunc = iota
	RangeFuncIncrease
	RangeFuncIrate
	RangeFuncDelta
	RangeFuncIdelta
	RangeFuncDeriv
	RangeFuncChanges
	RangeFuncResets
	RangeFuncAvgOverTime
	RangeFuncMinOverTime
	RangeFuncMaxOverTime
	RangeFuncSumOverTime
	RangeFuncCountOverTime
	RangeFuncStddevOverTime
	RangeFuncStdvarOverTime
	RangeFuncQuantileOverTime
	RangeFuncPredictLinear
	RangeFuncLastOverTime
)

// EvalRangeFunction evaluates a range function over a window of samples.
// Handles counter resets for rate/increase, NaN propagation, and staleness.
func EvalRangeFunction(fn PromQLRangeFunc, samples []PromQLSample, rangeMs int64) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}

	// Filter stale samples
	samples = filterStaleSamples(samples)
	if len(samples) == 0 {
		return math.NaN()
	}

	// NaN propagation: if any sample is NaN, certain functions propagate it
	for _, s := range samples {
		if math.IsNaN(s.Value) {
			switch fn {
			case RangeFuncRate, RangeFuncIncrease, RangeFuncIrate, RangeFuncDelta, RangeFuncIdelta, RangeFuncDeriv:
				return math.NaN()
			case RangeFuncChanges, RangeFuncResets:
				// changes/resets can skip NaN
			default:
				return math.NaN()
			}
		}
	}

	switch fn {
	case RangeFuncRate:
		return evalRate(samples, rangeMs, true)
	case RangeFuncIncrease:
		return evalRate(samples, rangeMs, false)
	case RangeFuncIrate:
		return evalIrate(samples, true)
	case RangeFuncDelta:
		return evalDelta(samples)
	case RangeFuncIdelta:
		return evalIrate(samples, false)
	case RangeFuncDeriv:
		return evalDeriv(samples)
	case RangeFuncChanges:
		return evalChanges(samples)
	case RangeFuncResets:
		return evalResets(samples)
	case RangeFuncAvgOverTime:
		return evalAvgOverTime(samples)
	case RangeFuncMinOverTime:
		return evalMinOverTime(samples)
	case RangeFuncMaxOverTime:
		return evalMaxOverTime(samples)
	case RangeFuncSumOverTime:
		return evalSumOverTime(samples)
	case RangeFuncCountOverTime:
		return float64(len(samples))
	case RangeFuncStddevOverTime:
		return evalStddevOverTime(samples)
	case RangeFuncStdvarOverTime:
		return evalStdvarOverTime(samples)
	case RangeFuncQuantileOverTime:
		return evalQuantileOverTime(samples, 0.5) // default to median
	case RangeFuncPredictLinear:
		return evalPredictLinear(samples, rangeMs)
	case RangeFuncLastOverTime:
		return evalLastOverTime(samples)
	case RangeFuncHoltWinters:
		return EvalHoltWinters(samples, 0.5, 0.5)
	case RangeFuncAbsentOverTime:
		return EvalAbsentOverTime(samples)
	case RangeFuncPresentOverTime:
		return EvalPresentOverTime(samples)
	default:
		return math.NaN()
	}
}

// EvalRangeFunctionWithParam evaluates a parameterized range function (e.g., quantile_over_time).
func EvalRangeFunctionWithParam(fn PromQLRangeFunc, samples []PromQLSample, rangeMs int64, param float64) float64 {
	if fn == RangeFuncQuantileOverTime {
		samples = filterStaleSamples(samples)
		if len(samples) == 0 {
			return math.NaN()
		}
		return evalQuantileOverTime(samples, param)
	}
	return EvalRangeFunction(fn, samples, rangeMs)
}

// evalRate computes rate (per-second) or increase (total) over the sample window.
// Handles counter resets by detecting decreases and compensating.
// Uses Prometheus-compatible extrapolation accounting for sample boundaries.
func evalRate(samples []PromQLSample, rangeMs int64, perSecond bool) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}

	var counterResetCompensation float64
	prev := samples[0].Value
	for _, s := range samples[1:] {
		if s.Value < prev {
			// Counter reset detected: add the previous value as compensation
			counterResetCompensation += prev
		}
		prev = s.Value
	}

	totalIncrease := (samples[len(samples)-1].Value - samples[0].Value) + counterResetCompensation

	if totalIncrease < 0 {
		totalIncrease = 0
	}

	// Prometheus-compatible extrapolation: account for partial coverage
	// of the requested range window relative to the actual sample span.
	sampleSpanMs := float64(samples[len(samples)-1].Timestamp - samples[0].Timestamp)
	if sampleSpanMs == 0 {
		return math.NaN()
	}

	// Estimate the average interval between samples
	avgIntervalMs := sampleSpanMs / float64(len(samples)-1)

	// Extend the sample span by half an interval on each side to estimate
	// where the "true" first and last samples would be
	extrapolateToMs := sampleSpanMs + avgIntervalMs

	// Cap extrapolation: don't extrapolate beyond 110% of the range
	requestedMs := float64(rangeMs)
	if extrapolateToMs > requestedMs*1.1 {
		extrapolateToMs = requestedMs * 1.1
	}

	extrapolationFactor := extrapolateToMs / sampleSpanMs
	totalIncrease *= extrapolationFactor

	if perSecond {
		rangeSec := extrapolateToMs / 1000.0
		if rangeSec == 0 {
			return math.NaN()
		}
		return totalIncrease / rangeSec
	}
	return totalIncrease
}

// evalIrate computes instantaneous rate from the last two samples.
func evalIrate(samples []PromQLSample, perSecond bool) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	last := samples[len(samples)-1]
	prev := samples[len(samples)-2]

	diff := last.Value - prev.Value
	if diff < 0 {
		diff = last.Value // counter reset
	}

	if perSecond {
		dtSec := float64(last.Timestamp-prev.Timestamp) / 1000.0
		if dtSec == 0 {
			return math.NaN()
		}
		return diff / dtSec
	}
	return diff
}

// evalDelta computes the difference between last and first sample.
func evalDelta(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	return samples[len(samples)-1].Value - samples[0].Value
}

// evalDeriv uses least-squares linear regression to compute derivative.
func evalDeriv(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	n := float64(len(samples))
	var sumX, sumY, sumXY, sumX2 float64
	for _, s := range samples {
		x := float64(s.Timestamp) / 1000.0
		sumX += x
		sumY += s.Value
		sumXY += x * s.Value
		sumX2 += x * x
	}
	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return math.NaN()
	}
	return (n*sumXY - sumX*sumY) / denom
}

func evalChanges(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	changes := 0.0
	for i := 1; i < len(samples); i++ {
		if !math.IsNaN(samples[i].Value) && !math.IsNaN(samples[i-1].Value) && samples[i].Value != samples[i-1].Value {
			changes++
		}
	}
	return changes
}

func evalResets(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	resets := 0.0
	for i := 1; i < len(samples); i++ {
		if !math.IsNaN(samples[i].Value) && !math.IsNaN(samples[i-1].Value) && samples[i].Value < samples[i-1].Value {
			resets++
		}
	}
	return resets
}

func evalAvgOverTime(samples []PromQLSample) float64 {
	sum := 0.0
	count := 0
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			sum += s.Value
			count++
		}
	}
	if count == 0 {
		return math.NaN()
	}
	return sum / float64(count)
}

func evalMinOverTime(samples []PromQLSample) float64 {
	min := math.Inf(1)
	for _, s := range samples {
		if !math.IsNaN(s.Value) && s.Value < min {
			min = s.Value
		}
	}
	if math.IsInf(min, 1) {
		return math.NaN()
	}
	return min
}

func evalMaxOverTime(samples []PromQLSample) float64 {
	max := math.Inf(-1)
	for _, s := range samples {
		if !math.IsNaN(s.Value) && s.Value > max {
			max = s.Value
		}
	}
	if math.IsInf(max, -1) {
		return math.NaN()
	}
	return max
}

func evalSumOverTime(samples []PromQLSample) float64 {
	sum := 0.0
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			sum += s.Value
		}
	}
	return sum
}

func evalStddevOverTime(samples []PromQLSample) float64 {
	v := evalStdvarOverTime(samples)
	if math.IsNaN(v) {
		return math.NaN()
	}
	return math.Sqrt(v)
}

func evalStdvarOverTime(samples []PromQLSample) float64 {
	var sum, sumSq float64
	count := 0
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			sum += s.Value
			sumSq += s.Value * s.Value
			count++
		}
	}
	if count < 2 {
		return math.NaN()
	}
	mean := sum / float64(count)
	return sumSq/float64(count) - mean*mean
}

func evalQuantileOverTime(samples []PromQLSample, q float64) float64 {
	if q < 0 || q > 1 {
		return math.NaN()
	}
	vals := make([]float64, 0, len(samples))
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			vals = append(vals, s.Value)
		}
	}
	if len(vals) == 0 {
		return math.NaN()
	}
	sort.Float64s(vals)
	if q == 0 {
		return vals[0]
	}
	if q == 1 {
		return vals[len(vals)-1]
	}
	idx := q * float64(len(vals)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(vals) {
		return vals[lower]
	}
	frac := idx - float64(lower)
	return vals[lower]*(1-frac) + vals[upper]*frac
}

// EvalSubquery evaluates a subquery by computing the inner expression at each step within the range.
// Returns a slice of samples representing evaluation results at each step.
func EvalSubquery(innerFn func(ts int64) float64, endMs int64, rangeMs int64, stepMs int64) []PromQLSample {
	if stepMs <= 0 || rangeMs <= 0 {
		return nil
	}
	startMs := endMs - rangeMs
	var results []PromQLSample
	for ts := startMs; ts <= endMs; ts += stepMs {
		val := innerFn(ts)
		results = append(results, PromQLSample{Timestamp: ts, Value: val})
	}
	return results
}

// evalPredictLinear uses linear regression to predict the value at rangeMs into the future.
func evalPredictLinear(samples []PromQLSample, rangeMs int64) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	slope := evalDeriv(samples)
	if math.IsNaN(slope) {
		return math.NaN()
	}
	lastVal := samples[len(samples)-1].Value
	predSec := float64(rangeMs) / 1000.0
	return lastVal + slope*predSec
}

// evalLastOverTime returns the most recent sample value.
func evalLastOverTime(samples []PromQLSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	return samples[len(samples)-1].Value
}

// filterStaleSamples removes samples that are older than StalenessWindow
// relative to the newest sample in the window.
func filterStaleSamples(samples []PromQLSample) []PromQLSample {
	if len(samples) == 0 {
		return nil
	}
	newest := samples[len(samples)-1].Timestamp
	cutoff := newest - StalenessWindow.Milliseconds()
	start := 0
	for i, s := range samples {
		if s.Timestamp >= cutoff {
			start = i
			break
		}
	}
	return samples[start:]
}
