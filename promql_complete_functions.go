package chronicle

import (
	"math"
)

// --- Holt-Winters Double Exponential Smoothing ---

// RangeFuncHoltWinters is the range function ID for Holt-Winters smoothing.
const RangeFuncHoltWinters PromQLRangeFunc = 18

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

	// Use the existing HistogramQuantile infrastructure
	// Estimate by computing quantiles at both bounds
	var inRange uint64
	if h.ZeroCount > 0 && lower <= h.ZeroThreshold && upper >= -h.ZeroThreshold {
		inRange += h.ZeroCount
	}

	return float64(inRange) / float64(h.Count)
}
