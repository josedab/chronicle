package chronicle

import (
	"math"
	"testing"
)

func TestEvalHoltWinters(t *testing.T) {
	samples := []PromQLSample{
		{Timestamp: 1000, Value: 10},
		{Timestamp: 2000, Value: 12},
		{Timestamp: 3000, Value: 14},
		{Timestamp: 4000, Value: 16},
		{Timestamp: 5000, Value: 18},
	}

	result := EvalHoltWinters(samples, 0.5, 0.5)
	if math.IsNaN(result) {
		t.Fatal("expected non-NaN result")
	}
	// With linear trend, prediction should be around 20
	if result < 18 || result > 22 {
		t.Fatalf("expected ~20, got %f", result)
	}
}

func TestEvalHoltWinters_InvalidParams(t *testing.T) {
	samples := []PromQLSample{
		{Timestamp: 1000, Value: 10},
		{Timestamp: 2000, Value: 20},
	}
	if !math.IsNaN(EvalHoltWinters(samples, 0, 0.5)) {
		t.Fatal("expected NaN for sf=0")
	}
	if !math.IsNaN(EvalHoltWinters(samples, 0.5, 1.0)) {
		t.Fatal("expected NaN for tf=1")
	}
}

func TestEvalHoltWinters_TooFewSamples(t *testing.T) {
	samples := []PromQLSample{{Timestamp: 1000, Value: 10}}
	if !math.IsNaN(EvalHoltWinters(samples, 0.5, 0.5)) {
		t.Fatal("expected NaN for single sample")
	}
}

func TestNativeHistogramSumCountAvg(t *testing.T) {
	h := &NativeHistogram{Count: 100, Sum: 500}

	if NativeHistogramSum(h) != 500 {
		t.Fatalf("expected sum 500, got %f", NativeHistogramSum(h))
	}
	if NativeHistogramCount(h) != 100 {
		t.Fatalf("expected count 100, got %f", NativeHistogramCount(h))
	}
	if NativeHistogramAvg(h) != 5.0 {
		t.Fatalf("expected avg 5.0, got %f", NativeHistogramAvg(h))
	}
}

func TestNativeHistogramNilCases(t *testing.T) {
	if !math.IsNaN(NativeHistogramSum(nil)) {
		t.Fatal("expected NaN")
	}
	if !math.IsNaN(NativeHistogramCount(nil)) {
		t.Fatal("expected NaN")
	}
	if !math.IsNaN(NativeHistogramAvg(nil)) {
		t.Fatal("expected NaN")
	}
}

func TestEvalSgn(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{5.0, 1.0},
		{-3.0, -1.0},
		{0.0, 0.0},
	}
	for _, tt := range tests {
		got := EvalSgn(tt.input)
		if got != tt.expected {
			t.Errorf("EvalSgn(%f) = %f, want %f", tt.input, got, tt.expected)
		}
	}
	if !math.IsNaN(EvalSgn(math.NaN())) {
		t.Error("expected NaN for NaN input")
	}
}

func TestEvalClamp(t *testing.T) {
	tests := []struct {
		v, min, max, expected float64
	}{
		{5.0, 0.0, 10.0, 5.0},
		{-1.0, 0.0, 10.0, 0.0},
		{15.0, 0.0, 10.0, 10.0},
		{5.0, 5.0, 5.0, 5.0},
	}
	for _, tt := range tests {
		got := EvalClamp(tt.v, tt.min, tt.max)
		if got != tt.expected {
			t.Errorf("EvalClamp(%f, %f, %f) = %f, want %f", tt.v, tt.min, tt.max, got, tt.expected)
		}
	}
	// min > max returns NaN
	if !math.IsNaN(EvalClamp(5.0, 10.0, 0.0)) {
		t.Error("expected NaN when min > max")
	}
	// NaN input returns NaN
	if !math.IsNaN(EvalClamp(math.NaN(), 0.0, 10.0)) {
		t.Error("expected NaN for NaN input")
	}
}

func TestEvalClampMinMax(t *testing.T) {
	if EvalClampMin(-5.0, 0.0) != 0.0 {
		t.Error("EvalClampMin failed")
	}
	if EvalClampMin(5.0, 0.0) != 5.0 {
		t.Error("EvalClampMin should not change value above min")
	}
	if EvalClampMax(15.0, 10.0) != 10.0 {
		t.Error("EvalClampMax failed")
	}
	if EvalClampMax(5.0, 10.0) != 5.0 {
		t.Error("EvalClampMax should not change value below max")
	}
}

func TestEvalAbsentOverTime(t *testing.T) {
	// Empty samples -> returns 1
	result := EvalAbsentOverTime(nil)
	if result != 1 {
		t.Fatalf("expected 1 for nil samples, got %f", result)
	}

	// All NaN samples -> returns 1
	result = EvalAbsentOverTime([]PromQLSample{
		{Timestamp: 1000, Value: math.NaN()},
	})
	if result != 1 {
		t.Fatalf("expected 1 for NaN samples, got %f", result)
	}

	// Valid samples -> returns NaN (series present)
	result = EvalAbsentOverTime([]PromQLSample{
		{Timestamp: 1000, Value: 5.0},
	})
	if !math.IsNaN(result) {
		t.Fatalf("expected NaN for present samples, got %f", result)
	}
}

func TestEvalSubqueryAligned(t *testing.T) {
	fn := func(ts int64) float64 { return float64(ts) }

	// Without alignment
	samples := EvalSubqueryAligned(SubqueryConfig{
		InnerFn: fn, EndMs: 10000, RangeMs: 5000, StepMs: 1000,
	})
	if len(samples) != 6 {
		t.Fatalf("expected 6 samples, got %d", len(samples))
	}

	// With alignment
	samples = EvalSubqueryAligned(SubqueryConfig{
		InnerFn: fn, EndMs: 10500, RangeMs: 5000, StepMs: 1000, AlignToStep: true,
	})
	// Start = 10500-5000=5500, aligned to 5000
	if samples[0].Timestamp != 5000 {
		t.Fatalf("expected aligned start at 5000, got %d", samples[0].Timestamp)
	}
}

func TestEvalSgnSeries(t *testing.T) {
	samples := []PromQLSample{
		{Timestamp: 1000, Value: 5.0},
		{Timestamp: 2000, Value: -3.0},
		{Timestamp: 3000, Value: 0.0},
	}
	result := EvalSgnSeries(samples)
	if result[0].Value != 1.0 || result[1].Value != -1.0 || result[2].Value != 0.0 {
		t.Fatal("EvalSgnSeries produced incorrect results")
	}
}

func TestEvalClampSeries(t *testing.T) {
	samples := []PromQLSample{
		{Timestamp: 1000, Value: -5.0},
		{Timestamp: 2000, Value: 5.0},
		{Timestamp: 3000, Value: 15.0},
	}
	result := EvalClampSeries(samples, 0.0, 10.0)
	if result[0].Value != 0.0 || result[1].Value != 5.0 || result[2].Value != 10.0 {
		t.Fatal("EvalClampSeries produced incorrect results")
	}
}

func TestNativeHistogramBucketBounds(t *testing.T) {
	h := &NativeHistogram{
		Count:         100,
		Sum:           500,
		ZeroCount:     10,
		ZeroThreshold: 0.001,
	}
	buckets := NativeHistogramBucketBounds(h)
	if len(buckets) == 0 {
		t.Fatal("expected non-empty bucket bounds")
	}
	// Last bucket should be +Inf with total count
	last := buckets[len(buckets)-1]
	if !math.IsInf(last.UpperBound, 1) {
		t.Fatal("last bucket should be +Inf")
	}
	if last.Count != 100 {
		t.Fatalf("expected total count 100, got %d", last.Count)
	}
}
