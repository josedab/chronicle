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
