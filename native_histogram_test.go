package chronicle

import (
	"math"
	"testing"
)

func TestNativeHistogramStore_Write(t *testing.T) {
	store := NewNativeHistogramStore(DefaultNativeHistogramConfig())

	h := NativeHistogram{
		Metric: "http_request_duration",
		Tags:   map[string]string{"service": "api"},
		Count:  100,
		Sum:    250.5,
		Schema: 3,
		ZeroThreshold: 1e-128,
		PositiveSpans:   []BucketSpan{{Offset: 0, Length: 3}},
		PositiveBuckets: []int64{10, 5, 3},
	}

	if err := store.Write(h); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	results := store.Query("http_request_duration", map[string]string{"service": "api"}, 0, 0)
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if results[0].Count != 100 {
		t.Errorf("expected count 100, got %d", results[0].Count)
	}
}

func TestNativeHistogramStore_Validation(t *testing.T) {
	store := NewNativeHistogramStore(DefaultNativeHistogramConfig())

	err := store.Write(NativeHistogram{})
	if err == nil {
		t.Error("expected error for empty metric")
	}
}

func TestHistogramQuantile(t *testing.T) {
	h := &NativeHistogram{
		Count:         100,
		Sum:           500.0,
		Schema:        3,
		ZeroThreshold: 1e-128,
		ZeroCount:     5,
		PositiveSpans:   []BucketSpan{{Offset: 0, Length: 5}},
		PositiveBuckets: []int64{10, 10, 20, 15, 5}, // Delta encoded: 10, 20, 40, 55, 60
	}

	// Median should be a finite number
	p50 := HistogramQuantile(0.5, h)
	if math.IsNaN(p50) {
		t.Error("p50 should not be NaN")
	}

	// p0 should return minimum
	p0 := HistogramQuantile(0.0, h)
	if math.IsNaN(p0) {
		t.Error("p0 should not be NaN")
	}

	// p100 should return maximum
	p100 := HistogramQuantile(1.0, h)
	if math.IsNaN(p100) {
		t.Error("p100 should not be NaN")
	}

	// Invalid quantile
	invalid := HistogramQuantile(1.5, h)
	if !math.IsNaN(invalid) {
		t.Error("quantile > 1 should be NaN")
	}
}

func TestHistogramQuantile_Empty(t *testing.T) {
	h := &NativeHistogram{Count: 0}
	result := HistogramQuantile(0.5, h)
	if !math.IsNaN(result) {
		t.Error("empty histogram quantile should be NaN")
	}
}

func TestMergeHistograms(t *testing.T) {
	a := &NativeHistogram{
		Metric:          "test",
		Count:           50,
		Sum:             100.0,
		Schema:          3,
		ZeroCount:       5,
		ZeroThreshold:   1e-128,
		PositiveSpans:   []BucketSpan{{Offset: 0, Length: 3}},
		PositiveBuckets: []int64{10, 5, 3},
	}

	b := &NativeHistogram{
		Metric:          "test",
		Count:           50,
		Sum:             150.0,
		Schema:          3,
		ZeroCount:       3,
		ZeroThreshold:   1e-128,
		PositiveSpans:   []BucketSpan{{Offset: 0, Length: 3}},
		PositiveBuckets: []int64{5, 10, 2},
	}

	merged := MergeHistograms(a, b)

	if merged.Count != 100 {
		t.Errorf("expected merged count 100, got %d", merged.Count)
	}
	if merged.Sum != 250.0 {
		t.Errorf("expected merged sum 250.0, got %f", merged.Sum)
	}
	if merged.ZeroCount != 8 {
		t.Errorf("expected merged zero count 8, got %d", merged.ZeroCount)
	}
}

func TestFromClassicHistogram(t *testing.T) {
	bounds := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0}
	counts := []uint64{10, 20, 35, 45, 60, 80, 90, 100}

	h := FromClassicHistogram("http_duration", nil, bounds, counts, 42.5, 100)

	if h.Metric != "http_duration" {
		t.Errorf("expected metric http_duration, got %s", h.Metric)
	}
	if h.Count != 100 {
		t.Errorf("expected count 100, got %d", h.Count)
	}
	if h.Sum != 42.5 {
		t.Errorf("expected sum 42.5, got %f", h.Sum)
	}
	if len(h.PositiveSpans) == 0 {
		t.Error("expected positive spans from conversion")
	}
}

func TestBucketBound(t *testing.T) {
	// Schema 0 should give powers of 2
	b := bucketBound(0, 0)
	if b != 2.0 {
		t.Errorf("expected bucket 0 bound of 2.0 for schema 0, got %f", b)
	}

	b = bucketBound(0, 1)
	if b != 4.0 {
		t.Errorf("expected bucket 1 bound of 4.0 for schema 0, got %f", b)
	}
}

func TestNativeHistogramStore_Stats(t *testing.T) {
	store := NewNativeHistogramStore(DefaultNativeHistogramConfig())
	store.Write(NativeHistogram{Metric: "m1", Count: 10})
	store.Write(NativeHistogram{Metric: "m2", Count: 20})

	stats := store.Stats()
	if stats["histogram_count"] != 2 {
		t.Errorf("expected 2 histograms, got %v", stats["histogram_count"])
	}
}
