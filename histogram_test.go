package chronicle

import (
	"math"
	"testing"
)

func TestHistogram_Observe(t *testing.T) {
	h := NewHistogram(3)

	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	for _, v := range values {
		h.Observe(v)
	}

	if h.Count != 5 {
		t.Errorf("expected count 5, got %d", h.Count)
	}

	expectedSum := 15.0
	if h.Sum != expectedSum {
		t.Errorf("expected sum %f, got %f", expectedSum, h.Sum)
	}
}

func TestHistogram_ObserveZero(t *testing.T) {
	h := NewHistogram(3)
	h.ZeroThreshold = 0.001

	h.Observe(0)
	h.Observe(0.0001)
	h.Observe(0.00001)

	if h.ZeroCount != 3 {
		t.Errorf("expected zero count 3, got %d", h.ZeroCount)
	}
}

func TestHistogram_Clone(t *testing.T) {
	h := NewHistogram(3)
	h.Observe(1.0)
	h.Observe(2.0)

	clone := h.Clone()

	if clone.Count != h.Count {
		t.Error("clone count mismatch")
	}
	if clone.Sum != h.Sum {
		t.Error("clone sum mismatch")
	}
	if clone.Schema != h.Schema {
		t.Error("clone schema mismatch")
	}

	// Modify original
	h.Observe(3.0)

	// Clone should be unchanged
	if clone.Count == h.Count {
		t.Error("clone should be independent of original")
	}
}

func TestHistogram_Merge(t *testing.T) {
	h1 := NewHistogram(3)
	h1.Observe(1.0)
	h1.Observe(2.0)

	h2 := NewHistogram(3)
	h2.Observe(3.0)
	h2.Observe(4.0)

	err := h1.Merge(h2)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if h1.Count != 4 {
		t.Errorf("expected merged count 4, got %d", h1.Count)
	}

	expectedSum := 10.0
	if h1.Sum != expectedSum {
		t.Errorf("expected merged sum %f, got %f", expectedSum, h1.Sum)
	}
}

func TestHistogram_MergeDifferentSchemas(t *testing.T) {
	h1 := NewHistogram(3)
	h2 := NewHistogram(5)

	err := h1.Merge(h2)
	if err == nil {
		t.Error("expected error when merging histograms with different schemas")
	}
}

func TestHistogram_Quantile(t *testing.T) {
	h := NewHistogram(3)

	// Add known values
	for i := 1; i <= 100; i++ {
		h.Observe(float64(i))
	}

	// Median should be around 50
	q50 := h.Quantile(0.5)
	if math.IsNaN(q50) {
		t.Error("quantile returned NaN")
	}

	// Edge cases
	q0 := h.Quantile(0)
	if math.IsNaN(q0) {
		t.Error("0th quantile returned NaN")
	}

	q100 := h.Quantile(1.0)
	if math.IsNaN(q100) {
		t.Error("100th quantile returned NaN")
	}

	// Invalid quantile
	qInvalid := h.Quantile(1.5)
	if !math.IsNaN(qInvalid) {
		t.Error("expected NaN for invalid quantile")
	}
}

func TestHistogram_QuantileEmpty(t *testing.T) {
	h := NewHistogram(3)

	q := h.Quantile(0.5)
	if !math.IsNaN(q) {
		t.Error("expected NaN for empty histogram quantile")
	}
}

func TestHistogram_EncodeDecode(t *testing.T) {
	h := NewHistogram(3)
	h.Observe(1.0)
	h.Observe(2.0)
	h.Observe(3.0)
	h.Observe(-1.0)

	encoded, err := h.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeHistogram(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Count != h.Count {
		t.Errorf("decoded count %d != original %d", decoded.Count, h.Count)
	}
	if decoded.Sum != h.Sum {
		t.Errorf("decoded sum %f != original %f", decoded.Sum, h.Sum)
	}
	if decoded.Schema != h.Schema {
		t.Errorf("decoded schema %d != original %d", decoded.Schema, h.Schema)
	}
}

func TestHistogramStore_WriteQuery(t *testing.T) {
	store := NewHistogramStore(nil)

	h := NewHistogram(3)
	h.Observe(10.0)
	h.Observe(20.0)

	p := HistogramPoint{
		Metric:    "http_request_duration",
		Tags:      map[string]string{"method": "GET"},
		Histogram: h,
		Timestamp: 1000,
	}

	if err := store.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	results, err := store.Query("http_request_duration", map[string]string{"method": "GET"}, 0, 2000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	if results[0].Histogram.Count != 2 {
		t.Errorf("expected count 2, got %d", results[0].Histogram.Count)
	}
}

func TestHistogramStore_QueryTagFilter(t *testing.T) {
	store := NewHistogramStore(nil)

	// Write histograms with different tags
	for _, method := range []string{"GET", "POST", "PUT"} {
		h := NewHistogram(3)
		h.Observe(float64(len(method)))
		store.Write(HistogramPoint{
			Metric:    "http_requests",
			Tags:      map[string]string{"method": method},
			Histogram: h,
			Timestamp: 1000,
		})
	}

	// Query for specific tag
	results, err := store.Query("http_requests", map[string]string{"method": "GET"}, 0, 2000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for GET, got %d", len(results))
	}

	// Query without tag filter should return all
	allResults, err := store.Query("http_requests", nil, 0, 2000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(allResults) != 3 {
		t.Errorf("expected 3 results without filter, got %d", len(allResults))
	}
}

func TestHistogram_NilClone(t *testing.T) {
	var h *Histogram
	clone := h.Clone()
	if clone != nil {
		t.Error("clone of nil histogram should be nil")
	}
}

func TestHistogram_MergeNil(t *testing.T) {
	h := NewHistogram(3)
	h.Observe(1.0)

	err := h.Merge(nil)
	if err != nil {
		t.Errorf("merge with nil should not error: %v", err)
	}

	if h.Count != 1 {
		t.Error("merge with nil should not change histogram")
	}
}
