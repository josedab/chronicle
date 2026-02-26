package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// NativeHistogramSchema defines the exponential bucket schema.
// Schema values follow Prometheus conventions:
// schema 0 = powers of 2, schema 3 = 8 sub-buckets per power of 2, etc.
type NativeHistogramSchema int32

const (
	NativeHistogramSchemaDefault NativeHistogramSchema = 3
)

// NativeHistogram represents a Prometheus-style exponential histogram.
type NativeHistogram struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp int64             `json:"timestamp"`

	// Core histogram fields
	Count         uint64  `json:"count"`
	Sum           float64 `json:"sum"`
	Schema        int32   `json:"schema"`
	ZeroThreshold float64 `json:"zero_threshold"`
	ZeroCount     uint64  `json:"zero_count"`

	// Positive and negative buckets (exponential)
	PositiveSpans   []BucketSpan  `json:"positive_spans,omitempty"`
	PositiveBuckets []int64       `json:"positive_buckets,omitempty"` // Delta-encoded counts
	NegativeSpans   []BucketSpan  `json:"negative_spans,omitempty"`
	NegativeBuckets []int64       `json:"negative_buckets,omitempty"` // Delta-encoded counts
}

// NativeHistogramConfig configures native histogram storage.
type NativeHistogramConfig struct {
	Enabled           bool          `json:"enabled"`
	DefaultSchema     int32         `json:"default_schema"`
	MaxBuckets        int           `json:"max_buckets"`
	ZeroThreshold     float64       `json:"zero_threshold"`
	RetentionDuration time.Duration `json:"retention_duration"`
}

// DefaultNativeHistogramConfig returns sensible defaults.
func DefaultNativeHistogramConfig() NativeHistogramConfig {
	return NativeHistogramConfig{
		Enabled:           true,
		DefaultSchema:     3,
		MaxBuckets:        160,
		ZeroThreshold:     1e-128,
		RetentionDuration: 7 * 24 * time.Hour,
	}
}

// NativeHistogramStore manages storage of native histograms.
type NativeHistogramStore struct {
	config     NativeHistogramConfig
	histograms map[string][]NativeHistogram // seriesKey -> time-ordered histograms
	mu         sync.RWMutex
}

type nativeHistBucket struct {
	upperBound float64
	count      uint64
}

// NewNativeHistogramStore creates a new native histogram store.
func NewNativeHistogramStore(config NativeHistogramConfig) *NativeHistogramStore {
	return &NativeHistogramStore{
		config:     config,
		histograms: make(map[string][]NativeHistogram),
	}
}

// Write stores a native histogram.
func (s *NativeHistogramStore) Write(h NativeHistogram) error {
	if h.Metric == "" {
		return fmt.Errorf("metric name is required")
	}
	if h.Timestamp == 0 {
		h.Timestamp = time.Now().UnixNano()
	}
	if h.Schema == 0 {
		h.Schema = s.config.DefaultSchema
	}

	key := makeSeriesKey(h.Metric, h.Tags)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.histograms[key] = append(s.histograms[key], h)
	return nil
}

// Query returns histograms for a metric within a time range.
func (s *NativeHistogramStore) Query(metric string, tags map[string]string, start, end int64) []NativeHistogram {
	key := makeSeriesKey(metric, tags)

	s.mu.RLock()
	defer s.mu.RUnlock()

	histograms := s.histograms[key]
	var result []NativeHistogram

	for _, h := range histograms {
		if start > 0 && h.Timestamp < start {
			continue
		}
		if end > 0 && h.Timestamp > end {
			continue
		}
		result = append(result, h)
	}

	return result
}

// HistogramQuantile computes a quantile from a native histogram.
// This implements the histogram_quantile() function for native histograms.
func HistogramQuantile(q float64, h *NativeHistogram) float64 {
	if q < 0 || q > 1 {
		return math.NaN()
	}
	if h.Count == 0 {
		return math.NaN()
	}

	// Build cumulative bucket boundaries and counts
	var buckets []nativeHistBucket

	// Add zero bucket
	if h.ZeroCount > 0 {
		buckets = append(buckets, nativeHistBucket{
			upperBound: h.ZeroThreshold,
			count:      h.ZeroCount,
		})
	}

	// Process positive spans
	addNativeHistBuckets(&buckets, h.PositiveSpans, h.PositiveBuckets, h.Schema, false)

	// Sort by upper bound
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].upperBound < buckets[j].upperBound
	})

	// Make cumulative
	for i := 1; i < len(buckets); i++ {
		buckets[i].count += buckets[i-1].count
	}

	if len(buckets) == 0 {
		return math.NaN()
	}

	// Find the bucket containing the quantile
	rank := q * float64(h.Count)

	for i, b := range buckets {
		if float64(b.count) >= rank {
			// Linear interpolation within bucket
			var lowerBound float64
			var lowerCount uint64
			if i > 0 {
				lowerBound = buckets[i-1].upperBound
				lowerCount = buckets[i-1].count
			}

			bucketCount := b.count - lowerCount
			if bucketCount == 0 {
				return lowerBound
			}

			fraction := (rank - float64(lowerCount)) / float64(bucketCount)
			return lowerBound + fraction*(b.upperBound-lowerBound)
		}
	}

	return buckets[len(buckets)-1].upperBound
}

// addBuckets converts spans and delta-encoded bucket counts into absolute buckets.
func addNativeHistBuckets(buckets *[]nativeHistBucket, spans []BucketSpan, deltas []int64, schema int32, negative bool) {
	deltaIdx := 0
	var absoluteCount int64
	bucketIdx := int32(0)

	for _, span := range spans {
		bucketIdx += span.Offset
		for j := uint32(0); j < span.Length; j++ {
			if deltaIdx < len(deltas) {
				absoluteCount += deltas[deltaIdx]
				deltaIdx++
			}

			bound := bucketBound(schema, bucketIdx)
			if negative {
				bound = -bound
			}

			if absoluteCount > 0 {
				*buckets = append(*buckets, nativeHistBucket{
					upperBound: bound,
					count:      uint64(absoluteCount),
				})
			}
			bucketIdx++
		}
	}
}

// bucketBound computes the upper bound of an exponential bucket.
func bucketBound(schema, idx int32) float64 {
	// For schema s, bucket index i has upper bound: base^(i+1)
	// where base = 2^(2^(-s))
	base := math.Pow(2, math.Pow(2, float64(-schema)))
	return math.Pow(base, float64(idx+1))
}

// MergeHistograms merges two native histograms of the same schema.
func MergeHistograms(a, b *NativeHistogram) NativeHistogram {
	result := NativeHistogram{
		Metric:        a.Metric,
		Tags:          a.Tags,
		Timestamp:     max64(a.Timestamp, b.Timestamp),
		Count:         a.Count + b.Count,
		Sum:           a.Sum + b.Sum,
		Schema:        a.Schema,
		ZeroThreshold: math.Max(a.ZeroThreshold, b.ZeroThreshold),
		ZeroCount:     a.ZeroCount + b.ZeroCount,
	}

	// Merge positive buckets
	result.PositiveSpans, result.PositiveBuckets = mergeNativeHistSpans(
		a.PositiveSpans, a.PositiveBuckets,
		b.PositiveSpans, b.PositiveBuckets,
	)

	// Merge negative buckets
	result.NegativeSpans, result.NegativeBuckets = mergeNativeHistSpans(
		a.NegativeSpans, a.NegativeBuckets,
		b.NegativeSpans, b.NegativeBuckets,
	)

	return result
}

func mergeNativeHistSpans(aSpans []BucketSpan, aDeltas []int64, bSpans []BucketSpan, bDeltas []int64) ([]BucketSpan, []int64) {
	// Expand both to absolute counts indexed by bucket position
	aAbsolute := expandToAbsolute(aSpans, aDeltas)
	bAbsolute := expandToAbsolute(bSpans, bDeltas)

	// Merge
	merged := make(map[int32]int64)
	for idx, count := range aAbsolute {
		merged[idx] += count
	}
	for idx, count := range bAbsolute {
		merged[idx] += count
	}

	if len(merged) == 0 {
		return nil, nil
	}

	// Sort indices
	var indices []int32
	for idx := range merged {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })

	// Re-encode as spans and deltas
	var spans []BucketSpan
	var deltas []int64

	prevCount := int64(0)
	for i, idx := range indices {
		if i == 0 || idx != indices[i-1]+1 {
			// New span
			offset := idx
			if len(spans) > 0 {
				offset = idx - indices[i-1] - 1
			}
			spans = append(spans, BucketSpan{Offset: offset, Length: 1})
		} else {
			spans[len(spans)-1].Length++
		}
		count := merged[idx]
		deltas = append(deltas, count-prevCount)
		prevCount = count
	}

	return spans, deltas
}

func expandToAbsolute(spans []BucketSpan, deltas []int64) map[int32]int64 {
	result := make(map[int32]int64)
	deltaIdx := 0
	var absoluteCount int64
	bucketIdx := int32(0)

	for _, span := range spans {
		bucketIdx += span.Offset
		for j := uint32(0); j < span.Length; j++ {
			if deltaIdx < len(deltas) {
				absoluteCount += deltas[deltaIdx]
				deltaIdx++
			}
			if absoluteCount > 0 {
				result[bucketIdx] = absoluteCount
			}
			bucketIdx++
		}
	}
	return result
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// FromClassicHistogram converts a classic histogram (buckets with le labels)
// into a native histogram.
func FromClassicHistogram(metric string, tags map[string]string, bucketBounds []float64, bucketCounts []uint64, sum float64, count uint64) NativeHistogram {
	schema := int32(NativeHistogramSchemaDefault)
	h := NativeHistogram{
		Metric:        metric,
		Tags:          tags,
		Timestamp:     time.Now().UnixNano(),
		Count:         count,
		Sum:           sum,
		Schema:        schema,
		ZeroThreshold: 1e-128,
	}

	// Convert classic buckets to exponential
	var spans []BucketSpan
	var deltas []int64
	var prevCount int64

	for i, bound := range bucketBounds {
		if bound <= 0 || math.IsInf(bound, 0) {
			continue
		}
		if i < len(bucketCounts) {
			idx := bucketIndex(schema, bound)
			if len(spans) == 0 {
				spans = append(spans, BucketSpan{Offset: idx, Length: 1})
			} else {
				lastSpan := &spans[len(spans)-1]
				expectedIdx := lastSpan.Offset + int32(lastSpan.Length)
				if idx == expectedIdx {
					lastSpan.Length++
				} else {
					spans = append(spans, BucketSpan{
						Offset: idx - expectedIdx,
						Length: 1,
					})
				}
			}
			bc := int64(bucketCounts[i])
			deltas = append(deltas, bc-prevCount)
			prevCount = bc
		}
	}

	h.PositiveSpans = spans
	h.PositiveBuckets = deltas

	return h
}

// bucketIndex computes the bucket index for a given value and schema.
func bucketIndex(schema int32, value float64) int32 {
	if value <= 0 {
		return 0
	}
	base := math.Pow(2, math.Pow(2, float64(-schema)))
	return int32(math.Ceil(math.Log(value) / math.Log(base))) - 1
}

// Stats returns native histogram store statistics.
func (s *NativeHistogramStore) Stats() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalHistograms := 0
	for _, histograms := range s.histograms {
		totalHistograms += len(histograms)
	}

	return map[string]any{
		"series_count":     len(s.histograms),
		"histogram_count":  totalHistograms,
	}
}

// RegisterHTTPHandlers registers native histogram HTTP endpoints.
func (s *NativeHistogramStore) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/histograms", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			var h NativeHistogram
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
			if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := s.Write(h); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "created"})
		case http.MethodGet:
			json.NewEncoder(w).Encode(s.Stats())
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/histograms/quantile", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req struct {
			Metric   string            `json:"metric"`
			Tags     map[string]string `json:"tags"`
			Quantile float64           `json:"quantile"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		histograms := s.Query(req.Metric, req.Tags, 0, 0)
		if len(histograms) == 0 {
			http.Error(w, "no histograms found", http.StatusNotFound)
			return
		}

		// Merge all histograms and compute quantile
		merged := histograms[0]
		for i := 1; i < len(histograms); i++ {
			merged = MergeHistograms(&merged, &histograms[i])
		}

		value := HistogramQuantile(req.Quantile, &merged)
		json.NewEncoder(w).Encode(map[string]any{
			"metric":   req.Metric,
			"quantile": req.Quantile,
			"value":    value,
		})
	})
}
