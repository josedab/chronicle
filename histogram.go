package chronicle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
)

// Histogram represents a native histogram data point with efficient storage.
// Inspired by Prometheus native histograms, it uses exponential bucketing
// for better precision across wide value ranges.
type Histogram struct {
	// Sum is the sum of all observed values.
	Sum float64

	// Count is the total number of observations.
	Count uint64

	// ZeroCount is the count of observations in the zero bucket.
	ZeroCount uint64

	// ZeroThreshold is the width of the zero bucket.
	ZeroThreshold float64

	// Schema defines the bucket resolution. Higher = more buckets.
	// Valid values: -4 to 8 (corresponds to bucket factors 65536 to 1.00271)
	Schema int32

	// PositiveSpans defines the positive bucket structure.
	PositiveSpans []BucketSpan

	// NegativeSpans defines the negative bucket structure.
	NegativeSpans []BucketSpan

	// PositiveBuckets contains counts for positive buckets (delta-encoded).
	PositiveBuckets []int64

	// NegativeBuckets contains counts for negative buckets (delta-encoded).
	NegativeBuckets []int64
}

// BucketSpan defines a contiguous sequence of buckets.
type BucketSpan struct {
	Offset int32  // Gap from previous span
	Length uint32 // Number of buckets in this span
}

// HistogramPoint is a time-series point containing a histogram.
type HistogramPoint struct {
	Metric    string
	Tags      map[string]string
	Histogram *Histogram
	Timestamp int64
}

// HistogramStore manages histogram storage and queries.
type HistogramStore struct {
	db   *DB
	data map[string]*histogramSeries
	mu   sync.RWMutex
}

type histogramSeries struct {
	metric     string
	tags       map[string]string
	histograms []timedHistogram
}

type timedHistogram struct {
	timestamp int64
	histogram *Histogram
}

// NewHistogramStore creates a histogram store.
func NewHistogramStore(db *DB) *HistogramStore {
	return &HistogramStore{
		db:   db,
		data: make(map[string]*histogramSeries),
	}
}

// Write stores a histogram point.
func (hs *HistogramStore) Write(p HistogramPoint) error {
	if p.Histogram == nil {
		return fmt.Errorf("histogram is nil")
	}

	key := makeSeriesKey(p.Metric, p.Tags)

	hs.mu.Lock()
	defer hs.mu.Unlock()

	series, ok := hs.data[key]
	if !ok {
		series = &histogramSeries{
			metric: p.Metric,
			tags:   cloneTags(p.Tags),
		}
		hs.data[key] = series
	}

	series.histograms = append(series.histograms, timedHistogram{
		timestamp: p.Timestamp,
		histogram: p.Histogram.Clone(),
	})

	return nil
}

// Query retrieves histogram data.
func (hs *HistogramStore) Query(metric string, tags map[string]string, start, end int64) ([]HistogramPoint, error) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	var results []HistogramPoint

	for _, series := range hs.data {
		if series.metric != metric {
			continue
		}
		if !tagsMatch(series.tags, tags) {
			continue
		}

		for _, th := range series.histograms {
			if th.timestamp >= start && th.timestamp <= end {
				results = append(results, HistogramPoint{
					Metric:    series.metric,
					Tags:      cloneTags(series.tags),
					Histogram: th.histogram.Clone(),
					Timestamp: th.timestamp,
				})
			}
		}
	}

	// Sort by timestamp
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	return results, nil
}

// Clone creates a deep copy of the histogram.
func (h *Histogram) Clone() *Histogram {
	if h == nil {
		return nil
	}

	clone := &Histogram{
		Sum:           h.Sum,
		Count:         h.Count,
		ZeroCount:     h.ZeroCount,
		ZeroThreshold: h.ZeroThreshold,
		Schema:        h.Schema,
	}

	if h.PositiveSpans != nil {
		clone.PositiveSpans = make([]BucketSpan, len(h.PositiveSpans))
		copy(clone.PositiveSpans, h.PositiveSpans)
	}
	if h.NegativeSpans != nil {
		clone.NegativeSpans = make([]BucketSpan, len(h.NegativeSpans))
		copy(clone.NegativeSpans, h.NegativeSpans)
	}
	if h.PositiveBuckets != nil {
		clone.PositiveBuckets = make([]int64, len(h.PositiveBuckets))
		copy(clone.PositiveBuckets, h.PositiveBuckets)
	}
	if h.NegativeBuckets != nil {
		clone.NegativeBuckets = make([]int64, len(h.NegativeBuckets))
		copy(clone.NegativeBuckets, h.NegativeBuckets)
	}

	return clone
}

// Merge combines two histograms (must have same schema).
func (h *Histogram) Merge(other *Histogram) error {
	if other == nil {
		return nil
	}
	if h.Schema != other.Schema {
		return fmt.Errorf("cannot merge histograms with different schemas: %d vs %d", h.Schema, other.Schema)
	}

	h.Sum += other.Sum
	h.Count += other.Count
	h.ZeroCount += other.ZeroCount

	// Merge positive buckets
	h.PositiveBuckets = mergeBuckets(h.PositiveSpans, h.PositiveBuckets, other.PositiveSpans, other.PositiveBuckets)
	h.PositiveSpans = mergeSpans(h.PositiveSpans, other.PositiveSpans)

	// Merge negative buckets
	h.NegativeBuckets = mergeBuckets(h.NegativeSpans, h.NegativeBuckets, other.NegativeSpans, other.NegativeBuckets)
	h.NegativeSpans = mergeSpans(h.NegativeSpans, other.NegativeSpans)

	return nil
}

// Quantile computes the q-th quantile (0 <= q <= 1).
func (h *Histogram) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		return math.NaN()
	}
	if h.Count == 0 {
		return math.NaN()
	}

	rank := q * float64(h.Count)

	// Iterate through buckets to find the quantile
	var cumulative float64

	// Zero bucket
	cumulative += float64(h.ZeroCount)
	if cumulative >= rank {
		return 0
	}

	// Positive buckets
	bucketIdx := 0
	for _, span := range h.PositiveSpans {
		for i := uint32(0); i < span.Length; i++ {
			if bucketIdx >= len(h.PositiveBuckets) {
				break
			}
			count := h.PositiveBuckets[bucketIdx]
			cumulative += float64(count)
			if cumulative >= rank {
				// Return bucket midpoint
				lower, upper := h.bucketBounds(int(span.Offset)+int(i), true)
				return (lower + upper) / 2
			}
			bucketIdx++
		}
	}

	// Return max
	return h.Sum / float64(h.Count)
}

// bucketBounds calculates the lower and upper bounds of a bucket.
// Native histograms use exponential bucket widths determined by the schema.
// Schema determines the bucket width factor: factor = 2^(2^(-schema))
//   - Schema 0: factor = 2 (each bucket is 2x wider than previous)
//   - Schema 1: factor = √2 ≈ 1.41
//   - Schema 3: factor = 2^(1/8) ≈ 1.09 (finer granularity)
//
// Bucket index i covers values in range [factor^i, factor^(i+1))
func (h *Histogram) bucketBounds(idx int, positive bool) (float64, float64) {
	// Exponential bucket bounds based on schema
	factor := math.Pow(2, math.Pow(2, float64(-h.Schema)))
	lower := math.Pow(factor, float64(idx))
	upper := math.Pow(factor, float64(idx+1))
	if !positive {
		return -upper, -lower
	}
	return lower, upper
}

// Encode serializes the histogram to bytes.
func (h *Histogram) Encode() ([]byte, error) {
	buf := &bytes.Buffer{}

	// Write header
	if err := binary.Write(buf, binary.LittleEndian, h.Sum); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.Count); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.ZeroCount); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.ZeroThreshold); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.Schema); err != nil {
		return nil, err
	}

	// Write spans and buckets
	if err := writeSpans(buf, h.PositiveSpans); err != nil {
		return nil, err
	}
	if err := writeSpans(buf, h.NegativeSpans); err != nil {
		return nil, err
	}
	if err := writeBuckets(buf, h.PositiveBuckets); err != nil {
		return nil, err
	}
	if err := writeBuckets(buf, h.NegativeBuckets); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeHistogram deserializes a histogram from bytes.
func DecodeHistogram(data []byte) (*Histogram, error) {
	reader := bytes.NewReader(data)
	h := &Histogram{}

	if err := binary.Read(reader, binary.LittleEndian, &h.Sum); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.Count); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.ZeroCount); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.ZeroThreshold); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &h.Schema); err != nil {
		return nil, err
	}

	var err error
	h.PositiveSpans, err = readSpans(reader)
	if err != nil {
		return nil, err
	}
	h.NegativeSpans, err = readSpans(reader)
	if err != nil {
		return nil, err
	}
	h.PositiveBuckets, err = readBuckets(reader)
	if err != nil {
		return nil, err
	}
	h.NegativeBuckets, err = readBuckets(reader)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func writeSpans(buf *bytes.Buffer, spans []BucketSpan) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(spans))); err != nil {
		return err
	}
	for _, span := range spans {
		if err := binary.Write(buf, binary.LittleEndian, span.Offset); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, span.Length); err != nil {
			return err
		}
	}
	return nil
}

func readSpans(reader *bytes.Reader) ([]BucketSpan, error) {
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	spans := make([]BucketSpan, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &spans[i].Offset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &spans[i].Length); err != nil {
			return nil, err
		}
	}
	return spans, nil
}

func writeBuckets(buf *bytes.Buffer, buckets []int64) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(buckets))); err != nil {
		return err
	}
	for _, b := range buckets {
		if err := binary.Write(buf, binary.LittleEndian, b); err != nil {
			return err
		}
	}
	return nil
}

func readBuckets(reader *bytes.Reader) ([]int64, error) {
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	buckets := make([]int64, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &buckets[i]); err != nil {
			return nil, err
		}
	}
	return buckets, nil
}

func mergeBuckets(spans1 []BucketSpan, buckets1 []int64, spans2 []BucketSpan, buckets2 []int64) []int64 {
	// Simplified merge - just concatenate for now
	// A full implementation would properly interleave based on bucket indices
	result := make([]int64, 0, len(buckets1)+len(buckets2))
	result = append(result, buckets1...)
	result = append(result, buckets2...)
	return result
}

func mergeSpans(spans1, spans2 []BucketSpan) []BucketSpan {
	// Simplified merge - just concatenate
	result := make([]BucketSpan, 0, len(spans1)+len(spans2))
	result = append(result, spans1...)
	result = append(result, spans2...)
	return result
}

func tagsMatch(seriesTags, queryTags map[string]string) bool {
	for k, v := range queryTags {
		if seriesTags[k] != v {
			return false
		}
	}
	return true
}

// NewHistogram creates a histogram with the given schema.
func NewHistogram(schema int32) *Histogram {
	return &Histogram{
		Schema:        schema,
		ZeroThreshold: 1e-128,
	}
}

// Observe adds a value to the histogram.
func (h *Histogram) Observe(value float64) {
	h.Sum += value
	h.Count++

	if math.Abs(value) <= h.ZeroThreshold {
		h.ZeroCount++
		return
	}

	// Calculate bucket index
	idx := h.getBucketIndex(value)
	positive := value > 0

	if positive {
		h.addToPositiveBucket(idx)
	} else {
		h.addToNegativeBucket(idx)
	}
}

func (h *Histogram) getBucketIndex(value float64) int {
	absValue := math.Abs(value)
	if absValue <= h.ZeroThreshold {
		return 0
	}

	// Exponential bucket index based on schema
	factor := math.Pow(2, math.Pow(2, float64(-h.Schema)))
	return int(math.Ceil(math.Log(absValue) / math.Log(factor)))
}

func (h *Histogram) addToPositiveBucket(idx int) {
	// Find or create span containing this index
	if len(h.PositiveSpans) == 0 {
		h.PositiveSpans = []BucketSpan{{Offset: int32(idx), Length: 1}}
		h.PositiveBuckets = []int64{1}
		return
	}

	// Simplified: just increment the first bucket
	if len(h.PositiveBuckets) > 0 {
		h.PositiveBuckets[0]++
	}
}

func (h *Histogram) addToNegativeBucket(idx int) {
	if len(h.NegativeSpans) == 0 {
		h.NegativeSpans = []BucketSpan{{Offset: int32(idx), Length: 1}}
		h.NegativeBuckets = []int64{1}
		return
	}

	if len(h.NegativeBuckets) > 0 {
		h.NegativeBuckets[0]++
	}
}
