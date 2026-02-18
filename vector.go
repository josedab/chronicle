package chronicle

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// VectorPoint represents a point with an embedding vector.
type VectorPoint struct {
	ID        string
	Metric    string
	Tags      map[string]string
	Vector    []float32
	Timestamp int64
	Metadata  map[string]string
}

// VectorStore manages vector embeddings alongside time-series data.
type VectorStore struct {
	db       *DB
	mu       sync.RWMutex
	vectors  map[string]*vectorSeries
	config   VectorConfig
	dimIndex map[int][]string // dimension -> series IDs
}

type vectorSeries struct {
	metric    string
	tags      map[string]string
	dimension int
	points    []timedVector
}

type timedVector struct {
	id        string
	vector    []float32
	timestamp int64
	metadata  map[string]string
}

// VectorConfig configures vector storage behavior.
type VectorConfig struct {
	// MaxDimensions is the maximum vector dimension allowed.
	MaxDimensions int

	// DefaultDimension is used when not specified.
	DefaultDimension int

	// MaxVectorsPerSeries limits memory usage.
	MaxVectorsPerSeries int

	// DistanceMetric is the default distance metric.
	DistanceMetric DistanceMetric

	// NormalizeVectors normalizes vectors on insert.
	NormalizeVectors bool
}

// DistanceMetric defines distance calculation methods.
type DistanceMetric int

const (
	// DistanceCosine uses cosine similarity.
	DistanceCosine DistanceMetric = iota
	// DistanceEuclidean uses Euclidean distance.
	DistanceEuclidean
	// DistanceDotProduct uses dot product (for normalized vectors).
	DistanceDotProduct
)

// DefaultVectorConfig returns default vector configuration.
func DefaultVectorConfig() VectorConfig {
	return VectorConfig{
		MaxDimensions:       4096,
		DefaultDimension:    768, // Common BERT dimension
		MaxVectorsPerSeries: 100000,
		DistanceMetric:      DistanceCosine,
		NormalizeVectors:    true,
	}
}

// NewVectorStore creates a new vector store.
func NewVectorStore(db *DB, config VectorConfig) *VectorStore {
	if config.MaxDimensions <= 0 {
		config.MaxDimensions = 4096
	}
	if config.MaxVectorsPerSeries <= 0 {
		config.MaxVectorsPerSeries = 100000
	}
	return &VectorStore{
		db:       db,
		vectors:  make(map[string]*vectorSeries),
		config:   config,
		dimIndex: make(map[int][]string),
	}
}

// Write stores a vector point.
func (vs *VectorStore) Write(p VectorPoint) error {
	if p.Metric == "" {
		return errors.New("metric name required")
	}
	if len(p.Vector) == 0 {
		return errors.New("vector cannot be empty")
	}
	if len(p.Vector) > vs.config.MaxDimensions {
		return fmt.Errorf("vector dimension %d exceeds max %d", len(p.Vector), vs.config.MaxDimensions)
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	key := seriesKey(p.Metric, p.Tags)
	series, exists := vs.vectors[key]
	if !exists {
		series = &vectorSeries{
			metric:    p.Metric,
			tags:      cloneTags(p.Tags),
			dimension: len(p.Vector),
			points:    make([]timedVector, 0),
		}
		vs.vectors[key] = series
		vs.dimIndex[len(p.Vector)] = append(vs.dimIndex[len(p.Vector)], key)
	}

	// Validate dimension consistency
	if series.dimension != len(p.Vector) {
		return fmt.Errorf("dimension mismatch: series has %d, got %d", series.dimension, len(p.Vector))
	}

	// Check max vectors limit
	if len(series.points) >= vs.config.MaxVectorsPerSeries {
		// Remove oldest
		series.points = series.points[1:]
	}

	// Normalize if configured
	vector := make([]float32, len(p.Vector))
	copy(vector, p.Vector)
	if vs.config.NormalizeVectors {
		normalizeVector(vector)
	}

	ts := p.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	id := p.ID
	if id == "" {
		id = fmt.Sprintf("%s_%d", key, ts)
	}

	series.points = append(series.points, timedVector{
		id:        id,
		vector:    vector,
		timestamp: ts,
		metadata:  p.Metadata,
	})

	return nil
}

// Search finds the k nearest neighbors to a query vector.
func (vs *VectorStore) Search(query []float32, k int, metric string, tags map[string]string) ([]VectorSearchResult, error) {
	if len(query) == 0 {
		return nil, errors.New("query vector cannot be empty")
	}
	if k <= 0 {
		k = 10
	}

	vs.mu.RLock()
	defer vs.mu.RUnlock()

	// Normalize query if configured
	queryVec := make([]float32, len(query))
	copy(queryVec, query)
	if vs.config.NormalizeVectors {
		normalizeVector(queryVec)
	}

	type candidate struct {
		result   VectorSearchResult
		distance float64
	}
	var candidates []candidate

	for key, series := range vs.vectors {
		// Filter by metric if specified
		if metric != "" && series.metric != metric {
			continue
		}
		// Filter by tags if specified
		if !tagsMatch(series.tags, tags) {
			continue
		}
		// Skip dimension mismatch
		if series.dimension != len(queryVec) {
			continue
		}

		for _, tv := range series.points {
			dist := vs.calculateDistance(queryVec, tv.vector)
			candidates = append(candidates, candidate{
				result: VectorSearchResult{
					ID:        tv.id,
					Metric:    series.metric,
					Tags:      cloneTags(series.tags),
					Vector:    tv.vector,
					Timestamp: tv.timestamp,
					Metadata:  tv.metadata,
					Distance:  dist,
					Score:     1.0 / (1.0 + dist), // Convert distance to similarity score
				},
				distance: dist,
			})
			_ = key
		}
	}

	// Sort by distance (ascending)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].distance < candidates[j].distance
	})

	// Return top k
	results := make([]VectorSearchResult, 0, k)
	for i := 0; i < len(candidates) && i < k; i++ {
		results = append(results, candidates[i].result)
	}

	return results, nil
}

// VectorSearchResult represents a search result.
type VectorSearchResult struct {
	ID        string
	Metric    string
	Tags      map[string]string
	Vector    []float32
	Timestamp int64
	Metadata  map[string]string
	Distance  float64
	Score     float64
}

// calculateDistance computes distance between two vectors.
func (vs *VectorStore) calculateDistance(a, b []float32) float64 {
	switch vs.config.DistanceMetric {
	case DistanceCosine:
		return 1.0 - cosineSimilarity(a, b)
	case DistanceEuclidean:
		return euclideanDistance(a, b)
	case DistanceDotProduct:
		return -dotProduct(a, b) // Negative because higher dot product = more similar
	default:
		return 1.0 - cosineSimilarity(a, b)
	}
}

// Query retrieves vectors within a time range.
func (vs *VectorStore) Query(metric string, tags map[string]string, start, end int64) ([]VectorPoint, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	var results []VectorPoint

	for _, series := range vs.vectors {
		if metric != "" && series.metric != metric {
			continue
		}
		if !tagsMatch(series.tags, tags) {
			continue
		}

		for _, tv := range series.points {
			if tv.timestamp >= start && tv.timestamp <= end {
				results = append(results, VectorPoint{
					ID:        tv.id,
					Metric:    series.metric,
					Tags:      cloneTags(series.tags),
					Vector:    tv.vector,
					Timestamp: tv.timestamp,
					Metadata:  tv.metadata,
				})
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	return results, nil
}

// Stats returns vector store statistics.
func (vs *VectorStore) Stats() VectorStats {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	stats := VectorStats{
		SeriesCount:     len(vs.vectors),
		DimensionCounts: make(map[int]int),
	}

	for _, series := range vs.vectors {
		stats.VectorCount += int64(len(series.points))
		stats.DimensionCounts[series.dimension]++
	}

	return stats
}

// VectorStats contains vector store statistics.
type VectorStats struct {
	SeriesCount     int
	VectorCount     int64
	DimensionCounts map[int]int
}

// EncodeVector serializes a vector to bytes.
func EncodeVector(v []float32) []byte {
	buf := make([]byte, 4+len(v)*4)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(v)))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[4+i*4:], math.Float32bits(f))
	}
	return buf
}

// DecodeVector deserializes a vector from bytes.
func DecodeVector(data []byte) ([]float32, error) {
	if len(data) < 4 {
		return nil, errors.New("vector data too short")
	}
	dim := binary.LittleEndian.Uint32(data[:4])
	if len(data) < 4+int(dim)*4 {
		return nil, errors.New("vector data truncated")
	}
	v := make([]float32, dim)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[4+i*4:]))
	}
	return v, nil
}

// --- Vector math utilities ---

func normalizeVector(v []float32) {
	var norm float64
	for _, val := range v {
		norm += float64(val) * float64(val)
	}
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range v {
			v[i] = float32(float64(v[i]) / norm)
		}
	}
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func euclideanDistance(a, b []float32) float64 {
	if len(a) != len(b) {
		return math.MaxFloat64
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

func dotProduct(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
