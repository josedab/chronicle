package chronicle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// SemanticSearchConfig configures the semantic time-series search system.
type SemanticSearchConfig struct {
	// Enabled turns on semantic search
	Enabled bool

	// WindowSize for pattern extraction (number of points)
	WindowSize int

	// EmbeddingDimension for pattern vectors
	EmbeddingDimension int

	// MaxPatterns to store in memory
	MaxPatterns int

	// SimilarityThreshold for matching (0-1)
	SimilarityThreshold float64

	// IndexType for nearest neighbor search
	IndexType SearchIndexType

	// HNSW parameters
	HNSWEfConstruction int
	HNSWEfSearch       int
	HNSWM              int

	// Auto-indexing
	AutoIndexInterval time.Duration
}

// SearchIndexType defines the index type for similarity search.
type SearchIndexType string

const (
	IndexTypeFlat SearchIndexType = "flat" // Brute force
	IndexTypeHNSW SearchIndexType = "hnsw" // Hierarchical Navigable Small World
	IndexTypeLSH  SearchIndexType = "lsh"  // Locality Sensitive Hashing
)

// DefaultSemanticSearchConfig returns default configuration.
func DefaultSemanticSearchConfig() SemanticSearchConfig {
	return SemanticSearchConfig{
		Enabled:             true,
		WindowSize:          64,
		EmbeddingDimension:  32,
		MaxPatterns:         100000,
		SimilarityThreshold: 0.7,
		IndexType:           IndexTypeHNSW,
		HNSWEfConstruction:  200,
		HNSWEfSearch:        50,
		HNSWM:               16,
		AutoIndexInterval:   5 * time.Minute,
	}
}

// TimeSeriesPattern represents an indexed pattern.
type TimeSeriesPattern struct {
	ID         string            `json:"id"`
	Metric     string            `json:"metric"`
	Tags       map[string]string `json:"tags"`
	StartTime  int64             `json:"start_time"`
	EndTime    int64             `json:"end_time"`
	Embedding  []float32         `json:"embedding"`
	RawValues  []float64         `json:"raw_values,omitempty"`
	Statistics *PatternStats     `json:"statistics"`
	Labels     []string          `json:"labels,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// PatternStats contains statistical features of a pattern.
type PatternStats struct {
	Mean        float64 `json:"mean"`
	StdDev      float64 `json:"std_dev"`
	Min         float64 `json:"min"`
	Max         float64 `json:"max"`
	Trend       float64 `json:"trend"`       // Linear trend coefficient
	Seasonality float64 `json:"seasonality"` // Dominant frequency
	Entropy     float64 `json:"entropy"`     // Information entropy
	Skewness    float64 `json:"skewness"`
	Kurtosis    float64 `json:"kurtosis"`
}

// SearchResult represents a similarity search result.
type SearchResult struct {
	Pattern    *TimeSeriesPattern `json:"pattern"`
	Similarity float64            `json:"similarity"`
	Distance   float64            `json:"distance"`
	Rank       int                `json:"rank"`
}

// SemanticQuery represents a semantic search query.
type SemanticQuery struct {
	// Query by example pattern
	ExampleValues []float64 `json:"example_values,omitempty"`

	// Query by natural language
	NaturalLanguage string `json:"natural_language,omitempty"`

	// Query by existing pattern ID
	PatternID string `json:"pattern_id,omitempty"`

	// Query by reference time range
	ReferenceMetric string            `json:"reference_metric,omitempty"`
	ReferenceTags   map[string]string `json:"reference_tags,omitempty"`
	ReferenceStart  time.Time         `json:"reference_start,omitempty"`
	ReferenceEnd    time.Time         `json:"reference_end,omitempty"`

	// Filters
	MetricFilter string            `json:"metric_filter,omitempty"`
	TagFilters   map[string]string `json:"tag_filters,omitempty"`
	TimeRange    *QueryTimeRange   `json:"time_range,omitempty"`

	// Options
	TopK      int     `json:"top_k"`
	Threshold float64 `json:"threshold"`
}

// SemanticSearchEngine provides semantic similarity search for time series.
type SemanticSearchEngine struct {
	db     *DB
	config SemanticSearchConfig

	// Pattern storage
	patterns   map[string]*TimeSeriesPattern
	patternsMu sync.RWMutex

	// HNSW index
	hnswIndex *HNSWIndex
	indexMu   sync.RWMutex

	// Pattern encoder
	encoder *PatternEncoder

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	patternsIndexed  int64
	searchesExecuted int64
	indexUpdates     int64
}

// HNSWIndex implements Hierarchical Navigable Small World graph for ANN search.
type HNSWIndex struct {
	nodes          map[string]*HNSWNode
	entryPoint     string
	maxLevel       int
	efConstruction int
	efSearch       int
	m              int     // Max connections per layer
	mL             float64 // Level generation factor
	dimension      int
	mu             sync.RWMutex
}

// HNSWNode represents a node in the HNSW graph.
type HNSWNode struct {
	ID        string
	Vector    []float32
	Level     int
	Neighbors [][]string // Neighbors per level
}

// PatternEncoder encodes time series patterns into embeddings.
type PatternEncoder struct {
	dimension  int
	windowSize int
}

// NewSemanticSearchEngine creates a new semantic search engine.
func NewSemanticSearchEngine(db *DB, config SemanticSearchConfig) *SemanticSearchEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &SemanticSearchEngine{
		db:       db,
		config:   config,
		patterns: make(map[string]*TimeSeriesPattern),
		encoder:  NewPatternEncoder(config.EmbeddingDimension, config.WindowSize),
		ctx:      ctx,
		cancel:   cancel,
	}

	engine.hnswIndex = NewHNSWIndex(
		config.EmbeddingDimension,
		config.HNSWEfConstruction,
		config.HNSWEfSearch,
		config.HNSWM,
	)

	if config.Enabled && config.AutoIndexInterval > 0 {
		engine.wg.Add(1)
		go engine.autoIndexLoop()
	}

	return engine
}

// IndexPattern indexes a time series pattern.
func (e *SemanticSearchEngine) IndexPattern(pattern *TimeSeriesPattern) error {
	if pattern.ID == "" {
		pattern.ID = generatePatternID()
	}

	// Generate embedding if not provided
	if len(pattern.Embedding) == 0 {
		embedding := e.encoder.Encode(pattern.RawValues)
		pattern.Embedding = embedding
	}

	// Compute statistics if not provided
	if pattern.Statistics == nil && len(pattern.RawValues) > 0 {
		pattern.Statistics = computePatternStats(pattern.RawValues)
	}

	e.patternsMu.Lock()
	// Check limit
	if len(e.patterns) >= e.config.MaxPatterns {
		e.evictOldestPattern()
	}
	e.patterns[pattern.ID] = pattern
	e.patternsMu.Unlock()

	// Add to HNSW index
	e.indexMu.Lock()
	e.hnswIndex.Insert(pattern.ID, pattern.Embedding)
	e.indexMu.Unlock()

	atomic.AddInt64(&e.patternsIndexed, 1)
	return nil
}

// Search performs semantic similarity search.
func (e *SemanticSearchEngine) Search(query *SemanticQuery) ([]SearchResult, error) {
	atomic.AddInt64(&e.searchesExecuted, 1)

	// Get query embedding
	var queryEmbedding []float32
	var err error

	if len(query.ExampleValues) > 0 {
		queryEmbedding = e.encoder.Encode(query.ExampleValues)
	} else if query.PatternID != "" {
		e.patternsMu.RLock()
		if p, ok := e.patterns[query.PatternID]; ok {
			queryEmbedding = p.Embedding
		}
		e.patternsMu.RUnlock()
	} else if query.ReferenceMetric != "" {
		queryEmbedding, err = e.getEmbeddingFromTimeRange(query)
		if err != nil {
			return nil, err
		}
	} else if query.NaturalLanguage != "" {
		queryEmbedding = e.encoder.EncodeDescription(query.NaturalLanguage)
	}

	if queryEmbedding == nil {
		return nil, fmt.Errorf("no valid query provided")
	}

	topK := query.TopK
	if topK <= 0 {
		topK = 10
	}

	threshold := query.Threshold
	if threshold <= 0 {
		threshold = e.config.SimilarityThreshold
	}

	// Search in HNSW index
	e.indexMu.RLock()
	neighbors := e.hnswIndex.SearchKNN(queryEmbedding, topK*2) // Get extra for filtering
	e.indexMu.RUnlock()

	// Build results with filtering
	results := make([]SearchResult, 0, topK)

	e.patternsMu.RLock()
	for rank, n := range neighbors {
		if len(results) >= topK {
			break
		}

		pattern, ok := e.patterns[n.ID]
		if !ok {
			continue
		}

		// Apply filters
		if query.MetricFilter != "" && pattern.Metric != query.MetricFilter {
			continue
		}
		if len(query.TagFilters) > 0 {
			match := true
			for k, v := range query.TagFilters {
				if pattern.Tags[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		similarity := 1.0 - n.Distance
		if similarity < threshold {
			continue
		}

		results = append(results, SearchResult{
			Pattern:    pattern,
			Similarity: similarity,
			Distance:   n.Distance,
			Rank:       rank + 1,
		})
	}
	e.patternsMu.RUnlock()

	return results, nil
}

// SearchSimilar finds patterns similar to a reference time range.
func (e *SemanticSearchEngine) SearchSimilar(metric string, tags map[string]string, start, end time.Time, topK int) ([]SearchResult, error) {
	query := &SemanticQuery{
		ReferenceMetric: metric,
		ReferenceTags:   tags,
		ReferenceStart:  start,
		ReferenceEnd:    end,
		TopK:            topK,
	}
	return e.Search(query)
}

// IndexFromDatabase indexes patterns from existing data.
func (e *SemanticSearchEngine) IndexFromDatabase(metric string, tags map[string]string, start, end time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Minute)
	defer cancel()

	q := &Query{
		Metric: metric,
		Tags:   tags,
		Start:  start.UnixNano(),
		End:    end.UnixNano(),
	}

	result, err := e.db.ExecuteContext(ctx, q)
	if err != nil {
		return 0, err
	}

	if len(result.Points) < e.config.WindowSize {
		return 0, nil
	}

	// Extract sliding windows and index them
	indexed := 0
	for i := 0; i <= len(result.Points)-e.config.WindowSize; i += e.config.WindowSize / 2 {
		window := result.Points[i : i+e.config.WindowSize]

		values := make([]float64, len(window))
		for j, p := range window {
			values[j] = p.Value
		}

		pattern := &TimeSeriesPattern{
			Metric:    metric,
			Tags:      tags,
			StartTime: window[0].Timestamp,
			EndTime:   window[len(window)-1].Timestamp,
			RawValues: values,
		}

		if err := e.IndexPattern(pattern); err == nil {
			indexed++
		}
	}

	atomic.AddInt64(&e.indexUpdates, 1)
	return indexed, nil
}

func (e *SemanticSearchEngine) getEmbeddingFromTimeRange(query *SemanticQuery) ([]float32, error) {
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	q := &Query{
		Metric: query.ReferenceMetric,
		Tags:   query.ReferenceTags,
		Start:  query.ReferenceStart.UnixNano(),
		End:    query.ReferenceEnd.UnixNano(),
		Limit:  e.config.WindowSize * 2,
	}

	result, err := e.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, err
	}

	if len(result.Points) == 0 {
		return nil, fmt.Errorf("no data found for reference time range")
	}

	values := make([]float64, len(result.Points))
	for i, p := range result.Points {
		values[i] = p.Value
	}

	return e.encoder.Encode(values), nil
}

func (e *SemanticSearchEngine) evictOldestPattern() {
	// Simple eviction - remove random pattern
	for id := range e.patterns {
		delete(e.patterns, id)
		e.hnswIndex.Remove(id)
		break
	}
}

func (e *SemanticSearchEngine) autoIndexLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.AutoIndexInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			// Auto-index recent data
			e.indexRecentData()
		}
	}
}

func (e *SemanticSearchEngine) indexRecentData() {
	// Index patterns from recent data (last hour)
	end := time.Now()
	start := end.Add(-time.Hour)

	// Get list of metrics
	metrics := e.db.Metrics()

	for _, metric := range metrics {
		_, _ = e.IndexFromDatabase(metric, nil, start, end)
	}
}

// GetPattern returns a pattern by ID.
func (e *SemanticSearchEngine) GetPattern(id string) (*TimeSeriesPattern, error) {
	e.patternsMu.RLock()
	defer e.patternsMu.RUnlock()

	pattern, ok := e.patterns[id]
	if !ok {
		return nil, fmt.Errorf("pattern not found: %s", id)
	}
	return pattern, nil
}

// ListPatterns returns all indexed patterns.
func (e *SemanticSearchEngine) ListPatterns(limit int) []*TimeSeriesPattern {
	e.patternsMu.RLock()
	defer e.patternsMu.RUnlock()

	patterns := make([]*TimeSeriesPattern, 0, limit)
	for _, p := range e.patterns {
		patterns = append(patterns, p)
		if len(patterns) >= limit {
			break
		}
	}
	return patterns
}

// Stats returns engine statistics.
func (e *SemanticSearchEngine) Stats() SemanticSearchStats {
	e.patternsMu.RLock()
	patternCount := len(e.patterns)
	e.patternsMu.RUnlock()

	return SemanticSearchStats{
		PatternsIndexed:  atomic.LoadInt64(&e.patternsIndexed),
		PatternsStored:   int64(patternCount),
		SearchesExecuted: atomic.LoadInt64(&e.searchesExecuted),
		IndexUpdates:     atomic.LoadInt64(&e.indexUpdates),
	}
}

// SemanticSearchStats contains engine statistics.
type SemanticSearchStats struct {
	PatternsIndexed  int64 `json:"patterns_indexed"`
	PatternsStored   int64 `json:"patterns_stored"`
	SearchesExecuted int64 `json:"searches_executed"`
	IndexUpdates     int64 `json:"index_updates"`
}

// Close shuts down the semantic search engine.
func (e *SemanticSearchEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// PatternEncoder implementation

// NewPatternEncoder creates a new pattern encoder.
func NewPatternEncoder(dimension, windowSize int) *PatternEncoder {
	return &PatternEncoder{
		dimension:  dimension,
		windowSize: windowSize,
	}
}

// Encode encodes a time series into an embedding vector.
func (e *PatternEncoder) Encode(values []float64) []float32 {
	if len(values) == 0 {
		return make([]float32, e.dimension)
	}

	embedding := make([]float32, e.dimension)

	// Normalize values
	normalized := normalizeValues(values)

	// Feature extraction
	stats := computePatternStats(values)

	// Statistical features (first 10 dimensions)
	embedding[0] = float32(stats.Mean)
	embedding[1] = float32(stats.StdDev)
	embedding[2] = float32(stats.Min)
	embedding[3] = float32(stats.Max)
	embedding[4] = float32(stats.Trend)
	embedding[5] = float32(stats.Skewness)
	embedding[6] = float32(stats.Kurtosis)
	embedding[7] = float32(stats.Entropy)
	embedding[8] = float32(stats.Seasonality)
	embedding[9] = float32((stats.Max - stats.Min) / (math.Abs(stats.Mean) + 0.001)) // Coefficient of variation

	// Temporal features using FFT-like representation (next dimensions)
	fftFeatures := computeFrequencyFeatures(normalized, e.dimension-10)
	for i, f := range fftFeatures {
		if i+10 < e.dimension {
			embedding[i+10] = f
		}
	}

	// Normalize embedding
	normalizeEmbedding(embedding)

	return embedding
}

// EncodeDescription encodes a natural language description.
func (e *PatternEncoder) EncodeDescription(description string) []float32 {
	embedding := make([]float32, e.dimension)

	// Simple keyword-based encoding
	keywords := map[string]int{
		"spike":    0,
		"drop":     1,
		"increase": 2,
		"decrease": 3,
		"stable":   4,
		"volatile": 5,
		"trend":    6,
		"seasonal": 7,
		"anomaly":  8,
		"outlier":  9,
		"high":     10,
		"low":      11,
		"normal":   12,
		"periodic": 13,
	}

	for word, idx := range keywords {
		if semContainsWord(description, word) && idx < e.dimension {
			embedding[idx] = 1.0
		}
	}

	normalizeEmbedding(embedding)
	return embedding
}

// HNSW Index implementation

// NewHNSWIndex creates a new HNSW index.
func NewHNSWIndex(dimension, efConstruction, efSearch, m int) *HNSWIndex {
	return &HNSWIndex{
		nodes:          make(map[string]*HNSWNode),
		efConstruction: efConstruction,
		efSearch:       efSearch,
		m:              m,
		mL:             1.0 / math.Log(float64(m)),
		dimension:      dimension,
		maxLevel:       0,
	}
}

// Insert adds a vector to the index.
func (h *HNSWIndex) Insert(id string, vector []float32) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Generate random level
	level := h.randomLevel()

	node := &HNSWNode{
		ID:        id,
		Vector:    vector,
		Level:     level,
		Neighbors: make([][]string, level+1),
	}

	for i := range node.Neighbors {
		node.Neighbors[i] = make([]string, 0, h.m)
	}

	// First node
	if len(h.nodes) == 0 {
		h.nodes[id] = node
		h.entryPoint = id
		h.maxLevel = level
		return
	}

	// Find entry point
	entryID := h.entryPoint

	// Traverse from top level
	for l := h.maxLevel; l > level; l-- {
		entryID = h.searchLevel(vector, entryID, 1, l)[0].ID
	}

	// Insert at each level
	for l := semMin(level, h.maxLevel); l >= 0; l-- {
		neighbors := h.searchLevel(vector, entryID, h.efConstruction, l)

		// Select best neighbors
		selected := h.selectNeighbors(vector, neighbors, h.m)

		// Add bidirectional connections
		for _, n := range selected {
			node.Neighbors[l] = append(node.Neighbors[l], n.ID)
			if existingNode, ok := h.nodes[n.ID]; ok {
				if l < len(existingNode.Neighbors) {
					existingNode.Neighbors[l] = append(existingNode.Neighbors[l], id)
					// Prune if too many
					if len(existingNode.Neighbors[l]) > h.m*2 {
						existingNode.Neighbors[l] = h.pruneNeighbors(existingNode.Vector, existingNode.Neighbors[l], h.m)
					}
				}
			}
		}

		if len(selected) > 0 {
			entryID = selected[0].ID
		}
	}

	h.nodes[id] = node

	if level > h.maxLevel {
		h.maxLevel = level
		h.entryPoint = id
	}
}

// SearchKNN finds k nearest neighbors.
func (h *HNSWIndex) SearchKNN(query []float32, k int) []Neighbor {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.nodes) == 0 {
		return nil
	}

	entryID := h.entryPoint

	// Traverse from top level
	for l := h.maxLevel; l > 0; l-- {
		results := h.searchLevel(query, entryID, 1, l)
		if len(results) > 0 {
			entryID = results[0].ID
		}
	}

	// Search at level 0
	results := h.searchLevel(query, entryID, semMax(h.efSearch, k), 0)

	if len(results) > k {
		results = results[:k]
	}

	return results
}

// Remove removes a vector from the index.
func (h *HNSWIndex) Remove(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, ok := h.nodes[id]
	if !ok {
		return
	}

	// Remove connections
	for l := 0; l <= node.Level; l++ {
		for _, neighborID := range node.Neighbors[l] {
			if neighbor, ok := h.nodes[neighborID]; ok && l < len(neighbor.Neighbors) {
				// Remove id from neighbor's list
				newNeighbors := make([]string, 0)
				for _, nid := range neighbor.Neighbors[l] {
					if nid != id {
						newNeighbors = append(newNeighbors, nid)
					}
				}
				neighbor.Neighbors[l] = newNeighbors
			}
		}
	}

	delete(h.nodes, id)

	// Update entry point if needed
	if h.entryPoint == id && len(h.nodes) > 0 {
		for newEntry := range h.nodes {
			h.entryPoint = newEntry
			break
		}
	}
}

// Neighbor represents a search result.
type Neighbor struct {
	ID       string
	Distance float64
}

func (h *HNSWIndex) searchLevel(query []float32, entryID string, ef, level int) []Neighbor {
	visited := make(map[string]bool)
	candidates := make([]Neighbor, 0)
	results := make([]Neighbor, 0)

	entryNode := h.nodes[entryID]
	if entryNode == nil {
		return nil
	}

	dist := cosineDistance(query, entryNode.Vector)
	candidates = append(candidates, Neighbor{ID: entryID, Distance: dist})
	results = append(results, Neighbor{ID: entryID, Distance: dist})
	visited[entryID] = true

	for len(candidates) > 0 {
		// Get closest candidate
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Distance < candidates[j].Distance
		})
		current := candidates[0]
		candidates = candidates[1:]

		// Get furthest result
		sort.Slice(results, func(i, j int) bool {
			return results[i].Distance < results[j].Distance
		})
		furthest := results[len(results)-1]

		if current.Distance > furthest.Distance && len(results) >= ef {
			break
		}

		// Explore neighbors
		node := h.nodes[current.ID]
		if node == nil || level >= len(node.Neighbors) {
			continue
		}

		for _, neighborID := range node.Neighbors[level] {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			neighbor := h.nodes[neighborID]
			if neighbor == nil {
				continue
			}

			dist := cosineDistance(query, neighbor.Vector)

			if len(results) < ef || dist < furthest.Distance {
				candidates = append(candidates, Neighbor{ID: neighborID, Distance: dist})
				results = append(results, Neighbor{ID: neighborID, Distance: dist})

				if len(results) > ef {
					sort.Slice(results, func(i, j int) bool {
						return results[i].Distance < results[j].Distance
					})
					results = results[:ef]
				}
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	return results
}

func (h *HNSWIndex) selectNeighbors(query []float32, candidates []Neighbor, m int) []Neighbor {
	if len(candidates) <= m {
		return candidates
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Distance < candidates[j].Distance
	})
	return candidates[:m]
}

func (h *HNSWIndex) pruneNeighbors(vector []float32, neighborIDs []string, m int) []string {
	neighbors := make([]Neighbor, 0, len(neighborIDs))
	for _, id := range neighborIDs {
		if node, ok := h.nodes[id]; ok {
			neighbors = append(neighbors, Neighbor{
				ID:       id,
				Distance: cosineDistance(vector, node.Vector),
			})
		}
	}
	sort.Slice(neighbors, func(i, j int) bool {
		return neighbors[i].Distance < neighbors[j].Distance
	})
	if len(neighbors) > m {
		neighbors = neighbors[:m]
	}
	result := make([]string, len(neighbors))
	for i, n := range neighbors {
		result[i] = n.ID
	}
	return result
}

func (h *HNSWIndex) randomLevel() int {
	level := 0
	for randFloat64() < 0.5 && level < 16 {
		level++
	}
	return level
}

// Helper functions

func computePatternStats(values []float64) *PatternStats {
	n := len(values)
	if n == 0 {
		return &PatternStats{}
	}

	// Mean
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(n)

	// StdDev, Skewness, Kurtosis
	sumSq := 0.0
	sumCube := 0.0
	sumQuad := 0.0
	min := values[0]
	max := values[0]

	for _, v := range values {
		diff := v - mean
		sumSq += diff * diff
		sumCube += diff * diff * diff
		sumQuad += diff * diff * diff * diff
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	variance := sumSq / float64(n)
	stdDev := math.Sqrt(variance)

	var skewness, kurtosis float64
	if stdDev > 0 {
		skewness = (sumCube / float64(n)) / (stdDev * stdDev * stdDev)
		kurtosis = (sumQuad/float64(n))/(variance*variance) - 3
	}

	// Trend (linear regression slope)
	sumX := 0.0
	sumXY := 0.0
	sumXX := 0.0
	for i, v := range values {
		x := float64(i)
		sumX += x
		sumXY += x * v
		sumXX += x * x
	}
	trend := 0.0
	denom := float64(n)*sumXX - sumX*sumX
	if denom != 0 {
		trend = (float64(n)*sumXY - sumX*sum) / denom
	}

	// Entropy
	entropy := computeEntropy(values)

	return &PatternStats{
		Mean:     mean,
		StdDev:   stdDev,
		Min:      min,
		Max:      max,
		Trend:    trend,
		Skewness: skewness,
		Kurtosis: kurtosis,
		Entropy:  entropy,
	}
}

func computeEntropy(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// Discretize into bins
	bins := 10
	min, max := values[0], values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	if max == min {
		return 0
	}

	counts := make([]int, bins)
	for _, v := range values {
		bin := int((v - min) / (max - min) * float64(bins-1))
		if bin >= bins {
			bin = bins - 1
		}
		counts[bin]++
	}

	entropy := 0.0
	n := float64(len(values))
	for _, c := range counts {
		if c > 0 {
			p := float64(c) / n
			entropy -= p * math.Log2(p)
		}
	}

	return entropy
}

func computeFrequencyFeatures(values []float64, dimension int) []float32 {
	n := len(values)
	if n == 0 {
		return make([]float32, dimension)
	}

	features := make([]float32, dimension)

	// Simple DFT-like features
	for k := 0; k < dimension; k++ {
		freq := float64(k+1) * 2 * math.Pi / float64(n)
		realPart := 0.0
		imagPart := 0.0
		for i, v := range values {
			angle := freq * float64(i)
			realPart += v * math.Cos(angle)
			imagPart += v * math.Sin(angle)
		}
		magnitude := math.Sqrt(realPart*realPart+imagPart*imagPart) / float64(n)
		features[k] = float32(magnitude)
	}

	return features
}

func normalizeValues(values []float64) []float64 {
	if len(values) == 0 {
		return values
	}

	// Z-score normalization
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	sumSq := 0.0
	for _, v := range values {
		sumSq += (v - mean) * (v - mean)
	}
	stdDev := math.Sqrt(sumSq / float64(len(values)))

	if stdDev == 0 {
		stdDev = 1
	}

	normalized := make([]float64, len(values))
	for i, v := range values {
		normalized[i] = (v - mean) / stdDev
	}

	return normalized
}

func normalizeEmbedding(embedding []float32) {
	sum := float32(0)
	for _, v := range embedding {
		sum += v * v
	}
	norm := float32(math.Sqrt(float64(sum)))
	if norm > 0 {
		for i := range embedding {
			embedding[i] /= norm
		}
	}
}

func cosineDistance(a, b []float32) float64 {
	if len(a) != len(b) {
		return 1.0
	}

	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i] * b[i])
		normA += float64(a[i] * a[i])
		normB += float64(b[i] * b[i])
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}

	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return 1.0 - similarity
}

func semContainsWord(s, word string) bool {
	// Simple contains check
	for i := 0; i <= len(s)-len(word); i++ {
		match := true
		for j := 0; j < len(word); j++ {
			c1 := s[i+j]
			c2 := word[j]
			// Case insensitive
			if c1 >= 'A' && c1 <= 'Z' {
				c1 += 32
			}
			if c2 >= 'A' && c2 <= 'Z' {
				c2 += 32
			}
			if c1 != c2 {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func generatePatternID() string {
	return fmt.Sprintf("pat_%s", generateID())
}

func semMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func semMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}
