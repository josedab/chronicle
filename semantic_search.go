package chronicle

import (
	"context"
	"fmt"
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
		_, _ = e.IndexFromDatabase(metric, nil, start, end) //nolint:errcheck // best-effort background indexing
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
