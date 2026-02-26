// hybrid_index_partitions.go contains extended hybrid index functionality.
package chronicle

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

type temporalPartition struct {
	startTime  int64
	endTime    int64
	graph      *hnswGraph
	pointCount int
	key        string
	points     map[string]*HybridPoint
}

// TemporalPartitionedIndex combines temporal partitioning with HNSW vector indexing.
type TemporalPartitionedIndex struct {
	config         *HybridIndexConfig
	partitions     map[string]*temporalPartition
	partitionOrder []string
	mu             sync.RWMutex
}

// NewTemporalPartitionedIndex creates a new temporal-partitioned hybrid index.
func NewTemporalPartitionedIndex(config HybridIndexConfig) *TemporalPartitionedIndex {
	return &TemporalPartitionedIndex{
		config:         &config,
		partitions:     make(map[string]*temporalPartition),
		partitionOrder: nil,
	}
}

func (idx *TemporalPartitionedIndex) partitionKey(timestamp int64) string {
	bucket := idx.config.TemporalBucketDuration.Nanoseconds()
	if bucket <= 0 {
		bucket = time.Hour.Nanoseconds()
	}
	start := (timestamp / bucket) * bucket
	return fmt.Sprintf("t_%d", start)
}

func (idx *TemporalPartitionedIndex) getOrCreatePartition(timestamp int64) *temporalPartition {
	key := idx.partitionKey(timestamp)
	if p, ok := idx.partitions[key]; ok {
		return p
	}
	bucket := idx.config.TemporalBucketDuration.Nanoseconds()
	if bucket <= 0 {
		bucket = time.Hour.Nanoseconds()
	}
	start := (timestamp / bucket) * bucket
	p := &temporalPartition{
		startTime: start,
		endTime:   start + bucket,
		graph:     newHNSWGraph(idx.config),
		key:       key,
		points:    make(map[string]*HybridPoint),
	}
	idx.partitions[key] = p
	idx.partitionOrder = append(idx.partitionOrder, key)
	sort.Slice(idx.partitionOrder, func(i, j int) bool {
		pi := idx.partitions[idx.partitionOrder[i]]
		pj := idx.partitions[idx.partitionOrder[j]]
		return pi.startTime < pj.startTime
	})
	return p
}

// Insert adds a hybrid point into the correct temporal partition.
func (idx *TemporalPartitionedIndex) Insert(point *HybridPoint) error {
	if point == nil {
		return errors.New("hybrid_index: nil point")
	}
	if len(point.Vector) == 0 {
		return errors.New("hybrid_index: empty vector")
	}
	if len(point.Vector) > idx.config.MaxDimensions {
		return fmt.Errorf("hybrid_index: vector dimension %d exceeds max %d", len(point.Vector), idx.config.MaxDimensions)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	p := idx.getOrCreatePartition(point.Timestamp)
	if p.pointCount >= idx.config.MaxVectorsPerPartition {
		return fmt.Errorf("hybrid_index: partition %s is full (%d points)", p.key, p.pointCount)
	}

	p.graph.insert(point.ID, point.Vector, point.Timestamp)
	p.points[point.ID] = point
	p.pointCount++
	return nil
}

// Search performs a hybrid search across temporal partitions in the given time range.
func (idx *TemporalPartitionedIndex) Search(query *HybridSearchQuery) ([]*HybridSearchResult, error) {
	if query == nil {
		return nil, errors.New("hybrid_index: nil query")
	}
	if len(query.Vector) == 0 {
		return nil, errors.New("hybrid_index: empty query vector")
	}
	if query.K <= 0 {
		return nil, errors.New("hybrid_index: k must be positive")
	}

	ef := query.Ef
	if ef <= 0 {
		ef = idx.config.HNSWEfSearch
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var allResults []*HybridSearchResult

	for _, key := range idx.partitionOrder {
		p := idx.partitions[key]
		if p == nil {
			continue
		}
		// Filter by time range if specified.
		if query.StartTime > 0 && p.endTime <= query.StartTime {
			continue
		}
		if query.EndTime > 0 && p.startTime >= query.EndTime {
			continue
		}

		raw := p.graph.search(query.Vector, ef, ef)
		for _, r := range raw {
			if query.MaxDistance > 0 && r.Distance > query.MaxDistance {
				continue
			}
			pt := p.points[r.ID]
			if pt == nil {
				continue
			}
			// Time range filtering on individual points.
			if query.StartTime > 0 && pt.Timestamp < query.StartTime {
				continue
			}
			if query.EndTime > 0 && pt.Timestamp >= query.EndTime {
				continue
			}
			if query.MetricFilter != "" && pt.Metric != query.MetricFilter {
				continue
			}
			if !matchTags(pt.Tags, query.TagFilters) {
				continue
			}
			allResults = append(allResults, &HybridSearchResult{
				ID:           r.ID,
				Distance:     r.Distance,
				Timestamp:    pt.Timestamp,
				Metric:       pt.Metric,
				Tags:         pt.Tags,
				Value:        pt.Value,
				PartitionKey: key,
			})
		}
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Distance < allResults[j].Distance
	})
	if len(allResults) > query.K {
		allResults = allResults[:query.K]
	}
	return allResults, nil
}

func matchTags(pointTags, filters map[string]string) bool {
	for k, v := range filters {
		if pointTags[k] != v {
			return false
		}
	}
	return true
}

// Delete removes a point by ID from all partitions.
func (idx *TemporalPartitionedIndex) Delete(id string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, p := range idx.partitions {
		if _, ok := p.points[id]; ok {
			p.graph.delete(id)
			delete(p.points, id)
			p.pointCount--
			return nil
		}
	}
	return fmt.Errorf("hybrid_index: point %q not found", id)
}

// Stats returns statistics about the hybrid index.
func (idx *TemporalPartitionedIndex) Stats() *HybridIndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := &HybridIndexStats{
		PartitionCount: len(idx.partitions),
	}
	for _, p := range idx.partitions {
		stats.TotalPoints += p.pointCount
		stats.TotalVectors += len(p.graph.nodes)
		if p.graph.maxLevel > stats.MaxLevel {
			stats.MaxLevel = p.graph.maxLevel
		}
	}
	if stats.PartitionCount > 0 {
		stats.AvgPointsPerPartition = float64(stats.TotalPoints) / float64(stats.PartitionCount)
	}
	// Rough memory estimate: per vector ~(dim*8 + 200) bytes overhead.
	dim := idx.config.DefaultDimension
	stats.MemoryEstimateBytes = int64(stats.TotalVectors) * int64(dim*8+200)
	return stats
}

// --- Hybrid Query Planner ---

// HybridStrategy indicates the scan strategy for a hybrid query.
type HybridStrategy int

const (
	// StrategyTemporalOnly scans only temporal partitions without vector search.
	StrategyTemporalOnly HybridStrategy = iota
	// StrategyVectorOnly performs vector-only search across all partitions.
	StrategyVectorOnly
	// StrategyHybridFilter applies temporal filter then vector search.
	StrategyHybridFilter
	// StrategyHybridScore scores using both temporal proximity and vector distance.
	StrategyHybridScore
)

// HybridQueryPlan describes the planned execution of a hybrid query.
type HybridQueryPlan struct {
	Strategy         HybridStrategy
	PartitionsToScan []string
	EstimatedCost    float64
	Steps            []string
}

// HybridQueryPlanner decides between temporal, vector, or hybrid scan strategies.
type HybridQueryPlanner struct {
	index *TemporalPartitionedIndex
}

// NewHybridQueryPlanner creates a planner for the given index.
func NewHybridQueryPlanner(index *TemporalPartitionedIndex) *HybridQueryPlanner {
	return &HybridQueryPlanner{index: index}
}

// Plan analyzes a query and returns an execution plan.
func (p *HybridQueryPlanner) Plan(query *HybridSearchQuery) *HybridQueryPlan {
	p.index.mu.RLock()
	defer p.index.mu.RUnlock()

	plan := &HybridQueryPlan{}
	totalPartitions := len(p.index.partitions)
	if totalPartitions == 0 {
		plan.Strategy = StrategyVectorOnly
		plan.EstimatedCost = 0
		plan.Steps = []string{"no partitions available"}
		return plan
	}

	// Determine which partitions overlap the time range.
	var matching []string
	for _, key := range p.index.partitionOrder {
		part := p.index.partitions[key]
		if part == nil {
			continue
		}
		inRange := true
		if query.StartTime > 0 && part.endTime <= query.StartTime {
			inRange = false
		}
		if query.EndTime > 0 && part.startTime >= query.EndTime {
			inRange = false
		}
		if inRange {
			matching = append(matching, key)
		}
	}
	plan.PartitionsToScan = matching

	hasVector := len(query.Vector) > 0
	hasTimeRange := query.StartTime > 0 || query.EndTime > 0
	scanRatio := float64(len(matching)) / float64(totalPartitions)

	switch {
	case !hasVector && hasTimeRange:
		plan.Strategy = StrategyTemporalOnly
		plan.Steps = []string{
			fmt.Sprintf("scan %d/%d temporal partitions", len(matching), totalPartitions),
			"apply metric/tag filters",
		}
		plan.EstimatedCost = float64(len(matching))

	case hasVector && !hasTimeRange:
		plan.Strategy = StrategyVectorOnly
		plan.PartitionsToScan = p.index.partitionOrder
		plan.Steps = []string{
			fmt.Sprintf("vector search across all %d partitions", totalPartitions),
			fmt.Sprintf("merge top-%d results", query.K),
		}
		plan.EstimatedCost = float64(totalPartitions) * math.Log2(float64(p.index.config.HNSWEfSearch+1))

	case hasVector && hasTimeRange && scanRatio <= 0.3:
		plan.Strategy = StrategyHybridFilter
		plan.Steps = []string{
			fmt.Sprintf("temporal filter: %d/%d partitions (%.0f%%)", len(matching), totalPartitions, scanRatio*100),
			"HNSW vector search on filtered partitions",
			fmt.Sprintf("merge top-%d results", query.K),
		}
		plan.EstimatedCost = float64(len(matching)) * math.Log2(float64(p.index.config.HNSWEfSearch+1))

	default:
		plan.Strategy = StrategyHybridScore
		plan.Steps = []string{
			fmt.Sprintf("scan %d partitions with hybrid scoring", len(matching)),
			"combine vector distance + temporal proximity",
			fmt.Sprintf("return top-%d by combined score", query.K),
		}
		plan.EstimatedCost = float64(len(matching)) * math.Log2(float64(p.index.config.HNSWEfSearch+1)) * 1.2
	}

	return plan
}

// --- Pattern Library ---

// HybridTimeSeriesPattern represents a named pattern for anomaly detection via vector similarity.
type HybridTimeSeriesPattern struct {
	Name        string
	Description string
	Vector      []float64
	Category    string
}

// PatternMatch is a result from searching the pattern library.
type PatternMatch struct {
	Pattern    *HybridTimeSeriesPattern
	Similarity float64
}

// PatternLibrary stores and matches pre-built time-series anomaly patterns.
type PatternLibrary struct {
	patterns map[string]*HybridTimeSeriesPattern
	mu       sync.RWMutex
}

// NewPatternLibrary creates a library pre-loaded with common anomaly patterns.
func NewPatternLibrary() *PatternLibrary {
	lib := &PatternLibrary{
		patterns: make(map[string]*HybridTimeSeriesPattern),
	}
	for _, p := range builtinPatterns() {
		lib.patterns[p.Name] = p
	}
	return lib
}

// RegisterPattern adds or replaces a pattern in the library.
func (lib *PatternLibrary) RegisterPattern(pattern *HybridTimeSeriesPattern) {
	if pattern == nil {
		return
	}
	lib.mu.Lock()
	lib.patterns[pattern.Name] = pattern
	lib.mu.Unlock()
}

// FindSimilarPatterns returns the top-K patterns most similar to the given data.
func (lib *PatternLibrary) FindSimilarPatterns(data []float64, topK int) []*PatternMatch {
	if len(data) == 0 || topK <= 0 {
		return nil
	}

	lib.mu.RLock()
	defer lib.mu.RUnlock()

	var matches []*PatternMatch
	for _, p := range lib.patterns {
		sim := 1.0 - hybridCosineDistance(normalizeFloat64Vector(data), normalizeFloat64Vector(p.Vector))
		matches = append(matches, &PatternMatch{
			Pattern:    p,
			Similarity: sim,
		})
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Similarity > matches[j].Similarity
	})
	if len(matches) > topK {
		matches = matches[:topK]
	}
	return matches
}

func normalizeFloat64Vector(v []float64) []float64 {
	var norm float64
	for _, x := range v {
		norm += x * x
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		return v
	}
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = x / norm
	}
	return out
}

func builtinPatterns() []*HybridTimeSeriesPattern {
	return []*HybridTimeSeriesPattern{
		{
			Name:        "Spike",
			Description: "Sudden sharp increase followed by return to baseline",
			Vector:      []float64{0, 0, 0, 0, 1, 0, 0, 0},
			Category:    "anomaly",
		},
		{
			Name:        "Dip",
			Description: "Sudden sharp decrease followed by return to baseline",
			Vector:      []float64{0, 0, 0, 0, -1, 0, 0, 0},
			Category:    "anomaly",
		},
		{
			Name:        "GradualIncrease",
			Description: "Steady upward trend over time",
			Vector:      []float64{0, 0.14, 0.28, 0.42, 0.57, 0.71, 0.85, 1},
			Category:    "trend",
		},
		{
			Name:        "GradualDecrease",
			Description: "Steady downward trend over time",
			Vector:      []float64{1, 0.85, 0.71, 0.57, 0.42, 0.28, 0.14, 0},
			Category:    "trend",
		},
		{
			Name:        "Flatline",
			Description: "No variation, constant value",
			Vector:      []float64{0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5},
			Category:    "anomaly",
		},
		{
			Name:        "SeasonalBreak",
			Description: "Regular pattern interrupted by anomalous segment",
			Vector:      []float64{1, -1, 1, -1, 0, 0, 1, -1},
			Category:    "seasonal",
		},
		{
			Name:        "LevelShift",
			Description: "Abrupt permanent change from one level to another",
			Vector:      []float64{0, 0, 0, 0, 1, 1, 1, 1},
			Category:    "change",
		},
		{
			Name:        "Oscillation",
			Description: "Rapid alternation between high and low values",
			Vector:      []float64{1, -1, 1, -1, 1, -1, 1, -1},
			Category:    "periodic",
		},
	}
}
