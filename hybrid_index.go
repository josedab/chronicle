package chronicle

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// HybridIndexConfig configures the hybrid vector + time-series index.
type HybridIndexConfig struct {
	MaxDimensions          int
	DefaultDimension       int
	MaxVectorsPerPartition int
	DistanceMetric         DistanceMetric
	HNSWMaxLevel           int
	HNSWEfConstruction     int
	HNSWEfSearch           int
	HNSWMaxConnections     int
	BuildIndexThreshold    int
	TemporalBucketDuration time.Duration
}

// DefaultHybridIndexConfig returns sensible defaults for the hybrid index.
func DefaultHybridIndexConfig() HybridIndexConfig {
	return HybridIndexConfig{
		MaxDimensions:          4096,
		DefaultDimension:       128,
		MaxVectorsPerPartition: 100000,
		DistanceMetric:         DistanceCosine,
		HNSWMaxLevel:           16,
		HNSWEfConstruction:     200,
		HNSWEfSearch:           50,
		HNSWMaxConnections:     16,
		BuildIndexThreshold:    1000,
		TemporalBucketDuration: time.Hour,
	}
}

// --- Distance functions ---

func hybridCosineDistance(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 1.0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	sim := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	if sim > 1.0 {
		sim = 1.0
	} else if sim < -1.0 {
		sim = -1.0
	}
	return 1.0 - sim
}

func hybridEuclideanDistance(a, b []float64) float64 {
	if len(a) != len(b) {
		return math.MaxFloat64
	}
	var sum float64
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum)
}

func hybridDotProductDistance(a, b []float64) float64 {
	if len(a) != len(b) {
		return math.MaxFloat64
	}
	var dot float64
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot // negate so smaller = more similar
}

// --- HNSW Graph ---

// hnswSearchResult holds a single search result from the HNSW graph.
type hnswSearchResult struct {
	ID        string
	Distance  float64
	Timestamp int64
}

type hnswNode struct {
	id          string
	vector      []float64
	level       int
	connections map[int][]string // level -> neighbor IDs
	timestamp   int64
}

type hnswGraph struct {
	nodes      map[string]*hnswNode
	maxLevel   int
	entryPoint string
	config     *HybridIndexConfig
	mu         sync.RWMutex
}

func newHNSWGraph(config *HybridIndexConfig) *hnswGraph {
	return &hnswGraph{
		nodes:    make(map[string]*hnswNode),
		maxLevel: 0,
		config:   config,
	}
}

// randomLevel generates a random level for a new node using exponential distribution.
func (g *hnswGraph) randomLevel() int {
	ml := 1.0 / math.Log(float64(g.config.HNSWMaxConnections))
	level := int(-math.Log(rand.Float64()) * ml)
	if level > g.config.HNSWMaxLevel-1 {
		level = g.config.HNSWMaxLevel - 1
	}
	return level
}

func (g *hnswGraph) distance(a, b []float64) float64 {
	switch g.config.DistanceMetric {
	case DistanceEuclidean:
		return hybridEuclideanDistance(a, b)
	case DistanceDotProduct:
		return hybridDotProductDistance(a, b)
	default:
		return hybridCosineDistance(a, b)
	}
}

func (g *hnswGraph) insert(id string, vector []float64, timestamp int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	level := g.randomLevel()
	node := &hnswNode{
		id:          id,
		vector:      vector,
		level:       level,
		connections: make(map[int][]string),
		timestamp:   timestamp,
	}
	for l := 0; l <= level; l++ {
		node.connections[l] = nil
	}
	g.nodes[id] = node

	if len(g.nodes) == 1 {
		g.entryPoint = id
		g.maxLevel = level
		return
	}

	ep := g.entryPoint
	epNode := g.nodes[ep]
	if epNode == nil {
		g.entryPoint = id
		g.maxLevel = level
		return
	}

	// Traverse from top level down to the node's level + 1 (greedy walk).
	for l := g.maxLevel; l > level; l-- {
		ep = g.greedyClosest(vector, ep, l)
	}

	// For levels node participates in, do ef-construction search and connect.
	for l := min(level, g.maxLevel); l >= 0; l-- {
		candidates := g.searchLevel(vector, ep, g.config.HNSWEfConstruction, l)
		neighbors := g.selectNeighbors(candidates, g.config.HNSWMaxConnections)

		node.connections[l] = make([]string, len(neighbors))
		for i, n := range neighbors {
			node.connections[l][i] = n.ID
		}

		// Add bidirectional connections and prune if needed.
		for _, n := range neighbors {
			nNode := g.nodes[n.ID]
			if nNode == nil {
				continue
			}
			nNode.connections[l] = append(nNode.connections[l], id)
			if len(nNode.connections[l]) > g.config.HNSWMaxConnections*2 {
				g.pruneConnections(nNode, l)
			}
		}

		if len(candidates) > 0 {
			ep = candidates[0].ID
		}
	}

	if level > g.maxLevel {
		g.maxLevel = level
		g.entryPoint = id
	}
}

// greedyClosest walks greedily at a single level to find the closest node.
func (g *hnswGraph) greedyClosest(query []float64, ep string, level int) string {
	current := ep
	currentDist := g.distance(query, g.nodes[current].vector)
	for {
		changed := false
		node := g.nodes[current]
		if node == nil {
			break
		}
		for _, neighborID := range node.connections[level] {
			neighbor := g.nodes[neighborID]
			if neighbor == nil {
				continue
			}
			d := g.distance(query, neighbor.vector)
			if d < currentDist {
				current = neighborID
				currentDist = d
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return current
}

// searchLevel performs beam search at a given level returning ef closest results.
func (g *hnswGraph) searchLevel(query []float64, ep string, ef int, level int) []*hnswSearchResult {
	visited := make(map[string]bool)
	visited[ep] = true

	epNode := g.nodes[ep]
	if epNode == nil {
		return nil
	}
	epDist := g.distance(query, epNode.vector)

	candidates := []*hnswSearchResult{{ID: ep, Distance: epDist, Timestamp: epNode.timestamp}}
	results := []*hnswSearchResult{{ID: ep, Distance: epDist, Timestamp: epNode.timestamp}}

	for len(candidates) > 0 {
		// Pick closest candidate.
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Distance < candidates[j].Distance
		})
		closest := candidates[0]
		candidates = candidates[1:]

		// Furthest in results.
		furthestDist := results[len(results)-1].Distance
		for _, r := range results {
			if r.Distance > furthestDist {
				furthestDist = r.Distance
			}
		}

		if closest.Distance > furthestDist && len(results) >= ef {
			break
		}

		node := g.nodes[closest.ID]
		if node == nil {
			continue
		}
		for _, neighborID := range node.connections[level] {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			neighbor := g.nodes[neighborID]
			if neighbor == nil {
				continue
			}
			d := g.distance(query, neighbor.vector)

			worstResult := 0.0
			for _, r := range results {
				if r.Distance > worstResult {
					worstResult = r.Distance
				}
			}

			if len(results) < ef || d < worstResult {
				entry := &hnswSearchResult{ID: neighborID, Distance: d, Timestamp: neighbor.timestamp}
				candidates = append(candidates, entry)
				results = append(results, entry)
				if len(results) > ef {
					// Remove the worst result.
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

// selectNeighbors picks the closest neighbors from candidates, up to maxConn.
func (g *hnswGraph) selectNeighbors(candidates []*hnswSearchResult, maxConn int) []*hnswSearchResult {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Distance < candidates[j].Distance
	})
	if len(candidates) > maxConn {
		candidates = candidates[:maxConn]
	}
	return candidates
}

// pruneConnections trims a node's connections at a given level to maxConnections.
func (g *hnswGraph) pruneConnections(node *hnswNode, level int) {
	conns := node.connections[level]
	type scored struct {
		id   string
		dist float64
	}
	scoredConns := make([]scored, 0, len(conns))
	for _, cid := range conns {
		cn := g.nodes[cid]
		if cn == nil {
			continue
		}
		scoredConns = append(scoredConns, scored{id: cid, dist: g.distance(node.vector, cn.vector)})
	}
	sort.Slice(scoredConns, func(i, j int) bool {
		return scoredConns[i].dist < scoredConns[j].dist
	})
	max := g.config.HNSWMaxConnections
	if len(scoredConns) > max {
		scoredConns = scoredConns[:max]
	}
	node.connections[level] = make([]string, len(scoredConns))
	for i, s := range scoredConns {
		node.connections[level][i] = s.id
	}
}

func (g *hnswGraph) search(query []float64, k int, ef int) []*hnswSearchResult {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if len(g.nodes) == 0 || g.entryPoint == "" {
		return nil
	}
	if ef < k {
		ef = k
	}

	ep := g.entryPoint
	for l := g.maxLevel; l > 0; l-- {
		ep = g.greedyClosest(query, ep, l)
	}

	results := g.searchLevel(query, ep, ef, 0)
	if len(results) > k {
		results = results[:k]
	}
	return results
}

func (g *hnswGraph) delete(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	node, ok := g.nodes[id]
	if !ok {
		return
	}

	// Remove references from neighbors.
	for l, neighbors := range node.connections {
		for _, nid := range neighbors {
			nNode := g.nodes[nid]
			if nNode == nil {
				continue
			}
			filtered := make([]string, 0, len(nNode.connections[l]))
			for _, cid := range nNode.connections[l] {
				if cid != id {
					filtered = append(filtered, cid)
				}
			}
			nNode.connections[l] = filtered
		}
	}

	delete(g.nodes, id)

	if g.entryPoint == id {
		g.entryPoint = ""
		g.maxLevel = 0
		for nid, n := range g.nodes {
			if n.level > g.maxLevel || g.entryPoint == "" {
				g.entryPoint = nid
				g.maxLevel = n.level
			}
		}
	}
}

// --- Hybrid Point, Query, Result ---

// HybridPoint represents a data point with both vector embedding and time-series data.
type HybridPoint struct {
	ID        string
	Vector    []float64
	Timestamp int64
	Metric    string
	Tags      map[string]string
	Value     float64
}

// HybridSearchQuery defines a hybrid search over vector similarity and time ranges.
type HybridSearchQuery struct {
	Vector       []float64
	K            int
	StartTime    int64
	EndTime      int64
	MetricFilter string
	TagFilters   map[string]string
	MaxDistance  float64
	Ef           int
}

// HybridSearchResult is a single result from a hybrid search.
type HybridSearchResult struct {
	ID           string
	Distance     float64
	Timestamp    int64
	Metric       string
	Tags         map[string]string
	Value        float64
	PartitionKey string
}

// HybridIndexStats reports statistics about the hybrid index.
type HybridIndexStats struct {
	TotalPoints           int
	PartitionCount        int
	AvgPointsPerPartition float64
	TotalVectors          int
	MaxLevel              int
	MemoryEstimateBytes   int64
}

// --- Temporal Partitioned Index ---

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
