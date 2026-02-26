package chronicle

import (
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
