package chronicle

import (
	"fmt"
	"math"
	"sort"
)

// Pattern encoder, HNSW index implementation, and similarity search algorithms for semantic search.

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
	for {
		r, _ := randFloat64()
		if r >= 0.5 || level >= 16 {
			break
		}
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
	genID1, _ := generateID()
	return fmt.Sprintf("pat_%s", genID1)
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
