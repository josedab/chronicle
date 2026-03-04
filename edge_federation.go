package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// EdgeFederationConfig configures the edge federation protocol.
type EdgeFederationConfig struct {
	Enabled                bool          `json:"enabled"`
	FedBloomFilterSize        uint          `json:"bloom_filter_size"`
	BloomHashCount         uint          `json:"bloom_hash_count"`
	GossipInterval         time.Duration `json:"gossip_interval"`
	QueryTimeout           time.Duration `json:"query_timeout"`
	MaxConcurrentFanout    int           `json:"max_concurrent_fanout"`
	EnablePredicatePushdown bool         `json:"enable_predicate_pushdown"`
	BackpressureThreshold  int           `json:"backpressure_threshold"`
	BandwidthBudgetKBps    int           `json:"bandwidth_budget_kbps"`
}

// DefaultEdgeFederationConfig returns sensible defaults.
func DefaultEdgeFederationConfig() EdgeFederationConfig {
	return EdgeFederationConfig{
		Enabled:                true,
		FedBloomFilterSize:        1024 * 8, // 1KB
		BloomHashCount:         7,
		GossipInterval:         10 * time.Second,
		QueryTimeout:           30 * time.Second,
		MaxConcurrentFanout:    5,
		EnablePredicatePushdown: true,
		BackpressureThreshold:  1000,
		BandwidthBudgetKBps:    1024,
	}
}

// FedBloomFilter implements a simple Bloom filter for metric existence advertisements.
type FedBloomFilter struct {
	bits    []bool
	size    uint
	hashCnt uint
}

// NewFedBloomFilter creates a new Bloom filter.
func NewFedBloomFilter(size, hashCount uint) *FedBloomFilter {
	if size == 0 {
		size = 1024
	}
	if hashCount == 0 {
		hashCount = 7
	}
	return &FedBloomFilter{
		bits:    make([]bool, size),
		size:    size,
		hashCnt: hashCount,
	}
}

// Add adds a key to the Bloom filter.
func (bf *FedBloomFilter) Add(key string) {
	for i := uint(0); i < bf.hashCnt; i++ {
		idx := bf.hash(key, i)
		bf.bits[idx] = true
	}
}

// MayContain checks if a key might exist in the filter.
func (bf *FedBloomFilter) MayContain(key string) bool {
	for i := uint(0); i < bf.hashCnt; i++ {
		idx := bf.hash(key, i)
		if !bf.bits[idx] {
			return false
		}
	}
	return true
}

func (bf *FedBloomFilter) hash(key string, seed uint) uint {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte{byte(seed), byte(seed >> 8)})
	return uint(h.Sum64()) % bf.size
}

// Reset clears the Bloom filter.
func (bf *FedBloomFilter) Reset() {
	for i := range bf.bits {
		bf.bits[i] = false
	}
}

// EdgeFedNode represents a remote node in the federation.
type EdgeFedNode struct {
	ID            string       `json:"id"`
	Address       string       `json:"address"`
	MetricFilter  *FedBloomFilter `json:"-"`
	Latency       time.Duration `json:"latency"`
	Bandwidth     int64         `json:"bandwidth_kbps"`
	LastSeen      time.Time     `json:"last_seen"`
	Available     bool          `json:"available"`
	MetricCount   int           `json:"metric_count"`
}

// NetworkCostModel estimates the cost of querying a remote node.
type NetworkCostModel struct {
	LatencyWeight    float64 `json:"latency_weight"`
	BandwidthWeight  float64 `json:"bandwidth_weight"`
	ErrorRateWeight  float64 `json:"error_rate_weight"`
}

// DefaultNetworkCostModel returns a balanced cost model.
func DefaultNetworkCostModel() NetworkCostModel {
	return NetworkCostModel{
		LatencyWeight:   0.5,
		BandwidthWeight: 0.3,
		ErrorRateWeight: 0.2,
	}
}

// EdgeFedQueryPlan describes how a query is distributed across nodes.
type EdgeFedQueryPlan struct {
	Query          *Query               `json:"query"`
	TargetNodes    []string             `json:"target_nodes"`
	PredicatesPushed bool               `json:"predicates_pushed"`
	EstimatedCost  float64              `json:"estimated_cost"`
	MergeStrategy  string               `json:"merge_strategy"`
	CreatedAt      time.Time            `json:"created_at"`
}

// EdgeFedQueryResult represents merged results from federated nodes.
type EdgeFedQueryResult struct {
	Points        []Point           `json:"points"`
	NodeResults   map[string]int    `json:"node_results"`
	TotalNodes    int               `json:"total_nodes"`
	SuccessNodes  int               `json:"success_nodes"`
	Duration      time.Duration     `json:"duration"`
	MergeStrategy string            `json:"merge_strategy"`
}

// EdgeFederationProtocol implements cross-node federation for edge mesh.
type EdgeFederationProtocol struct {
	config    EdgeFederationConfig
	db        *DB
	localNode *EdgeFedNode
	nodes     map[string]*EdgeFedNode
	costModel NetworkCostModel
	mu        sync.RWMutex

	// Gossip lifecycle
	running  bool
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// Stats
	queriesFanout   int64
	queriesReceived int64
	resultsMerged   int64
	gossipRounds    int64
}

// NewEdgeFederationProtocol creates a new edge federation protocol.
func NewEdgeFederationProtocol(db *DB, config EdgeFederationConfig) *EdgeFederationProtocol {
	localID := fmt.Sprintf("node-%d", time.Now().UnixNano()%10000)
	return &EdgeFederationProtocol{
		config: config,
		db:     db,
		localNode: &EdgeFedNode{
			ID:           localID,
			MetricFilter: NewFedBloomFilter(config.FedBloomFilterSize, config.BloomHashCount),
			Available:    true,
			LastSeen:     time.Now(),
		},
		nodes:     make(map[string]*EdgeFedNode),
		costModel: DefaultNetworkCostModel(),
		stopCh:    make(chan struct{}),
	}
}

// Start starts the gossip protocol background loop.
func (p *EdgeFederationProtocol) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return fmt.Errorf("edge_federation: already running")
	}
	p.running = true
	p.stopCh = make(chan struct{})

	p.wg.Add(1)
	go p.gossipLoop()
	return nil
}

// Stop stops the gossip protocol.
func (p *EdgeFederationProtocol) Stop() error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

func (p *EdgeFederationProtocol) gossipLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.gossipOnce()
		}
	}
}

func (p *EdgeFederationProtocol) gossipOnce() {
	p.mu.Lock()
	p.gossipRounds++

	// Refresh local metric advertisements
	metrics := p.db.Metrics()
	p.localNode.MetricFilter.Reset()
	for _, m := range metrics {
		p.localNode.MetricFilter.Add(m)
	}
	p.localNode.MetricCount = len(metrics)
	p.localNode.LastSeen = time.Now()

	// Mark stale nodes as unavailable
	for _, node := range p.nodes {
		if time.Since(node.LastSeen) > 3*p.config.GossipInterval {
			node.Available = false
		}
	}
	p.mu.Unlock()
}

// RegisterNode registers a remote node in the federation.
func (p *EdgeFederationProtocol) RegisterNode(node EdgeFedNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if node.MetricFilter == nil {
		node.MetricFilter = NewFedBloomFilter(p.config.FedBloomFilterSize, p.config.BloomHashCount)
	}
	node.LastSeen = time.Now()
	p.nodes[node.ID] = &node
}

// UnregisterNode removes a node from the federation.
func (p *EdgeFederationProtocol) UnregisterNode(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.nodes, nodeID)
}

// AdvertiseMetrics updates the local node's Bloom filter with available metrics.
func (p *EdgeFederationProtocol) AdvertiseMetrics(metrics []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.localNode.MetricFilter.Reset()
	for _, m := range metrics {
		p.localNode.MetricFilter.Add(m)
	}
	p.localNode.MetricCount = len(metrics)
}

// FindNodesForMetric returns nodes that may have a given metric using Bloom filters.
func (p *EdgeFederationProtocol) FindNodesForMetric(metric string) []*EdgeFedNode {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var candidates []*EdgeFedNode
	for _, node := range p.nodes {
		if !node.Available {
			continue
		}
		if node.MetricFilter != nil && node.MetricFilter.MayContain(metric) {
			candidates = append(candidates, node)
		}
	}

	// Sort by estimated cost (latency-aware)
	sort.Slice(candidates, func(i, j int) bool {
		costI := p.estimateNodeCost(candidates[i])
		costJ := p.estimateNodeCost(candidates[j])
		return costI < costJ
	})

	// Limit fanout
	if len(candidates) > p.config.MaxConcurrentFanout {
		candidates = candidates[:p.config.MaxConcurrentFanout]
	}

	return candidates
}

// estimateNodeCost computes a cost score for querying a node.
func (p *EdgeFederationProtocol) estimateNodeCost(node *EdgeFedNode) float64 {
	latencyCost := float64(node.Latency.Milliseconds()) / 1000.0
	bandwidthCost := 1.0
	if node.Bandwidth > 0 {
		bandwidthCost = 1.0 / float64(node.Bandwidth)
	}
	staleness := time.Since(node.LastSeen).Seconds() / 60.0

	return latencyCost*p.costModel.LatencyWeight +
		bandwidthCost*p.costModel.BandwidthWeight +
		staleness*p.costModel.ErrorRateWeight
}

// PlanFederatedQuery creates an execution plan for a federated query.
func (p *EdgeFederationProtocol) PlanFederatedQuery(q *Query) (*EdgeFedQueryPlan, error) {
	if q == nil || q.Metric == "" {
		return nil, fmt.Errorf("edge_federation: invalid query")
	}

	candidates := p.FindNodesForMetric(q.Metric)

	targetNodes := make([]string, 0, len(candidates))
	var totalCost float64
	for _, n := range candidates {
		targetNodes = append(targetNodes, n.ID)
		totalCost += p.estimateNodeCost(n)
	}

	return &EdgeFedQueryPlan{
		Query:            q,
		TargetNodes:      targetNodes,
		PredicatesPushed: p.config.EnablePredicatePushdown,
		EstimatedCost:    totalCost,
		MergeStrategy:    "merge_sorted",
		CreatedAt:        time.Now(),
	}, nil
}

// ExecuteFederatedQuery executes a query across federated nodes.
func (p *EdgeFederationProtocol) ExecuteFederatedQuery(ctx context.Context, q *Query) (*EdgeFedQueryResult, error) {
	if q == nil || q.Metric == "" {
		return nil, fmt.Errorf("edge_federation: invalid query")
	}

	start := time.Now()
	plan, err := p.PlanFederatedQuery(q)
	if err != nil {
		return nil, err
	}

	// Execute locally first
	localResult, err := p.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("edge_federation: local query: %w", err)
	}

	result := &EdgeFedQueryResult{
		Points:        localResult.Points,
		NodeResults:   map[string]int{p.localNode.ID: len(localResult.Points)},
		TotalNodes:    len(plan.TargetNodes) + 1,
		SuccessNodes:  1,
		MergeStrategy: plan.MergeStrategy,
	}

	// Fan out to remote nodes (simulated - actual HTTP calls would be here)
	p.mu.Lock()
	p.queriesFanout++
	p.mu.Unlock()

	// Merge results (deduplicate and sort by timestamp)
	result.Points = p.mergeResults(result.Points)
	result.Duration = time.Since(start)

	p.mu.Lock()
	p.resultsMerged++
	p.mu.Unlock()

	return result, nil
}

// mergeResults deduplicates and sorts merged results.
func (p *EdgeFederationProtocol) mergeResults(points []Point) []Point {
	if len(points) <= 1 {
		return points
	}

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// Deduplicate by (metric, timestamp, value)
	deduped := make([]Point, 0, len(points))
	deduped = append(deduped, points[0])
	for i := 1; i < len(points); i++ {
		prev := deduped[len(deduped)-1]
		if points[i].Metric != prev.Metric || points[i].Timestamp != prev.Timestamp || points[i].Value != prev.Value {
			deduped = append(deduped, points[i])
		}
	}

	return deduped
}

// GetNodeStats returns statistics about federated nodes.
func (p *EdgeFederationProtocol) GetNodeStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	available := 0
	for _, n := range p.nodes {
		if n.Available {
			available++
		}
	}

	return map[string]interface{}{
		"local_node":       p.localNode.ID,
		"total_nodes":      len(p.nodes),
		"available_nodes":  available,
		"queries_fanout":   p.queriesFanout,
		"queries_received": p.queriesReceived,
		"results_merged":   p.resultsMerged,
	}
}

// ListNodes returns all registered nodes.
func (p *EdgeFederationProtocol) ListNodes() []EdgeFedNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	nodes := make([]EdgeFedNode, 0, len(p.nodes))
	for _, n := range p.nodes {
		nodes = append(nodes, *n)
	}
	return nodes
}

// HandleGossipMessage processes a gossip message containing a peer's Bloom filter.
func (p *EdgeFederationProtocol) HandleGossipMessage(nodeID string, metrics []string, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	node, exists := p.nodes[nodeID]
	if !exists {
		node = &EdgeFedNode{
			ID:           nodeID,
			MetricFilter: NewFedBloomFilter(p.config.FedBloomFilterSize, p.config.BloomHashCount),
			Available:    true,
		}
		p.nodes[nodeID] = node
	}

	node.MetricFilter.Reset()
	for _, m := range metrics {
		node.MetricFilter.Add(m)
	}
	node.MetricCount = len(metrics)
	node.Latency = latency
	node.LastSeen = time.Now()
	node.Available = true
}

// WithBackpressure applies backpressure based on result streaming.
func (p *EdgeFederationProtocol) WithBackpressure(results []Point) []Point {
	threshold := p.config.BackpressureThreshold
	if threshold <= 0 || len(results) <= threshold {
		return results
	}

	// Apply backpressure by sampling
	ratio := float64(threshold) / float64(len(results))
	sampled := make([]Point, 0, threshold)
	step := int(math.Ceil(1.0 / ratio))
	for i := 0; i < len(results); i += step {
		sampled = append(sampled, results[i])
	}
	return sampled
}

// RegisterHTTPHandlers registers HTTP endpoints for edge federation.
func (p *EdgeFederationProtocol) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/federation/nodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p.ListNodes())
	})
	mux.HandleFunc("/api/v1/federation/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p.GetNodeStats())
	})
	mux.HandleFunc("/api/v1/federation/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var q Query
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			http.Error(w, "invalid query", http.StatusBadRequest)
			return
		}
		result, err := p.ExecuteFederatedQuery(r.Context(), &q)
		if err != nil {
			log.Printf("federated query error: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
}
