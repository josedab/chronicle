package chronicle

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// AutoShardingConfig configures the zero-config auto-sharding engine.
type AutoShardingConfig struct {
	// Enabled enables auto-sharding.
	Enabled bool `json:"enabled"`

	// VirtualNodes is the number of virtual nodes per physical node in the hash ring.
	VirtualNodes int `json:"virtual_nodes"`

	// RebalanceThreshold is the imbalance ratio to trigger rebalancing.
	RebalanceThreshold float64 `json:"rebalance_threshold"`

	// RebalanceInterval is how often to check for imbalance.
	RebalanceInterval time.Duration `json:"rebalance_interval"`

	// MaxShardsPerNode is the maximum number of shards per node.
	MaxShardsPerNode int `json:"max_shards_per_node"`

	// ReplicationFactor is the number of replicas per shard.
	ReplicationFactor int `json:"replication_factor"`

	// MigrationBatchSize is the number of shards to migrate at once.
	MigrationBatchSize int `json:"migration_batch_size"`
}

// DefaultAutoShardingConfig returns sensible defaults for auto-sharding.
func DefaultAutoShardingConfig() AutoShardingConfig {
	return AutoShardingConfig{
		Enabled:            true,
		VirtualNodes:       64,
		RebalanceThreshold: 0.05,
		RebalanceInterval:  30 * time.Second,
		MaxShardsPerNode:   256,
		ReplicationFactor:  2,
		MigrationBatchSize: 10,
	}
}

// ShardState represents the current state of a shard.
type ShardState int

const (
	// ShardActive means the shard is actively serving data.
	ShardActive ShardState = iota
	// ShardMigrating means the shard is being migrated to another node.
	ShardMigrating
	// ShardDraining means the shard is draining before removal.
	ShardDraining
	// ShardInactive means the shard is not serving data.
	ShardInactive
)

// String returns the string representation of ShardState.
func (s ShardState) String() string {
	switch s {
	case ShardActive:
		return "active"
	case ShardMigrating:
		return "migrating"
	case ShardDraining:
		return "draining"
	case ShardInactive:
		return "inactive"
	default:
		return "unknown"
	}
}

// ShardNode represents a node in the sharding cluster.
type ShardNode struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	State    string            `json:"state"` // "active", "draining", "joining"
	Capacity int64             `json:"capacity"`
	Used     int64             `json:"used"`
	Shards   []string          `json:"shards"`
	JoinedAt time.Time         `json:"joined_at"`
	LastSeen time.Time         `json:"last_seen"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Shard represents a data partition.
type Shard struct {
	ID           string     `json:"id"`
	NodeID       string     `json:"node_id"`
	State        ShardState `json:"state"`
	StartToken   uint64     `json:"start_token"`
	EndToken     uint64     `json:"end_token"`
	MetricCount  int64      `json:"metric_count"`
	PointCount   int64      `json:"point_count"`
	SizeBytes    int64      `json:"size_bytes"`
	CreatedAt    time.Time  `json:"created_at"`
	LastAccessed time.Time  `json:"last_accessed"`
}

// ShardAssignment maps a metric key to a shard and node.
type ShardAssignment struct {
	MetricKey string `json:"metric_key"`
	ShardID   string `json:"shard_id"`
	NodeID    string `json:"node_id"`
	Token     uint64 `json:"token"`
}

// ShardMigrationPlan describes a shard migration operation.
type ShardMigrationPlan struct {
	ID          string    `json:"id"`
	FromNode    string    `json:"from_node"`
	ToNode      string    `json:"to_node"`
	Shards      []string  `json:"shards"`
	State       string    `json:"state"` // "planned", "running", "completed", "failed"
	Progress    float64   `json:"progress"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	BytesMoved  int64     `json:"bytes_moved"`
	Error       string    `json:"error,omitempty"`
}

// shardHashRing provides consistent hashing with virtual nodes for shard routing.
type shardHashRing struct {
	mu           sync.RWMutex
	virtualNodes int
	ring         []shardHashEntry
	nodeMap      map[string]bool
}

type shardHashEntry struct {
	hash    uint64
	nodeID  string
	virtual int
}

func newShardHashRing(virtualNodes int) *shardHashRing {
	if virtualNodes <= 0 {
		virtualNodes = 64
	}
	return &shardHashRing{
		virtualNodes: virtualNodes,
		nodeMap:      make(map[string]bool),
	}
}

// addNode adds a node with virtual nodes to the hash ring.
func (r *shardHashRing) addNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodeMap[nodeID] {
		return
	}
	r.nodeMap[nodeID] = true

	for i := 0; i < r.virtualNodes; i++ {
		key := fmt.Sprintf("%s#%d", nodeID, i)
		h := fnv.New64a()
		h.Write([]byte(key))
		r.ring = append(r.ring, shardHashEntry{
			hash:    h.Sum64(),
			nodeID:  nodeID,
			virtual: i,
		})
	}
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i].hash < r.ring[j].hash
	})
}

// removeNode removes all entries for a node from the hash ring.
func (r *shardHashRing) removeNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.nodeMap, nodeID)
	filtered := r.ring[:0]
	for _, e := range r.ring {
		if e.nodeID != nodeID {
			filtered = append(filtered, e)
		}
	}
	r.ring = filtered
}

// getNode returns the node responsible for a given key.
func (r *shardHashRing) getNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return ""
	}

	h := fnv.New64a()
	h.Write([]byte(key))
	hash := h.Sum64()

	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].hash >= hash
	})
	if idx >= len(r.ring) {
		idx = 0
	}
	return r.ring[idx].nodeID
}

// getNodes returns up to count distinct nodes for a key (for replication).
func (r *shardHashRing) getNodes(key string, count int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return nil
	}

	h := fnv.New64a()
	h.Write([]byte(key))
	hash := h.Sum64()

	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].hash >= hash
	})

	seen := make(map[string]bool)
	var nodes []string
	for i := 0; i < len(r.ring) && len(nodes) < count; i++ {
		entry := r.ring[(idx+i)%len(r.ring)]
		if !seen[entry.nodeID] {
			seen[entry.nodeID] = true
			nodes = append(nodes, entry.nodeID)
		}
	}
	return nodes
}

// hashKey computes the FNV-64a hash for a key.
func (r *shardHashRing) hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// AutoShardingStats tracks auto-sharding metrics.
type AutoShardingStats struct {
	TotalNodes          int       `json:"total_nodes"`
	TotalShards         int       `json:"total_shards"`
	ActiveMigrations    int       `json:"active_migrations"`
	CompletedMigrations int64     `json:"completed_migrations"`
	TotalRebalances     int64     `json:"total_rebalances"`
	ImbalanceRatio      float64   `json:"imbalance_ratio"`
	LastRebalance       time.Time `json:"last_rebalance"`
	BytesMigrated       int64     `json:"bytes_migrated"`
}

// AutoShardingEngine implements zero-config auto-sharding with consistent hashing.
type AutoShardingEngine struct {
	db     *DB
	config AutoShardingConfig

	mu          sync.RWMutex
	ring        *shardHashRing
	nodes       map[string]*ShardNode
	shards      map[string]*Shard
	assignments map[string]*ShardAssignment
	migrations  map[string]*ShardMigrationPlan

	stopCh  chan struct{}
	running bool
	stats   AutoShardingStats
}

// NewAutoShardingEngine creates a new auto-sharding engine.
func NewAutoShardingEngine(db *DB, cfg AutoShardingConfig) *AutoShardingEngine {
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 64
	}
	if cfg.RebalanceThreshold <= 0 {
		cfg.RebalanceThreshold = 0.05
	}
	if cfg.RebalanceInterval <= 0 {
		cfg.RebalanceInterval = 30 * time.Second
	}
	if cfg.MaxShardsPerNode <= 0 {
		cfg.MaxShardsPerNode = 256
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 2
	}
	if cfg.MigrationBatchSize <= 0 {
		cfg.MigrationBatchSize = 10
	}

	return &AutoShardingEngine{
		db:          db,
		config:      cfg,
		ring:        newShardHashRing(cfg.VirtualNodes),
		nodes:       make(map[string]*ShardNode),
		shards:      make(map[string]*Shard),
		assignments: make(map[string]*ShardAssignment),
		migrations:  make(map[string]*ShardMigrationPlan),
		stopCh:      make(chan struct{}),
	}
}

// Start begins the auto-sharding background rebalance loop.
func (e *AutoShardingEngine) Start() error {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return fmt.Errorf("auto-sharding engine: already running")
	}
	e.running = true
	e.mu.Unlock()

	go e.rebalanceLoop()
	return nil
}

// Stop gracefully shuts down the auto-sharding engine.
func (e *AutoShardingEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}
	e.running = false
	close(e.stopCh)
	return nil
}

func (e *AutoShardingEngine) rebalanceLoop() {
	ticker := time.NewTicker(e.config.RebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			ratio := e.GetImbalanceRatio()
			if ratio > e.config.RebalanceThreshold {
				e.Rebalance()
			}
		}
	}
}

// AddNode adds a node to the sharding cluster.
func (e *AutoShardingEngine) AddNode(node ShardNode) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if node.ID == "" {
		return fmt.Errorf("auto-sharding: node ID is required")
	}
	if _, exists := e.nodes[node.ID]; exists {
		return fmt.Errorf("auto-sharding: node %s already exists", node.ID)
	}

	now := time.Now()
	if node.JoinedAt.IsZero() {
		node.JoinedAt = now
	}
	node.LastSeen = now
	if node.State == "" {
		node.State = "active"
	}
	if node.Shards == nil {
		node.Shards = []string{}
	}
	if node.Metadata == nil {
		node.Metadata = make(map[string]string)
	}

	e.nodes[node.ID] = &node
	e.ring.addNode(node.ID)
	e.stats.TotalNodes = len(e.nodes)

	return nil
}

// RemoveNode drains and removes a node from the sharding cluster.
func (e *AutoShardingEngine) RemoveNode(nodeID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	node, exists := e.nodes[nodeID]
	if !exists {
		return fmt.Errorf("auto-sharding: node %s not found", nodeID)
	}

	// Mark shards as draining
	for _, shardID := range node.Shards {
		if shard, ok := e.shards[shardID]; ok {
			shard.State = ShardDraining
		}
	}

	node.State = "draining"
	e.ring.removeNode(nodeID)

	// Reassign shards to other nodes
	for _, shardID := range node.Shards {
		shard, ok := e.shards[shardID]
		if !ok {
			continue
		}
		newNodeID := e.ring.getNode(shard.ID)
		if newNodeID == "" {
			shard.State = ShardInactive
			continue
		}
		shard.NodeID = newNodeID
		shard.State = ShardActive
		if newNode, nok := e.nodes[newNodeID]; nok {
			newNode.Shards = append(newNode.Shards, shardID)
		}
	}

	delete(e.nodes, nodeID)
	e.stats.TotalNodes = len(e.nodes)

	return nil
}

// buildMetricKey constructs a canonical metric key from metric name and tags.
func buildMetricKey(metric string, tags map[string]string) string {
	if len(tags) == 0 {
		return metric
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(metric)
	for _, k := range keys {
		sb.WriteByte(',')
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(tags[k])
	}
	return sb.String()
}

// GetNodeForMetric returns the node responsible for a given metric.
func (e *AutoShardingEngine) GetNodeForMetric(metric string, tags map[string]string) (*ShardNode, error) {
	key := buildMetricKey(metric, tags)

	e.mu.RLock()
	defer e.mu.RUnlock()

	nodeID := e.ring.getNode(key)
	if nodeID == "" {
		return nil, fmt.Errorf("auto-sharding: no nodes available")
	}

	node, ok := e.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("auto-sharding: node %s not found in cluster", nodeID)
	}

	return node, nil
}

// AssignShard assigns a metric key to a shard and node.
func (e *AutoShardingEngine) AssignShard(metricKey string) (*ShardAssignment, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if existing, ok := e.assignments[metricKey]; ok {
		return existing, nil
	}

	nodeID := e.ring.getNode(metricKey)
	if nodeID == "" {
		return nil, fmt.Errorf("auto-sharding: no nodes available")
	}

	node, ok := e.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("auto-sharding: node %s not found", nodeID)
	}

	token := e.ring.hashKey(metricKey)
	shardID := fmt.Sprintf("shard-%s-%d", nodeID, token%uint64(e.config.MaxShardsPerNode))

	// Create shard if it doesn't exist
	if _, exists := e.shards[shardID]; !exists {
		e.shards[shardID] = &Shard{
			ID:           shardID,
			NodeID:       nodeID,
			State:        ShardActive,
			StartToken:   token,
			EndToken:     token,
			MetricCount:  0,
			PointCount:   0,
			SizeBytes:    0,
			CreatedAt:    time.Now(),
			LastAccessed: time.Now(),
		}
		node.Shards = appendUnique(node.Shards, shardID)
		e.stats.TotalShards = len(e.shards)
	}

	shard := e.shards[shardID]
	shard.MetricCount++
	shard.LastAccessed = time.Now()

	assignment := &ShardAssignment{
		MetricKey: metricKey,
		ShardID:   shardID,
		NodeID:    nodeID,
		Token:     token,
	}
	e.assignments[metricKey] = assignment

	return assignment, nil
}

func appendUnique(slice []string, item string) []string {
	for _, s := range slice {
		if s == item {
			return slice
		}
	}
	return append(slice, item)
}

// GetImbalanceRatio computes the current imbalance ratio across nodes.
// Returns 0.0 for perfectly balanced, higher values indicate more imbalance.
func (e *AutoShardingEngine) GetImbalanceRatio() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.nodes) <= 1 {
		return 0.0
	}

	var totalShards int
	counts := make([]int, 0, len(e.nodes))
	for _, node := range e.nodes {
		n := len(node.Shards)
		counts = append(counts, n)
		totalShards += n
	}

	if totalShards == 0 {
		return 0.0
	}

	avg := float64(totalShards) / float64(len(e.nodes))
	if avg == 0 {
		return 0.0
	}

	var maxDev float64
	for _, c := range counts {
		dev := math.Abs(float64(c)-avg) / avg
		if dev > maxDev {
			maxDev = dev
		}
	}

	return maxDev
}

// Rebalance computes and executes a rebalance plan if imbalance exceeds threshold.
func (e *AutoShardingEngine) Rebalance() (*ShardMigrationPlan, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.nodes) <= 1 {
		return nil, fmt.Errorf("auto-sharding: need at least 2 nodes to rebalance")
	}

	// Find most and least loaded nodes
	var maxNode, minNode *ShardNode
	var maxCount, minCount int
	first := true
	for _, node := range e.nodes {
		n := len(node.Shards)
		if first || n > maxCount {
			maxCount = n
			maxNode = node
		}
		if first || n < minCount {
			minCount = n
			minNode = node
		}
		first = false
	}

	if maxNode == nil || minNode == nil || maxNode.ID == minNode.ID {
		return nil, fmt.Errorf("auto-sharding: no rebalance needed")
	}

	avg := float64(maxCount+minCount) / 2.0
	if avg == 0 {
		return nil, fmt.Errorf("auto-sharding: no shards to rebalance")
	}

	imbalance := float64(maxCount-minCount) / avg
	if imbalance <= e.config.RebalanceThreshold {
		return nil, fmt.Errorf("auto-sharding: imbalance %.4f below threshold %.4f", imbalance, e.config.RebalanceThreshold)
	}

	// Determine shards to migrate
	shardsToMove := (maxCount - minCount) / 2
	if shardsToMove <= 0 {
		shardsToMove = 1
	}
	if shardsToMove > e.config.MigrationBatchSize {
		shardsToMove = e.config.MigrationBatchSize
	}
	if shardsToMove > len(maxNode.Shards) {
		shardsToMove = len(maxNode.Shards)
	}

	migratingShards := make([]string, shardsToMove)
	copy(migratingShards, maxNode.Shards[:shardsToMove])

	now := time.Now()
	plan := &ShardMigrationPlan{
		ID:        fmt.Sprintf("migration-%d", now.UnixNano()),
		FromNode:  maxNode.ID,
		ToNode:    minNode.ID,
		Shards:    migratingShards,
		State:     "running",
		Progress:  0,
		StartedAt: now,
	}
	e.migrations[plan.ID] = plan

	e.stats.ActiveMigrations++

	// Execute migration
	var bytesMovedTotal int64
	for i, shardID := range migratingShards {
		shard, ok := e.shards[shardID]
		if !ok {
			continue
		}
		shard.State = ShardMigrating
		shard.NodeID = minNode.ID
		shard.State = ShardActive

		bytesMovedTotal += shard.SizeBytes
		minNode.Shards = appendUnique(minNode.Shards, shardID)

		plan.Progress = float64(i+1) / float64(len(migratingShards))
	}

	// Remove migrated shards from source node
	remaining := make([]string, 0, len(maxNode.Shards))
	migratedSet := make(map[string]bool, len(migratingShards))
	for _, s := range migratingShards {
		migratedSet[s] = true
	}
	for _, s := range maxNode.Shards {
		if !migratedSet[s] {
			remaining = append(remaining, s)
		}
	}
	maxNode.Shards = remaining

	plan.State = "completed"
	plan.Progress = 1.0
	plan.CompletedAt = time.Now()
	plan.BytesMoved = bytesMovedTotal

	e.stats.ActiveMigrations--
	e.stats.CompletedMigrations++
	e.stats.TotalRebalances++
	e.stats.BytesMigrated += bytesMovedTotal
	e.stats.LastRebalance = time.Now()
	e.stats.ImbalanceRatio = e.getImbalanceRatioLocked()

	return plan, nil
}

// getImbalanceRatioLocked computes imbalance without locking (caller must hold lock).
func (e *AutoShardingEngine) getImbalanceRatioLocked() float64 {
	if len(e.nodes) <= 1 {
		return 0.0
	}

	var totalShards int
	counts := make([]int, 0, len(e.nodes))
	for _, node := range e.nodes {
		n := len(node.Shards)
		counts = append(counts, n)
		totalShards += n
	}

	if totalShards == 0 {
		return 0.0
	}

	avg := float64(totalShards) / float64(len(e.nodes))
	if avg == 0 {
		return 0.0
	}

	var maxDev float64
	for _, c := range counts {
		dev := math.Abs(float64(c)-avg) / avg
		if dev > maxDev {
			maxDev = dev
		}
	}

	return maxDev
}

// MigrateShard migrates a single shard to a target node.
func (e *AutoShardingEngine) MigrateShard(shardID string, toNodeID string) (*ShardMigrationPlan, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	shard, ok := e.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("auto-sharding: shard %s not found", shardID)
	}

	toNode, ok := e.nodes[toNodeID]
	if !ok {
		return nil, fmt.Errorf("auto-sharding: target node %s not found", toNodeID)
	}

	fromNodeID := shard.NodeID
	if fromNodeID == toNodeID {
		return nil, fmt.Errorf("auto-sharding: shard %s already on node %s", shardID, toNodeID)
	}

	now := time.Now()
	plan := &ShardMigrationPlan{
		ID:        fmt.Sprintf("migration-%d", now.UnixNano()),
		FromNode:  fromNodeID,
		ToNode:    toNodeID,
		Shards:    []string{shardID},
		State:     "running",
		StartedAt: now,
	}
	e.migrations[plan.ID] = plan
	e.stats.ActiveMigrations++

	// Execute migration
	shard.State = ShardMigrating
	shard.NodeID = toNodeID
	shard.State = ShardActive

	toNode.Shards = appendUnique(toNode.Shards, shardID)

	// Remove from source node
	if fromNode, fok := e.nodes[fromNodeID]; fok {
		remaining := make([]string, 0, len(fromNode.Shards))
		for _, s := range fromNode.Shards {
			if s != shardID {
				remaining = append(remaining, s)
			}
		}
		fromNode.Shards = remaining
	}

	plan.State = "completed"
	plan.Progress = 1.0
	plan.CompletedAt = time.Now()
	plan.BytesMoved = shard.SizeBytes

	e.stats.ActiveMigrations--
	e.stats.CompletedMigrations++
	e.stats.BytesMigrated += shard.SizeBytes

	return plan, nil
}

// ListNodes returns all nodes in the cluster.
func (e *AutoShardingEngine) ListNodes() []*ShardNode {
	e.mu.RLock()
	defer e.mu.RUnlock()

	nodes := make([]*ShardNode, 0, len(e.nodes))
	for _, node := range e.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// ListShards returns all shards in the cluster.
func (e *AutoShardingEngine) ListShards() []*Shard {
	e.mu.RLock()
	defer e.mu.RUnlock()

	shards := make([]*Shard, 0, len(e.shards))
	for _, shard := range e.shards {
		shards = append(shards, shard)
	}
	return shards
}

// ListMigrations returns all migration plans.
func (e *AutoShardingEngine) ListMigrations() []*ShardMigrationPlan {
	e.mu.RLock()
	defer e.mu.RUnlock()

	plans := make([]*ShardMigrationPlan, 0, len(e.migrations))
	for _, plan := range e.migrations {
		plans = append(plans, plan)
	}
	return plans
}

// GetShard returns a shard by ID, or nil if not found.
func (e *AutoShardingEngine) GetShard(id string) *Shard {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.shards[id]
}

// Stats returns the current auto-sharding statistics.
func (e *AutoShardingEngine) Stats() AutoShardingStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := e.stats
	stats.TotalNodes = len(e.nodes)
	stats.TotalShards = len(e.shards)
	stats.ImbalanceRatio = e.getImbalanceRatioLocked()
	return stats
}

// RegisterHTTPHandlers registers auto-sharding HTTP endpoints.
func (e *AutoShardingEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sharding/nodes", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListNodes())
		case http.MethodPost:
			var node ShardNode
			if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
				http.Error(w, "invalid request body", http.StatusBadRequest)
				return
			}
			if err := e.AddNode(node); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(node)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/sharding/nodes/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		nodeID := strings.TrimPrefix(r.URL.Path, "/api/v1/sharding/nodes/")
		if nodeID == "" {
			http.Error(w, "node ID required", http.StatusBadRequest)
			return
		}
		if err := e.RemoveNode(nodeID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/api/v1/sharding/shards", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListShards())
	})

	mux.HandleFunc("/api/v1/sharding/rebalance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		plan, err := e.Rebalance()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	})

	mux.HandleFunc("/api/v1/sharding/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListMigrations())
	})

	mux.HandleFunc("/api/v1/sharding/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})

	mux.HandleFunc("/api/v1/sharding/route", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric parameter required", http.StatusBadRequest)
			return
		}

		tags := make(map[string]string)
		for key, values := range r.URL.Query() {
			if key != "metric" && len(values) > 0 {
				tags[key] = values[0]
			}
		}

		node, err := e.GetNodeForMetric(metric, tags)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"metric":  metric,
			"tags":    tags,
			"node_id": node.ID,
			"address": node.Address,
		})
	})
}
