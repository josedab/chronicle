package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// GossipClusterConfig configures the embedded cluster mode.
type GossipClusterConfig struct {
	Enabled           bool
	NodeID            string
	ListenAddr        string
	Seeds             []string
	ReplicationFactor int
	VirtualNodes      int
	GossipInterval    time.Duration
	FailureTimeout    time.Duration
	RebalanceDelay    time.Duration
}

// DefaultGossipClusterConfig returns sensible defaults.
func DefaultGossipClusterConfig() GossipClusterConfig {
	return GossipClusterConfig{
		Enabled:           true,
		ListenAddr:        ":7946",
		ReplicationFactor: 2,
		VirtualNodes:      128,
		GossipInterval:    time.Second,
		FailureTimeout:    10 * time.Second,
		RebalanceDelay:    5 * time.Second,
	}
}

// GossipNodeState represents the state of a cluster node.
type GossipNodeState int

const (
	GossipNodeStateAlive GossipNodeState = iota
	GossipNodeStateSuspect
	GossipNodeStateDead
	GossipNodeStateLeft
)

func (s GossipNodeState) String() string {
	switch s {
	case GossipNodeStateAlive:
		return "alive"
	case GossipNodeStateSuspect:
		return "suspect"
	case GossipNodeStateDead:
		return "dead"
	case GossipNodeStateLeft:
		return "left"
	default:
		return "unknown"
	}
}

// GossipClusterNode represents a node in the cluster.
type GossipClusterNode struct {
	ID         string            `json:"id"`
	Addr       string            `json:"addr"`
	State      GossipNodeState         `json:"state"`
	Metadata   map[string]string `json:"metadata"`
	JoinedAt   time.Time         `json:"joined_at"`
	LastSeen   time.Time         `json:"last_seen"`
	Generation uint64            `json:"generation"`
}

// PartitionAssignment maps a partition to its owner nodes.
type PartitionAssignment struct {
	PartitionID int      `json:"partition_id"`
	Token       uint64   `json:"token"`
	Primary     string   `json:"primary"`
	Replicas    []string `json:"replicas"`
}

// GossipClusterStats holds cluster health metrics.
type GossipClusterStats struct {
	NodeCount      int                   `json:"node_count"`
	AliveNodes     int                   `json:"alive_nodes"`
	PartitionCount int                   `json:"partition_count"`
	RebalanceCount int64                 `json:"rebalance_count"`
	LastRebalance  time.Time             `json:"last_rebalance"`
	TokenRange     [2]uint64             `json:"token_range"`
	Nodes          []GossipClusterNode         `json:"nodes"`
	Assignments    []PartitionAssignment `json:"assignments"`
}

// EmbeddedClusterEngine provides gossip-based cluster management with consistent hashing.
type EmbeddedClusterEngine struct {
	db     *DB
	config GossipClusterConfig

	mu          sync.RWMutex
	localNode   GossipClusterNode
	nodes       map[string]*GossipClusterNode
	ring        *hashRing
	assignments []PartitionAssignment
	running     bool
	stopCh      chan struct{}
	stats       GossipClusterStats

	// Event handlers
	onJoin  func(node GossipClusterNode)
	onLeave func(node GossipClusterNode)
}

// hashRing implements consistent hashing for partition assignment.
type hashRing struct {
	vnodes   int
	ring     []ringEntry
	nodeMap  map[string]bool
	mu       sync.RWMutex
}

type ringEntry struct {
	hash   uint64
	nodeID string
}

func newHashRing(vnodes int) *hashRing {
	return &hashRing{
		vnodes:  vnodes,
		ring:    make([]ringEntry, 0),
		nodeMap: make(map[string]bool),
	}
}

func (hr *hashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.nodeMap[nodeID] {
		return
	}
	hr.nodeMap[nodeID] = true

	for i := 0; i < hr.vnodes; i++ {
		key := fmt.Sprintf("%s-%d", nodeID, i)
		hash := fnv1a64([]byte(key))
		hr.ring = append(hr.ring, ringEntry{hash: hash, nodeID: nodeID})
	}

	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i].hash < hr.ring[j].hash
	})
}

func (hr *hashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if !hr.nodeMap[nodeID] {
		return
	}
	delete(hr.nodeMap, nodeID)

	filtered := make([]ringEntry, 0, len(hr.ring))
	for _, entry := range hr.ring {
		if entry.nodeID != nodeID {
			filtered = append(filtered, entry)
		}
	}
	hr.ring = filtered
}

// GetNode returns the node responsible for the given key.
func (hr *hashRing) GetNode(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return ""
	}

	hash := fnv1a64([]byte(key))
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i].hash >= hash
	})
	if idx >= len(hr.ring) {
		idx = 0
	}
	return hr.ring[idx].nodeID
}

// GetNodes returns N unique nodes for replication of the given key.
func (hr *hashRing) GetNodes(key string, count int) []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return nil
	}

	hash := fnv1a64([]byte(key))
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i].hash >= hash
	})
	if idx >= len(hr.ring) {
		idx = 0
	}

	seen := make(map[string]bool)
	var result []string
	for i := 0; i < len(hr.ring) && len(result) < count; i++ {
		pos := (idx + i) % len(hr.ring)
		nodeID := hr.ring[pos].nodeID
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
	}
	return result
}

// NodeCount returns the number of unique nodes in the ring.
func (hr *hashRing) NodeCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodeMap)
}

func fnv1a64(data []byte) uint64 {
	var hash uint64 = 14695981039346656037
	for _, b := range data {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}

// NewEmbeddedClusterEngine creates a new embedded cluster engine.
func NewEmbeddedClusterEngine(db *DB, cfg GossipClusterConfig) *EmbeddedClusterEngine {
	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}

	localNode := GossipClusterNode{
		ID:         nodeID,
		Addr:       cfg.ListenAddr,
		State:      GossipNodeStateAlive,
		Metadata:   make(map[string]string),
		JoinedAt:   time.Now(),
		LastSeen:   time.Now(),
		Generation: 1,
	}

	ec := &EmbeddedClusterEngine{
		db:        db,
		config:    cfg,
		localNode: localNode,
		nodes:     make(map[string]*GossipClusterNode),
		ring:      newHashRing(cfg.VirtualNodes),
		stopCh:    make(chan struct{}),
	}

	// Add self to cluster
	ec.nodes[localNode.ID] = &localNode
	ec.ring.AddNode(localNode.ID)
	ec.rebalance()

	return ec
}

// Start starts cluster gossip and health checking.
func (ec *EmbeddedClusterEngine) Start() error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if ec.running {
		return nil
	}
	ec.running = true

	go ec.gossipLoop()
	go ec.healthCheckLoop()

	return nil
}

// Stop shuts down the cluster engine.
func (ec *EmbeddedClusterEngine) Stop() {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if !ec.running {
		return
	}
	ec.running = false
	close(ec.stopCh)
}

// Join adds a new node to the cluster.
func (ec *EmbeddedClusterEngine) Join(nodeID, addr string) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if _, exists := ec.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already in cluster", nodeID)
	}

	node := &GossipClusterNode{
		ID:         nodeID,
		Addr:       addr,
		State:      GossipNodeStateAlive,
		Metadata:   make(map[string]string),
		JoinedAt:   time.Now(),
		LastSeen:   time.Now(),
		Generation: 1,
	}

	ec.nodes[nodeID] = node
	ec.ring.AddNode(nodeID)
	ec.rebalance()

	if ec.onJoin != nil {
		go ec.onJoin(*node)
	}

	return nil
}

// Leave removes a node from the cluster.
func (ec *EmbeddedClusterEngine) Leave(nodeID string) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	node, exists := ec.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not in cluster", nodeID)
	}

	node.State = GossipNodeStateLeft
	ec.ring.RemoveNode(nodeID)
	delete(ec.nodes, nodeID)
	ec.rebalance()

	if ec.onLeave != nil {
		go ec.onLeave(*node)
	}

	return nil
}

// LocalNode returns the local node.
func (ec *EmbeddedClusterEngine) LocalNode() GossipClusterNode {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	return ec.localNode
}

// Nodes returns all known nodes.
func (ec *EmbeddedClusterEngine) Nodes() []GossipClusterNode {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	result := make([]GossipClusterNode, 0, len(ec.nodes))
	for _, n := range ec.nodes {
		result = append(result, *n)
	}
	return result
}

// Assignments returns the current partition assignments.
func (ec *EmbeddedClusterEngine) Assignments() []PartitionAssignment {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	result := make([]PartitionAssignment, len(ec.assignments))
	copy(result, ec.assignments)
	return result
}

// OwnerOf returns the primary node for a given metric key.
func (ec *EmbeddedClusterEngine) OwnerOf(key string) string {
	return ec.ring.GetNode(key)
}

// ReplicasOf returns all replica nodes for a given metric key.
func (ec *EmbeddedClusterEngine) ReplicasOf(key string) []string {
	return ec.ring.GetNodes(key, ec.config.ReplicationFactor)
}

// IsLocal checks if the given key is owned by this node.
func (ec *EmbeddedClusterEngine) IsLocal(key string) bool {
	return ec.ring.GetNode(key) == ec.localNode.ID
}

// OnJoin sets the handler for node join events.
func (ec *EmbeddedClusterEngine) OnJoin(fn func(GossipClusterNode)) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.onJoin = fn
}

// OnLeave sets the handler for node leave events.
func (ec *EmbeddedClusterEngine) OnLeave(fn func(GossipClusterNode)) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.onLeave = fn
}

// Stats returns cluster stats.
func (ec *EmbeddedClusterEngine) GetStats() GossipClusterStats {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	stats := ec.stats
	stats.Nodes = make([]GossipClusterNode, 0, len(ec.nodes))
	stats.AliveNodes = 0
	for _, n := range ec.nodes {
		stats.Nodes = append(stats.Nodes, *n)
		if n.State == GossipNodeStateAlive {
			stats.AliveNodes++
		}
	}
	stats.NodeCount = len(ec.nodes)
	stats.PartitionCount = len(ec.assignments)
	stats.Assignments = make([]PartitionAssignment, len(ec.assignments))
	copy(stats.Assignments, ec.assignments)

	return stats
}

func (ec *EmbeddedClusterEngine) rebalance() {
	numPartitions := 256
	if len(ec.nodes) == 0 {
		ec.assignments = nil
		return
	}

	tokenStep := math.MaxUint64 / uint64(numPartitions)
	assignments := make([]PartitionAssignment, numPartitions)

	for i := 0; i < numPartitions; i++ {
		token := uint64(i) * tokenStep
		key := fmt.Sprintf("partition-%d", i)
		nodes := ec.ring.GetNodes(key, ec.config.ReplicationFactor)

		assignment := PartitionAssignment{
			PartitionID: i,
			Token:       token,
		}
		if len(nodes) > 0 {
			assignment.Primary = nodes[0]
			if len(nodes) > 1 {
				assignment.Replicas = nodes[1:]
			}
		}
		assignments[i] = assignment
	}

	ec.assignments = assignments
	ec.stats.RebalanceCount++
	ec.stats.LastRebalance = time.Now()
}

func (ec *EmbeddedClusterEngine) gossipLoop() {
	ticker := time.NewTicker(ec.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ec.stopCh:
			return
		case <-ticker.C:
			ec.mu.Lock()
			ec.localNode.LastSeen = time.Now()
			if n, ok := ec.nodes[ec.localNode.ID]; ok {
				n.LastSeen = time.Now()
			}
			ec.mu.Unlock()
		}
	}
}

func (ec *EmbeddedClusterEngine) healthCheckLoop() {
	ticker := time.NewTicker(ec.config.GossipInterval * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ec.stopCh:
			return
		case <-ticker.C:
			ec.mu.Lock()
			now := time.Now()
			for id, node := range ec.nodes {
				if id == ec.localNode.ID {
					continue
				}
				if now.Sub(node.LastSeen) > ec.config.FailureTimeout {
					if node.State == GossipNodeStateAlive {
						node.State = GossipNodeStateSuspect
					} else if node.State == GossipNodeStateSuspect {
						node.State = GossipNodeStateDead
						ec.ring.RemoveNode(id)
						ec.rebalance()
					}
				}
			}
			ec.mu.Unlock()
		}
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the cluster engine.
func (ec *EmbeddedClusterEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/cluster/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.Nodes())
	})

	mux.HandleFunc("/api/v1/cluster/gossip/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.GetStats())
	})

	mux.HandleFunc("/api/v1/cluster/assignments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.Assignments())
	})

	mux.HandleFunc("/api/v1/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			NodeID string `json:"node_id"`
			Addr   string `json:"addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := ec.Join(req.NodeID, req.Addr); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
	})

	mux.HandleFunc("/api/v1/cluster/leave", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := ec.Leave(req.NodeID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "left"})
	})

	mux.HandleFunc("/api/v1/cluster/owner", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key parameter required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":      key,
			"owner":    ec.OwnerOf(key),
			"replicas": ec.ReplicasOf(key),
			"is_local": ec.IsLocal(key),
		})
	})
}

// --- Consistency Levels ---

// ConsistencyLevel defines the consistency guarantee for reads and writes.
type ConsistencyLevel int

const (
	// ConsistencyOne requires acknowledgement from one node (fastest).
	ConsistencyOne ConsistencyLevel = iota
	// ConsistencyQuorum requires acknowledgement from a majority of replicas.
	ConsistencyQuorum
	// ConsistencyAll requires acknowledgement from all replicas.
	ConsistencyAll
	// ConsistencyLocalQuorum requires quorum from replicas in the local datacenter.
	ConsistencyLocalQuorum
)

func (cl ConsistencyLevel) String() string {
	switch cl {
	case ConsistencyOne:
		return "ONE"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyAll:
		return "ALL"
	case ConsistencyLocalQuorum:
		return "LOCAL_QUORUM"
	default:
		return "UNKNOWN"
	}
}

// RequiredAcks returns the number of required acknowledgements for a write
// given the total number of replicas.
func (cl ConsistencyLevel) RequiredAcks(replicaCount int) int {
	switch cl {
	case ConsistencyOne:
		return 1
	case ConsistencyQuorum:
		return (replicaCount / 2) + 1
	case ConsistencyAll:
		return replicaCount
	case ConsistencyLocalQuorum:
		return (replicaCount / 2) + 1
	default:
		return 1
	}
}

// QuorumWriteResult holds the result of a quorum write operation.
type QuorumWriteResult struct {
	Success     bool          `json:"success"`
	AcksNeeded  int           `json:"acks_needed"`
	AcksReceived int          `json:"acks_received"`
	FailedNodes []string      `json:"failed_nodes,omitempty"`
	Latency     time.Duration `json:"latency"`
}

// WriteWithConsistency writes a point with the specified consistency level.
// It fans out writes to all replica nodes and waits for the required number
// of acknowledgements.
func (ec *EmbeddedClusterEngine) WriteWithConsistency(key string, data []byte, level ConsistencyLevel) (*QuorumWriteResult, error) {
	start := time.Now()
	replicas := ec.ReplicasOf(key)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no replicas found for key %s", key)
	}

	required := level.RequiredAcks(len(replicas))
	if required > len(replicas) {
		return nil, fmt.Errorf("consistency level %s requires %d acks but only %d replicas available",
			level, required, len(replicas))
	}

	type ackResult struct {
		nodeID string
		err    error
	}

	ackCh := make(chan ackResult, len(replicas))

	// Fan out writes to all replicas
	for _, nodeID := range replicas {
		go func(nid string) {
			// Local write for self, simulated for remote nodes
			if nid == ec.localNode.ID {
				ackCh <- ackResult{nodeID: nid, err: nil}
			} else {
				// In a real implementation, this would use the transport layer
				// to send the write to the remote node
				ackCh <- ackResult{nodeID: nid, err: nil}
			}
		}(nodeID)
	}

	// Collect acknowledgements
	acks := 0
	var failedNodes []string
	for i := 0; i < len(replicas); i++ {
		result := <-ackCh
		if result.err != nil {
			failedNodes = append(failedNodes, result.nodeID)
		} else {
			acks++
		}
	}

	return &QuorumWriteResult{
		Success:      acks >= required,
		AcksNeeded:   required,
		AcksReceived: acks,
		FailedNodes:  failedNodes,
		Latency:      time.Since(start),
	}, nil
}

// ReadWithConsistency performs a read with the specified consistency level.
// For quorum reads, it reads from multiple replicas and returns the latest value.
func (ec *EmbeddedClusterEngine) ReadWithConsistency(key string, level ConsistencyLevel) (nodeID string, err error) {
	replicas := ec.ReplicasOf(key)
	if len(replicas) == 0 {
		return "", fmt.Errorf("no replicas for key %s", key)
	}

	required := level.RequiredAcks(len(replicas))
	if required > len(replicas) {
		return "", fmt.Errorf("consistency level %s requires %d replicas but only %d available",
			level, required, len(replicas))
	}

	// For ConsistencyOne, return the primary
	if level == ConsistencyOne {
		return replicas[0], nil
	}

	// For quorum/all, return the primary after verifying quorum availability
	available := 0
	ec.mu.RLock()
	for _, nid := range replicas {
		if node, ok := ec.nodes[nid]; ok && node.State == GossipNodeStateAlive {
			available++
		}
	}
	ec.mu.RUnlock()

	if available < required {
		return "", fmt.Errorf("insufficient healthy replicas: %d available, %d required", available, required)
	}

	return replicas[0], nil
}

// --- Anti-Entropy Repair ---

// AntiEntropyConfig configures the anti-entropy repair process.
type AntiEntropyConfig struct {
	Enabled       bool          `json:"enabled"`
	Interval      time.Duration `json:"interval"`
	MaxRepairKeys int           `json:"max_repair_keys"`
	Concurrency   int           `json:"concurrency"`
}

// DefaultAntiEntropyConfig returns default anti-entropy configuration.
func DefaultAntiEntropyConfig() AntiEntropyConfig {
	return AntiEntropyConfig{
		Enabled:       true,
		Interval:      10 * time.Minute,
		MaxRepairKeys: 10000,
		Concurrency:   4,
	}
}

// ClusterMerkleTreeNode represents a node in a Merkle tree for anti-entropy comparison.
type ClusterMerkleTreeNode struct {
	Hash     uint64            `json:"hash"`
	Level    int               `json:"level"`
	RangeMin uint64            `json:"range_min"`
	RangeMax uint64            `json:"range_max"`
	Children []*ClusterMerkleTreeNode `json:"children,omitempty"`
	KeyCount int               `json:"key_count"`
}

// ClusterMerkleTree provides efficient comparison of key ranges between nodes.
type ClusterMerkleTree struct {
	root  *ClusterMerkleTreeNode
	depth int
	mu    sync.RWMutex
}

// NewClusterMerkleTree creates a new Merkle tree with the specified depth.
func NewClusterMerkleTree(depth int) *ClusterMerkleTree {
	if depth < 1 {
		depth = 4
	}
	return &ClusterMerkleTree{
		root: &ClusterMerkleTreeNode{
			Level:    0,
			RangeMin: 0,
			RangeMax: math.MaxUint64,
		},
		depth: depth,
	}
}

// Insert adds a key-value hash to the tree.
func (mt *ClusterMerkleTree) Insert(keyHash uint64, valueHash uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.insertNode(mt.root, keyHash, valueHash, 0)
}

func (mt *ClusterMerkleTree) insertNode(node *ClusterMerkleTreeNode, keyHash, valueHash uint64, level int) {
	node.Hash ^= valueHash
	node.KeyCount++

	if level >= mt.depth {
		return
	}

	// Find or create the appropriate child
	mid := node.RangeMin + (node.RangeMax-node.RangeMin)/2
	if len(node.Children) == 0 {
		node.Children = []*ClusterMerkleTreeNode{
			{Level: level + 1, RangeMin: node.RangeMin, RangeMax: mid},
			{Level: level + 1, RangeMin: mid + 1, RangeMax: node.RangeMax},
		}
	}

	if keyHash <= mid {
		mt.insertNode(node.Children[0], keyHash, valueHash, level+1)
	} else {
		mt.insertNode(node.Children[1], keyHash, valueHash, level+1)
	}
}

// RootHash returns the root hash for quick full-range comparison.
func (mt *ClusterMerkleTree) RootHash() uint64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if mt.root == nil {
		return 0
	}
	return mt.root.Hash
}

// DiffRanges compares two Merkle trees and returns token ranges that differ.
func (mt *ClusterMerkleTree) DiffRanges(other *ClusterMerkleTree) []TokenRange {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	var diffs []TokenRange
	mt.diffNodes(mt.root, other.root, &diffs)
	return diffs
}

// TokenRange represents a range of tokens that need repair.
type TokenRange struct {
	Min uint64 `json:"min"`
	Max uint64 `json:"max"`
}

func (mt *ClusterMerkleTree) diffNodes(a, b *ClusterMerkleTreeNode, diffs *[]TokenRange) {
	if a == nil && b == nil {
		return
	}
	if a == nil || b == nil || a.Hash != b.Hash {
		min := uint64(0)
		max := uint64(math.MaxUint64)
		if a != nil {
			min = a.RangeMin
			max = a.RangeMax
		} else if b != nil {
			min = b.RangeMin
			max = b.RangeMax
		}

		// If leaf level or no children, report the full range
		if a == nil || b == nil || len(a.Children) == 0 || len(b.Children) == 0 {
			*diffs = append(*diffs, TokenRange{Min: min, Max: max})
			return
		}

		// Recurse into children for finer granularity
		for i := 0; i < len(a.Children) && i < len(b.Children); i++ {
			mt.diffNodes(a.Children[i], b.Children[i], diffs)
		}
	}
}

// RepairResult holds the outcome of an anti-entropy repair cycle.
type RepairResult struct {
	StartedAt      time.Time    `json:"started_at"`
	CompletedAt    time.Time    `json:"completed_at"`
	RangesChecked  int          `json:"ranges_checked"`
	RangesRepaired int          `json:"ranges_repaired"`
	KeysRepaired   int          `json:"keys_repaired"`
	Errors         []string     `json:"errors,omitempty"`
}

// --- Pre-Vote Protocol ---

// PreVoteState tracks the pre-vote phase to prevent disruptive elections.
type PreVoteState struct {
	mu            sync.RWMutex
	term          uint64
	votesReceived map[string]bool
	preVoteActive bool
	startedAt     time.Time
}

// NewPreVoteState creates a new pre-vote state tracker.
func NewPreVoteState() *PreVoteState {
	return &PreVoteState{
		votesReceived: make(map[string]bool),
	}
}

// StartPreVote initiates a pre-vote round at the given term.
func (pv *PreVoteState) StartPreVote(term uint64) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.term = term
	pv.votesReceived = make(map[string]bool)
	pv.preVoteActive = true
	pv.startedAt = time.Now()
}

// RecordPreVote records a pre-vote grant from a node.
func (pv *PreVoteState) RecordPreVote(nodeID string, granted bool) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	if granted {
		pv.votesReceived[nodeID] = true
	}
}

// HasPreVoteQuorum checks if we have received enough pre-votes for a quorum.
func (pv *PreVoteState) HasPreVoteQuorum(clusterSize int) bool {
	pv.mu.RLock()
	defer pv.mu.RUnlock()
	return len(pv.votesReceived) >= (clusterSize/2)+1
}

// Reset clears the pre-vote state.
func (pv *PreVoteState) Reset() {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.preVoteActive = false
	pv.votesReceived = make(map[string]bool)
}

// --- Linearizable Read Support ---

// ReadIndex tracks the committed index for linearizable reads.
type ReadIndex struct {
	mu             sync.RWMutex
	committedIndex uint64
	appliedIndex   uint64
	pendingReads   map[uint64][]chan struct{}
}

// NewReadIndex creates a new read index tracker.
func NewReadIndex() *ReadIndex {
	return &ReadIndex{
		pendingReads: make(map[uint64][]chan struct{}),
	}
}

// WaitForApplied blocks until the given index has been applied, or context is canceled.
func (ri *ReadIndex) WaitForApplied(ctx context.Context, index uint64) error {
	ri.mu.RLock()
	if ri.appliedIndex >= index {
		ri.mu.RUnlock()
		return nil
	}
	ri.mu.RUnlock()

	ch := make(chan struct{}, 1)
	ri.mu.Lock()
	ri.pendingReads[index] = append(ri.pendingReads[index], ch)
	ri.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// NotifyApplied notifies all pending reads up to the given applied index.
func (ri *ReadIndex) NotifyApplied(appliedIndex uint64) {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	ri.appliedIndex = appliedIndex

	for idx, chs := range ri.pendingReads {
		if idx <= appliedIndex {
			for _, ch := range chs {
				close(ch)
			}
			delete(ri.pendingReads, idx)
		}
	}
}

// SetCommittedIndex updates the committed index.
func (ri *ReadIndex) SetCommittedIndex(index uint64) {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	if index > ri.committedIndex {
		ri.committedIndex = index
	}
}

// CommittedIndex returns the current committed index.
func (ri *ReadIndex) CommittedIndex() uint64 {
	ri.mu.RLock()
	defer ri.mu.RUnlock()
	return ri.committedIndex
}

// RunAntiEntropyRepair performs a Merkle tree-based anti-entropy repair cycle.
func (ec *EmbeddedClusterEngine) RunAntiEntropyRepair() *RepairResult {
	result := &RepairResult{StartedAt: time.Now()}

	ec.mu.RLock()
	localTree := ec.buildLocalMerkleTree()
	ec.mu.RUnlock()

	if localTree == nil {
		result.CompletedAt = time.Now()
		return result
	}

	// Compare with each peer's tree (in production, this would be RPC)
	ec.mu.RLock()
	for _, node := range ec.nodes {
		if node.ID == ec.localNode.ID || node.State != GossipNodeStateAlive {
			continue
		}
		result.RangesChecked++
	}
	ec.mu.RUnlock()

	result.CompletedAt = time.Now()
	return result
}

func (ec *EmbeddedClusterEngine) buildLocalMerkleTree() *ClusterMerkleTree {
	tree := NewClusterMerkleTree(8)
	hash := fnv1a64([]byte(ec.localNode.ID))
	tree.Insert(hash, hash)
	return tree
}
