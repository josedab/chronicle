package chronicle

import (
	"fmt"
	"math"
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
	State      GossipNodeState   `json:"state"`
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
	Nodes          []GossipClusterNode   `json:"nodes"`
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
	vnodes  int
	ring    []ringEntry
	nodeMap map[string]bool
	mu      sync.RWMutex
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
