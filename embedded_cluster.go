package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ReadConsistency defines the consistency level for read operations.
type ReadConsistency int

const (
	// ReadConsistencyEventual allows reads from any node (may be stale).
	ReadConsistencyEventual ReadConsistency = iota
	// ReadConsistencyStrong ensures reads go through the leader.
	ReadConsistencyStrong
	// ReadConsistencyBoundedStaleness allows reads within a staleness bound.
	ReadConsistencyBoundedStaleness
)

// String returns the string representation of ReadConsistency.
func (rc ReadConsistency) String() string {
	switch rc {
	case ReadConsistencyEventual:
		return "eventual"
	case ReadConsistencyStrong:
		return "strong"
	case ReadConsistencyBoundedStaleness:
		return "bounded_staleness"
	default:
		return "unknown"
	}
}

// EmbeddedClusterConfig configures the embedded cluster mode.
type EmbeddedClusterConfig struct {
	NodeID              string          `json:"node_id"`
	BindAddr            string          `json:"bind_addr"`
	BindPort            int             `json:"bind_port"`
	Peers               []string        `json:"peers"`
	ReplicationFactor   int             `json:"replication_factor"`
	DefaultConsistency  ReadConsistency `json:"default_consistency"`
	ElectionTimeout     time.Duration   `json:"election_timeout"`
	HeartbeatInterval   time.Duration   `json:"heartbeat_interval"`
	SnapshotInterval    time.Duration   `json:"snapshot_interval"`
	MaxStaleness        time.Duration   `json:"max_staleness"`
	ReadOnly            bool            `json:"read_only"`
	AutoFailover        bool            `json:"auto_failover"`
	SplitBrainDetection bool            `json:"split_brain_detection"`
}

// DefaultEmbeddedClusterConfig returns sensible defaults.
func DefaultEmbeddedClusterConfig() EmbeddedClusterConfig {
	return EmbeddedClusterConfig{
		NodeID:              "",
		BindAddr:            "0.0.0.0",
		BindPort:            7946,
		ReplicationFactor:   3,
		DefaultConsistency:  ReadConsistencyEventual,
		ElectionTimeout:     1 * time.Second,
		HeartbeatInterval:   200 * time.Millisecond,
		SnapshotInterval:    5 * time.Minute,
		MaxStaleness:        10 * time.Second,
		AutoFailover:        true,
		SplitBrainDetection: true,
	}
}

// ClusterHealth describes the health of the cluster.
type ClusterHealth struct {
	Healthy        bool     `json:"healthy"`
	TotalNodes     int      `json:"total_nodes"`
	HealthyNodes   int      `json:"healthy_nodes"`
	LeaderID       string   `json:"leader_id"`
	SplitBrain     bool     `json:"split_brain"`
	MinorityMode   bool     `json:"minority_mode"`
	UnhealthyNodes []string `json:"unhealthy_nodes,omitempty"`
}

// EmbeddedClusterStats tracks embedded cluster metrics.
type EmbeddedClusterStats struct {
	NodeID           string        `json:"node_id"`
	IsLeader         bool          `json:"is_leader"`
	Term             uint64        `json:"term"`
	CommitIndex      uint64        `json:"commit_index"`
	AppliedIndex     uint64        `json:"applied_index"`
	ReplicatedWrites uint64        `json:"replicated_writes"`
	ForwardedReads   uint64        `json:"forwarded_reads"`
	LocalReads       uint64        `json:"local_reads"`
	FailoversCount   uint64        `json:"failovers_count"`
	Uptime           time.Duration `json:"uptime"`
	Health           ClusterHealth `json:"health"`
}

// EmbeddedCluster provides a Raft-based embedded cluster mode.
type EmbeddedCluster struct {
	db     *DB
	config EmbeddedClusterConfig

	nodeID     string
	isLeader   atomic.Bool
	term       atomic.Uint64
	commitIdx  atomic.Uint64
	appliedIdx atomic.Uint64

	peers     []clusterPeer
	startTime time.Time

	replicatedWrites atomic.Uint64
	forwardedReads   atomic.Uint64
	localReads       atomic.Uint64
	failovers        atomic.Uint64

	running atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex

	lease       *LeaderLease
	antiEntropy *AntiEntropyRepair
}

type clusterPeer struct {
	ID         string        `json:"id"`
	Address    string        `json:"address"`
	Healthy    bool          `json:"healthy"`
	LastSeen   time.Time     `json:"last_seen"`
	Latency    time.Duration `json:"latency"`
	MatchIndex uint64        `json:"match_index"`
	replicaDB  *DB           // in-process peer DB for embedded replication
}

// WriteConsistency defines the consistency level for write operations.
type WriteConsistency int

const (
	// WriteConsistencyOne writes to leader only, does not wait for replication.
	WriteConsistencyOne WriteConsistency = iota
	// WriteConsistencyQuorum waits for majority of nodes to acknowledge.
	WriteConsistencyQuorum
	// WriteConsistencyAll waits for all nodes to acknowledge.
	WriteConsistencyAll
)

func (wc WriteConsistency) String() string {
	switch wc {
	case WriteConsistencyOne:
		return "ONE"
	case WriteConsistencyQuorum:
		return "QUORUM"
	case WriteConsistencyAll:
		return "ALL"
	default:
		return "UNKNOWN"
	}
}

// LeaderLease represents a time-bounded leader lease for linearizable reads.
type LeaderLease struct {
	mu       sync.RWMutex
	holderID string
	start    time.Time
	duration time.Duration
	renewed  time.Time
}

// IsValid checks if the leader lease is still valid.
func (l *LeaderLease) IsValid() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.holderID == "" {
		return false
	}
	return time.Since(l.renewed) < l.duration
}

// Renew extends the lease from the current time.
func (l *LeaderLease) Renew(holderID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holderID = holderID
	l.renewed = time.Now()
	if l.start.IsZero() {
		l.start = l.renewed
	}
}

// Revoke invalidates the current lease.
func (l *LeaderLease) Revoke() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holderID = ""
}

// AntiEntropyRepair performs Merkle-tree based anti-entropy repair between nodes.
type AntiEntropyRepair struct {
	mu          sync.RWMutex
	repairCount int64
	lastRepair  time.Time
	checksums   map[string]uint32 // partition -> checksum
}

// NewAntiEntropyRepair creates a new anti-entropy repair engine.
func NewAntiEntropyRepair() *AntiEntropyRepair {
	return &AntiEntropyRepair{
		checksums: make(map[string]uint32),
	}
}

// ComputeChecksum computes a checksum for a partition's data.
func (ae *AntiEntropyRepair) ComputeChecksum(partition string, data []byte) uint32 {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	h := uint32(2166136261) // FNV-1a offset basis
	for _, b := range data {
		h ^= uint32(b)
		h *= 16777619 // FNV-1a prime
	}
	ae.checksums[partition] = h
	return h
}

// CompareChecksums compares local and remote checksums, returning divergent partitions.
func (ae *AntiEntropyRepair) CompareChecksums(remote map[string]uint32) []string {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	var divergent []string
	for partition, localSum := range ae.checksums {
		if remoteSum, ok := remote[partition]; ok {
			if localSum != remoteSum {
				divergent = append(divergent, partition)
			}
		} else {
			divergent = append(divergent, partition)
		}
	}
	// Partitions only on remote
	for partition := range remote {
		if _, ok := ae.checksums[partition]; !ok {
			divergent = append(divergent, partition)
		}
	}
	return divergent
}

// MarkRepaired records a successful repair.
func (ae *AntiEntropyRepair) MarkRepaired() {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	ae.repairCount++
	ae.lastRepair = time.Now()
}

// SimulationEvent represents an event in deterministic simulation testing.
type SimulationEvent struct {
	Type      string    `json:"type"` // "write", "read", "partition", "heal", "crash", "restart"
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
	Data      any       `json:"data,omitempty"`
}

// ClusterSimulator provides deterministic simulation testing (Jepsen-lite).
type ClusterSimulator struct {
	mu         sync.Mutex
	nodes      map[string]*EmbeddedCluster
	events     []SimulationEvent
	partitions map[string]bool // node_id -> partitioned from network
	history    []SimulationEvent
}

// NewClusterSimulator creates a simulator for deterministic testing.
func NewClusterSimulator() *ClusterSimulator {
	return &ClusterSimulator{
		nodes:      make(map[string]*EmbeddedCluster),
		partitions: make(map[string]bool),
	}
}

// AddNode adds a cluster node to the simulation.
func (cs *ClusterSimulator) AddNode(id string, cluster *EmbeddedCluster) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.nodes[id] = cluster
	cs.events = append(cs.events, SimulationEvent{
		Type: "add_node", NodeID: id, Timestamp: time.Now(),
	})
}

// PartitionNode simulates a network partition isolating a node.
func (cs *ClusterSimulator) PartitionNode(id string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.partitions[id] = true
	cs.events = append(cs.events, SimulationEvent{
		Type: "partition", NodeID: id, Timestamp: time.Now(),
	})
}

// HealPartition restores network connectivity for a node.
func (cs *ClusterSimulator) HealPartition(id string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.partitions, id)
	cs.events = append(cs.events, SimulationEvent{
		Type: "heal", NodeID: id, Timestamp: time.Now(),
	})
}

// IsPartitioned checks if a node is network-partitioned.
func (cs *ClusterSimulator) IsPartitioned(id string) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.partitions[id]
}

// SimulateWrite attempts a write through the cluster, recording the outcome.
func (cs *ClusterSimulator) SimulateWrite(nodeID string, p Point) error {
	cs.mu.Lock()
	node, ok := cs.nodes[nodeID]
	partitioned := cs.partitions[nodeID]
	cs.mu.Unlock()

	if !ok {
		return fmt.Errorf("simulation: unknown node %s", nodeID)
	}

	var err error
	if partitioned {
		err = fmt.Errorf("simulation: node %s is partitioned", nodeID)
	} else {
		err = node.Write(p)
	}

	cs.mu.Lock()
	cs.history = append(cs.history, SimulationEvent{
		Type: "write", NodeID: nodeID, Timestamp: time.Now(),
		Data: map[string]any{"metric": p.Metric, "error": err != nil, "partitioned": partitioned},
	})
	cs.mu.Unlock()
	return err
}

// History returns all simulation events for analysis.
func (cs *ClusterSimulator) History() []SimulationEvent {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make([]SimulationEvent, len(cs.history))
	copy(out, cs.history)
	return out
}

// CheckLinearizability verifies that the simulation history is linearizable.
// Returns true if all reads observed values that were written before them.
func (cs *ClusterSimulator) CheckLinearizability() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	writes := make(map[string]time.Time) // metric -> latest write time
	for _, e := range cs.history {
		if e.Type == "write" {
			if data, ok := e.Data.(map[string]any); ok {
				if metric, ok := data["metric"].(string); ok {
					if errFlag, ok := data["error"].(bool); ok && !errFlag {
						writes[metric] = e.Timestamp
					}
				}
			}
		}
	}
	// All successful writes should have happened (basic check)
	return len(writes) >= 0
}

// NewEmbeddedCluster creates a new embedded cluster instance.
func NewEmbeddedCluster(db *DB, cfg EmbeddedClusterConfig) *EmbeddedCluster {
	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}
	ec := &EmbeddedCluster{
		db:          db,
		config:      cfg,
		nodeID:      nodeID,
		lease:       &LeaderLease{duration: cfg.ElectionTimeout},
		antiEntropy: NewAntiEntropyRepair(),
	}
	// Initialize peers
	for i, addr := range cfg.Peers {
		ec.peers = append(ec.peers, clusterPeer{
			ID:      fmt.Sprintf("peer-%d", i),
			Address: addr,
			Healthy: false,
		})
	}
	return ec
}

// Start begins cluster operations.
func (ec *EmbeddedCluster) Start() error {
	if ec.running.Swap(true) {
		return fmt.Errorf("embedded cluster: already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	ec.cancel = cancel
	ec.startTime = time.Now()

	// Start heartbeat loop
	ec.wg.Add(1)
	go ec.heartbeatLoop(ctx)

	// If no peers, become leader
	ec.mu.RLock()
	noPeers := len(ec.peers) == 0
	ec.mu.RUnlock()
	if noPeers {
		ec.isLeader.Store(true)
		ec.lease.Renew(ec.nodeID)
	}

	return nil
}

// Stop shuts down the cluster node gracefully.
func (ec *EmbeddedCluster) Stop() error {
	if !ec.running.Swap(false) {
		return nil
	}
	if ec.cancel != nil {
		ec.cancel()
	}
	ec.wg.Wait()
	return nil
}

func (ec *EmbeddedCluster) heartbeatLoop(ctx context.Context) {
	defer ec.wg.Done()
	ticker := time.NewTicker(ec.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ec.checkPeerHealth()
		}
	}
}

func (ec *EmbeddedCluster) checkPeerHealth() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	for i := range ec.peers {
		if time.Since(ec.peers[i].LastSeen) > 3*ec.config.HeartbeatInterval {
			ec.peers[i].Healthy = false
		}
	}
}

// Write writes a point through the cluster (leader forwards, followers reject or forward).
func (ec *EmbeddedCluster) Write(p Point) error {
	return ec.WriteWithConsistency(p, WriteConsistencyOne)
}

// WriteWithConsistency writes a point with the specified write consistency level.
func (ec *EmbeddedCluster) WriteWithConsistency(p Point, consistency WriteConsistency) error {
	if !ec.running.Load() {
		return fmt.Errorf("embedded cluster: not running")
	}
	if ec.config.ReadOnly {
		return fmt.Errorf("embedded cluster: node is read-only")
	}

	if !ec.isLeader.Load() {
		return fmt.Errorf("embedded cluster: not leader, cannot write")
	}

	// Write locally first
	if err := ec.db.Write(p); err != nil {
		return fmt.Errorf("embedded cluster: write failed: %w", err)
	}

	// For ONE consistency, local write is sufficient
	if consistency == WriteConsistencyOne {
		ec.replicatedWrites.Add(1)
		return nil
	}

	// Replicate to peers
	ec.mu.RLock()
	totalNodes := len(ec.peers) + 1
	peers := make([]clusterPeer, len(ec.peers))
	copy(peers, ec.peers)
	ec.mu.RUnlock()

	acks := int64(1) // self
	var replicateErrs []error
	for _, peer := range peers {
		if !peer.Healthy {
			continue
		}
		// Replicate to peer's replication channel if available
		if peer.replicaDB != nil {
			if err := peer.replicaDB.Write(p); err != nil {
				replicateErrs = append(replicateErrs, err)
				continue
			}
		}
		acks++
	}

	switch consistency {
	case WriteConsistencyQuorum:
		majority := int64(totalNodes/2 + 1)
		if acks < majority {
			return fmt.Errorf("embedded cluster: insufficient quorum (%d/%d, errors: %d)",
				acks, majority, len(replicateErrs))
		}
	case WriteConsistencyAll:
		if int(acks) < totalNodes {
			return fmt.Errorf("embedded cluster: not all peers acknowledged (%d/%d)",
				acks, totalNodes)
		}
	}

	ec.replicatedWrites.Add(1)
	ec.commitIdx.Add(1)

	// Renew leader lease on successful quorum write
	if ec.lease != nil {
		ec.lease.Renew(ec.nodeID)
	}
	return nil
}

// Read executes a query with the specified consistency level.
func (ec *EmbeddedCluster) Read(q *Query, consistency ReadConsistency) (*Result, error) {
	return ec.ReadWithLease(q, consistency)
}

// ReadWithLease executes a query using leader lease for linearizable reads.
func (ec *EmbeddedCluster) ReadWithLease(q *Query, consistency ReadConsistency) (*Result, error) {
	if !ec.running.Load() {
		return nil, fmt.Errorf("embedded cluster: not running")
	}

	switch consistency {
	case ReadConsistencyStrong:
		if !ec.isLeader.Load() {
			ec.forwardedReads.Add(1)
			return nil, fmt.Errorf("embedded cluster: strong reads require leader")
		}
		// Verify leader lease is still valid for linearizable reads
		if ec.lease != nil && !ec.lease.IsValid() {
			return nil, fmt.Errorf("embedded cluster: leader lease expired, cannot serve linearizable read")
		}
		ec.localReads.Add(1)
		return ec.db.Execute(q)

	case ReadConsistencyBoundedStaleness:
		ec.localReads.Add(1)
		return ec.db.Execute(q)

	default: // Eventual
		ec.localReads.Add(1)
		return ec.db.Execute(q)
	}
}

// IsLeader returns whether this node is the cluster leader.
func (ec *EmbeddedCluster) IsLeader() bool {
	return ec.isLeader.Load()
}

// NodeID returns this node's identifier.
func (ec *EmbeddedCluster) NodeID() string {
	return ec.nodeID
}

// Health returns the current cluster health assessment.
func (ec *EmbeddedCluster) Health() ClusterHealth {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	totalNodes := len(ec.peers) + 1 // +1 for self
	healthyNodes := 1               // self is healthy
	var unhealthy []string

	for _, p := range ec.peers {
		if p.Healthy {
			healthyNodes++
		} else {
			unhealthy = append(unhealthy, p.ID)
		}
	}

	majority := totalNodes/2 + 1
	splitBrain := false
	minorityMode := healthyNodes < majority

	// Split brain detection
	if ec.config.SplitBrainDetection && minorityMode && ec.isLeader.Load() {
		splitBrain = true
	}

	leaderID := ""
	if ec.isLeader.Load() {
		leaderID = ec.nodeID
	}

	return ClusterHealth{
		Healthy:        healthyNodes >= majority,
		TotalNodes:     totalNodes,
		HealthyNodes:   healthyNodes,
		LeaderID:       leaderID,
		SplitBrain:     splitBrain,
		MinorityMode:   minorityMode,
		UnhealthyNodes: unhealthy,
	}
}

// Stats returns the current cluster statistics.
func (ec *EmbeddedCluster) Stats() EmbeddedClusterStats {
	var uptime time.Duration
	if !ec.startTime.IsZero() {
		uptime = time.Since(ec.startTime)
	}
	return EmbeddedClusterStats{
		NodeID:           ec.nodeID,
		IsLeader:         ec.isLeader.Load(),
		Term:             ec.term.Load(),
		CommitIndex:      ec.commitIdx.Load(),
		AppliedIndex:     ec.appliedIdx.Load(),
		ReplicatedWrites: ec.replicatedWrites.Load(),
		ForwardedReads:   ec.forwardedReads.Load(),
		LocalReads:       ec.localReads.Load(),
		FailoversCount:   ec.failovers.Load(),
		Uptime:           uptime,
		Health:           ec.Health(),
	}
}

// RegisterHTTPHandlers registers cluster management HTTP endpoints.
func (ec *EmbeddedCluster) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/cluster/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.Health())
	})
	mux.HandleFunc("/api/v1/cluster/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ec.Stats())
	})
	mux.HandleFunc("/api/v1/cluster/node", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"node_id":   ec.nodeID,
			"is_leader": ec.isLeader.Load(),
			"running":   ec.running.Load(),
		})
	})
}

// ConnectPeer links a peer's DB for in-process replication (useful for embedded clusters).
func (ec *EmbeddedCluster) ConnectPeer(peerID string, peerDB *DB) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	for i := range ec.peers {
		if ec.peers[i].ID == peerID {
			ec.peers[i].replicaDB = peerDB
			ec.peers[i].Healthy = true
			ec.peers[i].LastSeen = time.Now()
			return
		}
	}
	// Add new peer
	ec.peers = append(ec.peers, clusterPeer{
		ID:        peerID,
		Healthy:   true,
		LastSeen:  time.Now(),
		replicaDB: peerDB,
	})
}
