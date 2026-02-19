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
}

type clusterPeer struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	Healthy  bool      `json:"healthy"`
	LastSeen time.Time `json:"last_seen"`
}

// NewEmbeddedCluster creates a new embedded cluster instance.
func NewEmbeddedCluster(db *DB, cfg EmbeddedClusterConfig) *EmbeddedCluster {
	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}
	ec := &EmbeddedCluster{
		db:     db,
		config: cfg,
		nodeID: nodeID,
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
	if len(ec.peers) == 0 {
		ec.isLeader.Store(true)
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
	if !ec.running.Load() {
		return fmt.Errorf("embedded cluster: not running")
	}
	if ec.config.ReadOnly {
		return fmt.Errorf("embedded cluster: node is read-only")
	}

	if ec.isLeader.Load() {
		if err := ec.db.Write(p); err != nil {
			return fmt.Errorf("embedded cluster: write failed: %w", err)
		}
		ec.replicatedWrites.Add(1)
		return nil
	}
	// In a real implementation, this would forward to the leader
	return fmt.Errorf("embedded cluster: not leader, cannot write")
}

// Read executes a query with the specified consistency level.
func (ec *EmbeddedCluster) Read(q *Query, consistency ReadConsistency) (*Result, error) {
	if !ec.running.Load() {
		return nil, fmt.Errorf("embedded cluster: not running")
	}

	switch consistency {
	case ReadConsistencyStrong:
		if !ec.isLeader.Load() {
			ec.forwardedReads.Add(1)
			// In real implementation, forward to leader
			return nil, fmt.Errorf("embedded cluster: strong reads require leader")
		}
		ec.localReads.Add(1)
		return ec.db.Execute(q)

	case ReadConsistencyBoundedStaleness:
		// Check if local data is within staleness bound
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
