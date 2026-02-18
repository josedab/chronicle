package raft

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// RaftStateMachine is the interface applications implement for Raft state application.
type RaftStateMachine interface {
	// Apply applies a committed log entry to the state machine.
	Apply(entry *RaftLogEntry) error

	// Snapshot returns a serialized snapshot of the current state.
	Snapshot() ([]byte, error)

	// Restore restores state from a snapshot.
	Restore(data []byte) error
}

// RaftReadConsistency defines the read consistency level.
type RaftReadConsistency int

const (
	// RaftReadDefault uses the node's default consistency.
	RaftReadDefault RaftReadConsistency = iota
	// RaftReadLeader guarantees linearizable reads (leader-only).
	RaftReadLeader
	// RaftReadFollower allows stale reads from any node.
	RaftReadFollower
	// RaftReadLeaderLease uses leader lease for reads without heartbeat round-trip.
	RaftReadLeaderLease
)

func (c RaftReadConsistency) String() string {
	switch c {
	case RaftReadDefault:
		return "DEFAULT"
	case RaftReadLeader:
		return "LEADER"
	case RaftReadFollower:
		return "FOLLOWER"
	case RaftReadLeaderLease:
		return "LEADER_LEASE"
	default:
		return "UNKNOWN"
	}
}

// RaftHealthStatus represents the health of a Raft node.
type RaftHealthStatus struct {
	NodeID       string        `json:"node_id"`
	Role         string        `json:"role"`
	Term         uint64        `json:"term"`
	CommitIndex  uint64        `json:"commit_index"`
	LastApplied  uint64        `json:"last_applied"`
	LogLength    int           `json:"log_length"`
	LeaderID     string        `json:"leader_id"`
	PeerCount    int           `json:"peer_count"`
	Healthy      bool          `json:"healthy"`
	HealthIssues []string      `json:"health_issues,omitempty"`
	Uptime       time.Duration `json:"uptime"`
	LastContact  time.Time     `json:"last_contact"`
}

// RaftHealthChecker monitors and reports Raft cluster health.
type RaftHealthChecker struct {
	mu              sync.RWMutex
	node            *RaftNode
	startTime       time.Time
	lastContact     time.Time
	healthThreshold time.Duration
}

// NewRaftHealthChecker creates a new health checker.
func NewRaftHealthChecker(node *RaftNode, threshold time.Duration) *RaftHealthChecker {
	if threshold == 0 {
		threshold = 10 * time.Second
	}
	return &RaftHealthChecker{
		node:            node,
		startTime:       time.Now(),
		lastContact:     time.Now(),
		healthThreshold: threshold,
	}
}

// Check performs a health check.
func (hc *RaftHealthChecker) Check() RaftHealthStatus {
	hc.mu.RLock()
	startTime := hc.startTime
	lastContact := hc.lastContact
	threshold := hc.healthThreshold
	hc.mu.RUnlock()

	status := RaftHealthStatus{
		Uptime:      time.Since(startTime),
		LastContact: lastContact,
	}

	if hc.node == nil {
		status.Healthy = false
		status.HealthIssues = []string{"node is nil"}
		return status
	}

	status.NodeID = hc.node.config.NodeID
	status.Role = hc.node.Role().String()
	status.Term = hc.node.Term()
	status.LeaderID = hc.node.Leader()

	stats := hc.node.Stats()
	status.CommitIndex = stats.CommitIndex
	status.LastApplied = stats.LastApplied
	status.LogLength = int(stats.LogLength)
	status.PeerCount = len(hc.node.config.Peers)

	var issues []string

	// Check leader contact
	if time.Since(lastContact) > threshold {
		issues = append(issues, fmt.Sprintf("no leader contact for %v", time.Since(lastContact).Truncate(time.Second)))
	}

	// Check log lag
	if stats.CommitIndex > 0 && stats.LastApplied+100 < stats.CommitIndex {
		issues = append(issues, fmt.Sprintf("apply lag: committed=%d applied=%d", stats.CommitIndex, stats.LastApplied))
	}

	// Check no leader
	if status.LeaderID == "" && status.Role != "LEADER" {
		issues = append(issues, "no known leader")
	}

	status.Healthy = len(issues) == 0
	status.HealthIssues = issues
	return status
}

// RecordContact records a successful leader contact.
func (hc *RaftHealthChecker) RecordContact() {
	hc.mu.Lock()
	hc.lastContact = time.Now()
	hc.mu.Unlock()
}

// RaftEdgeConfig provides edge-specific consensus configuration.
type RaftEdgeConfig struct {
	// AdaptiveTimeout enables dynamic timeout adjustment based on network latency.
	AdaptiveTimeout bool `json:"adaptive_timeout"`

	// MinimumPeers is the minimum peers required before attempting leader election.
	MinimumPeers int `json:"minimum_peers"`

	// PartitionTolerance is how long to tolerate a network partition before stepping down.
	PartitionTolerance time.Duration `json:"partition_tolerance"`

	// WitnessMode enables witness nodes that vote but don't store full data.
	WitnessMode bool `json:"witness_mode"`

	// PriorityElection enables priority-based leader election (lower = higher priority).
	PriorityElection bool `json:"priority_election"`

	// NodePriority is this node's election priority (0 = highest).
	NodePriority int `json:"node_priority"`

	// CompressSnapshots enables gzip compression for snapshot transfer.
	CompressSnapshots bool `json:"compress_snapshots"`
}

// DefaultRaftEdgeConfig returns edge-optimized defaults.
func DefaultRaftEdgeConfig() RaftEdgeConfig {
	return RaftEdgeConfig{
		AdaptiveTimeout:    true,
		MinimumPeers:       1,
		PartitionTolerance: 30 * time.Second,
		WitnessMode:        false,
		PriorityElection:   false,
		NodePriority:       100,
		CompressSnapshots:  true,
	}
}

// RaftLatencyTracker tracks peer-to-peer latency for adaptive timeouts.
type RaftLatencyTracker struct {
	mu         sync.RWMutex
	samples    map[string][]time.Duration
	maxSamples int
}

// NewRaftLatencyTracker creates a latency tracker.
func NewRaftLatencyTracker(maxSamples int) *RaftLatencyTracker {
	if maxSamples <= 0 {
		maxSamples = 100
	}
	return &RaftLatencyTracker{
		samples:    make(map[string][]time.Duration),
		maxSamples: maxSamples,
	}
}

// Record records a latency sample for a peer.
func (lt *RaftLatencyTracker) Record(peerID string, latency time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	s := lt.samples[peerID]
	s = append(s, latency)
	if len(s) > lt.maxSamples {
		s = s[len(s)-lt.maxSamples:]
	}
	lt.samples[peerID] = s
}

// P99 returns the 99th percentile latency for a peer.
func (lt *RaftLatencyTracker) P99(peerID string) time.Duration {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	s := lt.samples[peerID]
	if len(s) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(s))
	copy(sorted, s)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	idx := int(float64(len(sorted)-1) * 0.99)
	return sorted[idx]
}

// Median returns the median latency for a peer.
func (lt *RaftLatencyTracker) Median(peerID string) time.Duration {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	s := lt.samples[peerID]
	if len(s) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(s))
	copy(sorted, s)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	return sorted[len(sorted)/2]
}

// AdaptiveElectionTimeout calculates optimal election timeout based on latency.
func (lt *RaftLatencyTracker) AdaptiveElectionTimeout(baseFactor int) time.Duration {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	if baseFactor <= 0 {
		baseFactor = 10
	}

	var maxP99 time.Duration
	for _, s := range lt.samples {
		if len(s) == 0 {
			continue
		}
		sorted := make([]time.Duration, len(s))
		copy(sorted, s)
		for i := 1; i < len(sorted); i++ {
			for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
				sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			}
		}
		idx := int(float64(len(sorted)-1) * 0.99)
		p99 := sorted[idx]
		if p99 > maxP99 {
			maxP99 = p99
		}
	}

	if maxP99 == 0 {
		return 150 * time.Millisecond // safe default
	}

	timeout := maxP99 * time.Duration(baseFactor)
	if timeout < 50*time.Millisecond {
		timeout = 50 * time.Millisecond
	}
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	return timeout
}

// AllPeers returns latency statistics for all tracked peers.
func (lt *RaftLatencyTracker) AllPeers() map[string]time.Duration {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	result := make(map[string]time.Duration, len(lt.samples))
	for peer := range lt.samples {
		result[peer] = lt.Median(peer)
	}
	return result
}

// RaftClusterTopology provides a view of the cluster topology.
type RaftClusterTopology struct {
	Nodes    []RaftNodeInfo `json:"nodes"`
	LeaderID string         `json:"leader_id"`
	Term     uint64         `json:"term"`
	Quorum   int            `json:"quorum"`
}

// RaftNodeInfo describes a node in the cluster.
type RaftNodeInfo struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Role     string `json:"role"`
	Witness  bool   `json:"witness"`
	Priority int    `json:"priority"`
}

// NewRaftClusterTopology builds topology from a node.
func NewRaftClusterTopology(nodeID string, peers []RaftPeer, edgeCfg RaftEdgeConfig) *RaftClusterTopology {
	nodes := make([]RaftNodeInfo, 0, len(peers)+1)
	nodes = append(nodes, RaftNodeInfo{
		ID:       nodeID,
		Witness:  edgeCfg.WitnessMode,
		Priority: edgeCfg.NodePriority,
	})
	for _, p := range peers {
		nodes = append(nodes, RaftNodeInfo{
			ID:      p.ID,
			Address: p.Addr,
		})
	}

	total := len(nodes)
	return &RaftClusterTopology{
		Nodes:  nodes,
		Quorum: total/2 + 1,
	}
}

// MarshalJSON implements json.Marshaler for RaftClusterTopology.
func (t *RaftClusterTopology) MarshalJSON() ([]byte, error) {
	type alias RaftClusterTopology
	return json.Marshal((*alias)(t))
}
