package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ClusterConfig configures the distributed clustering behavior.
type ClusterConfig struct {
	// NodeID is the unique identifier for this node.
	NodeID string

	// BindAddr is the address to bind for cluster communication.
	BindAddr string

	// AdvertiseAddr is the address to advertise to other nodes.
	AdvertiseAddr string

	// Seeds are initial nodes to join the cluster.
	Seeds []string

	// ElectionTimeout is the timeout for leader election.
	ElectionTimeout time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats.
	HeartbeatInterval time.Duration

	// ReplicationMode determines how data is replicated.
	ReplicationMode ReplicationMode

	// MinReplicas is the minimum replicas required for write acknowledgment.
	MinReplicas int

	// EnableLocalWrites allows writes on followers (eventual consistency).
	EnableLocalWrites bool

	// GossipInterval is how often to exchange cluster state.
	GossipInterval time.Duration

	// FailureDetectorTimeout is how long before a node is considered failed.
	FailureDetectorTimeout time.Duration
}

// ReplicationMode defines how data is replicated across the cluster.
type ReplicationMode int

const (
	// ReplicationAsync performs asynchronous replication.
	ReplicationAsync ReplicationMode = iota
	// ReplicationSync performs synchronous replication (strong consistency).
	ReplicationSync
	// ReplicationQuorum requires majority acknowledgment.
	ReplicationQuorum
)

// NodeState represents the state of a cluster node.
type NodeState int

const (
	// NodeStateFollower is a node following the leader.
	NodeStateFollower NodeState = iota
	// NodeStateCandidate is a node running for leader election.
	NodeStateCandidate
	// NodeStateLeader is the cluster leader.
	NodeStateLeader
)

// DefaultClusterConfig returns default cluster configuration.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		NodeID:                 fmt.Sprintf("node-%d", time.Now().UnixNano()%10000),
		ElectionTimeout:        150 * time.Millisecond,
		HeartbeatInterval:      50 * time.Millisecond,
		ReplicationMode:        ReplicationQuorum,
		MinReplicas:            1,
		EnableLocalWrites:      true,
		GossipInterval:         time.Second,
		FailureDetectorTimeout: 5 * time.Second,
	}
}

// ClusterNode represents a node in the cluster.
type ClusterNode struct {
	ID            string
	Addr          string
	State         NodeState
	Term          uint64
	LastHeartbeat time.Time
	Healthy       bool
	Metadata      map[string]string
}

// Cluster manages distributed coordination for Chronicle.
type Cluster struct {
	pw     PointWriter
	config ClusterConfig

	// Raft-like state
	currentTerm  uint64
	votedFor     string
	state        NodeState
	leaderID     string
	lastLogIndex uint64
	lastLogTerm  uint64
	commitIndex  uint64

	// Cluster membership
	nodes   map[string]*ClusterNode
	nodesMu sync.RWMutex

	// Log replication
	log     []LogEntry
	logMu   sync.RWMutex
	applied uint64

	// Election state
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	votesReceived   map[string]bool

	// Communication
	client *http.Client
	server *http.Server

	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   atomic.Bool
	stateMu   sync.RWMutex
	listeners []ClusterEventListener
}

// LogEntry represents a replicated log entry.
type LogEntry struct {
	Index     uint64          `json:"index"`
	Term      uint64          `json:"term"`
	Type      LogEntryType    `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}

// LogEntryType identifies the type of log entry.
type LogEntryType int

const (
	// LogEntryWrite is a write operation.
	LogEntryWrite LogEntryType = iota
	// LogEntryConfig is a configuration change.
	LogEntryConfig
	// LogEntryNoop is a no-op entry for leader confirmation.
	LogEntryNoop
)

// ClusterEventListener receives cluster events.
type ClusterEventListener interface {
	OnLeaderChange(leaderID string)
	OnNodeJoin(nodeID string)
	OnNodeLeave(nodeID string)
	OnStateChange(state NodeState)
}

// NewCluster creates a new cluster coordinator.
func NewCluster(pw PointWriter, config ClusterConfig) (*Cluster, error) {
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("node-%d", time.Now().UnixNano()%10000)
	}
	if config.ElectionTimeout <= 0 {
		config.ElectionTimeout = 150 * time.Millisecond
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 50 * time.Millisecond
	}
	if config.FailureDetectorTimeout <= 0 {
		config.FailureDetectorTimeout = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Cluster{
		pw:            pw,
		config:        config,
		state:         NodeStateFollower,
		nodes:         make(map[string]*ClusterNode),
		log:           make([]LogEntry, 0),
		votesReceived: make(map[string]bool),
		client: &http.Client{
			Timeout: config.ElectionTimeout,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	c.nodes[config.NodeID] = &ClusterNode{
		ID:            config.NodeID,
		Addr:          config.AdvertiseAddr,
		State:         NodeStateFollower,
		LastHeartbeat: time.Now(),
		Healthy:       true,
		Metadata:      make(map[string]string),
	}

	return c, nil
}

// ClusterStats contains cluster statistics.
type ClusterStats struct {
	NodeID       string `json:"node_id"`
	State        string `json:"state"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id"`
	NodeCount    int    `json:"node_count"`
	HealthyNodes int    `json:"healthy_nodes"`
	LogLength    int    `json:"log_length"`
	LastLogIndex uint64 `json:"last_log_index"`
	CommitIndex  uint64 `json:"commit_index"`
}

// VoteRequest is a request for vote in leader election.
type VoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// VoteResponse is a response to a vote request.
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRequest is the heartbeat/replication message.
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesResponse is the response to append entries.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// GossipMessage is used for cluster membership gossip.
type GossipMessage struct {
	Nodes []*ClusterNode `json:"nodes"`
}

// ClusteredDB wraps a PointWriter with clustering capabilities.
type ClusteredDB struct {
	pw      PointWriter
	cluster *Cluster
}

// NewClusteredDB creates a clustering-enabled database wrapper.
func NewClusteredDB(pw PointWriter, config ClusterConfig) (*ClusteredDB, error) {
	cluster, err := NewCluster(pw, config)
	if err != nil {
		return nil, err
	}

	return &ClusteredDB{
		pw:      pw,
		cluster: cluster,
	}, nil
}

// Helper to sort nodes by priority
type nodesByPriority []*ClusterNode

var _ sort.Interface = nodesByPriority{}

// jsonReader creates an io.Reader from JSON-encoded data
func jsonReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

// writeJSONResponse writes a JSON response to the http.ResponseWriter
func writeJSONResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
