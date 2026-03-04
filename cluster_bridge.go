// Bridge: cluster_bridge.go
//
// This file bridges internal/cluster/ into the public chronicle package.
// It re-exports types via type aliases so that callers use the top-level
// chronicle API while implementation stays private.
//
// Pattern: internal/cluster/ (implementation) → cluster_bridge.go (public API)

package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/cluster"
)

// Cluster is the cluster coordinator managing node membership and consensus.
type Cluster = cluster.Cluster

// ClusteredDB wraps a DB with clustering capabilities.
type ClusteredDB = cluster.ClusteredDB

// ClusterConfig holds configuration for cluster setup.
type ClusterConfig = cluster.ClusterConfig

// ClusterNode represents a single node in the cluster.
type ClusterNode = cluster.ClusterNode

// ClusterStats contains runtime statistics for the cluster.
type ClusterStats = cluster.ClusterStats

// ClusterEventListener receives notifications about cluster events.
type ClusterEventListener = cluster.ClusterEventListener

// ReplicationMode specifies how writes are replicated across nodes.
type ReplicationMode = cluster.ReplicationMode

// NodeState is the current state of a cluster node (follower, candidate, leader).
type NodeState = cluster.NodeState

// LogEntry is a single entry in the cluster replication log.
type LogEntry = cluster.LogEntry

// LogEntryType identifies the type of a log entry (write, config, noop).
type LogEntryType = cluster.LogEntryType

// VoteRequest is a request message for leader election.
type VoteRequest = cluster.VoteRequest

// VoteResponse is a response message for leader election.
type VoteResponse = cluster.VoteResponse

// AppendEntriesRequest is a request to replicate log entries to a follower.
type AppendEntriesRequest = cluster.AppendEntriesRequest

// AppendEntriesResponse is the response to an append-entries replication request.
type AppendEntriesResponse = cluster.AppendEntriesResponse

// GossipMessage is a message exchanged via the gossip protocol for membership.
type GossipMessage = cluster.GossipMessage

// Re-export constants.
const (
	ReplicationAsync  = cluster.ReplicationAsync
	ReplicationSync   = cluster.ReplicationSync
	ReplicationQuorum = cluster.ReplicationQuorum
)

const (
	NodeStateFollower  = cluster.NodeStateFollower
	NodeStateCandidate = cluster.NodeStateCandidate
	NodeStateLeader    = cluster.NodeStateLeader
)

const (
	LogEntryWrite  = cluster.LogEntryWrite
	LogEntryConfig = cluster.LogEntryConfig
	LogEntryNoop   = cluster.LogEntryNoop
)

// Re-export constructor functions.
var DefaultClusterConfig = cluster.DefaultClusterConfig

// NewCluster creates a new cluster coordinator.
func NewCluster(db *DB, config ClusterConfig) (*Cluster, error) {
	adapter := &dbClusterAdapter{db: db}
	return cluster.NewCluster(adapter, config)
}

// NewClusteredDB creates a clustering-enabled database wrapper.
func NewClusteredDB(db *DB, config ClusterConfig) (*ClusteredDB, error) {
	adapter := &dbClusterAdapter{db: db}
	return cluster.NewClusteredDB(adapter, config)
}

// dbClusterAdapter makes *DB satisfy cluster.PointWriter.
type dbClusterAdapter struct{ db *DB }

func (a *dbClusterAdapter) WritePoint(p cluster.Point) error {
	return a.db.Write(Point{Metric: p.Metric, Timestamp: p.Timestamp, Value: p.Value, Tags: p.Tags})
}
