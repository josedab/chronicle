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

// Type aliases re-export cluster types for backward compatibility.
type Cluster = cluster.Cluster
type ClusteredDB = cluster.ClusteredDB
type ClusterConfig = cluster.ClusterConfig
type ClusterNode = cluster.ClusterNode
type ClusterStats = cluster.ClusterStats
type ClusterEventListener = cluster.ClusterEventListener
type ReplicationMode = cluster.ReplicationMode
type NodeState = cluster.NodeState
type LogEntry = cluster.LogEntry
type LogEntryType = cluster.LogEntryType
type VoteRequest = cluster.VoteRequest
type VoteResponse = cluster.VoteResponse
type AppendEntriesRequest = cluster.AppendEntriesRequest
type AppendEntriesResponse = cluster.AppendEntriesResponse
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
