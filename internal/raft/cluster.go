package raft

import (
	"context"
	"encoding/json"
	"time"
)

// NewRaftCluster creates a new Raft cluster.
func NewRaftCluster(store StorageEngine, config RaftConfig) (*RaftCluster, error) {
	node, err := NewRaftNode(store, config)
	if err != nil {
		return nil, err
	}

	return &RaftCluster{
		node:  node,
		store: store,
	}, nil
}

// Start starts the cluster.
func (rc *RaftCluster) Start() error {
	return rc.node.Start()
}

// Stop stops the cluster.
func (rc *RaftCluster) Stop() error {
	return rc.node.Stop()
}

// Write performs a linearizable write.
func (rc *RaftCluster) Write(ctx context.Context, p Point) error {
	cmd := RaftCommand{
		Op:  "write",
		Key: p.Metric,
	}
	cmd.Value, _ = json.Marshal(p)

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return rc.node.Propose(ctx, data)
}

// Read performs a linearizable read.
func (rc *RaftCluster) Read(ctx context.Context, metric string, start, end time.Time) ([]Point, error) {
	if err := rc.node.LinearizableRead(ctx); err != nil {
		return nil, err
	}

	result, err := rc.store.ExecuteQuery(ctx, metric, start.UnixNano(), end.UnixNano())
	if err != nil {
		return nil, err
	}
	return result.Points, nil
}

// Node returns the underlying Raft node.
func (rc *RaftCluster) Node() *RaftNode {
	return rc.node
}

// IsLeader returns true if this node is the leader.
func (rc *RaftCluster) IsLeader() bool {
	return rc.node.IsLeader()
}

// Leader returns the current leader ID.
func (rc *RaftCluster) Leader() string {
	return rc.node.Leader()
}

// Stats returns cluster statistics.
func (rc *RaftCluster) Stats() RaftStats {
	return rc.node.Stats()
}
