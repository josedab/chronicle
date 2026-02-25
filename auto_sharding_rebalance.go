package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"
)

// Imbalance detection and rebalance planning for auto-sharding.

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
				http.Error(w, "conflict", http.StatusConflict)
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
			http.Error(w, "not found", http.StatusNotFound)
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
			http.Error(w, "bad request", http.StatusBadRequest)
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
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"metric":  metric,
			"tags":    tags,
			"node_id": node.ID,
			"address": node.Address,
		})
	})
}
