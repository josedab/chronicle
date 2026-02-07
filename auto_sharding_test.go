package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDefaultAutoShardingConfig(t *testing.T) {
	cfg := DefaultAutoShardingConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.VirtualNodes != 64 {
		t.Errorf("expected VirtualNodes=64, got %d", cfg.VirtualNodes)
	}
	if cfg.RebalanceThreshold != 0.05 {
		t.Errorf("expected RebalanceThreshold=0.05, got %f", cfg.RebalanceThreshold)
	}
	if cfg.MaxShardsPerNode != 256 {
		t.Errorf("expected MaxShardsPerNode=256, got %d", cfg.MaxShardsPerNode)
	}
	if cfg.ReplicationFactor != 2 {
		t.Errorf("expected ReplicationFactor=2, got %d", cfg.ReplicationFactor)
	}
	if cfg.MigrationBatchSize != 10 {
		t.Errorf("expected MigrationBatchSize=10, got %d", cfg.MigrationBatchSize)
	}
}

func TestShardHashRingAddRemoveGetNode(t *testing.T) {
	ring := newShardHashRing(64)

	// Empty ring returns empty string
	if got := ring.getNode("test"); got != "" {
		t.Errorf("expected empty string from empty ring, got %q", got)
	}

	ring.addNode("node-1")
	ring.addNode("node-2")
	ring.addNode("node-3")

	// Should return a node for any key
	got := ring.getNode("cpu.usage")
	if got == "" {
		t.Error("expected non-empty node from ring")
	}

	// Same key should always map to same node (consistency)
	for i := 0; i < 10; i++ {
		if ring.getNode("cpu.usage") != got {
			t.Error("consistent hashing violated: same key returned different node")
		}
	}

	// Remove the node and verify key maps elsewhere
	ring.removeNode(got)
	newGot := ring.getNode("cpu.usage")
	if newGot == "" {
		t.Error("expected non-empty node after removal")
	}
	if newGot == got {
		t.Errorf("expected different node after removing %s", got)
	}
}

func TestShardHashRingGetNodes(t *testing.T) {
	ring := newShardHashRing(64)
	ring.addNode("node-1")
	ring.addNode("node-2")
	ring.addNode("node-3")

	nodes := ring.getNodes("cpu.usage", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Error("expected distinct nodes for replication")
	}

	// Request more nodes than available
	nodes = ring.getNodes("cpu.usage", 5)
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes (all available), got %d", len(nodes))
	}
}

func TestShardHashRingDuplicateAdd(t *testing.T) {
	ring := newShardHashRing(16)
	ring.addNode("node-1")
	ring.addNode("node-1") // duplicate

	ring.mu.RLock()
	count := len(ring.ring)
	ring.mu.RUnlock()

	if count != 16 {
		t.Errorf("expected 16 entries (no duplicates), got %d", count)
	}
}

func TestAutoShardingEngineAddRemoveNode(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	// Add node
	err := engine.AddNode(ShardNode{
		ID:       "node-1",
		Address:  "localhost:8001",
		Capacity: 1 << 30,
	})
	if err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	// Add duplicate should fail
	err = engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001"})
	if err == nil {
		t.Error("expected error adding duplicate node")
	}

	// Add node without ID should fail
	err = engine.AddNode(ShardNode{Address: "localhost:8002"})
	if err == nil {
		t.Error("expected error adding node without ID")
	}

	nodes := engine.ListNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].State != "active" {
		t.Errorf("expected state 'active', got %q", nodes[0].State)
	}

	// Remove node
	err = engine.RemoveNode("node-1")
	if err != nil {
		t.Fatalf("RemoveNode failed: %v", err)
	}

	nodes = engine.ListNodes()
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes after removal, got %d", len(nodes))
	}

	// Remove non-existent should fail
	err = engine.RemoveNode("node-999")
	if err == nil {
		t.Error("expected error removing non-existent node")
	}
}

func TestAutoShardingEngineMetricRouting(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	// No nodes available
	_, err := engine.GetNodeForMetric("cpu.usage", nil)
	if err == nil {
		t.Error("expected error with no nodes")
	}

	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001", Capacity: 1 << 30})
	engine.AddNode(ShardNode{ID: "node-2", Address: "localhost:8002", Capacity: 1 << 30})
	engine.AddNode(ShardNode{ID: "node-3", Address: "localhost:8003", Capacity: 1 << 30})

	// Consistent routing
	node1, err := engine.GetNodeForMetric("cpu.usage", map[string]string{"host": "server1"})
	if err != nil {
		t.Fatalf("GetNodeForMetric failed: %v", err)
	}
	node2, err := engine.GetNodeForMetric("cpu.usage", map[string]string{"host": "server1"})
	if err != nil {
		t.Fatalf("GetNodeForMetric failed: %v", err)
	}
	if node1.ID != node2.ID {
		t.Error("same metric+tags should route to same node")
	}

	// Different tags may route to different nodes (not guaranteed but test it doesn't fail)
	_, err = engine.GetNodeForMetric("cpu.usage", map[string]string{"host": "server999"})
	if err != nil {
		t.Fatalf("GetNodeForMetric failed for different tags: %v", err)
	}
}

func TestAutoShardingEngineAssignShard(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001", Capacity: 1 << 30})
	engine.AddNode(ShardNode{ID: "node-2", Address: "localhost:8002", Capacity: 1 << 30})

	// Assign a shard
	assignment, err := engine.AssignShard("cpu.usage,host=server1")
	if err != nil {
		t.Fatalf("AssignShard failed: %v", err)
	}
	if assignment.MetricKey != "cpu.usage,host=server1" {
		t.Errorf("expected metric key 'cpu.usage,host=server1', got %q", assignment.MetricKey)
	}
	if assignment.ShardID == "" || assignment.NodeID == "" {
		t.Error("expected non-empty shard and node ID")
	}

	// Same key should return same assignment
	assignment2, err := engine.AssignShard("cpu.usage,host=server1")
	if err != nil {
		t.Fatalf("AssignShard failed: %v", err)
	}
	if assignment.ShardID != assignment2.ShardID {
		t.Error("same metric key should get same shard assignment")
	}

	// Verify shard was created
	shard := engine.GetShard(assignment.ShardID)
	if shard == nil {
		t.Fatal("expected shard to exist")
	}
	if shard.State != ShardActive {
		t.Errorf("expected shard state active, got %v", shard.State)
	}

	// No nodes should fail
	engine2 := NewAutoShardingEngine(nil, cfg)
	_, err = engine2.AssignShard("test.metric")
	if err == nil {
		t.Error("expected error with no nodes")
	}
}

func TestAutoShardingEngineRebalance(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	cfg.RebalanceThreshold = 0.01
	engine := NewAutoShardingEngine(nil, cfg)

	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001", Capacity: 1 << 30})
	engine.AddNode(ShardNode{ID: "node-2", Address: "localhost:8002", Capacity: 1 << 30})

	// Manually create an imbalance by assigning all shards to node-1
	engine.mu.Lock()
	for i := 0; i < 20; i++ {
		shardID := "shard-" + string(rune('A'+i))
		engine.shards[shardID] = &Shard{
			ID:        shardID,
			NodeID:    "node-1",
			State:     ShardActive,
			SizeBytes: 1024,
		}
		engine.nodes["node-1"].Shards = append(engine.nodes["node-1"].Shards, shardID)
	}
	engine.mu.Unlock()

	// Imbalance should be significant
	ratio := engine.GetImbalanceRatio()
	if ratio <= 0 {
		t.Errorf("expected positive imbalance ratio, got %f", ratio)
	}

	// Rebalance
	plan, err := engine.Rebalance()
	if err != nil {
		t.Fatalf("Rebalance failed: %v", err)
	}
	if plan.State != "completed" {
		t.Errorf("expected migration state 'completed', got %q", plan.State)
	}
	if plan.FromNode != "node-1" {
		t.Errorf("expected migration from node-1, got %q", plan.FromNode)
	}
	if plan.ToNode != "node-2" {
		t.Errorf("expected migration to node-2, got %q", plan.ToNode)
	}
	if len(plan.Shards) == 0 {
		t.Error("expected at least one shard in migration plan")
	}

	// After rebalance, imbalance should be reduced
	newRatio := engine.GetImbalanceRatio()
	if newRatio >= ratio {
		t.Errorf("expected reduced imbalance after rebalance: was %f, now %f", ratio, newRatio)
	}

	// Stats should reflect the rebalance
	stats := engine.Stats()
	if stats.CompletedMigrations < 1 {
		t.Error("expected at least 1 completed migration")
	}
	if stats.TotalRebalances < 1 {
		t.Error("expected at least 1 total rebalance")
	}
}

func TestAutoShardingEngineRebalanceSingleNode(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001"})

	_, err := engine.Rebalance()
	if err == nil {
		t.Error("expected error rebalancing with single node")
	}
}

func TestAutoShardingEngineMigrateShard(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001", Capacity: 1 << 30})
	engine.AddNode(ShardNode{ID: "node-2", Address: "localhost:8002", Capacity: 1 << 30})

	// Create a shard on node-1
	engine.mu.Lock()
	engine.shards["shard-A"] = &Shard{
		ID:        "shard-A",
		NodeID:    "node-1",
		State:     ShardActive,
		SizeBytes: 4096,
	}
	engine.nodes["node-1"].Shards = append(engine.nodes["node-1"].Shards, "shard-A")
	engine.mu.Unlock()

	// Migrate to node-2
	plan, err := engine.MigrateShard("shard-A", "node-2")
	if err != nil {
		t.Fatalf("MigrateShard failed: %v", err)
	}
	if plan.State != "completed" {
		t.Errorf("expected state 'completed', got %q", plan.State)
	}
	if plan.BytesMoved != 4096 {
		t.Errorf("expected 4096 bytes moved, got %d", plan.BytesMoved)
	}

	// Verify shard moved
	shard := engine.GetShard("shard-A")
	if shard.NodeID != "node-2" {
		t.Errorf("expected shard on node-2, got %q", shard.NodeID)
	}

	// Migration to same node should fail
	_, err = engine.MigrateShard("shard-A", "node-2")
	if err == nil {
		t.Error("expected error migrating to same node")
	}

	// Non-existent shard should fail
	_, err = engine.MigrateShard("shard-999", "node-2")
	if err == nil {
		t.Error("expected error for non-existent shard")
	}

	// Non-existent target should fail
	_, err = engine.MigrateShard("shard-A", "node-999")
	if err == nil {
		t.Error("expected error for non-existent target node")
	}
}

func TestAutoShardingEngineListMigrations(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	migrations := engine.ListMigrations()
	if len(migrations) != 0 {
		t.Errorf("expected 0 migrations, got %d", len(migrations))
	}
}

func TestAutoShardingEngineStats(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	stats := engine.Stats()
	if stats.TotalNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", stats.TotalNodes)
	}

	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001"})
	engine.AddNode(ShardNode{ID: "node-2", Address: "localhost:8002"})

	stats = engine.Stats()
	if stats.TotalNodes != 2 {
		t.Errorf("expected 2 nodes, got %d", stats.TotalNodes)
	}
}

func TestAutoShardingEngineStartStop(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	cfg.RebalanceInterval = 100 * time.Millisecond
	engine := NewAutoShardingEngine(nil, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Double start should fail
	if err := engine.Start(); err == nil {
		t.Error("expected error on double start")
	}

	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Double stop should be a no-op
	if err := engine.Stop(); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}
}

func TestAutoShardingImbalanceRatioEdgeCases(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	// No nodes
	if ratio := engine.GetImbalanceRatio(); ratio != 0.0 {
		t.Errorf("expected 0.0 imbalance with no nodes, got %f", ratio)
	}

	// Single node
	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001"})
	if ratio := engine.GetImbalanceRatio(); ratio != 0.0 {
		t.Errorf("expected 0.0 imbalance with 1 node, got %f", ratio)
	}

	// Two nodes, no shards
	engine.AddNode(ShardNode{ID: "node-2", Address: "localhost:8002"})
	if ratio := engine.GetImbalanceRatio(); ratio != 0.0 {
		t.Errorf("expected 0.0 imbalance with no shards, got %f", ratio)
	}
}

func TestBuildMetricKey(t *testing.T) {
	// No tags
	key := buildMetricKey("cpu.usage", nil)
	if key != "cpu.usage" {
		t.Errorf("expected 'cpu.usage', got %q", key)
	}

	// With tags (should be sorted)
	key = buildMetricKey("cpu.usage", map[string]string{"host": "server1", "dc": "us-east"})
	if key != "cpu.usage,dc=us-east,host=server1" {
		t.Errorf("expected sorted tag key, got %q", key)
	}
}

func TestShardStateString(t *testing.T) {
	tests := []struct {
		state ShardState
		want  string
	}{
		{ShardActive, "active"},
		{ShardMigrating, "migrating"},
		{ShardDraining, "draining"},
		{ShardInactive, "inactive"},
		{ShardState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("ShardState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestAutoShardingHTTPHandlers(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// GET /api/v1/sharding/nodes - empty
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sharding/nodes", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /nodes: expected 200, got %d", w.Code)
	}

	// POST /api/v1/sharding/nodes - add node
	body := `{"id":"node-1","address":"localhost:8001","capacity":1073741824}`
	req = httptest.NewRequest(http.MethodPost, "/api/v1/sharding/nodes", strings.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("POST /nodes: expected 201, got %d", w.Code)
	}

	// POST duplicate node
	req = httptest.NewRequest(http.MethodPost, "/api/v1/sharding/nodes", strings.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("POST /nodes duplicate: expected 409, got %d", w.Code)
	}

	// Add a second node for routing and rebalance tests
	body2 := `{"id":"node-2","address":"localhost:8002","capacity":1073741824}`
	req = httptest.NewRequest(http.MethodPost, "/api/v1/sharding/nodes", strings.NewReader(body2))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("POST /nodes second: expected 201, got %d", w.Code)
	}

	// GET /api/v1/sharding/stats
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sharding/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /stats: expected 200, got %d", w.Code)
	}
	var stats AutoShardingStats
	json.NewDecoder(w.Body).Decode(&stats)
	if stats.TotalNodes != 2 {
		t.Errorf("expected 2 nodes in stats, got %d", stats.TotalNodes)
	}

	// GET /api/v1/sharding/route
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sharding/route?metric=cpu.usage&host=server1", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /route: expected 200, got %d", w.Code)
	}

	// GET /api/v1/sharding/route without metric
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sharding/route", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("GET /route no metric: expected 400, got %d", w.Code)
	}

	// GET /api/v1/sharding/shards
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sharding/shards", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /shards: expected 200, got %d", w.Code)
	}

	// GET /api/v1/sharding/migrations
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sharding/migrations", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /migrations: expected 200, got %d", w.Code)
	}

	// POST /api/v1/sharding/rebalance (no shards, will fail with "no rebalance needed" type error)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/sharding/rebalance", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	// May return 400 since there's nothing to rebalance
	if w.Code != http.StatusOK && w.Code != http.StatusBadRequest {
		t.Errorf("POST /rebalance: expected 200 or 400, got %d", w.Code)
	}

	// DELETE /api/v1/sharding/nodes/node-1
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/sharding/nodes/node-1", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("DELETE /nodes/node-1: expected 204, got %d", w.Code)
	}

	// DELETE non-existent
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/sharding/nodes/node-999", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("DELETE /nodes/node-999: expected 404, got %d", w.Code)
	}
}

func TestAutoShardingRemoveNodeReassignsShards(t *testing.T) {
	cfg := DefaultAutoShardingConfig()
	engine := NewAutoShardingEngine(nil, cfg)

	engine.AddNode(ShardNode{ID: "node-1", Address: "localhost:8001"})
	engine.AddNode(ShardNode{ID: "node-2", Address: "localhost:8002"})

	// Create shards on node-1
	engine.mu.Lock()
	for i := 0; i < 5; i++ {
		shardID := fmt.Sprintf("shard-%d", i)
		engine.shards[shardID] = &Shard{
			ID:     shardID,
			NodeID: "node-1",
			State:  ShardActive,
		}
		engine.nodes["node-1"].Shards = append(engine.nodes["node-1"].Shards, shardID)
	}
	engine.mu.Unlock()

	// Remove node-1 should reassign shards to node-2
	err := engine.RemoveNode("node-1")
	if err != nil {
		t.Fatalf("RemoveNode failed: %v", err)
	}

	// All shards should be on node-2 now
	engine.mu.RLock()
	for _, shard := range engine.shards {
		if shard.NodeID != "node-2" {
			t.Errorf("expected shard %s on node-2, got %s", shard.ID, shard.NodeID)
		}
	}
	engine.mu.RUnlock()
}

func TestAutoShardingHashDistribution(t *testing.T) {
	ring := newShardHashRing(128)
	ring.addNode("node-1")
	ring.addNode("node-2")
	ring.addNode("node-3")

	counts := make(map[string]int)
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("metric-%d", i)
		node := ring.getNode(key)
		counts[node]++
	}

	// Each node should get a reasonable share (within 50% of expected)
	expected := 1000 // 3000 / 3
	for node, count := range counts {
		if count < expected/2 || count > expected*2 {
			t.Errorf("node %s got %d keys, expected roughly %d (poor distribution)", node, count, expected)
		}
	}
}
