package chronicle

import (
	"fmt"
	"testing"
)

func TestConsistentHashRing(t *testing.T) {
	ring := NewConsistentHashRing(64)

	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	if ring.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got %d", ring.NodeCount())
	}

	// Should consistently map the same key to the same node
	node1 := ring.GetNode("metric-cpu")
	node2 := ring.GetNode("metric-cpu")
	if node1 != node2 {
		t.Fatalf("inconsistent mapping: %s vs %s", node1, node2)
	}

	// GetNodes should return distinct nodes
	nodes := ring.GetNodes("metric-cpu", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Fatal("GetNodes returned duplicate nodes")
	}

	// Remove a node
	ring.RemoveNode("node-2")
	if ring.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes after removal, got %d", ring.NodeCount())
	}
}

func TestConsistentHashRingDistribution(t *testing.T) {
	ring := NewConsistentHashRing(128)
	ring.AddNode("a")
	ring.AddNode("b")
	ring.AddNode("c")

	counts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ring.GetNode(key)
		counts[node]++
	}

	// Each node should get at least 20% of keys (rough check)
	for node, count := range counts {
		pct := float64(count) / 10000.0
		if pct < 0.2 {
			t.Errorf("node %s got only %.1f%% of keys", node, pct*100)
		}
	}
}

func TestEdgeMeshCreation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.Enabled = true
	config.NodeID = "test-node-1"

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("failed to create edge mesh: %v", err)
	}

	if mesh.NodeID() != "test-node-1" {
		t.Fatalf("expected node ID test-node-1, got %s", mesh.NodeID())
	}

	stats := mesh.Stats()
	if stats.NodeID != "test-node-1" {
		t.Fatalf("expected node ID in stats, got %s", stats.NodeID)
	}
	if stats.PeerCount != 0 {
		t.Fatalf("expected 0 peers, got %d", stats.PeerCount)
	}
}

func TestEdgeMeshAutoNodeID(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.Enabled = true
	// NodeID left empty — should auto-generate

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("failed to create edge mesh: %v", err)
	}

	if mesh.NodeID() == "" {
		t.Fatal("expected auto-generated node ID")
	}
}

func TestDeduplicatePoints(t *testing.T) {
	points := []Point{
		{Metric: "cpu", Timestamp: 1000, Value: 42.0},
		{Metric: "cpu", Timestamp: 1000, Value: 42.0}, // duplicate
		{Metric: "cpu", Timestamp: 2000, Value: 43.0},
		{Metric: "mem", Timestamp: 1000, Value: 80.0},
	}

	deduped := deduplicatePoints(points)
	if len(deduped) != 3 {
		t.Fatalf("expected 3 points after dedup, got %d", len(deduped))
	}
}

func TestDefaultEdgeMeshConfig(t *testing.T) {
	cfg := DefaultEdgeMeshConfig()
	if cfg.ReplicationFactor != 2 {
		t.Fatalf("expected replication factor 2, got %d", cfg.ReplicationFactor)
	}
	if cfg.VirtualNodes != 64 {
		t.Fatalf("expected 64 virtual nodes, got %d", cfg.VirtualNodes)
	}
	if cfg.MaxPeers != 32 {
		t.Fatalf("expected max 32 peers, got %d", cfg.MaxPeers)
	}
}
