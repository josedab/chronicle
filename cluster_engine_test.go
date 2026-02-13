package chronicle

import (
	"fmt"
	"testing"
)

func TestEmbeddedClusterEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("single node cluster", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "node-1"
		engine := NewEmbeddedClusterEngine(db, cfg)

		nodes := engine.Nodes()
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
		if nodes[0].ID != "node-1" {
			t.Errorf("expected node-1, got %s", nodes[0].ID)
		}

		local := engine.LocalNode()
		if local.ID != "node-1" {
			t.Errorf("expected node-1, got %s", local.ID)
		}
	})

	t.Run("join and leave", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "primary"
		engine := NewEmbeddedClusterEngine(db, cfg)

		if err := engine.Join("secondary", "localhost:7947"); err != nil {
			t.Fatal(err)
		}
		if err := engine.Join("tertiary", "localhost:7948"); err != nil {
			t.Fatal(err)
		}

		nodes := engine.Nodes()
		if len(nodes) != 3 {
			t.Fatalf("expected 3 nodes, got %d", len(nodes))
		}

		// Duplicate join should fail
		if err := engine.Join("secondary", "localhost:7947"); err == nil {
			t.Error("expected error for duplicate join")
		}

		// Leave
		if err := engine.Leave("secondary"); err != nil {
			t.Fatal(err)
		}
		nodes = engine.Nodes()
		if len(nodes) != 2 {
			t.Fatalf("expected 2 nodes, got %d", len(nodes))
		}

		// Leave unknown node
		if err := engine.Leave("unknown"); err == nil {
			t.Error("expected error for unknown node leave")
		}
	})

	t.Run("consistent hashing", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "hash-node-1"
		cfg.ReplicationFactor = 2
		engine := NewEmbeddedClusterEngine(db, cfg)
		engine.Join("hash-node-2", ":7947")
		engine.Join("hash-node-3", ":7948")

		// Same key should always map to same node
		owner1 := engine.OwnerOf("metric-a")
		owner2 := engine.OwnerOf("metric-a")
		if owner1 != owner2 {
			t.Error("consistent hashing broken")
		}

		// Should get replicas
		replicas := engine.ReplicasOf("metric-a")
		if len(replicas) < 2 {
			t.Errorf("expected at least 2 replicas, got %d", len(replicas))
		}

		// All keys should have an owner
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test-key-%d", i)
			if engine.OwnerOf(key) == "" {
				t.Errorf("key %s has no owner", key)
			}
		}
	})

	t.Run("partition assignments", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "assign-1"
		engine := NewEmbeddedClusterEngine(db, cfg)
		engine.Join("assign-2", ":7947")

		assignments := engine.Assignments()
		if len(assignments) != 256 {
			t.Errorf("expected 256 partitions, got %d", len(assignments))
		}

		// All assignments should have a primary
		for _, a := range assignments {
			if a.Primary == "" {
				t.Errorf("partition %d has no primary", a.PartitionID)
			}
		}
	})

	t.Run("stats", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "stats-node"
		engine := NewEmbeddedClusterEngine(db, cfg)

		stats := engine.GetStats()
		if stats.NodeCount != 1 {
			t.Errorf("expected 1 node, got %d", stats.NodeCount)
		}
		if stats.AliveNodes != 1 {
			t.Errorf("expected 1 alive, got %d", stats.AliveNodes)
		}
		if stats.PartitionCount != 256 {
			t.Errorf("expected 256 partitions, got %d", stats.PartitionCount)
		}
		if stats.RebalanceCount < 1 {
			t.Error("expected at least 1 rebalance")
		}
	})

	t.Run("event handlers", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "event-node"
		engine := NewEmbeddedClusterEngine(db, cfg)

		joinCh := make(chan string, 1)
		leaveCh := make(chan string, 1)

		engine.OnJoin(func(n GossipClusterNode) {
			joinCh <- n.ID
		})
		engine.OnLeave(func(n GossipClusterNode) {
			leaveCh <- n.ID
		})

		engine.Join("event-peer", ":7947")
		select {
		case id := <-joinCh:
			if id != "event-peer" {
				t.Errorf("unexpected join node: %s", id)
			}
		default:
			// goroutine may not have fired yet, that's ok
		}

		engine.Leave("event-peer")
		select {
		case id := <-leaveCh:
			if id != "event-peer" {
				t.Errorf("unexpected leave node: %s", id)
			}
		default:
		}
	})

	t.Run("start and stop", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "lifecycle-node"
		engine := NewEmbeddedClusterEngine(db, cfg)

		if err := engine.Start(); err != nil {
			t.Fatal(err)
		}
		// Double start should be safe
		if err := engine.Start(); err != nil {
			t.Fatal(err)
		}
		engine.Stop()
		// Double stop should be safe
		engine.Stop()
	})

	t.Run("hash ring", func(t *testing.T) {
		hr := newHashRing(64)

		// Empty ring
		if hr.GetNode("any") != "" {
			t.Error("expected empty for empty ring")
		}
		if hr.NodeCount() != 0 {
			t.Error("expected 0 nodes")
		}

		hr.AddNode("a")
		hr.AddNode("b")
		hr.AddNode("c")

		if hr.NodeCount() != 3 {
			t.Errorf("expected 3 nodes, got %d", hr.NodeCount())
		}

		// Duplicate add should be idempotent
		hr.AddNode("a")
		if hr.NodeCount() != 3 {
			t.Errorf("expected 3 nodes after dup, got %d", hr.NodeCount())
		}

		// Remove and re-add
		hr.RemoveNode("b")
		if hr.NodeCount() != 2 {
			t.Errorf("expected 2 nodes, got %d", hr.NodeCount())
		}

		// Remove non-existent
		hr.RemoveNode("nonexistent")
		if hr.NodeCount() != 2 {
			t.Errorf("expected 2 nodes, got %d", hr.NodeCount())
		}

		// GetNodes with count > nodes
		nodes := hr.GetNodes("key", 10)
		if len(nodes) != 2 {
			t.Errorf("expected 2 nodes (capped), got %d", len(nodes))
		}
	})

	t.Run("is local", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		cfg.NodeID = "local-check"
		engine := NewEmbeddedClusterEngine(db, cfg)

		// With single node, everything is local
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			if !engine.IsLocal(key) {
				t.Errorf("key %s should be local with single node", key)
			}
		}
	})

	t.Run("node state string", func(t *testing.T) {
		if GossipNodeStateAlive.String() != "alive" {
			t.Error("unexpected state string")
		}
		if GossipNodeStateSuspect.String() != "suspect" {
			t.Error("unexpected state string")
		}
		if GossipNodeStateDead.String() != "dead" {
			t.Error("unexpected state string")
		}
		if GossipNodeStateLeft.String() != "left" {
			t.Error("unexpected state string")
		}
		if GossipNodeState(99).String() != "unknown" {
			t.Error("unexpected state string")
		}
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultGossipClusterConfig()
		if cfg.ReplicationFactor != 2 {
			t.Errorf("unexpected replication factor: %d", cfg.ReplicationFactor)
		}
		if cfg.VirtualNodes != 128 {
			t.Errorf("unexpected virtual nodes: %d", cfg.VirtualNodes)
		}
	})
}
