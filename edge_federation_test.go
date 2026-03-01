package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestFedBloomFilter(t *testing.T) {
	bf := NewFedBloomFilter(1024, 7)

	t.Run("add and check", func(t *testing.T) {
		bf.Add("cpu_usage")
		bf.Add("mem_usage")
		bf.Add("disk_io")

		if !bf.MayContain("cpu_usage") {
			t.Error("expected cpu_usage to be present")
		}
		if !bf.MayContain("mem_usage") {
			t.Error("expected mem_usage to be present")
		}
		// False positive is possible but "unknown_metric" should likely not match
	})

	t.Run("reset", func(t *testing.T) {
		bf.Add("test")
		bf.Reset()
		// After reset, should not contain (with high probability)
		// Can't guarantee no false positive, but reset should clear all bits
		allFalse := true
		for _, b := range bf.bits {
			if b {
				allFalse = false
				break
			}
		}
		if !allFalse {
			t.Error("expected all bits false after reset")
		}
	})

	t.Run("default values", func(t *testing.T) {
		bf2 := NewFedBloomFilter(0, 0)
		if bf2.size != 1024 {
			t.Errorf("expected default size 1024, got %d", bf2.size)
		}
	})
}

func TestEdgeFederationProtocol(t *testing.T) {
	db := setupTestDB(t)
	config := DefaultEdgeFederationConfig()
	proto := NewEdgeFederationProtocol(db, config)

	t.Run("start and stop gossip", func(t *testing.T) {
		p := NewEdgeFederationProtocol(db, config)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		if err := p.Stop(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("double start", func(t *testing.T) {
		p := NewEdgeFederationProtocol(db, config)
		p.Start()
		defer p.Stop()
		if err := p.Start(); err == nil {
			t.Error("expected error on double start")
		}
	})

	t.Run("register and unregister node", func(t *testing.T) {
		proto.RegisterNode(EdgeFedNode{
			ID:      "node-1",
			Address: "192.168.1.1:9090",
			Latency: 5 * time.Millisecond,
		})

		nodes := proto.ListNodes()
		if len(nodes) != 1 {
			t.Errorf("expected 1 node, got %d", len(nodes))
		}

		proto.UnregisterNode("node-1")
		nodes = proto.ListNodes()
		if len(nodes) != 0 {
			t.Errorf("expected 0 nodes, got %d", len(nodes))
		}
	})

	t.Run("advertise metrics", func(t *testing.T) {
		proto.AdvertiseMetrics([]string{"cpu", "mem", "disk"})
		if proto.localNode.MetricCount != 3 {
			t.Errorf("expected 3 metrics, got %d", proto.localNode.MetricCount)
		}
	})

	t.Run("find nodes for metric", func(t *testing.T) {
		node := EdgeFedNode{
			ID:      "node-2",
			Address: "192.168.1.2:9090",
			Latency: 10 * time.Millisecond,
		}
		proto.RegisterNode(node)
		defer proto.UnregisterNode("node-2")

		// Add metrics to node's bloom filter
		proto.HandleGossipMessage("node-2", []string{"cpu", "mem"}, 10*time.Millisecond)

		candidates := proto.FindNodesForMetric("cpu")
		found := false
		for _, c := range candidates {
			if c.ID == "node-2" {
				found = true
			}
		}
		if !found {
			t.Error("expected node-2 to be a candidate for 'cpu'")
		}
	})

	t.Run("plan federated query", func(t *testing.T) {
		plan, err := proto.PlanFederatedQuery(&Query{Metric: "cpu"})
		if err != nil {
			t.Fatal(err)
		}
		if plan.MergeStrategy == "" {
			t.Error("expected merge strategy")
		}
	})

	t.Run("plan invalid query", func(t *testing.T) {
		_, err := proto.PlanFederatedQuery(nil)
		if err == nil {
			t.Error("expected error for nil query")
		}
	})

	t.Run("execute federated query", func(t *testing.T) {
		// Write some local data
		now := time.Now()
		for i := 0; i < 5; i++ {
			db.Write(Point{Metric: "fed_metric", Value: float64(i), Timestamp: now.Add(-time.Duration(5-i) * time.Second).UnixNano()})
		}
		db.Flush()

		result, err := proto.ExecuteFederatedQuery(context.Background(), &Query{Metric: "fed_metric"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if result.SuccessNodes < 1 {
			t.Error("expected at least 1 success node")
		}
	})

	t.Run("execute invalid query", func(t *testing.T) {
		_, err := proto.ExecuteFederatedQuery(context.Background(), nil)
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("handle gossip message", func(t *testing.T) {
		proto.HandleGossipMessage("node-3", []string{"net_rx", "net_tx"}, 20*time.Millisecond)
		nodes := proto.ListNodes()
		found := false
		for _, n := range nodes {
			if n.ID == "node-3" {
				found = true
				if n.MetricCount != 2 {
					t.Errorf("expected 2 metrics, got %d", n.MetricCount)
				}
			}
		}
		if !found {
			t.Error("expected node-3 after gossip")
		}
	})

	t.Run("backpressure", func(t *testing.T) {
		points := make([]Point, 5000)
		for i := range points {
			points[i] = Point{Value: float64(i)}
		}

		cfg := DefaultEdgeFederationConfig()
		cfg.BackpressureThreshold = 100
		p := NewEdgeFederationProtocol(db, cfg)
		result := p.WithBackpressure(points)
		if len(result) > 200 {
			t.Errorf("expected backpressure to limit results, got %d", len(result))
		}
	})

	t.Run("backpressure below threshold", func(t *testing.T) {
		points := make([]Point, 50)
		result := proto.WithBackpressure(points)
		if len(result) != 50 {
			t.Errorf("expected no backpressure, got %d", len(result))
		}
	})

	t.Run("get node stats", func(t *testing.T) {
		stats := proto.GetNodeStats()
		if stats["local_node"] == nil {
			t.Error("expected local_node in stats")
		}
	})

	t.Run("merge results dedup", func(t *testing.T) {
		points := []Point{
			{Metric: "cpu", Timestamp: 1, Value: 10},
			{Metric: "cpu", Timestamp: 1, Value: 10}, // duplicate
			{Metric: "cpu", Timestamp: 2, Value: 20},
		}
		merged := proto.mergeResults(points)
		if len(merged) != 2 {
			t.Errorf("expected 2 after dedup, got %d", len(merged))
		}
	})
}
