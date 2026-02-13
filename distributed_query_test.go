package chronicle

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDistributedQueryCoordinator(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "dq_metric", Value: float64(i), Timestamp: now + int64(i)})
	}
	db.Flush()

	t.Run("execute local scatter-gather", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		q := &Query{Metric: "dq_metric", Start: 0, End: now + 100}
		result, err := c.Execute(context.Background(), q, []string{"local"})
		if err != nil {
			t.Fatal(err)
		}
		if result.NodesQueried != 1 {
			t.Errorf("expected 1 node queried, got %d", result.NodesQueried)
		}
		if result.TotalPoints == 0 {
			t.Error("expected points in result")
		}
	})

	t.Run("multi-node merge", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		q := &Query{Metric: "dq_metric", Start: 0, End: now + 100}
		result, err := c.Execute(context.Background(), q, []string{"node-a", "node-b"})
		if err != nil {
			t.Fatal(err)
		}
		if result.NodesQueried != 2 {
			t.Errorf("expected 2 nodes, got %d", result.NodesQueried)
		}
		// Both should succeed (both use local executor)
		if result.NodesResponded != 2 {
			t.Errorf("expected 2 responded, got %d", result.NodesResponded)
		}
	})

	t.Run("sort-preserving merge", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		q := &Query{Metric: "dq_metric", Start: 0, End: now + 100}
		result, err := c.Execute(context.Background(), q, []string{"a", "b"})
		if err != nil {
			t.Fatal(err)
		}
		// Verify sorted order
		for i := 1; i < len(result.Points); i++ {
			if result.Points[i].Timestamp < result.Points[i-1].Timestamp {
				t.Error("points not sorted by timestamp")
				break
			}
		}
	})

	t.Run("failing node", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		c.SetExecutor(func(ctx context.Context, nodeID string, q *Query) (*Result, error) {
			if nodeID == "bad-node" {
				return nil, fmt.Errorf("connection refused")
			}
			return db.Execute(q)
		})
		q := &Query{Metric: "dq_metric", Start: 0, End: now + 100}
		result, err := c.Execute(context.Background(), q, []string{"good-node", "bad-node"})
		if err != nil {
			t.Fatal(err)
		}
		if result.NodesFailed != 1 {
			t.Errorf("expected 1 failed, got %d", result.NodesFailed)
		}
		if result.NodesResponded != 1 {
			t.Errorf("expected 1 responded, got %d", result.NodesResponded)
		}
	})

	t.Run("nil query", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		_, err := c.Execute(context.Background(), nil, []string{"a"})
		if err == nil {
			t.Error("expected error for nil query")
		}
	})

	t.Run("no nodes", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		_, err := c.Execute(context.Background(), &Query{Metric: "x"}, nil)
		if err == nil {
			t.Error("expected error for no nodes")
		}
	})

	t.Run("max fanout", func(t *testing.T) {
		cfg := DefaultDistributedQueryConfig()
		cfg.MaxFanOut = 2
		c := NewDistributedQueryCoordinator(db, cfg)
		nodes := []string{"a", "b", "c", "d", "e"}
		plan := c.Plan(&Query{Metric: "x"}, nodes)
		if plan.FanOut != 2 {
			t.Errorf("expected fanout 2, got %d", plan.FanOut)
		}
	})

	t.Run("max result size", func(t *testing.T) {
		cfg := DefaultDistributedQueryConfig()
		cfg.MaxResultSize = 5
		c := NewDistributedQueryCoordinator(db, cfg)
		q := &Query{Metric: "dq_metric", Start: 0, End: now + 100}
		result, _ := c.Execute(context.Background(), q, []string{"a"})
		if result.TotalPoints > 5 {
			t.Errorf("expected max 5 points, got %d", result.TotalPoints)
		}
	})

	t.Run("stats", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		q := &Query{Metric: "dq_metric", Start: 0, End: now + 100}
		c.Execute(context.Background(), q, []string{"a"})
		c.Execute(context.Background(), q, []string{"a", "b"})

		stats := c.GetStats()
		if stats.TotalQueries != 2 {
			t.Errorf("expected 2 queries, got %d", stats.TotalQueries)
		}
	})

	t.Run("start stop", func(t *testing.T) {
		c := NewDistributedQueryCoordinator(db, DefaultDistributedQueryConfig())
		c.Start()
		c.Start()
		c.Stop()
		c.Stop()
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultDistributedQueryConfig()
		if cfg.MaxFanOut != 10 {
			t.Errorf("unexpected max fanout: %d", cfg.MaxFanOut)
		}
		if cfg.MergeStrategy != "sort_preserve" {
			t.Errorf("unexpected strategy: %s", cfg.MergeStrategy)
		}
	})
}
