package chronicle

import (
	"testing"
)

func TestPartitionPrunerTimeRange(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewPartitionPrunerEngine(db, DefaultPartitionPrunerConfig())

	e.RegisterPartition(PartitionMeta{ID: "p1", StartTime: 100, EndTime: 200, Metric: "cpu"})
	e.RegisterPartition(PartitionMeta{ID: "p2", StartTime: 200, EndTime: 300, Metric: "cpu"})
	e.RegisterPartition(PartitionMeta{ID: "p3", StartTime: 300, EndTime: 400, Metric: "cpu"})

	// Query only covers 250-350, so p1 should be pruned
	q := &Query{Start: 250, End: 350}
	result := e.Prune(q)

	if result.TotalPartitions != 3 {
		t.Errorf("expected 3 total partitions, got %d", result.TotalPartitions)
	}
	if result.PrunedPartitions != 1 {
		t.Errorf("expected 1 pruned partition, got %d", result.PrunedPartitions)
	}
	if result.ScannedPartitions != 2 {
		t.Errorf("expected 2 scanned partitions, got %d", result.ScannedPartitions)
	}
}

func TestPartitionPrunerUnboundedQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewPartitionPrunerEngine(db, DefaultPartitionPrunerConfig())

	e.RegisterPartition(PartitionMeta{ID: "p1", StartTime: 100, EndTime: 200, Metric: "cpu"})
	e.RegisterPartition(PartitionMeta{ID: "p2", StartTime: 200, EndTime: 300, Metric: "cpu"})

	q := &Query{Start: 0, End: 0}
	result := e.Prune(q)

	if result.PrunedPartitions != 0 {
		t.Errorf("expected 0 pruned for unbounded query, got %d", result.PrunedPartitions)
	}
	if result.Reason != "unbounded_query" {
		t.Errorf("expected reason 'unbounded_query', got %q", result.Reason)
	}
}

func TestPartitionPrunerStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewPartitionPrunerEngine(db, DefaultPartitionPrunerConfig())

	e.RegisterPartition(PartitionMeta{ID: "p1", StartTime: 100, EndTime: 200, Metric: "cpu"})
	e.RegisterPartition(PartitionMeta{ID: "p2", StartTime: 200, EndTime: 300, Metric: "cpu"})
	e.RegisterPartition(PartitionMeta{ID: "p3", StartTime: 300, EndTime: 400, Metric: "cpu"})

	e.Prune(&Query{Start: 250, End: 350})
	e.Prune(&Query{Start: 0, End: 0})

	stats := e.GetStats()
	if stats.TotalPruneOps != 2 {
		t.Errorf("expected 2 prune ops, got %d", stats.TotalPruneOps)
	}
	if stats.RegisteredCount != 3 {
		t.Errorf("expected 3 registered partitions, got %d", stats.RegisteredCount)
	}
}

func TestPartitionPrunerBelowMinPartitions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPartitionPrunerConfig()
	cfg.MinPartitions = 5
	e := NewPartitionPrunerEngine(db, cfg)

	e.RegisterPartition(PartitionMeta{ID: "p1", StartTime: 100, EndTime: 200, Metric: "cpu"})
	e.RegisterPartition(PartitionMeta{ID: "p2", StartTime: 200, EndTime: 300, Metric: "cpu"})

	result := e.Prune(&Query{Start: 250, End: 350})
	if result.Reason != "below_min_partitions" {
		t.Errorf("expected reason 'below_min_partitions', got %q", result.Reason)
	}
}
