package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestIncrementalViewEngine(t *testing.T) {
	db := setupTestDB(t)
	engine := NewIncrementalViewEngine(db, DefaultIncrementalViewConfig())

	t.Run("create and drop view", func(t *testing.T) {
		err := engine.CreateView(IncrementalViewDefinition{
			Name:         "test_view",
			SourceMetric: "cpu",
			Aggregation:  AggSum,
			OutputMetric: "matview_cpu_sum",
			Enabled:      true,
		})
		if err != nil {
			t.Fatalf("create view failed: %v", err)
		}

		views := engine.ListViews()
		if len(views) == 0 {
			t.Error("expected views")
		}

		err = engine.DropView("test_view")
		if err != nil {
			t.Fatalf("drop view failed: %v", err)
		}
	})

	t.Run("create duplicate view", func(t *testing.T) {
		engine.CreateView(IncrementalViewDefinition{Name: "dup", SourceMetric: "x", Enabled: true})
		err := engine.CreateView(IncrementalViewDefinition{Name: "dup", SourceMetric: "x", Enabled: true})
		if err == nil {
			t.Error("expected error for duplicate view")
		}
		engine.DropView("dup")
	})

	t.Run("create view empty name", func(t *testing.T) {
		err := engine.CreateView(IncrementalViewDefinition{})
		if err == nil {
			t.Error("expected error for empty name")
		}
	})

	t.Run("drop nonexistent view", func(t *testing.T) {
		err := engine.DropView("nonexistent")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("apply points", func(t *testing.T) {
		engine.CreateView(IncrementalViewDefinition{
			Name:         "sum_view",
			SourceMetric: "cpu",
			Aggregation:  AggSum,
			OutputMetric: "matview_cpu",
			GroupBy:      []string{"host"},
			Enabled:      true,
		})
		defer engine.DropView("sum_view")

		points := []Point{
			{Metric: "cpu", Value: 10.0, Tags: map[string]string{"host": "a"}},
			{Metric: "cpu", Value: 20.0, Tags: map[string]string{"host": "a"}},
			{Metric: "cpu", Value: 30.0, Tags: map[string]string{"host": "b"}},
		}

		err := engine.ApplyPoints(points)
		if err != nil {
			t.Fatalf("apply points failed: %v", err)
		}

		state, err := engine.GetViewState("sum_view")
		if err != nil {
			t.Fatal(err)
		}
		if state.GroupCount != 2 {
			t.Errorf("expected 2 groups, got %d", state.GroupCount)
		}
	})

	t.Run("record delta and refresh", func(t *testing.T) {
		now := time.Now()
		for i := 0; i < 10; i++ {
			db.Write(Point{
				Metric:    "mem",
				Value:     float64(i * 10),
				Timestamp: now.Add(-time.Duration(10-i) * time.Second).UnixNano(),
			})
		}
		db.Flush()

		engine.CreateView(IncrementalViewDefinition{
			Name:         "mem_avg",
			SourceMetric: "mem",
			Aggregation:  AggMean,
			OutputMetric: "matview_mem_avg",
			RefreshMode:  RefreshIncremental,
			Enabled:      true,
		})
		defer engine.DropView("mem_avg")

		engine.RecordDelta(1, "mem", 10, now.Add(-10*time.Second).UnixNano(), now.UnixNano())

		err := engine.RefreshView(context.Background(), "mem_avg")
		if err != nil {
			t.Fatalf("refresh failed: %v", err)
		}

		state, err := engine.GetViewState("mem_avg")
		if err != nil {
			t.Fatal(err)
		}
		if state.PendingDeltas != 0 {
			t.Errorf("expected 0 pending deltas after refresh, got %d", state.PendingDeltas)
		}
	})

	t.Run("full refresh", func(t *testing.T) {
		now := time.Now()
		for i := 0; i < 5; i++ {
			db.Write(Point{
				Metric:    "disk",
				Value:     float64(i * 100),
				Timestamp: now.Add(-time.Duration(5-i) * time.Second).UnixNano(),
			})
		}
		db.Flush()

		engine.CreateView(IncrementalViewDefinition{
			Name:         "disk_max",
			SourceMetric: "disk",
			Aggregation:  AggMax,
			OutputMetric: "matview_disk_max",
			RefreshMode:  RefreshFull,
			Enabled:      true,
		})
		defer engine.DropView("disk_max")

		err := engine.RefreshView(context.Background(), "disk_max")
		if err != nil {
			t.Fatalf("full refresh failed: %v", err)
		}
	})

	t.Run("refresh nonexistent view", func(t *testing.T) {
		err := engine.RefreshView(context.Background(), "nope")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("refresh all", func(t *testing.T) {
		err := engine.RefreshAll(context.Background())
		if err != nil {
			t.Fatalf("refresh all failed: %v", err)
		}
	})
}

func TestViewAggState(t *testing.T) {
	t.Run("avg", func(t *testing.T) {
		s := &ViewAggState{Sum: 100, Count: 4}
		if s.Avg() != 25 {
			t.Errorf("expected 25, got %f", s.Avg())
		}
	})

	t.Run("avg zero count", func(t *testing.T) {
		s := &ViewAggState{}
		if s.Avg() != 0 {
			t.Errorf("expected 0, got %f", s.Avg())
		}
	})

	t.Run("merge", func(t *testing.T) {
		a := &ViewAggState{Sum: 10, Count: 2, Min: 3, Max: 7}
		b := &ViewAggState{Sum: 20, Count: 3, Min: 1, Max: 10}
		a.Merge(b)
		if a.Sum != 30 || a.Count != 5 || a.Min != 1 || a.Max != 10 {
			t.Errorf("unexpected merge result: %+v", a)
		}
	})

	t.Run("merge nil", func(t *testing.T) {
		a := &ViewAggState{Sum: 10, Count: 2}
		a.Merge(nil)
		if a.Sum != 10 {
			t.Error("merge nil should be no-op")
		}
	})
}

func TestIncrementalViewCQL(t *testing.T) {
	db := setupTestDB(t)
	engine := NewIncrementalViewEngine(db, DefaultIncrementalViewConfig())

	t.Run("create view CQL", func(t *testing.T) {
		result, err := engine.ExecuteCQL("CREATE MATERIALIZED VIEW my_view AS SELECT sum(cpu)")
		if err != nil {
			t.Fatalf("CQL create failed: %v", err)
		}
		if result == "" {
			t.Error("expected non-empty result")
		}
	})

	t.Run("drop view CQL", func(t *testing.T) {
		result, err := engine.ExecuteCQL("DROP VIEW my_view")
		if err != nil {
			t.Fatalf("CQL drop failed: %v", err)
		}
		if result == "" {
			t.Error("expected non-empty result")
		}
	})

	t.Run("unknown CQL", func(t *testing.T) {
		_, err := engine.ExecuteCQL("INVALID COMMAND")
		if err == nil {
			t.Error("expected error for unknown CQL")
		}
	})
}

func TestOnCompaction(t *testing.T) {
	db := setupTestDB(t)
	engine := NewIncrementalViewEngine(db, DefaultIncrementalViewConfig())

	engine.CreateView(IncrementalViewDefinition{
		Name:         "compaction_view",
		SourceMetric: "cpu",
		Aggregation:  AggSum,
		Enabled:      true,
	})

	engine.OnCompaction(1, "cpu", 100, 1000, 2000)

	// Give the async goroutine time to run
	time.Sleep(100 * time.Millisecond)
}

func TestIncrViewGroupKey(t *testing.T) {
	p := &Point{Tags: map[string]string{"host": "a", "dc": "us"}}

	key := incrViewGroupKey(p, nil)
	if key != "__global__" {
		t.Errorf("expected __global__, got %s", key)
	}

	key = incrViewGroupKey(p, []string{"host"})
	if key != "host=a" {
		t.Errorf("expected host=a, got %s", key)
	}

	key = incrViewGroupKey(p, []string{"host", "dc"})
	if key != "host=a|dc=us" {
		t.Errorf("expected host=a|dc=us, got %s", key)
	}
}
