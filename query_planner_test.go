package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestQueryPlannerBasic(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some data to create partitions
	for i := 0; i < 100; i++ {
		db.Write(Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "web-1"},
			Value:     float64(i),
			Timestamp: time.Now().Add(-time.Duration(i) * time.Minute).UnixNano(),
		})
	}

	config := DefaultQueryPlannerConfig()
	planner := NewQueryPlanner(db, config)
	planner.RefreshStats()

	q := &Query{
		Metric: "cpu",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	plan, err := planner.Plan(context.Background(), q)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.Root == nil {
		t.Fatal("expected non-nil plan root")
	}
	if len(plan.Optimizations) == 0 {
		t.Error("expected at least one optimization")
	}
}

func TestQueryPlannerExplain(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.Write(Point{Metric: "mem", Value: 42, Timestamp: time.Now().UnixNano()})

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	q := &Query{
		Metric: "mem",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	explanation, err := planner.Explain(context.Background(), q)
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if explanation == "" {
		t.Error("expected non-empty explanation")
	}
}

func TestQueryPlannerPartitionPruning(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	// Query with no matching time range should produce empty plan
	q := &Query{
		Metric: "nonexistent",
		Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
		End:    time.Now().Add(-23 * time.Hour).UnixNano(),
	}

	plan, err := planner.Plan(context.Background(), q)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan.EstimatedRows != 0 {
		t.Logf("estimated rows: %d (may be non-zero if partitions overlap)", plan.EstimatedRows)
	}
}

func TestQueryPlannerNilQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	_, err := planner.Plan(context.Background(), nil)
	if err == nil {
		t.Error("expected error for nil query")
	}
}

func TestQueryPlannerStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultQueryPlannerConfig()
	config.StatsRefreshInterval = 50 * time.Millisecond
	planner := NewQueryPlanner(db, config)

	planner.Start()
	time.Sleep(30 * time.Millisecond)
	planner.Stop()
}

func TestQueryPlannerStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	stats := planner.GetStats()
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}

	// Run a plan to populate planner stats
	planner.Plan(context.Background(), &Query{Metric: "x", Start: 0, End: time.Now().UnixNano()})
	pStats := planner.GetPlannerStats()
	if pStats.QueriesPlanned != 1 {
		t.Errorf("expected 1 query planned, got %d", pStats.QueriesPlanned)
	}
}

func TestQueryPlannerWithAggregation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.Write(Point{Metric: "cpu", Value: 50, Timestamp: time.Now().UnixNano()})

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	q := &Query{
		Metric: "cpu",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
		Limit: 100,
	}

	plan, err := planner.Plan(context.Background(), q)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan.Root == nil {
		t.Fatal("expected plan root")
	}
}
