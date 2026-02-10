package chronicle

import (
	"testing"
	"time"
)

func TestIncrementalAggregator_Sum(t *testing.T) {
	agg := NewIncrementalAggregator(AggSum, time.Minute)

	pts := []*Point{
		{Metric: "cpu", Value: 10, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu", Value: 20, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu", Value: 30, Timestamp: time.Now().UnixNano()},
	}

	var lastUpdate *AggregateUpdate
	for _, p := range pts {
		update, err := agg.Apply(p)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		lastUpdate = update
	}

	if lastUpdate.NewValue != 60 {
		t.Errorf("expected sum=60, got %f", lastUpdate.NewValue)
	}
	if lastUpdate.PointCount != 3 {
		t.Errorf("expected count=3, got %d", lastUpdate.PointCount)
	}
}

func TestIncrementalAggregator_Mean(t *testing.T) {
	agg := NewIncrementalAggregator(AggMean, time.Minute)

	pts := []*Point{
		{Metric: "cpu", Value: 10, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu", Value: 20, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu", Value: 30, Timestamp: time.Now().UnixNano()},
	}

	var lastUpdate *AggregateUpdate
	for _, p := range pts {
		update, err := agg.Apply(p)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		lastUpdate = update
	}

	if lastUpdate.NewValue != 20 {
		t.Errorf("expected mean=20, got %f", lastUpdate.NewValue)
	}
}

func TestIncrementalAggregator_MinMax(t *testing.T) {
	t.Run("Min", func(t *testing.T) {
		agg := NewIncrementalAggregator(AggMin, time.Minute)
		pts := []*Point{
			{Metric: "temp", Value: 25, Timestamp: time.Now().UnixNano()},
			{Metric: "temp", Value: 10, Timestamp: time.Now().UnixNano()},
			{Metric: "temp", Value: 30, Timestamp: time.Now().UnixNano()},
		}
		var last *AggregateUpdate
		for _, p := range pts {
			u, err := agg.Apply(p)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			last = u
		}
		if last.NewValue != 10 {
			t.Errorf("expected min=10, got %f", last.NewValue)
		}
	})

	t.Run("Max", func(t *testing.T) {
		agg := NewIncrementalAggregator(AggMax, time.Minute)
		pts := []*Point{
			{Metric: "temp", Value: 25, Timestamp: time.Now().UnixNano()},
			{Metric: "temp", Value: 10, Timestamp: time.Now().UnixNano()},
			{Metric: "temp", Value: 30, Timestamp: time.Now().UnixNano()},
		}
		var last *AggregateUpdate
		for _, p := range pts {
			u, err := agg.Apply(p)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			last = u
		}
		if last.NewValue != 30 {
			t.Errorf("expected max=30, got %f", last.NewValue)
		}
	})
}

func TestDependencyTracker_AddRemove(t *testing.T) {
	dt := NewDependencyTracker()

	dt.AddDependency("view1", "cpu")
	dt.AddDependency("view1", "memory")
	dt.AddDependency("view2", "cpu")

	deps := dt.GetDependencies("view1")
	if len(deps) != 2 {
		t.Errorf("expected 2 dependencies for view1, got %d", len(deps))
	}

	affected := dt.GetAffectedViews("cpu")
	if len(affected) != 2 {
		t.Errorf("expected 2 affected views for cpu, got %d", len(affected))
	}

	dt.RemoveDependency("view1")
	deps = dt.GetDependencies("view1")
	if len(deps) != 0 {
		t.Errorf("expected 0 dependencies after remove, got %d", len(deps))
	}

	affected = dt.GetAffectedViews("cpu")
	if len(affected) != 1 {
		t.Errorf("expected 1 affected view for cpu after remove, got %d", len(affected))
	}
}

func TestDependencyTracker_CycleDetection(t *testing.T) {
	dt := NewDependencyTracker()

	dt.AddDependency("A", "B")
	dt.AddDependency("B", "C")

	cycles := dt.DetectCycles()
	if len(cycles) != 0 {
		t.Errorf("expected no cycles, got %d", len(cycles))
	}

	// Create a cycle: C depends on A.
	dt.AddDependency("C", "A")
	cycles = dt.DetectCycles()
	if len(cycles) == 0 {
		t.Errorf("expected at least one cycle")
	}
}

func TestMaterializedViewEngine_CreateDrop(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	cfg := DefaultMaterializedViewConfig()
	engine := NewMaterializedViewEngine(db, cfg)

	def := &MaterializedViewDefinition{
		Name:         "avg_cpu",
		SourceMetric: "cpu",
		Aggregation:  AggMean,
		Window:       time.Minute,
		Enabled:      true,
	}

	if err := engine.CreateView(def); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	views := engine.ListViews()
	if len(views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(views))
	}
	if views[0].Name != "avg_cpu" {
		t.Errorf("expected view name 'avg_cpu', got %q", views[0].Name)
	}

	// Duplicate creation should fail.
	if err := engine.CreateView(def); err == nil {
		t.Errorf("expected error for duplicate view")
	}

	if err := engine.DropView("avg_cpu"); err != nil {
		t.Fatalf("DropView: %v", err)
	}
	if len(engine.ListViews()) != 0 {
		t.Errorf("expected 0 views after drop")
	}

	// Drop nonexistent should fail.
	if err := engine.DropView("nonexistent"); err == nil {
		t.Errorf("expected error for dropping nonexistent view")
	}
}

func TestMaterializedViewEngine_OnWrite(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	cfg := DefaultMaterializedViewConfig()
	engine := NewMaterializedViewEngine(db, cfg)

	def := &MaterializedViewDefinition{
		Name:         "sum_cpu",
		SourceMetric: "cpu",
		Aggregation:  AggSum,
		Window:       time.Minute,
		RefreshMode:  RefreshEager,
		Enabled:      true,
	}
	if err := engine.CreateView(def); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	now := time.Now().UnixNano()
	engine.OnWrite(&Point{Metric: "cpu", Value: 10, Timestamp: now})
	engine.OnWrite(&Point{Metric: "cpu", Value: 20, Timestamp: now})

	info, err := engine.GetView("sum_cpu")
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if info.PointsProcessed != 2 {
		t.Errorf("expected 2 points processed, got %d", info.PointsProcessed)
	}
}

func TestShadowVerifier_Match(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	cfg := DefaultMaterializedViewConfig()
	engine := NewMaterializedViewEngine(db, cfg)

	def := &MaterializedViewDefinition{
		Name:         "test_view",
		SourceMetric: "cpu",
		Aggregation:  AggSum,
		Window:       time.Minute,
		RefreshMode:  RefreshEager,
		Enabled:      true,
	}
	if err := engine.CreateView(def); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	verifier := NewShadowVerifier(engine)

	// With no data, both incremental and full should match (both empty).
	result, err := verifier.Verify("test_view")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !result.Match {
		t.Errorf("expected match for empty view, discrepancies: %v", result.Discrepancies)
	}
}
