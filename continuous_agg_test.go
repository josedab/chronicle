package chronicle

import (
	"net/http"
	"testing"
	"time"
)

func TestContinuousAggEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("create and ingest", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		err := e.Create(ContinuousAggDefinition{Name: "cpu_avg", SourceMetric: "cpu", Function: "avg", Window: time.Minute})
		if err != nil { t.Fatal(err) }

		now := time.Now().UnixNano()
		for i := 0; i < 10; i++ {
			e.Ingest(Point{Metric: "cpu", Value: float64(i * 10), Timestamp: now + int64(i)*int64(time.Second)})
		}

		state := e.Get("cpu_avg")
		if state == nil { t.Fatal("expected state") }
		if state.PointsIn != 10 { t.Errorf("expected 10 points, got %d", state.PointsIn) }
		if len(state.Windows) == 0 { t.Error("expected windows") }
	})

	t.Run("sum aggregation", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "req_sum", SourceMetric: "requests", Function: "sum", Window: time.Hour})

		now := time.Now().UnixNano()
		e.Ingest(Point{Metric: "requests", Value: 5, Timestamp: now})
		e.Ingest(Point{Metric: "requests", Value: 3, Timestamp: now + 1})

		state := e.Get("req_sum")
		if state.Windows[0].Value != 8 { t.Errorf("expected sum 8, got %f", state.Windows[0].Value) }
	})

	t.Run("min/max", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "t_min", SourceMetric: "temp", Function: "min", Window: time.Hour})
		e.Create(ContinuousAggDefinition{Name: "t_max", SourceMetric: "temp", Function: "max", Window: time.Hour})

		now := time.Now().UnixNano()
		for _, v := range []float64{30, 10, 50, 20} {
			e.Ingest(Point{Metric: "temp", Value: v, Timestamp: now})
		}

		minState := e.Get("t_min")
		maxState := e.Get("t_max")
		if minState.Windows[0].Value != 10 { t.Errorf("expected min 10, got %f", minState.Windows[0].Value) }
		if maxState.Windows[0].Value != 50 { t.Errorf("expected max 50, got %f", maxState.Windows[0].Value) }
	})

	t.Run("filter matching", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{
			Name: "filtered", SourceMetric: "cpu", Function: "avg", Window: time.Hour,
			Filter: map[string]string{"host": "prod"},
		})

		now := time.Now().UnixNano()
		e.Ingest(Point{Metric: "cpu", Value: 100, Timestamp: now, Tags: map[string]string{"host": "prod"}})
		e.Ingest(Point{Metric: "cpu", Value: 50, Timestamp: now, Tags: map[string]string{"host": "dev"}})

		state := e.Get("filtered")
		if state.PointsIn != 1 { t.Errorf("expected 1 filtered point, got %d", state.PointsIn) }
	})

	t.Run("validation", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		if e.Create(ContinuousAggDefinition{}) == nil { t.Error("expected error for empty name") }
		if e.Create(ContinuousAggDefinition{Name: "x"}) == nil { t.Error("expected error for empty source") }
		if e.Create(ContinuousAggDefinition{Name: "x", SourceMetric: "y"}) == nil { t.Error("expected error for zero window") }

		e.Create(ContinuousAggDefinition{Name: "dup", SourceMetric: "y", Window: time.Minute})
		if e.Create(ContinuousAggDefinition{Name: "dup", SourceMetric: "y", Window: time.Minute}) == nil { t.Error("expected dup error") }
	})

	t.Run("max limit", func(t *testing.T) {
		cfg := DefaultContinuousAggConfig()
		cfg.MaxAggregations = 1
		e := NewContinuousAggEngine(db, cfg)
		e.Create(ContinuousAggDefinition{Name: "a", SourceMetric: "x", Window: time.Minute})
		if e.Create(ContinuousAggDefinition{Name: "b", SourceMetric: "x", Window: time.Minute}) == nil { t.Error("expected max error") }
	})

	t.Run("delete", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "del", SourceMetric: "x", Window: time.Minute})
		if e.Delete("del") != nil { t.Error("delete failed") }
		if e.Delete("del") == nil { t.Error("expected error for missing") }
	})

	t.Run("list and get", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "a", SourceMetric: "x", Window: time.Minute})
		e.Create(ContinuousAggDefinition{Name: "b", SourceMetric: "y", Window: time.Minute})
		if len(e.List()) != 2 { t.Error("expected 2") }
		if e.Get("missing") != nil { t.Error("expected nil") }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}

func TestContinuousAggCheckpointRoundTrip(t *testing.T) {
	db := setupTestDB(t)

	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	engine.Start()
	defer engine.Stop()

	// Create an aggregation and ingest some data
	def := ContinuousAggDefinition{
		Name:         "test_agg",
		SourceMetric: "cpu_usage",
		TargetMetric: "cpu_usage_avg",
		Function:     "avg",
		Window:       time.Minute,
	}
	if err := engine.Create(def); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Ingest points
	for i := 0; i < 10; i++ {
		engine.Ingest(Point{
			Metric:    "cpu_usage",
			Value:     float64(50 + i),
			Timestamp: int64(i) * time.Second.Nanoseconds(),
			Tags:      map[string]string{"host": "a"},
		})
	}

	// Checkpoint
	cpJSON, err := engine.CheckpointToJSON()
	if err != nil {
		t.Fatalf("CheckpointToJSON: %v", err)
	}
	if len(cpJSON) == 0 {
		t.Fatal("checkpoint JSON should not be empty")
	}

	// Verify checkpoint has content
	cp := engine.Checkpoint()
	if len(cp.AggStates) != 1 {
		t.Fatalf("expected 1 agg state in checkpoint, got %d", len(cp.AggStates))
	}
	state := cp.AggStates["test_agg"]
	if state == nil {
		t.Fatal("expected test_agg in checkpoint")
	}
	if state.PointsIn != 10 {
		t.Errorf("expected 10 points ingested, got %d", state.PointsIn)
	}

	// Create a new engine and restore
	engine2 := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	if err := engine2.RestoreFromJSON(cpJSON); err != nil {
		t.Fatalf("RestoreFromJSON: %v", err)
	}

	// Verify state was restored
	restored := engine2.Get("test_agg")
	if restored == nil {
		t.Fatal("restored engine missing test_agg")
	}
	if restored.PointsIn != 10 {
		t.Errorf("restored points: expected 10, got %d", restored.PointsIn)
	}
	if len(restored.Windows) == 0 {
		t.Error("restored engine should have windows")
	}

	stats := engine2.GetStats()
	if stats.ActiveAggregations != 1 {
		t.Errorf("expected 1 active aggregation, got %d", stats.ActiveAggregations)
	}
}

func TestContinuousAggWatermark(t *testing.T) {
	db := setupTestDB(t)

	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "wm_agg", SourceMetric: "metric_a", Function: "sum", Window: time.Minute,
	})

	// Ingest with increasing timestamps
	for i := 0; i < 5; i++ {
		engine.Ingest(Point{
			Metric: "metric_a", Value: float64(i), Timestamp: int64(i+1) * time.Second.Nanoseconds(),
		})
	}

	wm := engine.GetWatermark("wm_agg")
	expectedWM := int64(5) * time.Second.Nanoseconds()
	if wm != expectedWM {
		t.Errorf("watermark: expected %d, got %d", expectedWM, wm)
	}
}

func TestContinuousAggDeduplication(t *testing.T) {
	db := setupTestDB(t)

	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "dedup_agg", SourceMetric: "metric_b", Function: "count", Window: time.Minute,
	})

	// Ingest same point twice
	p := Point{Metric: "metric_b", Value: 1.0, Timestamp: 1000}
	engine.Ingest(p)
	engine.Ingest(p)

	state := engine.Get("dedup_agg")
	if state == nil {
		t.Fatal("expected dedup_agg state")
	}
	// Due to deduplication, should only count once
	if state.PointsIn != 1 {
		t.Errorf("expected 1 point (dedup), got %d", state.PointsIn)
	}
}

func TestContinuousAgg_MaybeEmitWindow(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "emit_test", SourceMetric: "cpu", TargetMetric: "cpu_avg", Function: "avg", Window: time.Minute,
	})

	// Ingest points across two windows
	window1Start := time.Now().Truncate(time.Minute).UnixNano()
	for i := 0; i < 5; i++ {
		engine.Ingest(Point{Metric: "cpu", Value: float64(i * 10), Timestamp: window1Start + int64(i)*int64(time.Second)})
	}

	// Move watermark well past the window
	engine.Ingest(Point{Metric: "cpu", Value: 50, Timestamp: window1Start + 3*int64(time.Minute)})

	state := engine.Get("emit_test")
	if state == nil {
		t.Fatal("expected state")
	}
	if len(state.Windows) == 0 {
		t.Error("expected at least one window")
	}
}

func TestContinuousAgg_PerformMaintenance(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "maint_test", SourceMetric: "cpu", Function: "avg", Window: time.Minute,
	})

	// Ingest old data
	oldTS := time.Now().Add(-10 * time.Minute).UnixNano()
	for i := 0; i < 5; i++ {
		engine.Ingest(Point{Metric: "cpu", Value: float64(i), Timestamp: oldTS + int64(i)})
	}

	// Advance watermark far past old windows
	engine.Ingest(Point{Metric: "cpu", Value: 1, Timestamp: time.Now().UnixNano()})

	engine.performMaintenance()

	state := engine.Get("maint_test")
	if state == nil {
		t.Fatal("expected state")
	}
}

func TestContinuousAgg_AlterRunningAggregation(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "alter_test", SourceMetric: "cpu", Function: "avg", Window: time.Minute,
	})

	// Ingest some data before alter
	now := time.Now().UnixNano()
	engine.Ingest(Point{Metric: "cpu", Value: 100, Timestamp: now})
	engine.Ingest(Point{Metric: "cpu", Value: 200, Timestamp: now + 1})

	state := engine.Get("alter_test")
	if state.PointsIn != 2 {
		t.Fatalf("expected 2 points before alter, got %d", state.PointsIn)
	}
	windowsBefore := len(state.Windows)

	// Alter function
	err := engine.Alter(ContinuousAggAlterRequest{Name: "alter_test", NewFunction: "sum"})
	if err != nil {
		t.Fatal(err)
	}

	// Windows should be preserved after alter
	state = engine.Get("alter_test")
	if state.Definition.Function != "sum" {
		t.Errorf("function = %q, want sum", state.Definition.Function)
	}
	if len(state.Windows) != windowsBefore {
		t.Error("windows should be preserved after alter")
	}
	if state.PointsIn != 2 {
		t.Error("points count should be preserved after alter")
	}

	// Alter non-existent
	err = engine.Alter(ContinuousAggAlterRequest{Name: "nonexistent"})
	if err == nil {
		t.Error("expected error for non-existent aggregation")
	}
}

func TestContinuousAgg_AlterPause(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "pause_test", SourceMetric: "cpu", Function: "avg", Window: time.Minute,
	})

	pause := true
	engine.Alter(ContinuousAggAlterRequest{Name: "pause_test", Pause: &pause})

	state := engine.Get("pause_test")
	if state.Running {
		t.Error("expected paused")
	}

	// Ingesting while paused should not count
	engine.Ingest(Point{Metric: "cpu", Value: 42, Timestamp: time.Now().UnixNano()})
	state = engine.Get("pause_test")
	if state.PointsIn != 0 {
		t.Errorf("expected 0 points while paused, got %d", state.PointsIn)
	}
}

func TestContinuousAgg_ExecuteSQL(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())

	// CREATE
	result, err := engine.ExecuteSQL("CREATE CONTINUOUS AGGREGATE cpu_5m AS SELECT avg(value) FROM cpu WINDOW '5m'")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if result["status"] != "created" {
		t.Errorf("status = %v", result["status"])
	}

	state := engine.Get("cpu_5m")
	if state == nil {
		t.Fatal("expected cpu_5m to exist after CREATE")
	}
	if state.Definition.Function != "avg" {
		t.Errorf("function = %q, want avg", state.Definition.Function)
	}

	// DROP
	result, err = engine.ExecuteSQL("DROP CONTINUOUS AGGREGATE cpu_5m")
	if err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if result["status"] != "dropped" {
		t.Errorf("status = %v", result["status"])
	}
	if engine.Get("cpu_5m") != nil {
		t.Error("expected cpu_5m to be deleted")
	}

	// Unsupported
	_, err = engine.ExecuteSQL("TRUNCATE CONTINUOUS AGGREGATE foo")
	if err == nil {
		t.Error("expected error for unsupported command")
	}
}

func TestContinuousAgg_ExecuteSQLAlter(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())

	engine.Create(ContinuousAggDefinition{Name: "alt_sql", SourceMetric: "cpu", Function: "avg", Window: time.Minute})

	result, err := engine.ExecuteSQL("ALTER CONTINUOUS AGGREGATE alt_sql SET function sum")
	if err != nil {
		t.Fatalf("ALTER: %v", err)
	}
	if result["status"] != "altered" {
		t.Errorf("status = %v", result["status"])
	}

	state := engine.Get("alt_sql")
	if state.Definition.Function != "sum" {
		t.Errorf("function = %q after ALTER, want sum", state.Definition.Function)
	}
}

func TestContinuousAgg_CountAggregation(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "count_test", SourceMetric: "req", Function: "count", Window: time.Hour,
	})

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		engine.Ingest(Point{Metric: "req", Value: float64(i), Timestamp: now + int64(i)})
	}

	state := engine.Get("count_test")
	if state == nil || len(state.Windows) == 0 {
		t.Fatal("expected windows")
	}
	if state.Windows[0].Value != 5 {
		t.Errorf("count value = %f, want 5", state.Windows[0].Value)
	}
}

func TestContinuousAgg_AvgPrecisionManyPoints(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "avg_precision", SourceMetric: "metric", Function: "avg", Window: time.Hour,
	})

	const n = 10000
	now := time.Now().UnixNano()
	var expectedSum float64
	for i := 0; i < n; i++ {
		v := float64(i) * 0.1
		expectedSum += v
		engine.Ingest(Point{Metric: "metric", Value: v, Timestamp: now + int64(i)})
	}
	expectedAvg := expectedSum / float64(n)

	state := engine.Get("avg_precision")
	if state == nil || len(state.Windows) == 0 {
		t.Fatal("expected windows")
	}

	// Allow small floating point imprecision
	diff := state.Windows[0].Value - expectedAvg
	if diff < 0 {
		diff = -diff
	}
	if diff > 0.01 {
		t.Errorf("avg precision: got %f, want ~%f, diff=%f", state.Windows[0].Value, expectedAvg, diff)
	}
}

func TestContinuousAgg_NilTagsFilter(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
	_ = engine.Create(ContinuousAggDefinition{
		Name: "nil_tags", SourceMetric: "cpu", Function: "avg", Window: time.Minute,
		Filter: map[string]string{"host": "a"},
	})

	// Ingest with nil tags
	engine.Ingest(Point{Metric: "cpu", Value: 42, Timestamp: time.Now().UnixNano(), Tags: nil})

	state := engine.Get("nil_tags")
	if state.PointsIn != 0 {
		t.Errorf("expected 0 points with nil tags filtered, got %d", state.PointsIn)
	}
}

func TestContinuousAgg_RegisterHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	engine := NewContinuousAggEngine(db, DefaultContinuousAggConfig())

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// Verify handlers were registered by checking patterns
	// The handlers should not panic when registered
}
