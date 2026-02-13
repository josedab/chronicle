package chronicle

import (
	"testing"
	"time"
)

func TestMaterializedViewV2Create(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewMaterializedViewV2Engine(db, DefaultMaterializedViewV2Config())

	err := engine.CreateView(ViewV2Definition{
		Name:         "avg_cpu",
		SourceMetric: "cpu",
		Aggregation:  AggMean,
		WindowType:   WindowTumbling,
		WindowSize:   time.Minute,
		Enabled:      true,
	})
	if err != nil {
		t.Fatalf("create view failed: %v", err)
	}

	views := engine.ListViews()
	if len(views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(views))
	}
	if views[0].Definition.Name != "avg_cpu" {
		t.Errorf("expected avg_cpu, got %s", views[0].Definition.Name)
	}
}

func TestMaterializedViewV2Duplicate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewMaterializedViewV2Engine(db, DefaultMaterializedViewV2Config())

	def := ViewV2Definition{Name: "v1", SourceMetric: "cpu", Aggregation: AggSum, WindowType: WindowTumbling, WindowSize: time.Minute, Enabled: true}
	engine.CreateView(def)
	err := engine.CreateView(def)
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestMaterializedViewV2Drop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewMaterializedViewV2Engine(db, DefaultMaterializedViewV2Config())

	engine.CreateView(ViewV2Definition{Name: "temp", SourceMetric: "cpu", Aggregation: AggSum, WindowType: WindowTumbling, WindowSize: time.Minute, Enabled: true})
	err := engine.DropView("temp")
	if err != nil {
		t.Fatalf("drop failed: %v", err)
	}

	if len(engine.ListViews()) != 0 {
		t.Error("expected 0 views after drop")
	}
}

func TestMaterializedViewV2TumblingWindow(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewMaterializedViewV2Engine(db, DefaultMaterializedViewV2Config())

	engine.CreateView(ViewV2Definition{
		Name:         "sum_cpu",
		SourceMetric: "cpu",
		Aggregation:  AggSum,
		WindowType:   WindowTumbling,
		WindowSize:   time.Minute,
		Enabled:      true,
	})

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

	updates, err := engine.Apply(&Point{Metric: "cpu", Value: 10, Timestamp: baseTime})
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	if updates[0].NewValue != 10 {
		t.Errorf("expected sum 10, got %f", updates[0].NewValue)
	}

	// Same window
	updates, _ = engine.Apply(&Point{Metric: "cpu", Value: 20, Timestamp: baseTime + int64(30*time.Second)})
	if updates[0].NewValue != 30 {
		t.Errorf("expected sum 30, got %f", updates[0].NewValue)
	}
}

func TestMaterializedViewV2SessionWindow(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewMaterializedViewV2Engine(db, DefaultMaterializedViewV2Config())

	engine.CreateView(ViewV2Definition{
		Name:         "session_count",
		SourceMetric: "events",
		Aggregation:  AggCount,
		WindowType:   WindowSession,
		SessionGap:   10 * time.Second,
		Enabled:      true,
	})

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

	// Events within session gap
	engine.Apply(&Point{Metric: "events", Value: 1, Timestamp: base})
	engine.Apply(&Point{Metric: "events", Value: 1, Timestamp: base + int64(5*time.Second)})
	updates, _ := engine.Apply(&Point{Metric: "events", Value: 1, Timestamp: base + int64(8*time.Second)})

	if len(updates) > 0 && updates[0].NewValue != 3 {
		t.Errorf("expected session count 3, got %f", updates[0].NewValue)
	}
}

func TestMaterializedViewV2LateData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultMaterializedViewV2Config()
	config.EnableExactlyOnce = false
	engine := NewMaterializedViewV2Engine(db, config)

	engine.CreateView(ViewV2Definition{
		Name:            "sum_with_late",
		SourceMetric:    "cpu",
		Aggregation:     AggSum,
		WindowType:      WindowTumbling,
		WindowSize:      time.Minute,
		LatePolicy:      LateDataUpdate,
		AllowedLateness: time.Hour,
		Enabled:         true,
	})

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

	// Advance watermark
	engine.Apply(&Point{Metric: "cpu", Value: 10, Timestamp: base + int64(time.Minute)})

	// Late data (before watermark)
	updates, _ := engine.Apply(&Point{Metric: "cpu", Value: 5, Timestamp: base})
	if len(updates) == 0 {
		t.Fatal("expected late data to be accepted with Update policy")
	}
}

func TestMaterializedViewV2ExactlyOnce(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultMaterializedViewV2Config()
	config.EnableExactlyOnce = true
	engine := NewMaterializedViewV2Engine(db, config)

	engine.CreateView(ViewV2Definition{
		Name:         "exact_sum",
		SourceMetric: "cpu",
		Aggregation:  AggSum,
		WindowType:   WindowTumbling,
		WindowSize:   time.Minute,
		Enabled:      true,
	})

	p := &Point{Metric: "cpu", Value: 42, Timestamp: time.Now().UnixNano()}

	// Apply same point twice
	updates1, _ := engine.Apply(p)
	updates2, _ := engine.Apply(p)

	if len(updates1) != 1 {
		t.Fatal("expected first apply to produce update")
	}
	if len(updates2) != 0 {
		t.Fatal("expected second apply to be deduplicated")
	}
}

func TestMaterializedViewV2GetView(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewMaterializedViewV2Engine(db, DefaultMaterializedViewV2Config())
	engine.CreateView(ViewV2Definition{
		Name: "test_view", SourceMetric: "cpu", Aggregation: AggMean, WindowType: WindowTumbling, WindowSize: time.Minute, Enabled: true,
	})

	state, ok := engine.GetView("test_view")
	if !ok {
		t.Fatal("expected view to exist")
	}
	if state.Definition.Name != "test_view" {
		t.Errorf("expected test_view, got %s", state.Definition.Name)
	}

	_, ok = engine.GetView("nonexistent")
	if ok {
		t.Fatal("expected nonexistent view to return false")
	}
}

func TestWindowTypeString(t *testing.T) {
	if WindowTumbling.String() != "tumbling" {
		t.Errorf("expected tumbling, got %s", WindowTumbling.String())
	}
	if WindowSession.String() != "session" {
		t.Errorf("expected session, got %s", WindowSession.String())
	}
}
