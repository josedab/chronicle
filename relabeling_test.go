package chronicle

import (
	"testing"
	"time"
)

func TestRelabelEngine_Drop(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{SourceLabel: "secret", Action: RelabelDrop})

	p := Point{Metric: "test", Value: 1.0, Tags: map[string]string{"host": "a", "secret": "x"}}
	result, dropped := engine.Apply(p)

	if dropped {
		t.Error("point should not be dropped")
	}
	if _, exists := result.Tags["secret"]; exists {
		t.Error("secret label should be removed")
	}
	if result.Tags["host"] != "a" {
		t.Error("host label should be preserved")
	}
}

func TestRelabelEngine_Rename(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{SourceLabel: "hostname", TargetLabel: "host", Action: RelabelRename})

	p := Point{Metric: "test", Tags: map[string]string{"hostname": "server1"}}
	result, _ := engine.Apply(p)

	if result.Tags["host"] != "server1" {
		t.Errorf("expected renamed label host=server1, got %s", result.Tags["host"])
	}
	if _, exists := result.Tags["hostname"]; exists {
		t.Error("old label should be removed")
	}
}

func TestRelabelEngine_Hash(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{SourceLabel: "user_id", Action: RelabelHash})

	p := Point{Metric: "test", Tags: map[string]string{"user_id": "user123"}}
	result, _ := engine.Apply(p)

	if result.Tags["user_id"] == "user123" {
		t.Error("label value should be hashed")
	}
	if len(result.Tags["user_id"]) == 0 {
		t.Error("hashed value should not be empty")
	}
}

func TestRelabelEngine_Replace(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{
		SourceLabel: "path",
		Action:      RelabelReplace,
		Regex:       `/api/v[0-9]+/users/[0-9]+`,
		Replacement: "/api/v1/users/:id",
	})

	p := Point{Metric: "test", Tags: map[string]string{"path": "/api/v2/users/12345"}}
	result, _ := engine.Apply(p)

	if result.Tags["path"] != "/api/v1/users/:id" {
		t.Errorf("expected replaced path, got %s", result.Tags["path"])
	}
}

func TestRelabelEngine_MetricFilter(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{
		SourceLabel:  "debug_info",
		Action:       RelabelDrop,
		MetricFilter: "http_*",
	})

	// Should apply to matching metric
	p1 := Point{Metric: "http_requests", Tags: map[string]string{"debug_info": "x"}}
	result1, _ := engine.Apply(p1)
	if _, exists := result1.Tags["debug_info"]; exists {
		t.Error("debug_info should be dropped for http_* metrics")
	}

	// Should not apply to non-matching metric
	p2 := Point{Metric: "cpu_usage", Tags: map[string]string{"debug_info": "x"}}
	result2, _ := engine.Apply(p2)
	if _, exists := result2.Tags["debug_info"]; !exists {
		t.Error("debug_info should be preserved for non-matching metrics")
	}
}

func TestCardinalityCircuitBreaker(t *testing.T) {
	db := setupTestDB(t)

	tracker := NewCardinalityTracker(db, DefaultCardinalityConfig())
	cb := NewCardinalityCircuitBreaker(tracker, 10) // Max 10 series/sec

	// Should not be tripped initially
	if cb.IsTripped() {
		t.Error("should not be tripped initially")
	}

	if err := cb.Check(); err != nil {
		t.Errorf("Check should pass: %v", err)
	}

	// Reset works
	cb.Reset()
	if cb.IsTripped() {
		t.Error("should not be tripped after reset")
	}
}

func TestStaleSeriesManager(t *testing.T) {
	config := StaleSeriesConfig{
		Enabled:         true,
		StaleTimeout:    50 * time.Millisecond,
		CleanupInterval: time.Second,
		EvictionPolicy:  "lru",
	}
	mgr := NewStaleSeriesManager(config)

	mgr.RecordAccess("series_1")
	mgr.RecordAccess("series_2")

	if mgr.ActiveCount() != 2 {
		t.Errorf("expected 2 active series, got %d", mgr.ActiveCount())
	}

	// Wait for staleness
	time.Sleep(100 * time.Millisecond)

	stale := mgr.GetStaleSeries()
	if len(stale) != 2 {
		t.Errorf("expected 2 stale series, got %d", len(stale))
	}

	removed := mgr.CleanupStale()
	if removed != 2 {
		t.Errorf("expected 2 removed, got %d", removed)
	}

	if mgr.ActiveCount() != 0 {
		t.Errorf("expected 0 active after cleanup, got %d", mgr.ActiveCount())
	}
}

func TestRelabelEngine_DropEntirePoint(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{SourceLabel: "__name__", Action: RelabelDrop})

	p := Point{Metric: "test", Tags: map[string]string{"host": "a"}}
	_, dropped := engine.Apply(p)

	if !dropped {
		t.Error("point should be dropped when __name__ is dropped")
	}
}

func TestRelabelEngine_WriteHook(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{SourceLabel: "debug", Action: RelabelDrop})

	hook := engine.RelabelWriteHook()

	p := Point{Metric: "test", Value: 1.0, Tags: map[string]string{"host": "a", "debug": "true"}}
	result, err := hook(p)
	if err != nil {
		t.Fatalf("hook failed: %v", err)
	}
	if _, exists := result.Tags["debug"]; exists {
		t.Error("debug tag should be removed by write hook")
	}
}

func TestRelabelEngine_WriteHook_Drop(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{SourceLabel: "__name__", Action: RelabelDrop})

	hook := engine.RelabelWriteHook()

	p := Point{Metric: "test", Value: 1.0}
	_, err := hook(p)
	if err == nil {
		t.Error("expected error when point is dropped")
	}
}

func TestTenantCardinalityLimits(t *testing.T) {
	limits := NewTenantCardinalityLimits()
	limits.SetLimit("tenant1", 3)

	// First 3 should succeed
	for i := 0; i < 3; i++ {
		if err := limits.CheckAndTrack("tenant1"); err != nil {
			t.Errorf("series %d should be allowed: %v", i, err)
		}
	}

	// 4th should fail
	if err := limits.CheckAndTrack("tenant1"); err == nil {
		t.Error("expected cardinality limit error")
	}

	// Different tenant without limit should succeed
	if err := limits.CheckAndTrack("tenant2"); err != nil {
		t.Errorf("tenant without limit should succeed: %v", err)
	}
}

func TestTenantCardinalityLimits_GetUsage(t *testing.T) {
	limits := NewTenantCardinalityLimits()
	limits.SetLimit("t1", 100)
	limits.CheckAndTrack("t1")
	limits.CheckAndTrack("t1")

	current, limit := limits.GetUsage("t1")
	if current != 2 {
		t.Errorf("expected current 2, got %d", current)
	}
	if limit != 100 {
		t.Errorf("expected limit 100, got %d", limit)
	}
}

func TestRelabelEngine_Keep(t *testing.T) {
	engine := NewRelabelEngine()
	engine.AddRule(RelabelRule{SourceLabel: "host,region", Action: RelabelKeep})

	p := Point{Metric: "test", Tags: map[string]string{"host": "a", "region": "us", "debug": "true", "env": "prod"}}
	result, _ := engine.Apply(p)

	if _, exists := result.Tags["host"]; !exists {
		t.Error("host should be kept")
	}
	if _, exists := result.Tags["region"]; !exists {
		t.Error("region should be kept")
	}
	if _, exists := result.Tags["debug"]; exists {
		t.Error("debug should be dropped")
	}
	if _, exists := result.Tags["env"]; exists {
		t.Error("env should be dropped")
	}
}
