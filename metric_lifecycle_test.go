package chronicle

import (
	"testing"
	"time"
)

func TestMetricLifecycleConfig(t *testing.T) {
	cfg := DefaultMetricLifecycleConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.EvaluationInterval != 5*time.Minute {
		t.Errorf("expected 5m interval, got %v", cfg.EvaluationInterval)
	}
	if cfg.MaxPolicies != 100 {
		t.Errorf("expected 100 max policies, got %d", cfg.MaxPolicies)
	}
}

func TestMetricLifecycleAddPolicy(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())

	err := mgr.AddPolicy(LifecyclePolicy{
		MetricPattern: "cpu.*",
		MaxAge:        30 * 24 * time.Hour,
		ArchiveAfter:  7 * 24 * time.Hour,
		AutoDelete:    false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := mgr.Stats()
	if stats.PolicyCount != 1 {
		t.Errorf("expected 1 policy, got %d", stats.PolicyCount)
	}
}

func TestMetricLifecycleTrackMetric(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())

	mgr.TrackMetric("cpu_usage")
	mgr.TrackMetric("cpu_usage")
	mgr.TrackMetric("mem_usage")

	t.Run("get state", func(t *testing.T) {
		state, exists := mgr.GetState("cpu_usage")
		if !exists {
			t.Fatal("expected metric to exist")
		}
		if state.PointCount != 2 {
			t.Errorf("expected 2 points, got %d", state.PointCount)
		}
		if state.State != "active" {
			t.Errorf("expected active state, got %s", state.State)
		}
		if state.Metric != "cpu_usage" {
			t.Errorf("expected cpu_usage, got %s", state.Metric)
		}
	})

	t.Run("list metrics", func(t *testing.T) {
		metrics := mgr.ListMetrics()
		if len(metrics) != 2 {
			t.Errorf("expected 2 metrics, got %d", len(metrics))
		}
	})
}

func TestMetricLifecycleArchiveRestore(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())
	mgr.TrackMetric("cpu_usage")

	t.Run("archive", func(t *testing.T) {
		err := mgr.Archive("cpu_usage")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		state, _ := mgr.GetState("cpu_usage")
		if state.State != "archived" {
			t.Errorf("expected archived, got %s", state.State)
		}
	})

	t.Run("restore", func(t *testing.T) {
		err := mgr.Restore("cpu_usage")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		state, _ := mgr.GetState("cpu_usage")
		if state.State != "active" {
			t.Errorf("expected active, got %s", state.State)
		}
	})

	t.Run("restore already active", func(t *testing.T) {
		err := mgr.Restore("cpu_usage")
		if err == nil {
			t.Error("expected error restoring already active metric")
		}
	})
}

func TestMetricLifecycleTombstone(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())
	mgr.TrackMetric("old_metric")

	err := mgr.Tombstone("old_metric")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	state, _ := mgr.GetState("old_metric")
	if state.State != "deleted" {
		t.Errorf("expected deleted, got %s", state.State)
	}

	// Cannot archive a deleted metric
	err = mgr.Archive("old_metric")
	if err == nil {
		t.Error("expected error archiving deleted metric")
	}
}

func TestMetricLifecycleStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())
	mgr.TrackMetric("active1")
	mgr.TrackMetric("active2")
	mgr.TrackMetric("to_archive")
	mgr.TrackMetric("to_tombstone")

	mgr.Archive("to_archive")
	mgr.Tombstone("to_tombstone")

	stats := mgr.Stats()
	if stats.TotalMetrics != 4 {
		t.Errorf("expected 4 total, got %d", stats.TotalMetrics)
	}
	if stats.ActiveMetrics != 2 {
		t.Errorf("expected 2 active, got %d", stats.ActiveMetrics)
	}
	if stats.ArchivedMetrics != 1 {
		t.Errorf("expected 1 archived, got %d", stats.ArchivedMetrics)
	}
	if stats.DeletedMetrics != 1 {
		t.Errorf("expected 1 deleted, got %d", stats.DeletedMetrics)
	}
}

func TestMetricLifecycleNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())

	_, exists := mgr.GetState("nonexistent")
	if exists {
		t.Error("expected metric to not exist")
	}

	if err := mgr.Archive("nonexistent"); err == nil {
		t.Error("expected error for nonexistent metric")
	}
	if err := mgr.Tombstone("nonexistent"); err == nil {
		t.Error("expected error for nonexistent metric")
	}
	if err := mgr.Restore("nonexistent"); err == nil {
		t.Error("expected error for nonexistent metric")
	}
}

func TestMetricLifecyclePolicyMatching(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())
	mgr.AddPolicy(LifecyclePolicy{
		MetricPattern: "*",
		MaxAge:        90 * 24 * time.Hour,
		ArchiveAfter:  30 * 24 * time.Hour,
		AutoDelete:    false,
	})
	mgr.AddPolicy(LifecyclePolicy{
		MetricPattern: "cpu_usage",
		MaxAge:        7 * 24 * time.Hour,
		ArchiveAfter:  24 * time.Hour,
		AutoDelete:    true,
	})

	mgr.TrackMetric("cpu_usage")
	state, _ := mgr.GetState("cpu_usage")

	if len(state.Policies) != 2 {
		t.Errorf("expected 2 matching policies, got %d", len(state.Policies))
	}
}

func TestMetricLifecycle_DeprecateMetric(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())
	mgr.TrackMetric("old_cpu")

	err := mgr.DeprecateMetric("old_cpu", "Use cpu_v2 instead", "cpu_v2")
	if err != nil {
		t.Fatalf("DeprecateMetric failed: %v", err)
	}

	deprecated, msg, replacement := mgr.CheckDeprecated("old_cpu")
	if !deprecated {
		t.Error("expected old_cpu to be deprecated")
	}
	if msg != "Use cpu_v2 instead" {
		t.Errorf("expected deprecation message, got %q", msg)
	}
	if replacement != "cpu_v2" {
		t.Errorf("expected replacement cpu_v2, got %q", replacement)
	}

	// Non-deprecated metric
	mgr.TrackMetric("new_cpu")
	deprecated, _, _ = mgr.CheckDeprecated("new_cpu")
	if deprecated {
		t.Error("new_cpu should not be deprecated")
	}
}

func TestMetricLifecycle_DeprecateNonexistent(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())

	err := mgr.DeprecateMetric("nonexistent", "test", "")
	if err == nil {
		t.Error("expected error for nonexistent metric")
	}
}

func TestMetricLifecycle_QueryTracking(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())
	mgr.TrackMetric("queried_metric")

	mgr.TrackQuery("queried_metric")
	mgr.TrackQuery("queried_metric")
	mgr.TrackQuery("queried_metric")

	state, _ := mgr.GetState("queried_metric")
	if state.QueryCount != 3 {
		t.Errorf("expected 3 queries, got %d", state.QueryCount)
	}
	if state.LastQueried.IsZero() {
		t.Error("expected non-zero LastQueried time")
	}
}

func TestMetricLifecycle_StaleTransition(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultMetricLifecycleConfig()
	cfg.StaleTimeout = 10 * time.Millisecond
	cfg.ArchiveTimeout = 50 * time.Millisecond
	cfg.EvaluationInterval = 5 * time.Millisecond
	mgr := NewMetricLifecycleManager(db, cfg)

	mgr.TrackMetric("ephemeral")

	// Wait for staleness
	time.Sleep(30 * time.Millisecond)
	mgr.evaluateTransitions()

	state, _ := mgr.GetState("ephemeral")
	if state.State != "stale" {
		t.Errorf("expected stale state, got %s", state.State)
	}
}

func TestMetricLifecycle_ReactivateFromStale(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultMetricLifecycleConfig()
	cfg.StaleTimeout = 10 * time.Millisecond
	mgr := NewMetricLifecycleManager(db, cfg)

	mgr.TrackMetric("revived")

	// Wait for staleness
	time.Sleep(20 * time.Millisecond)
	mgr.evaluateTransitions()

	state, _ := mgr.GetState("revived")
	if state.State != "stale" {
		t.Errorf("expected stale, got %s", state.State)
	}

	// New data should reactivate
	mgr.TrackMetric("revived")
	state, _ = mgr.GetState("revived")
	if state.State != "active" {
		t.Errorf("expected reactivated to active, got %s", state.State)
	}
}

func TestMetricLifecycle_DeprecatedStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mgr := NewMetricLifecycleManager(db, DefaultMetricLifecycleConfig())
	mgr.TrackMetric("m1")
	mgr.TrackMetric("m2")
	mgr.DeprecateMetric("m1", "old", "m2")

	stats := mgr.Stats()
	if stats.DeprecatedMetrics != 1 {
		t.Errorf("expected 1 deprecated, got %d", stats.DeprecatedMetrics)
	}
}
