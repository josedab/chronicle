package chronicle

import (
	"testing"
)

func TestRetentionOptimizerConfig(t *testing.T) {
	cfg := DefaultRetentionOptimizerConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.StorageBudgetMB != 10240 {
		t.Errorf("expected 10240 MB budget, got %d", cfg.StorageBudgetMB)
	}
}

func TestRetentionOptimizerRecordAccess(t *testing.T) {
	db := setupTestDB(t)

	e := NewRetentionOptimizerEngine(db, DefaultRetentionOptimizerConfig())

	t.Run("record single access", func(t *testing.T) {
		e.RecordAccess("cpu_usage")
		imp, ok := e.GetImportance("cpu_usage")
		if !ok {
			t.Fatal("expected metric to exist")
		}
		if imp.AccessFrequency != 1 {
			t.Errorf("expected 1 access, got %d", imp.AccessFrequency)
		}
		if imp.QueryCount != 1 {
			t.Errorf("expected 1 query, got %d", imp.QueryCount)
		}
	})

	t.Run("record multiple accesses", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			e.RecordAccess("cpu_usage")
		}
		imp, _ := e.GetImportance("cpu_usage")
		if imp.AccessFrequency != 11 {
			t.Errorf("expected 11 accesses, got %d", imp.AccessFrequency)
		}
	})

	t.Run("nonexistent metric", func(t *testing.T) {
		_, ok := e.GetImportance("nonexistent")
		if ok {
			t.Error("expected metric not found")
		}
	})
}

func TestRetentionOptimizerImportance(t *testing.T) {
	db := setupTestDB(t)

	e := NewRetentionOptimizerEngine(db, DefaultRetentionOptimizerConfig())

	// Create metrics with different access patterns
	for i := 0; i < 100; i++ {
		e.RecordAccess("hot_metric")
	}
	for i := 0; i < 50; i++ {
		e.RecordAccess("warm_metric")
	}
	e.RecordAccess("cold_metric")

	t.Run("hot metric has highest importance", func(t *testing.T) {
		imp, _ := e.GetImportance("hot_metric")
		if imp.Importance != 1.0 {
			t.Errorf("expected importance 1.0 for hot metric, got %f", imp.Importance)
		}
	})

	t.Run("warm metric has medium importance", func(t *testing.T) {
		imp, _ := e.GetImportance("warm_metric")
		if imp.Importance < 0.4 || imp.Importance > 0.6 {
			t.Errorf("expected ~0.5 importance for warm metric, got %f", imp.Importance)
		}
	})

	t.Run("cold metric has low importance", func(t *testing.T) {
		imp, _ := e.GetImportance("cold_metric")
		if imp.Importance > 0.1 {
			t.Errorf("expected low importance for cold metric, got %f", imp.Importance)
		}
	})
}

func TestRetentionOptimizerAnalyze(t *testing.T) {
	db := setupTestDB(t)

	e := NewRetentionOptimizerEngine(db, DefaultRetentionOptimizerConfig())

	for i := 0; i < 100; i++ {
		e.RecordAccess("high_freq")
	}
	for i := 0; i < 50; i++ {
		e.RecordAccess("med_freq")
	}
	e.RecordAccess("low_freq")

	recs := e.Analyze()
	if len(recs) != 3 {
		t.Fatalf("expected 3 recommendations, got %d", len(recs))
	}

	t.Run("recommendations sorted by priority", func(t *testing.T) {
		for i := 1; i < len(recs); i++ {
			if recs[i].Priority < recs[i-1].Priority {
				t.Error("expected recommendations sorted by ascending priority")
			}
		}
	})

	t.Run("high frequency gets max retention", func(t *testing.T) {
		var found bool
		for _, r := range recs {
			if r.Metric == "high_freq" {
				found = true
				if r.RecommendedRetention != e.config.MaxRetention {
					t.Errorf("expected max retention for high freq, got %v", r.RecommendedRetention)
				}
			}
		}
		if !found {
			t.Error("expected high_freq in recommendations")
		}
	})

	t.Run("low frequency gets min retention", func(t *testing.T) {
		for _, r := range recs {
			if r.Metric == "low_freq" {
				if r.RecommendedRetention != e.config.MinRetention {
					t.Errorf("expected min retention for low freq, got %v", r.RecommendedRetention)
				}
			}
		}
	})
}

func TestRetentionOptimizerStats(t *testing.T) {
	db := setupTestDB(t)

	e := NewRetentionOptimizerEngine(db, DefaultRetentionOptimizerConfig())
	e.RecordAccess("m1")
	e.RecordAccess("m2")
	e.RecordAccess("m1")

	stats := e.GetStats()
	if stats.MetricsTracked != 2 {
		t.Errorf("expected 2 metrics tracked, got %d", stats.MetricsTracked)
	}
	if stats.TotalAccesses != 3 {
		t.Errorf("expected 3 total accesses, got %d", stats.TotalAccesses)
	}
}

func TestRetentionOptimizerStartStop(t *testing.T) {
	db := setupTestDB(t)

	e := NewRetentionOptimizerEngine(db, DefaultRetentionOptimizerConfig())
	e.Start()
	e.Start() // idempotent
	e.Stop()
}
