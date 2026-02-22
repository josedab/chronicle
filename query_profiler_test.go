package chronicle

import (
	"testing"
	"time"
)

func TestQueryProfilerEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		db.Write(Point{Metric: "profiled_metric", Value: float64(i), Timestamp: now + int64(i)})
	}
	db.Flush()

	t.Run("profile query", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, QueryProfilerConfig{
			Enabled:           true,
			SlowQueryThreshold: time.Hour, // nothing is slow
			MaxProfiles:       100,
			RecordAllQueries:  true,
		})

		profile, err := engine.Profile("profiled_metric", "default")
		if err != nil {
			t.Fatal(err)
		}
		if profile.ID == "" {
			t.Error("expected profile ID")
		}
		if len(profile.Stages) < 3 {
			t.Errorf("expected at least 3 stages, got %d", len(profile.Stages))
		}
		if profile.Duration <= 0 {
			t.Error("expected positive duration")
		}
	})

	t.Run("profile sql query", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, QueryProfilerConfig{
			Enabled:          true,
			MaxProfiles:      100,
			RecordAllQueries: true,
			SlowQueryThreshold: time.Hour,
		})

		profile, err := engine.Profile("SELECT * FROM profiled_metric", "sql")
		if err != nil {
			t.Fatal(err)
		}
		if profile.QueryType != "sql" {
			t.Errorf("expected sql, got %s", profile.QueryType)
		}
	})

	t.Run("profile promql query", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, QueryProfilerConfig{
			Enabled:          true,
			MaxProfiles:      100,
			RecordAllQueries: true,
			SlowQueryThreshold: time.Hour,
		})

		profile, err := engine.Profile("rate(profiled_metric[5m])", "promql")
		if err != nil {
			t.Fatal(err)
		}
		if profile.QueryType != "promql" {
			t.Errorf("expected promql, got %s", profile.QueryType)
		}
	})

	t.Run("empty query", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, DefaultQueryProfilerConfig())
		_, err := engine.Profile("", "sql")
		if err == nil {
			t.Error("expected error for empty query")
		}
	})

	t.Run("slow query detection", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, QueryProfilerConfig{
			Enabled:           true,
			SlowQueryThreshold: 0, // everything is slow
			MaxProfiles:       100,
			RecordAllQueries:  true,
		})

		profile, _ := engine.Profile("profiled_metric", "default")
		if !profile.IsSlow {
			t.Error("expected query to be marked as slow")
		}

		slow := engine.ListProfiles(true, 10)
		if len(slow) == 0 {
			t.Error("expected slow query in list")
		}
	})

	t.Run("explain plan", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, DefaultQueryProfilerConfig())

		plan := engine.Explain("SELECT * FROM profiled_metric", "sql")
		if plan.Metric != "profiled_metric" {
			t.Errorf("expected profiled_metric, got %s", plan.Metric)
		}
		if len(plan.Steps) < 3 {
			t.Errorf("expected at least 3 steps, got %d", len(plan.Steps))
		}
		if !plan.IndexUsed {
			t.Error("expected index used")
		}
		if plan.EstimatedCost <= 0 {
			t.Error("expected positive cost")
		}
	})

	t.Run("explain empty metric warning", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, DefaultQueryProfilerConfig())

		plan := engine.Explain("SELECT 1", "sql")
		if len(plan.Warnings) == 0 {
			t.Error("expected warnings for missing metric")
		}
	})

	t.Run("list and clear profiles", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, QueryProfilerConfig{
			Enabled:          true,
			MaxProfiles:      100,
			RecordAllQueries: true,
			SlowQueryThreshold: time.Hour,
		})

		engine.Profile("profiled_metric", "default")
		engine.Profile("profiled_metric", "default")

		profiles := engine.ListProfiles(false, 0)
		if len(profiles) < 2 {
			t.Errorf("expected at least 2 profiles, got %d", len(profiles))
		}

		engine.ClearProfiles()
		profiles = engine.ListProfiles(false, 0)
		if len(profiles) != 0 {
			t.Errorf("expected 0 profiles after clear, got %d", len(profiles))
		}
	})

	t.Run("max profiles cap", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, QueryProfilerConfig{
			Enabled:          true,
			MaxProfiles:      3,
			RecordAllQueries: true,
			SlowQueryThreshold: time.Hour,
		})

		for i := 0; i < 10; i++ {
			engine.Profile("profiled_metric", "default")
		}

		profiles := engine.ListProfiles(false, 0)
		if len(profiles) > 3 {
			t.Errorf("expected max 3 profiles, got %d", len(profiles))
		}
	})

	t.Run("stats", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, QueryProfilerConfig{
			Enabled:          true,
			MaxProfiles:      100,
			RecordAllQueries: true,
			SlowQueryThreshold: time.Hour,
		})

		engine.Profile("profiled_metric", "default")
		engine.Profile("profiled_metric", "default")

		stats := engine.GetStats()
		if stats.TotalProfiled != 2 {
			t.Errorf("expected 2 profiled, got %d", stats.TotalProfiled)
		}
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewQueryProfilerEngine(db, DefaultQueryProfilerConfig())
		engine.Start()
		engine.Start()
		engine.Stop()
		engine.Stop()
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultQueryProfilerConfig()
		if cfg.SlowQueryThreshold != 100*time.Millisecond {
			t.Error("unexpected slow query threshold")
		}
		if cfg.MaxProfiles != 1000 {
			t.Error("unexpected max profiles")
		}
	})
}
