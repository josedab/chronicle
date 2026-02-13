package chronicle

import (
	"testing"
	"time"
)

func TestDuckDBBackendEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write test data
	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "test_metric", Value: float64(i * 10), Timestamp: now + int64(i)})
	}
	db.Flush()

	t.Run("simple query", func(t *testing.T) {
		engine := NewDuckDBBackendEngine(db, DefaultDuckDBBackendConfig())

		result, err := engine.ExecuteSQL("SELECT * FROM test_metric")
		if err != nil {
			t.Fatal(err)
		}
		if result.Engine != "native" {
			t.Errorf("expected native engine, got %s", result.Engine)
		}
		if result.RowCount == 0 {
			t.Error("expected rows")
		}
	})

	t.Run("complex query routes to duckdb", func(t *testing.T) {
		cfg := DefaultDuckDBBackendConfig()
		cfg.ComplexThreshold = 50
		engine := NewDuckDBBackendEngine(db, cfg)

		sql := "WITH cte AS (SELECT * FROM test_metric) SELECT *, ROW_NUMBER() OVER (ORDER BY timestamp) FROM cte JOIN other ON cte.id = other.id"
		plan := engine.Analyze(sql)

		if !plan.UsesWindow {
			t.Error("expected window detection")
		}
		if !plan.UsesCTE {
			t.Error("expected CTE detection")
		}
		if !plan.UsesJoin {
			t.Error("expected join detection")
		}
		if plan.RoutedTo != "duckdb" {
			t.Errorf("expected duckdb routing, got %s", plan.RoutedTo)
		}
		if plan.EstimatedCost < 100 {
			t.Errorf("expected high cost, got %f", plan.EstimatedCost)
		}
	})

	t.Run("empty query", func(t *testing.T) {
		engine := NewDuckDBBackendEngine(db, DefaultDuckDBBackendConfig())
		_, err := engine.ExecuteSQL("")
		if err == nil {
			t.Error("expected error for empty SQL")
		}
	})

	t.Run("caching", func(t *testing.T) {
		cfg := DefaultDuckDBBackendConfig()
		cfg.EnableCaching = true
		engine := NewDuckDBBackendEngine(db, cfg)

		sql := "SELECT * FROM test_metric"
		_, err := engine.ExecuteSQL(sql)
		if err != nil {
			t.Fatal(err)
		}

		// Second call should hit cache
		result, err := engine.ExecuteSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if !result.FromCache {
			t.Error("expected cache hit")
		}

		stats := engine.GetStats()
		if stats.CacheHits < 1 {
			t.Errorf("expected cache hits, got %d", stats.CacheHits)
		}
	})

	t.Run("cache disabled", func(t *testing.T) {
		cfg := DefaultDuckDBBackendConfig()
		cfg.EnableCaching = false
		engine := NewDuckDBBackendEngine(db, cfg)

		sql := "SELECT * FROM test_metric"
		engine.ExecuteSQL(sql)
		result, _ := engine.ExecuteSQL(sql)
		if result.FromCache {
			t.Error("expected no cache with caching disabled")
		}
	})

	t.Run("register function", func(t *testing.T) {
		engine := NewDuckDBBackendEngine(db, DefaultDuckDBBackendConfig())

		fn := DuckDBFunction{
			Name:        "CUSTOM_AGG",
			Category:    "custom",
			Args:        []string{"column"},
			ReturnType:  "DOUBLE",
			Description: "Custom aggregation",
		}
		engine.RegisterFunction(fn)

		funcs := engine.ListFunctions("custom")
		if len(funcs) != 1 {
			t.Errorf("expected 1 custom function, got %d", len(funcs))
		}
		if funcs[0].Name != "CUSTOM_AGG" {
			t.Errorf("expected CUSTOM_AGG, got %s", funcs[0].Name)
		}
	})

	t.Run("list functions", func(t *testing.T) {
		engine := NewDuckDBBackendEngine(db, DefaultDuckDBBackendConfig())

		all := engine.ListFunctions("")
		if len(all) == 0 {
			t.Error("expected default functions")
		}

		window := engine.ListFunctions("window")
		if len(window) == 0 {
			t.Error("expected window functions")
		}

		ts := engine.ListFunctions("timeseries")
		if len(ts) == 0 {
			t.Error("expected timeseries functions")
		}
	})

	t.Run("stats tracking", func(t *testing.T) {
		engine := NewDuckDBBackendEngine(db, DefaultDuckDBBackendConfig())

		engine.ExecuteSQL("SELECT * FROM test_metric")
		engine.ExecuteSQL("SELECT * FROM test_metric")

		stats := engine.GetStats()
		if stats.TotalQueries != 2 {
			t.Errorf("expected 2 queries, got %d", stats.TotalQueries)
		}
	})

	t.Run("sql translation", func(t *testing.T) {
		engine := NewDuckDBBackendEngine(db, DefaultDuckDBBackendConfig())

		plan := engine.Analyze("SELECT TIMEBUCKET('1h', ts) FROM metrics")
		if plan.TranslatedSQL == plan.OriginalSQL {
			t.Error("expected SQL translation")
		}
	})

	t.Run("extract metric from SQL", func(t *testing.T) {
		cases := []struct {
			sql    string
			metric string
		}{
			{"SELECT * FROM cpu_usage", "cpu_usage"},
			{"SELECT * FROM \"my.metric\" WHERE x > 1", "my.metric"},
			{"SELECT 1", ""},
			{"SELECT * FROM metrics;", "metrics"},
		}
		for _, tc := range cases {
			got := extractMetricFromSQLQuery(tc.sql)
			if got != tc.metric {
				t.Errorf("extractMetricFromSQLQuery(%q) = %q, want %q", tc.sql, got, tc.metric)
			}
		}
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewDuckDBBackendEngine(db, DefaultDuckDBBackendConfig())
		engine.Start()
		engine.Start() // idempotent
		engine.Stop()
		engine.Stop() // idempotent
	})

	t.Run("window query execution", func(t *testing.T) {
		cfg := DefaultDuckDBBackendConfig()
		cfg.AutoRoute = false // force native for test
		engine := NewDuckDBBackendEngine(db, cfg)

		result, err := engine.ExecuteSQL("SELECT *, ROW_NUMBER() OVER (ORDER BY ts) FROM test_metric")
		if err != nil {
			t.Fatal(err)
		}
		_ = result
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultDuckDBBackendConfig()
		if cfg.MaxMemoryMB != 256 {
			t.Errorf("unexpected max memory: %d", cfg.MaxMemoryMB)
		}
		if cfg.WorkerThreads != 4 {
			t.Errorf("unexpected worker threads: %d", cfg.WorkerThreads)
		}
		if cfg.ComplexThreshold != 100 {
			t.Errorf("unexpected threshold: %d", cfg.ComplexThreshold)
		}
	})
}
