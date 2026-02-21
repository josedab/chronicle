package chronicle

import (
	"testing"
)

func TestBenchRunnerConfig(t *testing.T) {
	cfg := DefaultBenchRunnerConfig()
	if cfg.WriteCount != 1000 {
		t.Errorf("expected 1000 write count, got %d", cfg.WriteCount)
	}
	if cfg.QueryCount != 100 {
		t.Errorf("expected 100 query count, got %d", cfg.QueryCount)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("expected 4 concurrency, got %d", cfg.Concurrency)
	}
}

func TestBenchRunSuiteWrite(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultBenchRunnerConfig()
	cfg.WriteCount = 50 // small for tests
	suite := NewBenchRunSuite(db, cfg)

	result := suite.RunWrite()
	if result.Operation != "write" {
		t.Errorf("expected write operation, got %s", result.Operation)
	}
	if result.PointCount != 50 {
		t.Errorf("expected 50 points, got %d", result.PointCount)
	}
	if result.Throughput <= 0 {
		t.Errorf("expected positive throughput, got %f", result.Throughput)
	}
	if result.Duration <= 0 {
		t.Error("expected positive duration")
	}
	if result.AvgLatency <= 0 {
		t.Error("expected positive avg latency")
	}
}

func TestBenchRunSuiteQuery(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultBenchRunnerConfig()
	cfg.QueryCount = 10 // small for tests
	suite := NewBenchRunSuite(db, cfg)

	result := suite.RunQuery()
	if result.Operation != "query" {
		t.Errorf("expected query operation, got %s", result.Operation)
	}
	if result.Throughput <= 0 {
		t.Errorf("expected positive throughput, got %f", result.Throughput)
	}
	if result.Duration <= 0 {
		t.Error("expected positive duration")
	}
}

func TestBenchRunSuiteRunAll(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultBenchRunnerConfig()
	cfg.WriteCount = 20
	cfg.QueryCount = 5
	suite := NewBenchRunSuite(db, cfg)

	results := suite.RunAll()
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Operation != "write" {
		t.Errorf("expected first result to be write, got %s", results[0].Operation)
	}
	if results[1].Operation != "query" {
		t.Errorf("expected second result to be query, got %s", results[1].Operation)
	}
}

func TestBenchRunnerEngine(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultBenchRunnerConfig()
	cfg.WriteCount = 20
	cfg.QueryCount = 5
	e := NewBenchRunnerEngine(db, cfg)

	t.Run("run suite", func(t *testing.T) {
		results := e.RunSuite()
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		for _, r := range results {
			if r.Throughput <= 0 {
				t.Errorf("expected positive throughput for %s, got %f", r.Operation, r.Throughput)
			}
		}
	})

	t.Run("last results", func(t *testing.T) {
		last := e.LastResults()
		if len(last) != 2 {
			t.Fatalf("expected 2 last results, got %d", len(last))
		}
	})

	t.Run("compare results", func(t *testing.T) {
		a := []BenchRunResult{
			{Operation: "write", Throughput: 1000, AvgLatency: 100},
			{Operation: "query", Throughput: 500, AvgLatency: 200},
		}
		b := []BenchRunResult{
			{Operation: "write", Throughput: 1200, AvgLatency: 80},
			{Operation: "query", Throughput: 450, AvgLatency: 220},
		}
		comps := e.CompareResults(a, b)
		if len(comps) != 2 {
			t.Fatalf("expected 2 comparisons, got %d", len(comps))
		}
		for _, c := range comps {
			if c.Operation == "write" {
				if c.ThroughputChange != 20 {
					t.Errorf("expected 20%% throughput change, got %f", c.ThroughputChange)
				}
			}
		}
	})
}

func TestBenchRunnerStartStop(t *testing.T) {
	db := setupTestDB(t)

	e := NewBenchRunnerEngine(db, DefaultBenchRunnerConfig())
	e.Start()
	e.Start() // idempotent
	e.Stop()
}
