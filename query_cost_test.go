package chronicle

import "testing"

func TestQueryCostEstimator(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("basic estimate", func(t *testing.T) {
		e := NewQueryCostEstimator(db, DefaultQueryCostConfig())
		est, err := e.Estimate(&Query{Metric: "cpu", Start: 0, End: 3600000000000}) // 1 hour
		if err != nil { t.Fatal(err) }
		if est.EstimatedCost <= 0 { t.Error("expected positive cost") }
		if est.EstPartitions < 1 { t.Error("expected at least 1 partition") }
		if est.Verdict != "allow" { t.Errorf("expected allow, got %s", est.Verdict) }
	})

	t.Run("high cost warns", func(t *testing.T) {
		cfg := DefaultQueryCostConfig()
		cfg.WarnThreshold = 10
		e := NewQueryCostEstimator(db, cfg)
		est, _ := e.Estimate(&Query{Metric: "cpu", Start: 0, End: 86400000000000000}) // huge range
		if est.Verdict == "allow" { t.Error("expected warn or reject for huge range") }
	})

	t.Run("reject threshold", func(t *testing.T) {
		cfg := DefaultQueryCostConfig()
		cfg.RejectThreshold = 1 // very low
		e := NewQueryCostEstimator(db, cfg)
		est, _ := e.Estimate(&Query{Metric: "cpu", Start: 0, End: 86400000000000})
		if est.Verdict != "reject" { t.Errorf("expected reject, got %s", est.Verdict) }
	})

	t.Run("should execute", func(t *testing.T) {
		e := NewQueryCostEstimator(db, DefaultQueryCostConfig())
		ok, est, err := e.ShouldExecute(&Query{Metric: "cpu"})
		if err != nil { t.Fatal(err) }
		if !ok { t.Error("expected allow") }
		if est == nil { t.Error("expected estimate") }
	})

	t.Run("should reject", func(t *testing.T) {
		cfg := DefaultQueryCostConfig()
		cfg.RejectThreshold = 0.001
		e := NewQueryCostEstimator(db, cfg)
		ok, _, err := e.ShouldExecute(&Query{Metric: "cpu"})
		if err != nil { t.Fatal(err) }
		if ok { t.Error("expected reject") }
	})

	t.Run("nil query", func(t *testing.T) {
		e := NewQueryCostEstimator(db, DefaultQueryCostConfig())
		if _, err := e.Estimate(nil); err == nil { t.Error("expected error") }
	})

	t.Run("empty metric", func(t *testing.T) {
		e := NewQueryCostEstimator(db, DefaultQueryCostConfig())
		if _, err := e.Estimate(&Query{}); err == nil { t.Error("expected error") }
	})

	t.Run("limit reduces estimate", func(t *testing.T) {
		e := NewQueryCostEstimator(db, DefaultQueryCostConfig())
		full, _ := e.Estimate(&Query{Metric: "cpu"})
		limited, _ := e.Estimate(&Query{Metric: "cpu", Limit: 10})
		if limited.EstimatedCost >= full.EstimatedCost { t.Error("limit should reduce cost") }
	})

	t.Run("stats tracking", func(t *testing.T) {
		e := NewQueryCostEstimator(db, DefaultQueryCostConfig())
		e.Estimate(&Query{Metric: "a"})
		e.Estimate(&Query{Metric: "b"})
		stats := e.GetStats()
		if stats.TotalEstimates != 2 { t.Errorf("expected 2, got %d", stats.TotalEstimates) }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewQueryCostEstimator(db, DefaultQueryCostConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultQueryCostConfig()
		if cfg.MaxCostBudget != 10000 { t.Error("unexpected budget") }
		if cfg.CostPerPartition != 10 { t.Error("unexpected partition cost") }
	})
}
