package chronicle

import "testing"

func TestMetricCorrelationEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("strong positive correlation", func(t *testing.T) {
		e := NewMetricCorrelationEngine(db, DefaultMetricCorrelationConfig())
		for i := 0; i < 100; i++ {
			e.Ingest("cpu", float64(i))
			e.Ingest("memory", float64(i)*2+1) // linear relationship
		}
		result := e.Correlate("cpu", "memory")
		if result == nil { t.Fatal("expected result") }
		if result.Pearson < 0.99 { t.Errorf("expected strong correlation, got %f", result.Pearson) }
		if result.Strength != "strong" { t.Errorf("expected strong, got %s", result.Strength) }
		if result.Direction != "positive" { t.Errorf("expected positive, got %s", result.Direction) }
	})

	t.Run("negative correlation", func(t *testing.T) {
		e := NewMetricCorrelationEngine(db, DefaultMetricCorrelationConfig())
		for i := 0; i < 100; i++ {
			e.Ingest("cache_hit", float64(i))
			e.Ingest("cache_miss", float64(100-i))
		}
		result := e.Correlate("cache_hit", "cache_miss")
		if result == nil { t.Fatal("expected result") }
		if result.Pearson > -0.99 { t.Errorf("expected strong negative, got %f", result.Pearson) }
		if result.Direction != "negative" { t.Error("expected negative") }
	})

	t.Run("no correlation", func(t *testing.T) {
		cfg := DefaultMetricCorrelationConfig()
		cfg.MinCorrelation = 0.9
		e := NewMetricCorrelationEngine(db, cfg)
		for i := 0; i < 50; i++ {
			e.Ingest("a", float64(i%7))
			e.Ingest("b", float64(i%3))
		}
		// With low correlation, Correlate returns result with strength "weak" or "none"
		result := e.Correlate("a", "b")
		if result != nil && result.Strength == "strong" { t.Error("unexpected strong correlation") }
	})

	t.Run("analyze all pairs", func(t *testing.T) {
		e := NewMetricCorrelationEngine(db, DefaultMetricCorrelationConfig())
		for i := 0; i < 50; i++ {
			e.Ingest("x", float64(i))
			e.Ingest("y", float64(i)*3)
			e.Ingest("z", float64(i)*-2)
		}
		results := e.Analyze()
		if len(results) == 0 { t.Error("expected correlations") }
	})

	t.Run("insufficient data", func(t *testing.T) {
		e := NewMetricCorrelationEngine(db, DefaultMetricCorrelationConfig())
		e.Ingest("a", 1)
		e.Ingest("b", 2)
		result := e.Correlate("a", "b")
		if result != nil { t.Error("expected nil for insufficient data") }
	})

	t.Run("unknown metric", func(t *testing.T) {
		e := NewMetricCorrelationEngine(db, DefaultMetricCorrelationConfig())
		if e.Correlate("x", "y") != nil { t.Error("expected nil") }
	})

	t.Run("stats", func(t *testing.T) {
		e := NewMetricCorrelationEngine(db, DefaultMetricCorrelationConfig())
		for i := 0; i < 20; i++ { e.Ingest("m1", float64(i)); e.Ingest("m2", float64(i)) }
		e.Analyze()
		stats := e.GetStats()
		if stats.MetricsTracked != 2 { t.Errorf("expected 2, got %d", stats.MetricsTracked) }
	})

	t.Run("pearson edge cases", func(t *testing.T) {
		if computePearson(nil, nil) != 0 { t.Error("expected 0 for nil") }
		if computePearson([]float64{1}, []float64{1, 2}) != 0 { t.Error("expected 0 for mismatched") }
		if computePearson([]float64{5, 5, 5}, []float64{1, 2, 3}) != 0 { t.Error("expected 0 for constant") }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewMetricCorrelationEngine(db, DefaultMetricCorrelationConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
