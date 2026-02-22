package chronicle

import (
	"testing"
	"time"
)

func TestAdaptiveSamplingEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("full rate keeps all", func(t *testing.T) {
		cfg := DefaultAdaptiveSamplingConfig()
		cfg.DefaultRate = 1.0
		cfg.MinRate = 1.0 // prevent adaptive reduction
		e := NewAdaptiveSamplingEngine(db, cfg)
		kept := 0
		for i := 0; i < 100; i++ {
			if e.ShouldSample(Point{Metric: "cpu", Value: float64(i), Timestamp: time.Now().UnixNano() + int64(i)}) { kept++ }
		}
		if kept != 100 { t.Errorf("expected all 100 kept at rate 1.0, got %d", kept) }
	})

	t.Run("zero rate drops all", func(t *testing.T) {
		cfg := DefaultAdaptiveSamplingConfig()
		cfg.DefaultRate = 0.0
		cfg.MinRate = 0.0
		e := NewAdaptiveSamplingEngine(db, cfg)
		kept := 0
		for i := 0; i < 100; i++ {
			if e.ShouldSample(Point{Metric: "cpu", Value: float64(i), Timestamp: int64(i)}) { kept++ }
		}
		if kept > 5 { t.Errorf("expected near-zero kept, got %d", kept) }
	})

	t.Run("volatility tracking", func(t *testing.T) {
		e := NewAdaptiveSamplingEngine(db, DefaultAdaptiveSamplingConfig())
		for i := 0; i < 100; i++ {
			e.ShouldSample(Point{Metric: "volatile", Value: float64(i%10) * 100, Timestamp: int64(i)})
		}
		mv := e.GetMetricVolatility("volatile")
		if mv == nil { t.Fatal("expected metric") }
		if mv.PointsIn != 100 { t.Errorf("expected 100, got %d", mv.PointsIn) }
		if mv.Volatility <= 0 { t.Error("expected non-zero volatility") }
	})

	t.Run("stable data low volatility", func(t *testing.T) {
		e := NewAdaptiveSamplingEngine(db, DefaultAdaptiveSamplingConfig())
		for i := 0; i < 100; i++ {
			e.ShouldSample(Point{Metric: "stable", Value: 50.0, Timestamp: int64(i)})
		}
		mv := e.GetMetricVolatility("stable")
		if mv == nil { t.Fatal("expected metric") }
		if mv.Volatility > 0.01 { t.Errorf("expected low volatility for constant data, got %f", mv.Volatility) }
	})

	t.Run("add rule", func(t *testing.T) {
		e := NewAdaptiveSamplingEngine(db, DefaultAdaptiveSamplingConfig())
		e.AddRule(SamplingRule{MetricPattern: "cpu.*", Rate: 0.5, Priority: 1})
		e.mu.RLock(); count := len(e.rules); e.mu.RUnlock()
		if count != 1 { t.Error("expected 1 rule") }
	})

	t.Run("stats", func(t *testing.T) {
		e := NewAdaptiveSamplingEngine(db, DefaultAdaptiveSamplingConfig())
		for i := 0; i < 10; i++ {
			e.ShouldSample(Point{Metric: "x", Value: float64(i), Timestamp: int64(i)})
		}
		stats := e.GetStats()
		if stats.TotalIn != 10 { t.Errorf("expected 10 in, got %d", stats.TotalIn) }
		if stats.TotalKept+stats.TotalDropped != 10 { t.Error("kept+dropped should equal in") }
	})

	t.Run("unknown metric nil", func(t *testing.T) {
		e := NewAdaptiveSamplingEngine(db, DefaultAdaptiveSamplingConfig())
		if e.GetMetricVolatility("nope") != nil { t.Error("expected nil") }
	})

	t.Run("coefficient of variation", func(t *testing.T) {
		if coefficientOfVariation(nil) != 0 { t.Error("expected 0 for nil") }
		if coefficientOfVariation([]float64{0, 0, 0}) != 0 { t.Error("expected 0 for zeros") }
		cv := coefficientOfVariation([]float64{10, 20, 30, 40, 50})
		if cv <= 0 { t.Errorf("expected positive CV, got %f", cv) }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewAdaptiveSamplingEngine(db, DefaultAdaptiveSamplingConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
