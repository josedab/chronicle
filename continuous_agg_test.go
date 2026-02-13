package chronicle

import (
	"testing"
	"time"
)

func TestContinuousAggEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("create and ingest", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		err := e.Create(ContinuousAggDefinition{Name: "cpu_avg", SourceMetric: "cpu", Function: "avg", Window: time.Minute})
		if err != nil { t.Fatal(err) }

		now := time.Now().UnixNano()
		for i := 0; i < 10; i++ {
			e.Ingest(Point{Metric: "cpu", Value: float64(i * 10), Timestamp: now + int64(i)*int64(time.Second)})
		}

		state := e.Get("cpu_avg")
		if state == nil { t.Fatal("expected state") }
		if state.PointsIn != 10 { t.Errorf("expected 10 points, got %d", state.PointsIn) }
		if len(state.Windows) == 0 { t.Error("expected windows") }
	})

	t.Run("sum aggregation", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "req_sum", SourceMetric: "requests", Function: "sum", Window: time.Hour})

		now := time.Now().UnixNano()
		e.Ingest(Point{Metric: "requests", Value: 5, Timestamp: now})
		e.Ingest(Point{Metric: "requests", Value: 3, Timestamp: now + 1})

		state := e.Get("req_sum")
		if state.Windows[0].Value != 8 { t.Errorf("expected sum 8, got %f", state.Windows[0].Value) }
	})

	t.Run("min/max", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "t_min", SourceMetric: "temp", Function: "min", Window: time.Hour})
		e.Create(ContinuousAggDefinition{Name: "t_max", SourceMetric: "temp", Function: "max", Window: time.Hour})

		now := time.Now().UnixNano()
		for _, v := range []float64{30, 10, 50, 20} {
			e.Ingest(Point{Metric: "temp", Value: v, Timestamp: now})
		}

		minState := e.Get("t_min")
		maxState := e.Get("t_max")
		if minState.Windows[0].Value != 10 { t.Errorf("expected min 10, got %f", minState.Windows[0].Value) }
		if maxState.Windows[0].Value != 50 { t.Errorf("expected max 50, got %f", maxState.Windows[0].Value) }
	})

	t.Run("filter matching", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{
			Name: "filtered", SourceMetric: "cpu", Function: "avg", Window: time.Hour,
			Filter: map[string]string{"host": "prod"},
		})

		now := time.Now().UnixNano()
		e.Ingest(Point{Metric: "cpu", Value: 100, Timestamp: now, Tags: map[string]string{"host": "prod"}})
		e.Ingest(Point{Metric: "cpu", Value: 50, Timestamp: now, Tags: map[string]string{"host": "dev"}})

		state := e.Get("filtered")
		if state.PointsIn != 1 { t.Errorf("expected 1 filtered point, got %d", state.PointsIn) }
	})

	t.Run("validation", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		if e.Create(ContinuousAggDefinition{}) == nil { t.Error("expected error for empty name") }
		if e.Create(ContinuousAggDefinition{Name: "x"}) == nil { t.Error("expected error for empty source") }
		if e.Create(ContinuousAggDefinition{Name: "x", SourceMetric: "y"}) == nil { t.Error("expected error for zero window") }

		e.Create(ContinuousAggDefinition{Name: "dup", SourceMetric: "y", Window: time.Minute})
		if e.Create(ContinuousAggDefinition{Name: "dup", SourceMetric: "y", Window: time.Minute}) == nil { t.Error("expected dup error") }
	})

	t.Run("max limit", func(t *testing.T) {
		cfg := DefaultContinuousAggConfig()
		cfg.MaxAggregations = 1
		e := NewContinuousAggEngine(db, cfg)
		e.Create(ContinuousAggDefinition{Name: "a", SourceMetric: "x", Window: time.Minute})
		if e.Create(ContinuousAggDefinition{Name: "b", SourceMetric: "x", Window: time.Minute}) == nil { t.Error("expected max error") }
	})

	t.Run("delete", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "del", SourceMetric: "x", Window: time.Minute})
		if e.Delete("del") != nil { t.Error("delete failed") }
		if e.Delete("del") == nil { t.Error("expected error for missing") }
	})

	t.Run("list and get", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Create(ContinuousAggDefinition{Name: "a", SourceMetric: "x", Window: time.Minute})
		e.Create(ContinuousAggDefinition{Name: "b", SourceMetric: "y", Window: time.Minute})
		if len(e.List()) != 2 { t.Error("expected 2") }
		if e.Get("missing") != nil { t.Error("expected nil") }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewContinuousAggEngine(db, DefaultContinuousAggConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
