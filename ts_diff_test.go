package chronicle

import "testing"

func TestTSDiffEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("compare identical data", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		a := make([]float64, 100)
		b := make([]float64, 100)
		for i := range a { a[i] = 50; b[i] = 50 }
		result := e.CompareValues("cpu", a, b)
		if result.Significant { t.Error("expected no significant diff for identical data") }
		if result.Verdict != "no_change" { t.Errorf("expected no_change, got %s", result.Verdict) }
	})

	t.Run("significant increase", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		a := make([]float64, 100)
		b := make([]float64, 100)
		for i := range a { a[i] = 50 + float64(i%3); b[i] = 80 + float64(i%3) }
		result := e.CompareValues("latency", a, b)
		if !result.Significant { t.Error("expected significant diff") }
		if result.Verdict != "increase" { t.Errorf("expected increase, got %s", result.Verdict) }
	})

	t.Run("significant decrease", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		a := make([]float64, 100)
		b := make([]float64, 100)
		for i := range a { a[i] = 100 + float64(i%3); b[i] = 20 + float64(i%3) }
		result := e.CompareValues("errors", a, b)
		if result.Verdict != "decrease" { t.Errorf("expected decrease, got %s", result.Verdict) }
	})

	t.Run("effect size", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		a := []float64{10, 12, 11, 13, 9, 14, 10, 12, 11, 13}
		b := []float64{50, 52, 51, 53, 49, 54, 50, 52, 51, 53}
		result := e.CompareValues("metric", a, b)
		if result.EffectSize < 1.0 { t.Errorf("expected large effect size, got %f", result.EffectSize) }
	})

	t.Run("empty data", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		result := e.CompareValues("empty", nil, nil)
		if result.Significant { t.Error("empty data should not be significant") }
		if result.CountA != 0 { t.Error("expected 0 count") }
	})

	t.Run("single value", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		result := e.CompareValues("single", []float64{1}, []float64{100})
		if result.MeanDiff != 99 { t.Errorf("expected diff 99, got %f", result.MeanDiff) }
	})

	t.Run("mean diff pct", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		a := make([]float64, 50)
		b := make([]float64, 50)
		for i := range a { a[i] = 100; b[i] = 150 }
		result := e.CompareValues("pct", a, b)
		if result.MeanDiffPct != 50 { t.Errorf("expected 50%%, got %f", result.MeanDiffPct) }
	})

	t.Run("compare via db", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		_, err := e.Compare(TSDiffRequest{Metric: ""})
		if err == nil { t.Error("expected error for empty metric") }

		_, err = e.Compare(TSDiffRequest{Metric: "test", WindowA: [2]int64{0, 100}, WindowB: [2]int64{100, 200}})
		if err != nil { t.Errorf("unexpected error: %v", err) }
	})

	t.Run("history", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		e.CompareValues("a", []float64{1, 2}, []float64{3, 4})
		e.CompareValues("b", []float64{5, 6}, []float64{7, 8})
		h := e.History(0)
		if len(h) != 2 { t.Errorf("expected 2, got %d", len(h)) }
	})

	t.Run("stats", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		a := make([]float64, 50); b := make([]float64, 50)
		for i := range a { a[i] = 0; b[i] = 1000 }
		e.CompareValues("x", a, b)
		stats := e.GetStats()
		if stats.TotalComparisons != 1 { t.Error("expected 1 comparison") }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewTSDiffEngine(db, DefaultTSDiffConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})

	t.Run("descriptive stats", func(t *testing.T) {
		s := computeTSDescriptiveStats([]float64{1, 2, 3, 4, 5})
		if s.min != 1 || s.max != 5 { t.Error("wrong min/max") }
		if s.mean != 3 { t.Errorf("expected mean 3, got %f", s.mean) }
		empty := computeTSDescriptiveStats(nil)
		if empty.mean != 0 { t.Error("expected 0 for empty") }
	})
}
