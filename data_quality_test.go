package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestDataQualityEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("detect NaN", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		issues := e.Check(Point{Metric: "cpu", Value: math.NaN(), Timestamp: time.Now().UnixNano()})
		found := false
		for _, i := range issues { if i.Type == QualityNaN { found = true } }
		if !found { t.Error("expected NaN detection") }
	})

	t.Run("detect Inf", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		issues := e.Check(Point{Metric: "cpu", Value: math.Inf(1), Timestamp: time.Now().UnixNano()})
		found := false
		for _, i := range issues { if i.Type == QualityInf { found = true } }
		if !found { t.Error("expected Inf detection") }
	})

	t.Run("detect gap", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		now := time.Now().UnixNano()
		e.Check(Point{Metric: "cpu", Value: 1, Timestamp: now})
		// 10 minutes gap
		issues := e.Check(Point{Metric: "cpu", Value: 2, Timestamp: now + int64(10*time.Minute)})
		found := false
		for _, i := range issues { if i.Type == QualityGap { found = true } }
		if !found { t.Error("expected gap detection") }
	})

	t.Run("no gap for normal interval", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		now := time.Now().UnixNano()
		e.Check(Point{Metric: "ok", Value: 1, Timestamp: now})
		issues := e.Check(Point{Metric: "ok", Value: 2, Timestamp: now + int64(time.Second)})
		for _, i := range issues {
			if i.Type == QualityGap { t.Error("unexpected gap for 1s interval") }
		}
	})

	t.Run("detect duplicate", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		now := time.Now().UnixNano()
		e.Check(Point{Metric: "cpu", Value: 42, Timestamp: now})
		issues := e.Check(Point{Metric: "cpu", Value: 42, Timestamp: now + 100})
		found := false
		for _, i := range issues { if i.Type == QualityDuplicate { found = true } }
		if !found { t.Error("expected duplicate detection") }
	})

	t.Run("detect outlier", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		now := time.Now().UnixNano()
		// Feed slightly varying data
		for i := 0; i < 50; i++ {
			e.Check(Point{Metric: "stable", Value: 50 + float64(i%3), Timestamp: now + int64(i)*int64(time.Second)})
		}
		// Feed extreme outlier
		issues := e.Check(Point{Metric: "stable", Value: 50000, Timestamp: now + int64(51)*int64(time.Second)})
		found := false
		for _, i := range issues { if i.Type == QualityOutlier { found = true } }
		if !found { t.Error("expected outlier detection") }
	})

	t.Run("detect clock skew", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		futureTS := time.Now().Add(time.Hour).UnixNano()
		issues := e.Check(Point{Metric: "skewed", Value: 1, Timestamp: futureTS})
		found := false
		for _, i := range issues { if i.Type == QualityClockSkew { found = true } }
		if !found { t.Error("expected clock skew detection") }
	})

	t.Run("quality score", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		now := time.Now().UnixNano()
		for i := 0; i < 100; i++ {
			e.Check(Point{Metric: "scored", Value: float64(i), Timestamp: now + int64(i)*int64(time.Second)})
		}
		score := e.GetScore("scored")
		if score == nil { t.Fatal("expected score") }
		if score.TotalPoints != 100 { t.Errorf("expected 100 points, got %d", score.TotalPoints) }
		if score.Score < 0 || score.Score > 100 { t.Errorf("score out of range: %f", score.Score) }
	})

	t.Run("score nil for unknown", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		if e.GetScore("nope") != nil { t.Error("expected nil") }
	})

	t.Run("list issues with filter", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		e.Check(Point{Metric: "a", Value: math.NaN(), Timestamp: 1})
		e.Check(Point{Metric: "b", Value: math.NaN(), Timestamp: 1})
		all := e.ListIssues("", 0)
		filtered := e.ListIssues("a", 0)
		if len(all) < 2 { t.Error("expected at least 2 issues") }
		if len(filtered) != 1 { t.Errorf("expected 1 filtered, got %d", len(filtered)) }
	})

	t.Run("stats", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		e.Check(Point{Metric: "x", Value: math.NaN(), Timestamp: 1})
		stats := e.GetStats()
		if stats.MetricsMonitored != 1 { t.Error("expected 1 metric") }
		if stats.TotalIssues != 1 { t.Error("expected 1 issue") }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewDataQualityEngine(db, DefaultDataQualityConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})

	t.Run("helper functions", func(t *testing.T) {
		if absInt64(-5) != 5 { t.Error("absInt64 failed") }
		if absInt64(5) != 5 { t.Error("absInt64 failed") }
		m, s := meanAndStddev(nil)
		if m != 0 || s != 0 { t.Error("expected 0 for nil") }
	})
}
