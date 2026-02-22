package chronicle

import (
	"testing"
	"time"
)

func TestSeriesDedupCheckDuplicate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewSeriesDedupEngine(db, DefaultSeriesDedupConfig())

	now := time.Now().UnixNano()
	p := Point{Metric: "cpu", Value: 42.0, Timestamp: now}

	if e.CheckDuplicate(p) {
		t.Error("first point should not be a duplicate")
	}
	if !e.CheckDuplicate(p) {
		t.Error("exact same point should be a duplicate")
	}
}

func TestSeriesDedupNearDuplicate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultSeriesDedupConfig()
	cfg.DeduplicateWindow = 10 * time.Second
	e := NewSeriesDedupEngine(db, cfg)

	now := time.Now().UnixNano()
	p1 := Point{Metric: "cpu", Value: 42.0, Timestamp: now}
	p2 := Point{Metric: "cpu", Value: 42.0, Timestamp: now + int64(5*time.Second)}

	if e.CheckDuplicate(p1) {
		t.Error("first point should not be a duplicate")
	}
	if !e.CheckDuplicate(p2) {
		t.Error("near-duplicate within window should be detected")
	}
}

func TestSeriesDedupBatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewSeriesDedupEngine(db, DefaultSeriesDedupConfig())

	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "cpu", Value: 42.0, Timestamp: now},
		{Metric: "cpu", Value: 42.0, Timestamp: now},
		{Metric: "mem", Value: 80.0, Timestamp: now},
		{Metric: "cpu", Value: 43.0, Timestamp: now + int64(time.Minute)},
	}

	result := e.Deduplicate(points)
	if len(result) != 3 {
		t.Errorf("expected 3 deduplicated points, got %d", len(result))
	}
}

func TestSeriesDedupNonDuplicate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewSeriesDedupEngine(db, DefaultSeriesDedupConfig())

	now := time.Now().UnixNano()
	p1 := Point{Metric: "cpu", Value: 42.0, Timestamp: now}
	p2 := Point{Metric: "cpu", Value: 99.0, Timestamp: now}

	if e.CheckDuplicate(p1) {
		t.Error("first point should not be a duplicate")
	}
	if e.CheckDuplicate(p2) {
		t.Error("different value should not be a duplicate")
	}
}

func TestSeriesDedupStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewSeriesDedupEngine(db, DefaultSeriesDedupConfig())

	now := time.Now().UnixNano()
	p := Point{Metric: "cpu", Value: 42.0, Timestamp: now}

	e.CheckDuplicate(p)
	e.CheckDuplicate(p)
	e.CheckDuplicate(p)

	stats := e.GetStats()
	if stats.TotalChecked != 3 {
		t.Errorf("expected 3 total checked, got %d", stats.TotalChecked)
	}
	if stats.TotalDuplicates != 2 {
		t.Errorf("expected 2 duplicates, got %d", stats.TotalDuplicates)
	}
}
