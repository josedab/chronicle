package chronicle

import (
	"testing"
	"time"
)

func TestStorageStatsCollect(t *testing.T) {
	db := setupTestDB(t)

	e := NewStorageStatsEngine(db, DefaultStorageStatsConfig())

	snap := e.Collect()
	if snap.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}

	stats := e.GetStats()
	if stats.TotalSnapshots != 1 {
		t.Errorf("expected 1 snapshot, got %d", stats.TotalSnapshots)
	}
}

func TestStorageStatsHistory(t *testing.T) {
	db := setupTestDB(t)

	e := NewStorageStatsEngine(db, DefaultStorageStatsConfig())

	e.Collect()
	e.Collect()
	e.Collect()

	history := e.History(2)
	if len(history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(history))
	}

	// Get all
	all := e.History(0)
	if len(all) != 3 {
		t.Errorf("expected 3 history entries, got %d", len(all))
	}
}

func TestStorageStatsGrowthRate(t *testing.T) {
	db := setupTestDB(t)

	e := NewStorageStatsEngine(db, DefaultStorageStatsConfig())

	// Manually inject snapshots with known values for deterministic test
	e.mu.Lock()
	now := time.Now()
	e.snapshots = append(e.snapshots, StorageSnapshot{
		Timestamp:      now.Add(-10 * time.Second),
		TotalSizeBytes: 1000,
	})
	e.snapshots = append(e.snapshots, StorageSnapshot{
		Timestamp:      now,
		TotalSizeBytes: 2000,
	})
	e.mu.Unlock()

	rate := e.GetGrowthRate()
	if rate < 90 || rate > 110 {
		t.Errorf("expected growth rate ~100 bytes/sec, got %.2f", rate)
	}
}

func TestStorageStatsGrowthRateNoData(t *testing.T) {
	db := setupTestDB(t)

	e := NewStorageStatsEngine(db, DefaultStorageStatsConfig())

	rate := e.GetGrowthRate()
	if rate != 0 {
		t.Errorf("expected 0 growth rate with no snapshots, got %.2f", rate)
	}
}

func TestStorageStatsOverview(t *testing.T) {
	db := setupTestDB(t)

	e := NewStorageStatsEngine(db, DefaultStorageStatsConfig())

	e.Collect()
	e.Collect()

	stats := e.GetStats()
	if stats.TotalSnapshots != 2 {
		t.Errorf("expected 2 snapshots, got %d", stats.TotalSnapshots)
	}
}
