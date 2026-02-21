package chronicle

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWALSnapshotCreateAndList(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWALSnapshotConfig()
	engine := NewWALSnapshotEngine(db, cfg)

	snap, err := engine.CreateSnapshot()
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if snap.ID == "" {
		t.Error("snapshot ID should not be empty")
	}
	if snap.Checksum == "" {
		t.Error("snapshot checksum should not be empty")
	}

	snapshots := engine.ListSnapshots()
	if len(snapshots) != 1 {
		t.Errorf("snapshot count: got %d, want 1", len(snapshots))
	}
}

func TestWALSnapshotMaxCap(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWALSnapshotConfig()
	cfg.MaxSnapshots = 3
	engine := NewWALSnapshotEngine(db, cfg)

	for i := 0; i < 7; i++ {
		if _, err := engine.CreateSnapshot(); err != nil {
			t.Fatalf("create snapshot %d: %v", i, err)
		}
	}

	snapshots := engine.ListSnapshots()
	if len(snapshots) != 3 {
		t.Errorf("snapshot count after cap: got %d, want 3", len(snapshots))
	}
}

func TestWALSnapshotRecoveryPlan(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWALSnapshotConfig()
	engine := NewWALSnapshotEngine(db, cfg)

	// No snapshots yet
	_, err := engine.GetRecoveryPlan()
	if err == nil {
		t.Error("expected error for recovery with no snapshots")
	}

	engine.CreateSnapshot()
	engine.CreateSnapshot()

	plan, err := engine.GetRecoveryPlan()
	if err != nil {
		t.Fatalf("recovery plan: %v", err)
	}
	if plan.SnapshotID == "" {
		t.Error("recovery plan snapshot ID should not be empty")
	}
}

func TestWALSnapshotCompact(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWALSnapshotConfig()
	cfg.CompactAfterSnapshot = false
	engine := NewWALSnapshotEngine(db, cfg)

	if err := engine.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	stats := engine.Stats()
	if stats.TotalCompactions != 1 {
		t.Errorf("compactions: got %d, want 1", stats.TotalCompactions)
	}
}

func TestWALSnapshotStartStop(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWALSnapshotConfig()
	engine := NewWALSnapshotEngine(db, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Idempotent
	if err := engine.Start(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestWALSnapshotStats(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWALSnapshotConfig()
	engine := NewWALSnapshotEngine(db, cfg)

	stats := engine.Stats()
	if stats.TotalSnapshots != 0 {
		t.Errorf("initial snapshots: got %d, want 0", stats.TotalSnapshots)
	}

	engine.CreateSnapshot()
	engine.CreateSnapshot()

	stats = engine.Stats()
	if stats.TotalSnapshots != 2 {
		t.Errorf("snapshots after create: got %d, want 2", stats.TotalSnapshots)
	}
	if stats.AvgSnapshotSizeBytes <= 0 {
		t.Error("avg snapshot size should be positive")
	}
	if stats.LastSnapshotAt.IsZero() {
		t.Error("last snapshot time should be set")
	}
}

func TestWALSnapshotHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWALSnapshotConfig()
	engine := NewWALSnapshotEngine(db, cfg)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/wal/snapshot/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("stats status: got %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q, want %q", ct, "application/json")
	}
}
