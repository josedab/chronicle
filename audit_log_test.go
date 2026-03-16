package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestAuditLogEngine(t *testing.T) {
	db := setupTestDB(t)

	e := NewAuditLogEngine(db, DefaultAuditLogConfig())
	e.Start()
	defer e.Stop()

	t.Run("log entries", func(t *testing.T) {
		e.Log("write", "user1", "metrics/cpu", "wrote 100 points", true)
		e.Log("query", "user2", "metrics/mem", "queried last 1h", true)
		e.Log("config_change", "admin", "config", "changed retention", true)

		stats := e.GetStats()
		if stats.TotalEntries != 3 {
			t.Errorf("expected 3 entries, got %d", stats.TotalEntries)
		}
	})

	t.Run("query by action", func(t *testing.T) {
		results := e.Query("write", 10)
		if len(results) != 1 {
			t.Errorf("expected 1 write entry, got %d", len(results))
		}
		if results[0].Action != "write" {
			t.Errorf("expected write action, got %s", results[0].Action)
		}
	})

	t.Run("query all actions", func(t *testing.T) {
		results := e.Query("", 10)
		if len(results) < 3 {
			t.Errorf("expected at least 3 entries, got %d", len(results))
		}
	})

	t.Run("entries by action stats", func(t *testing.T) {
		stats := e.GetStats()
		if stats.EntriesByAction["write"] != 1 {
			t.Errorf("expected 1 write, got %d", stats.EntriesByAction["write"])
		}
		if stats.EntriesByAction["query"] != 1 {
			t.Errorf("expected 1 query, got %d", stats.EntriesByAction["query"])
		}
	})

	t.Run("max entries cap", func(t *testing.T) {
		cfg := DefaultAuditLogConfig()
		cfg.MaxEntries = 5
		e2 := NewAuditLogEngine(db, cfg)
		e2.Start()
		defer e2.Stop()

		for i := 0; i < 10; i++ {
			e2.Log("write", "user", "resource", "details", true)
		}
		stats := e2.GetStats()
		if stats.TotalEntries != 5 {
			t.Errorf("expected 5 entries after cap, got %d", stats.TotalEntries)
		}
	})

	t.Run("disabled actions skipped", func(t *testing.T) {
		cfg := DefaultAuditLogConfig()
		cfg.LogWrites = false
		e3 := NewAuditLogEngine(db, cfg)
		e3.Start()
		defer e3.Stop()

		e3.Log("write", "user", "resource", "should be skipped", true)
		e3.Log("query", "user", "resource", "should be logged", true)
		stats := e3.GetStats()
		if stats.TotalEntries != 1 {
			t.Errorf("expected 1 entry (write disabled), got %d", stats.TotalEntries)
		}
	})

	t.Run("disabled engine logs nothing", func(t *testing.T) {
		cfg := DefaultAuditLogConfig()
		cfg.Enabled = false
		e4 := NewAuditLogEngine(db, cfg)
		e4.Start()
		defer e4.Stop()

		e4.Log("write", "user", "resource", "details", true)
		stats := e4.GetStats()
		if stats.TotalEntries != 0 {
			t.Errorf("expected 0 entries when disabled, got %d", stats.TotalEntries)
		}
	})

	t.Run("clear removes all", func(t *testing.T) {
		e.Clear()
		stats := e.GetStats()
		if stats.TotalEntries != 0 {
			t.Errorf("expected 0 after clear, got %d", stats.TotalEntries)
		}
	})

	t.Run("entry has ID and timestamp", func(t *testing.T) {
		e.Log("backup", "admin", "db", "full backup", true)
		results := e.Query("backup", 1)
		if len(results) == 0 {
			t.Fatal("expected backup entry")
		}
		if results[0].ID == "" {
			t.Error("expected entry ID")
		}
		if results[0].Timestamp.IsZero() {
			t.Error("expected timestamp")
		}
	})
}

func TestAuditLogQueryWithFilter(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewAuditLogEngine(db, DefaultAuditLogConfig())
	e.Start()
	defer e.Stop()

	// Insert entries with known timestamps
	now := time.Now()
	e.Log("write", "alice", "metrics/cpu", "wrote points", true)
	e.Log("query", "bob", "metrics/mem", "queried data", true)
	e.Log("write", "alice", "metrics/disk", "wrote more", true)
	e.Log("config_change", "admin", "config", "changed setting", true)

	t.Run("FilterByAction", func(t *testing.T) {
		results := e.QueryWithFilter(AuditQueryFilter{Action: "write"})
		if len(results) != 2 {
			t.Errorf("expected 2 write entries, got %d", len(results))
		}
	})

	t.Run("FilterByActor", func(t *testing.T) {
		results := e.QueryWithFilter(AuditQueryFilter{Actor: "alice"})
		if len(results) != 2 {
			t.Errorf("expected 2 entries for alice, got %d", len(results))
		}
	})

	t.Run("FilterBySince", func(t *testing.T) {
		pastTime := now.Add(-1 * time.Hour)
		results := e.QueryWithFilter(AuditQueryFilter{Since: pastTime})
		if len(results) != 4 {
			t.Errorf("expected 4 entries since 1h ago, got %d", len(results))
		}

		futureTime := now.Add(1 * time.Hour)
		results = e.QueryWithFilter(AuditQueryFilter{Since: futureTime})
		if len(results) != 0 {
			t.Errorf("expected 0 entries since future, got %d", len(results))
		}
	})

	t.Run("FilterByUntil", func(t *testing.T) {
		futureTime := now.Add(1 * time.Hour)
		results := e.QueryWithFilter(AuditQueryFilter{Until: futureTime})
		if len(results) != 4 {
			t.Errorf("expected 4 entries until future, got %d", len(results))
		}

		pastTime := now.Add(-1 * time.Hour)
		results = e.QueryWithFilter(AuditQueryFilter{Until: pastTime})
		if len(results) != 0 {
			t.Errorf("expected 0 entries until past, got %d", len(results))
		}
	})

	t.Run("CombinedFilters", func(t *testing.T) {
		results := e.QueryWithFilter(AuditQueryFilter{
			Action: "write",
			Actor:  "alice",
		})
		if len(results) != 2 {
			t.Errorf("expected 2 entries for alice writes, got %d", len(results))
		}
	})

	t.Run("LimitRespected", func(t *testing.T) {
		results := e.QueryWithFilter(AuditQueryFilter{Limit: 2})
		if len(results) != 2 {
			t.Errorf("expected 2 entries with limit, got %d", len(results))
		}
	})

	t.Run("DefaultLimitApplied", func(t *testing.T) {
		results := e.QueryWithFilter(AuditQueryFilter{Limit: 0})
		if len(results) > 100 {
			t.Errorf("expected default limit of 100, got %d", len(results))
		}
	})
}

func TestAuditLogHTTPTimeRange(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewAuditLogEngine(db, DefaultAuditLogConfig())
	e.Start()
	defer e.Stop()

	e.Log("write", "user1", "m1", "d1", true)
	e.Log("query", "user2", "m2", "d2", true)

	mux := http.NewServeMux()
	e.RegisterHTTPHandlers(mux)

	t.Run("EntriesWithSinceParam", func(t *testing.T) {
		since := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/audit/log/entries?since="+since, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		var entries []AuditRecord
		json.Unmarshal(w.Body.Bytes(), &entries)
		if len(entries) != 2 {
			t.Errorf("expected 2 entries, got %d", len(entries))
		}
	})

	t.Run("EntriesWithInvalidSince", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/audit/log/entries?since=not-a-date", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid since, got %d", w.Code)
		}
	})

	t.Run("EntriesWithActorParam", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/audit/log/entries?actor=user1", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		var entries []AuditRecord
		json.Unmarshal(w.Body.Bytes(), &entries)
		if len(entries) != 1 {
			t.Errorf("expected 1 entry for user1, got %d", len(entries))
		}
	})
}

func TestAuditLogPersistence(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	persistPath := t.TempDir() + "/audit.jsonl"

	// Write entries with persistence enabled
	cfg := DefaultAuditLogConfig()
	cfg.PersistPath = persistPath

	e1 := NewAuditLogEngine(db, cfg)
	e1.Start()
	e1.Log("write", "alice", "metrics/cpu", "wrote 100 points", true)
	e1.Log("query", "bob", "metrics/mem", "queried 1h", true)
	e1.Log("backup", "admin", "db", "full backup", true)
	e1.Stop()

	// Verify file was created
	info, err := os.Stat(persistPath)
	if err != nil {
		t.Fatalf("persist file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("persist file is empty")
	}

	// Create new engine and verify entries are replayed
	e2 := NewAuditLogEngine(db, cfg)
	e2.Start()
	defer e2.Stop()

	stats := e2.GetStats()
	if stats.TotalEntries != 3 {
		t.Errorf("expected 3 replayed entries, got %d", stats.TotalEntries)
	}

	results := e2.QueryWithFilter(AuditQueryFilter{Action: "write"})
	if len(results) != 1 {
		t.Errorf("expected 1 replayed write entry, got %d", len(results))
	}
	if len(results) > 0 && results[0].Actor != "alice" {
		t.Errorf("expected actor alice, got %s", results[0].Actor)
	}
}

func TestAuditLogFileRotation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	persistPath := t.TempDir() + "/audit.jsonl"

	cfg := DefaultAuditLogConfig()
	cfg.PersistPath = persistPath
	cfg.MaxFileSize = 200 // very small to trigger rotation

	e := NewAuditLogEngine(db, cfg)
	e.Start()

	// Write enough entries to trigger rotation
	for i := 0; i < 20; i++ {
		e.Log("write", "user", "resource", "details for rotation test entry", true)
	}
	e.Stop()

	// Verify rotated file exists
	if _, err := os.Stat(persistPath + ".1"); err != nil {
		t.Errorf("expected rotated file %s.1 to exist: %v", persistPath, err)
	}

	// Verify current file still exists
	if _, err := os.Stat(persistPath); err != nil {
		t.Fatalf("persist file missing after rotation: %v", err)
	}
}
