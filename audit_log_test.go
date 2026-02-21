package chronicle

import (
	"testing"
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
