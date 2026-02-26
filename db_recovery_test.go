package chronicle

import (
	"testing"
	"time"
)

func TestDbRecovery(t *testing.T) {
	t.Run("recovery_runs_on_open", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Fatal("expected non-nil DB after recovery")
		}
	})

	t.Run("db_usable_after_reopen", func(t *testing.T) {
		dir := t.TempDir()
		path := dir + "/recovery.db"

		db1, err := Open(path, DefaultConfig(path))
		if err != nil {
			t.Fatalf("first Open() error = %v", err)
		}
		ts := time.Now().UnixNano()
		if err := db1.Write(Point{Metric: "recover.test", Value: 42, Timestamp: ts}); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := db1.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
		db1.Close()

		// Reopen - recovery should replay WAL
		db2, err := Open(path, DefaultConfig(path))
		if err != nil {
			t.Fatalf("second Open() error = %v", err)
		}
		defer db2.Close()

		metrics := db2.Metrics()
		found := false
		for _, m := range metrics {
			if m == "recover.test" {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected to find 'recover.test' metric after reopen")
		}
	})
}
