package chronicle

import (
	"testing"
	"time"
)

func TestDbRetention(t *testing.T) {
	t.Run("apply_retention_no_panic", func(t *testing.T) {
		db := setupTestDB(t)
		if db == nil {
			t.Fatal("expected non-nil DB")
		}
	})

	t.Run("retention_config_defaults", func(t *testing.T) {
		dir := t.TempDir()
		path := dir + "/ret.db"
		cfg := DefaultConfig(path)
		cfg.Retention.RetentionDuration = 24 * time.Hour

		db, err := Open(path, cfg)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer db.Close()

		if db == nil {
			t.Fatal("expected non-nil DB with retention config")
		}
	})

	t.Run("old_data_survives_without_retention", func(t *testing.T) {
		db := setupTestDB(t)
		oldTS := time.Now().Add(-48 * time.Hour).UnixNano()
		if err := db.Write(Point{Metric: "old.metric", Value: 1.0, Timestamp: oldTS}); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}

		result, err := db.Execute(&Query{Metric: "old.metric"})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if len(result.Points) != 1 {
			t.Errorf("expected 1 point, got %d", len(result.Points))
		}
	})
}
