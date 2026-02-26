package chronicle

import (
	"testing"
	"time"
)

func TestDbMetrics(t *testing.T) {
	db := setupTestDB(t)

	t.Run("metrics_empty_db", func(t *testing.T) {
		m := db.Metrics()
		if m == nil {
			t.Error("expected non-nil metrics slice")
		}
		if len(m) != 0 {
			t.Errorf("expected 0 metrics on empty db, got %d", len(m))
		}
	})

	t.Run("series_count_empty_db", func(t *testing.T) {
		c := db.SeriesCount()
		if c != 0 {
			t.Errorf("expected 0 series on empty db, got %d", c)
		}
	})

	t.Run("metrics_after_write", func(t *testing.T) {
		ts := time.Now().UnixNano()
		if err := db.Write(Point{Metric: "cpu.load", Value: 0.5, Timestamp: ts, Tags: map[string]string{"host": "srv1"}}); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}

		metrics := db.Metrics()
		found := false
		for _, m := range metrics {
			if m == "cpu.load" {
				found = true
			}
		}
		if !found {
			t.Error("expected 'cpu.load' in Metrics()")
		}
	})

	t.Run("tag_keys_after_write", func(t *testing.T) {
		keys := db.TagKeys()
		found := false
		for _, k := range keys {
			if k == "host" {
				found = true
			}
		}
		if !found {
			t.Error("expected 'host' in TagKeys()")
		}
	})

	t.Run("tag_values_after_write", func(t *testing.T) {
		vals := db.TagValues("host")
		found := false
		for _, v := range vals {
			if v == "srv1" {
				found = true
			}
		}
		if !found {
			t.Error("expected 'srv1' in TagValues('host')")
		}
	})

	t.Run("series_count_after_write", func(t *testing.T) {
		c := db.SeriesCount()
		if c < 1 {
			t.Errorf("expected at least 1 series, got %d", c)
		}
	})
}
