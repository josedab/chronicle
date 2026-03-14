package chronicle

import (
	"testing"
	"time"
)

func TestStorage(t *testing.T) {
	db := setupTestDB(t)

	t.Run("storage_init_via_open", func(t *testing.T) {
		if db == nil {
			t.Fatal("expected non-nil DB with initialised storage")
		}
	})

	t.Run("write_flush_persists_data", func(t *testing.T) {
		now := time.Now().UnixNano()
		for i := 0; i < 10; i++ {
			if err := db.Write(Point{
				Metric:    "storage.persist",
				Value:     float64(i),
				Timestamp: now + int64(i)*int64(time.Second),
			}); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		result, err := db.Execute(&Query{Metric: "storage.persist"})
		if err != nil {
			t.Fatalf("query after flush: %v", err)
		}
		if result == nil || len(result.Points) != 10 {
			count := 0
			if result != nil {
				count = len(result.Points)
			}
			t.Errorf("expected 10 points after flush, got %d", count)
		}
	})

	t.Run("metrics_listing", func(t *testing.T) {
		metrics := db.Metrics()
		found := false
		for _, m := range metrics {
			if m == "storage.persist" {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected storage.persist in metrics list")
		}
	})

	t.Run("empty_query_returns_no_error", func(t *testing.T) {
		result, err := db.Execute(&Query{Metric: "nonexistent.metric"})
		if err != nil {
			t.Fatalf("query for nonexistent metric: %v", err)
		}
		if result != nil && len(result.Points) != 0 {
			t.Errorf("expected 0 points for nonexistent metric, got %d", len(result.Points))
		}
	})

	t.Run("batch_write_roundtrip", func(t *testing.T) {
		now := time.Now().UnixNano()
		batch := make([]Point, 25)
		for i := range batch {
			batch[i] = Point{
				Metric:    "storage.batch",
				Value:     float64(i * 3),
				Tags:      map[string]string{"src": "test"},
				Timestamp: now + int64(i)*int64(time.Millisecond),
			}
		}
		if err := db.WriteBatch(batch); err != nil {
			t.Fatalf("batch write: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush after batch: %v", err)
		}
		result, err := db.Execute(&Query{Metric: "storage.batch"})
		if err != nil {
			t.Fatalf("query batch: %v", err)
		}
		if result == nil || len(result.Points) != 25 {
			count := 0
			if result != nil {
				count = len(result.Points)
			}
			t.Errorf("expected 25 batch points, got %d", count)
		}
	})

	t.Run("delete_metric", func(t *testing.T) {
		now := time.Now().UnixNano()
		if err := db.Write(Point{
			Metric: "storage.deleteme", Value: 42, Timestamp: now,
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := db.DeleteMetric("storage.deleteme"); err != nil {
			t.Fatalf("delete metric: %v", err)
		}
		result, err := db.Execute(&Query{Metric: "storage.deleteme"})
		if err != nil {
			t.Fatalf("query after delete: %v", err)
		}
		if result != nil && len(result.Points) != 0 {
			t.Errorf("expected 0 points after delete, got %d", len(result.Points))
		}
	})
}
