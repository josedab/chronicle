package chronicle

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDbCore(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Fatal("expected non-nil DB")
	}
}

func TestDbCore_OpenClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	cfg := DefaultConfig(path)

	db, err := Open(path, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil DB")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDbCore_DoubleClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should not panic. It may or may not return an error
	// depending on implementation — the key guarantee is no crash.
	_ = db.Close()
}

func TestDbCore_WriteAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.Close()
	err = db.Write(Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()})
	if err == nil {
		t.Error("expected error writing to closed DB")
	}
}

func TestDbCore_FlushAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.Close()
	err = db.Flush()
	if err == nil {
		t.Error("expected error flushing closed DB")
	}
}

func TestDbCore_WriteAndQuery(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()
	if err := db.Write(Point{
		Metric:    "cpu.usage",
		Value:     42.5,
		Timestamp: now,
		Tags:      map[string]string{"host": "h1"},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Query without time bounds to get all points.
	result, err := db.Execute(&Query{Metric: "cpu.usage"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point, got %d", len(result.Points))
	}
}

func TestDbCore_Metrics(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UnixNano()

	db.Write(Point{Metric: "metric_a", Value: 1, Timestamp: now})
	db.Write(Point{Metric: "metric_b", Value: 2, Timestamp: now})
	db.Flush()

	metrics := db.Metrics()
	if len(metrics) < 2 {
		t.Errorf("expected at least 2 metrics, got %d", len(metrics))
	}

	found := map[string]bool{}
	for _, m := range metrics {
		found[m] = true
	}
	if !found["metric_a"] || !found["metric_b"] {
		t.Errorf("expected metric_a and metric_b in %v", metrics)
	}
}

func TestDbCore_BatchWrite(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UnixNano()

	batch := make([]Point, 100)
	for i := range batch {
		batch[i] = Point{
			Metric:    "batch.test",
			Value:     float64(i),
			Timestamp: now + int64(i)*int64(time.Millisecond),
		}
	}

	if err := db.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Query without time bounds.
	result, err := db.Execute(&Query{Metric: "batch.test"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Points) != 100 {
		t.Errorf("expected 100 points, got %d", len(result.Points))
	}
}

func TestDbCore_InvalidQueryReturnsError(t *testing.T) {
	db := setupTestDB(t)
	_, err := db.Execute(&Query{Metric: ""})
	if err == nil {
		t.Error("expected error for empty metric query")
	}
}
