package chronicle

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWAL_WriteAndRecover verifies WAL write and crash recovery.
func TestWAL_WriteAndRecover(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	// Phase 1: Write points to WAL
	wal, err := NewWAL(walPath, 100*time.Millisecond, 10*1024*1024, 2)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	points := []Point{
		{Metric: "wal.test", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "wal.test", Value: 2.0, Timestamp: time.Now().UnixNano() + 1},
		{Metric: "wal.test", Value: 3.0, Timestamp: time.Now().UnixNano() + 2},
	}

	if err := wal.Write(points); err != nil {
		t.Fatalf("WAL write: %v", err)
	}
	wal.Close()

	// Phase 2: Recover from WAL
	wal2, err := NewWAL(walPath, 100*time.Millisecond, 10*1024*1024, 2)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	recovered, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("WAL ReadAll: %v", err)
	}

	if len(recovered) < len(points) {
		t.Errorf("WAL recovery: expected %d+ points, got %d", len(points), len(recovered))
	}
}

// TestWAL_EmptyFile verifies WAL handles empty/new file.
func TestWAL_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "empty.wal")

	wal, err := NewWAL(walPath, 100*time.Millisecond, 10*1024*1024, 2)
	if err != nil {
		t.Fatalf("new WAL: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("empty WAL read: %v", err)
	}
	if len(recovered) != 0 {
		t.Errorf("expected 0 from empty WAL, got %d", len(recovered))
	}
}

// TestWAL_CorruptedFile verifies WAL handles corrupted data gracefully.
func TestWAL_CorruptedFile(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "corrupt.wal")

	// Write garbage data
	os.WriteFile(walPath, []byte("this is not a valid WAL file\x00\x01\x02"), 0644)

	// Should not panic — may error, but must not crash
	wal, err := NewWAL(walPath, 100*time.Millisecond, 10*1024*1024, 2)
	if err != nil {
		return // acceptable to fail on open
	}
	defer wal.Close()
	wal.ReadAll() // may return error, must not panic
}

// TestIndex_RegisterAndFilter_Coverage verifies index series registration and filtering.
func TestIndex_RegisterAndFilter_Coverage(t *testing.T) {
	idx := newIndex()

	idx.RegisterSeries("cpu", map[string]string{"host": "a", "region": "us"})
	idx.RegisterSeries("cpu", map[string]string{"host": "b", "region": "us"})
	idx.RegisterSeries("cpu", map[string]string{"host": "a", "region": "eu"})
	idx.RegisterSeries("memory", map[string]string{"host": "a"})

	allowed := idx.FilterSeries("cpu", nil)
	if allowed == nil {
		t.Fatal("expected non-nil for cpu")
	}

	allowed = idx.FilterSeries("cpu", map[string]string{"host": "a"})
	if allowed == nil {
		t.Fatal("expected non-nil for cpu host=a")
	}

	// Nonexistent returns nil or empty
	_ = idx.FilterSeries("nonexistent", nil)
}

// TestIndex_GetOrCreatePartition_Coverage verifies partition management.
func TestIndex_GetOrCreatePartition_Coverage(t *testing.T) {
	idx := newIndex()

	now := time.Now().UnixNano()
	hour := int64(time.Hour)

	p1 := idx.GetOrCreatePartition(1, now, now+hour)
	if p1 == nil {
		t.Fatal("expected partition")
	}

	p1again := idx.GetOrCreatePartition(1, now, now+hour)
	if p1 != p1again {
		t.Error("expected same partition for same ID")
	}

	p2 := idx.GetOrCreatePartition(2, now+hour, now+2*hour)
	if p2 == p1 {
		t.Error("expected different partition")
	}
}

// TestBuffer_AddAndDrain_Coverage verifies the write buffer.
func TestBuffer_AddAndDrain_Coverage(t *testing.T) {
	buf := NewWriteBuffer(100)

	for i := 0; i < 50; i++ {
		n := buf.AddAndLen(Point{Metric: "buf.test", Value: float64(i), Timestamp: int64(i)})
		if n != i+1 {
			t.Errorf("expected len %d, got %d", i+1, n)
		}
	}

	points := buf.Drain()
	if len(points) != 50 {
		t.Errorf("expected 50, got %d", len(points))
	}

	points2 := buf.Drain()
	if len(points2) != 0 {
		t.Errorf("expected 0 after drain, got %d", len(points2))
	}
}

// TestSeriesKey_Equality_Coverage verifies SeriesKey comparison.
func TestSeriesKey_Equality_Coverage(t *testing.T) {
	k1 := NewSeriesKey("cpu", map[string]string{"host": "a", "region": "us"})
	k2 := NewSeriesKey("cpu", map[string]string{"region": "us", "host": "a"})
	k3 := NewSeriesKey("cpu", map[string]string{"host": "b"})

	if !k1.Equals(k2) {
		t.Error("same tags different order should be equal")
	}
	if k1.Equals(k3) {
		t.Error("different tags should not be equal")
	}
	if k1.String() != k2.String() {
		t.Error("canonical strings should match")
	}
}
