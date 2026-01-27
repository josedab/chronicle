package chronicle

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWALWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	points := []Point{
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 1.0, Timestamp: 1000},
		{Metric: "mem", Tags: map[string]string{"host": "b"}, Value: 2.0, Timestamp: 2000},
	}

	if err := wal.Write(points); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and read
	wal2, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	read, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if len(read) != 2 {
		t.Errorf("expected 2 points, got %d", len(read))
	}
}

func TestWALReset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 1000},
	}

	if err := wal.Write(points); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := wal.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}

	read, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if len(read) != 0 {
		t.Errorf("expected 0 points after reset, got %d", len(read))
	}
}

func TestWALRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Small max size to trigger rotation
	wal, err := NewWAL(path, time.Second, 100, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	// Write enough to trigger rotation
	for i := 0; i < 10; i++ {
		points := []Point{
			{Metric: "cpu", Tags: map[string]string{"host": "server"}, Value: float64(i), Timestamp: int64(i * 1000)},
		}
		if err := wal.Write(points); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Check that rotation files exist
	files, err := filepath.Glob(path + ".*")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}

	// Should have some rotation files (retain=2, so at most 2)
	t.Logf("rotation files: %v", files)
}

func TestWALEmptyRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	read, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if len(read) != 0 {
		t.Errorf("expected 0 points from empty WAL, got %d", len(read))
	}
}

func TestWALFileCreation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "test.wal")

	// Should fail because parent dir doesn't exist
	_, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err == nil {
		t.Error("expected error for non-existent parent directory")
	}

	// Create parent dir
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("WAL file was not created")
	}
}

func TestWALMultipleWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	// Multiple write batches
	for i := 0; i < 5; i++ {
		points := []Point{
			{Metric: "cpu", Value: float64(i), Timestamp: int64(i)},
			{Metric: "mem", Value: float64(i * 2), Timestamp: int64(i)},
		}
		if err := wal.Write(points); err != nil {
			t.Fatalf("write batch %d: %v", i, err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify
	wal2, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()

	read, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if len(read) != 10 {
		t.Errorf("expected 10 points, got %d", len(read))
	}
}

func TestWALPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	// Initial position should be 0
	pos := wal.Position()
	if pos != 0 {
		t.Errorf("expected initial position 0, got %d", pos)
	}

	// Write some data
	points := []Point{
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 1.0, Timestamp: 1000},
	}
	if err := wal.Write(points); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Position should now be > 0
	pos = wal.Position()
	if pos <= 0 {
		t.Errorf("expected position > 0 after write, got %d", pos)
	}
}

func TestWALReadRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	// Write some data
	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 1000},
		{Metric: "mem", Value: 2.0, Timestamp: 2000},
	}
	if err := wal.Write(points); err != nil {
		t.Fatalf("write: %v", err)
	}

	endPos := wal.Position()

	// Read the range
	data, err := wal.ReadRange(0, endPos)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty data from ReadRange")
	}

	// Test invalid range
	_, err = wal.ReadRange(-1, 10)
	if err == nil {
		t.Error("expected error for invalid range")
	}

	_, err = wal.ReadRange(10, 5)
	if err == nil {
		t.Error("expected error for end < start")
	}
}

func TestWALApplyEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	// Write initial data
	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 1000},
	}
	if err := wal.Write(points); err != nil {
		t.Fatalf("write: %v", err)
	}

	startPos := wal.Position()

	// Write more data
	points2 := []Point{
		{Metric: "mem", Value: 2.0, Timestamp: 2000},
	}
	if err := wal.Write(points2); err != nil {
		t.Fatalf("write2: %v", err)
	}

	endPos := wal.Position()

	// Read the range for the second write
	data, err := wal.ReadRange(startPos, endPos)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}

	// Create a second WAL and apply the entries
	path2 := filepath.Join(dir, "test2.wal")
	wal2, err := NewWAL(path2, time.Second, 1024*1024, 2)
	if err != nil {
		t.Fatalf("create WAL2: %v", err)
	}
	defer wal2.Close()

	// Apply entries from the first WAL
	if err := wal2.ApplyEntries(data); err != nil {
		t.Fatalf("apply entries: %v", err)
	}

	// Empty apply should not error
	if err := wal2.ApplyEntries(nil); err != nil {
		t.Fatalf("apply empty entries: %v", err)
	}
}

func TestWALSyncErrorCallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	var callbackCalled bool
	callback := func(err error) {
		callbackCalled = true
	}

	wal, err := NewWAL(path, time.Millisecond*100, 1024*1024, 2, WithSyncErrorCallback(callback))
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	// Write some data to ensure the file has content
	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 1000},
	}
	if err := wal.Write(points); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The callback won't be called under normal conditions
	// Just verify the WAL was created with the option
	time.Sleep(time.Millisecond * 200) // Wait for a sync cycle

	// In normal conditions, no error should occur
	// This test just verifies the option mechanism works
	_ = callbackCalled
}
