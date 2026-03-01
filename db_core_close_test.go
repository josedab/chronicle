package chronicle

import (
	"sync"
	"testing"
	"time"
)

func TestCloseSucceedsOnCleanDB(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed on clean DB: %v", err)
	}
}

func TestDoubleCloseReturnsNilNotPanic(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	// Second close should return nil, not panic.
	err = db.Close()
	if err != nil {
		t.Errorf("Double close should return nil, got: %v", err)
	}
}

func TestCloseAfterWrites(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write some data before closing.
	for i := 0; i < 100; i++ {
		if err := db.Write(Point{
			Metric:    "cpu.usage",
			Value:     float64(i),
			Timestamp: time.Now().Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		}); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close after writes failed: %v", err)
	}
}

func TestCloseAfterConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Start concurrent writers.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = db.Write(Point{
					Metric:    "concurrent.test",
					Value:     float64(id*50 + j),
					Timestamp: time.Now().Add(time.Duration(id*50+j) * time.Millisecond).UnixNano(),
				})
			}
		}(i)
	}

	// Wait for writers, then close.
	wg.Wait()

	if err := db.Close(); err != nil {
		t.Fatalf("Close after concurrent writes failed: %v", err)
	}
}

func TestCloseDataPersists(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"

	// Write data and close.
	db1, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = db1.Write(Point{
			Metric:    "persist.test",
			Value:     float64(i),
			Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "a"},
		})
	}
	_ = db1.Flush()

	if err := db1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify data persisted.
	db2, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	result, err := db2.Execute(&Query{Metric: "persist.test"})
	if err != nil {
		t.Fatalf("Query after reopen failed: %v", err)
	}
	if len(result.Points) == 0 {
		t.Error("Expected data to persist after Close and reopen")
	}
}

func TestCloseStopsBackgroundWorkers(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Close should complete in reasonable time (background workers stop).
	done := make(chan error, 1)
	go func() {
		done <- db.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close timed out – background workers may not have stopped")
	}
}

func TestConcurrentCloseDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Call Close concurrently from multiple goroutines.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Close()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success – no deadlock
	case <-time.After(10 * time.Second):
		t.Fatal("Concurrent Close deadlocked")
	}
}
