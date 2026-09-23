package chronicle

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentWriteStress_100x10K implements Bounty #7:
// "Add concurrent write stress test: 100 goroutines × 10K points each, verify no data loss".
func TestConcurrentWriteStress_100x10K(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/stress.db"

	cfg := DefaultConfig(path)
	// Allow sufficient memory and large buffer for 1M point stress ingestion
	cfg.Storage.MaxMemory = 512 * 1024 * 1024 // 512MB
	cfg.Storage.BufferSize = 50_000
	cfg.WAL.SyncInterval = 100 * time.Millisecond

	db, err := Open(path, cfg)
	if err != nil {
		t.Fatalf("failed to open db for stress test: %v", err)
	}
	defer db.Close()

	numWorkers := 100
	pointsPerWorker := 10000
	if testing.Short() {
		numWorkers = 20
		pointsPerWorker = 1000
		t.Logf("Running shortened stress test: %d workers × %d points (%d total)",
			numWorkers, pointsPerWorker, numWorkers*pointsPerWorker)
	} else {
		t.Logf("Running full stress test: %d workers × %d points (%d total)",
			numWorkers, pointsPerWorker, numWorkers*pointsPerWorker)
	}

	baseTime := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC).UnixNano()

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	var writeErrors int64

	start := time.Now()

	// Spin up 100 concurrent worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Wait for synchronized start signal so all 100 goroutines write simultaneously
			<-startBarrier

			workerTag := fmt.Sprintf("worker-%03d", workerID)
			for i := 0; i < pointsPerWorker; i++ {
				offset := int64(workerID*pointsPerWorker + i)
				p := Point{
					Metric: "stress_concurrent_metric",
					Tags: map[string]string{
						"worker": workerTag,
						"host":   "stress-cluster",
					},
					Value:     float64(offset),
					Timestamp: baseTime + offset*int64(time.Millisecond),
				}

				if err := db.Write(p); err != nil {
					atomic.AddInt64(&writeErrors, 1)
				}
			}
		}(w)
	}

	// Release all workers simultaneously
	close(startBarrier)
	wg.Wait()

	writeElapsed := time.Since(start)
	t.Logf("Completed writes in %v (throughput: %.0f points/sec)",
		writeElapsed, float64(numWorkers*pointsPerWorker)/writeElapsed.Seconds())

	if errCount := atomic.LoadInt64(&writeErrors); errCount > 0 {
		t.Fatalf("encountered %d write errors during concurrent ingestion", errCount)
	}

	// Flush write buffer to storage partitions
	if err := db.Flush(); err != nil {
		t.Fatalf("db.Flush() failed: %v", err)
	}

	// Verify zero data loss per worker
	verifyStart := time.Now()
	totalPointsFound := 0

	for w := 0; w < numWorkers; w++ {
		workerTag := fmt.Sprintf("worker-%03d", w)
		res, err := db.Execute(&Query{
			Metric: "stress_concurrent_metric",
			Tags:   map[string]string{"worker": workerTag},
		})
		if err != nil {
			t.Fatalf("query for %s failed: %v", workerTag, err)
		}

		if len(res.Points) != pointsPerWorker {
			t.Fatalf("worker %s: data loss detected! Expected %d points, got %d",
				workerTag, pointsPerWorker, len(res.Points))
		}
		totalPointsFound += len(res.Points)

		// Spot check first and last point values for integrity
		expectedFirstVal := float64(w * pointsPerWorker)
		expectedLastVal := float64(w*pointsPerWorker + pointsPerWorker - 1)
		if res.Points[0].Value != expectedFirstVal {
			t.Errorf("worker %s first point value mismatch: expected %g, got %g",
				workerTag, expectedFirstVal, res.Points[0].Value)
		}
		if res.Points[len(res.Points)-1].Value != expectedLastVal {
			t.Errorf("worker %s last point value mismatch: expected %g, got %g",
				workerTag, expectedLastVal, res.Points[len(res.Points)-1].Value)
		}
	}

	t.Logf("Verification verified all %d points in %v with ZERO data loss!",
		totalPointsFound, time.Since(verifyStart))
}

// TestConcurrentBatchWriteStress verifies concurrent batch writes with multiple goroutines.
func TestConcurrentBatchWriteStress(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/stress_batch.db"

	cfg := DefaultConfig(path)
	cfg.Storage.MaxMemory = 256 * 1024 * 1024
	cfg.Storage.BufferSize = 50_000

	db, err := Open(path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	numWorkers := 20
	batchesPerWorker := 10
	pointsPerBatch := 200
	totalExpected := numWorkers * batchesPerWorker * pointsPerBatch

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	var writeErrors int64

	baseTime := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC).UnixNano()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startBarrier

			workerTag := fmt.Sprintf("batch-w-%02d", workerID)
			for b := 0; b < batchesPerWorker; b++ {
				batch := make([]Point, pointsPerBatch)
				for i := 0; i < pointsPerBatch; i++ {
					idx := int64(workerID*batchesPerWorker*pointsPerBatch + b*pointsPerBatch + i)
					batch[i] = Point{
						Metric:    "batch_stress_metric",
						Tags:      map[string]string{"worker": workerTag},
						Value:     float64(idx),
						Timestamp: baseTime + idx*int64(time.Microsecond),
					}
				}
				if err := db.WriteBatch(batch); err != nil {
					atomic.AddInt64(&writeErrors, 1)
				}
			}
		}(w)
	}

	close(startBarrier)
	wg.Wait()

	if errCount := atomic.LoadInt64(&writeErrors); errCount > 0 {
		t.Fatalf("encountered %d batch write errors", errCount)
	}
	_ = db.Flush()

	// Verify all points across workers
	for w := 0; w < numWorkers; w++ {
		workerTag := fmt.Sprintf("batch-w-%02d", w)
		res, err := db.Execute(&Query{
			Metric: "batch_stress_metric",
			Tags:   map[string]string{"worker": workerTag},
		})
		if err != nil {
			t.Fatalf("query for %s failed: %v", workerTag, err)
		}
		expectedPoints := batchesPerWorker * pointsPerBatch
		if len(res.Points) != expectedPoints {
			t.Fatalf("%s data loss: expected %d, got %d", workerTag, expectedPoints, len(res.Points))
		}
	}
	t.Logf("Batch stress test passed: %d points verified", totalExpected)
}

// TestConcurrentReadWriteStress verifies stability under simultaneous reads and writes.
func TestConcurrentReadWriteStress(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/rw_stress.db"

	cfg := DefaultConfig(path)
	cfg.Storage.MaxMemory = 256 * 1024 * 1024

	db, err := Open(path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	stopSignal := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: 20 goroutines
	numWriters := 20
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stopSignal:
					return
				default:
					_ = db.Write(Point{
						Metric:    "rw_metric",
						Tags:      map[string]string{"worker": fmt.Sprintf("writer-%d", workerID)},
						Value:     float64(i),
						Timestamp: time.Now().UnixNano(),
					})
					i++
					time.Sleep(100 * time.Microsecond)
				}
			}
		}(w)
	}

	// Readers: 10 goroutines concurrently querying
	numReaders := 10
	var queryErrors int64
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopSignal:
					return
				default:
					_, err := db.Execute(&Query{Metric: "rw_metric"})
					if err != nil {
						atomic.AddInt64(&queryErrors, 1)
					}
					time.Sleep(500 * time.Microsecond)
				}
			}
		}(r)
	}

	// Run concurrent read/write for 2 seconds
	time.Sleep(2 * time.Second)
	close(stopSignal)
	wg.Wait()

	if qErrors := atomic.LoadInt64(&queryErrors); qErrors > 0 {
		t.Fatalf("encountered %d query errors during read/write stress", qErrors)
	}
	t.Log("Concurrent read/write stress test passed cleanly")
}
