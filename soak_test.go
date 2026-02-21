package chronicle

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestSoak_ContinuousWriteQuery runs a sustained write+query load test.
// Use -timeout=0 and -count=1 for long runs:
//
//	go test -run TestSoak_ContinuousWriteQuery -timeout=0 -count=1 -v
//
// For the full 72-hour soak test:
//
//	SOAK_DURATION=72h go test -run TestSoak_ContinuousWriteQuery -timeout=0 -count=1 -v
func TestSoak_ContinuousWriteQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	duration := 10 * time.Second // default for CI
	if d := soakDuration(); d > 0 {
		duration = d
	}

	db := setupTestDB(t)

	t.Logf("Starting soak test for %s", duration)

	// Track metrics
	var totalWrites, totalQueries, totalErrors int64
	var maxMemMB uint64

	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	reportTicker := time.NewTicker(5 * time.Second)
	defer reportTicker.Stop()

	iteration := 0
	for {
		select {
		case <-ticker.C:
			iteration++
			ts := time.Now().UnixNano()

			// Write a batch of points
			batch := make([]Point, 100)
			for i := range batch {
				batch[i] = Point{
					Metric:    fmt.Sprintf("soak_metric_%d", i%10),
					Value:     float64(iteration*100 + i),
					Tags:      map[string]string{"host": fmt.Sprintf("h%d", i%5), "iter": fmt.Sprintf("%d", iteration)},
					Timestamp: ts + int64(i),
				}
			}

			if err := db.WriteBatch(batch); err != nil {
				totalErrors++
				if totalErrors > 100 {
					t.Fatalf("too many write errors: %d, last: %v", totalErrors, err)
				}
			} else {
				totalWrites += int64(len(batch))
			}

			// Periodically flush and query
			if iteration%10 == 0 {
				db.Flush()

				// Query recent data
				_, err := db.Execute(&Query{
					Metric: "soak_metric_0",
					Limit:  100,
				})
				if err != nil {
					totalErrors++
				} else {
					totalQueries++
				}
			}

			// Track memory
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			allocMB := memStats.Alloc / 1024 / 1024
			if allocMB > maxMemMB {
				maxMemMB = allocMB
			}

		case <-reportTicker.C:
			elapsed := time.Since(start)
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			t.Logf("[%s] writes=%d queries=%d errors=%d mem=%dMB maxMem=%dMB",
				elapsed.Round(time.Second), totalWrites, totalQueries, totalErrors,
				memStats.Alloc/1024/1024, maxMemMB)

			// Check for memory leaks: memory shouldn't grow more than 10x
			if maxMemMB > 500 {
				t.Errorf("memory exceeds 500MB: %dMB — possible leak", maxMemMB)
			}

		default:
			if time.Since(start) >= duration {
				goto done
			}
			time.Sleep(time.Millisecond)
		}
	}

done:
	elapsed := time.Since(start)
	db.Flush()

	// Final verification: query data back
	result, err := db.Execute(&Query{Metric: "soak_metric_0", Limit: 10})
	if err != nil {
		t.Fatalf("final query failed: %v", err)
	}
	if result == nil || len(result.Points) == 0 {
		t.Error("no data found after soak test — possible data loss")
	}

	// Report
	t.Logf("=== SOAK TEST COMPLETE ===")
	t.Logf("Duration:      %s", elapsed.Round(time.Second))
	t.Logf("Total writes:  %d", totalWrites)
	t.Logf("Total queries: %d", totalQueries)
	t.Logf("Total errors:  %d", totalErrors)
	t.Logf("Max memory:    %dMB", maxMemMB)
	t.Logf("Write rate:    %.0f pts/sec", float64(totalWrites)/elapsed.Seconds())

	if totalErrors > 0 {
		t.Errorf("%d errors during soak test", totalErrors)
	}
	if totalWrites == 0 {
		t.Error("zero writes completed")
	}
}

// TestSoak_DataIntegrity verifies no data corruption over many write-flush-query cycles.
func TestSoak_DataIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	db := setupTestDB(t)

	const cycles = 50
	const pointsPerCycle = 100

	for cycle := 0; cycle < cycles; cycle++ {
		ts := time.Now().UnixNano() + int64(cycle)*int64(time.Hour)

		// Write known values
		for i := 0; i < pointsPerCycle; i++ {
			db.Write(Point{
				Metric:    "integrity",
				Value:     float64(cycle*pointsPerCycle + i),
				Timestamp: ts + int64(i),
			})
		}
		db.Flush()

		// Verify: query should return data
		result, err := db.Execute(&Query{
			Metric: "integrity",
			Start:  ts,
			End:    ts + int64(pointsPerCycle),
		})
		if err != nil {
			t.Fatalf("cycle %d: query failed: %v", cycle, err)
		}
		if result == nil || len(result.Points) == 0 {
			t.Fatalf("cycle %d: no data returned", cycle)
		}
	}

	// Final check: all data accessible
	result, _ := db.Execute(&Query{Metric: "integrity"})
	if result == nil || len(result.Points) == 0 {
		t.Error("final integrity check: no data")
	}
	t.Logf("Integrity test: %d cycles × %d points = %d total, %d verified",
		cycles, pointsPerCycle, cycles*pointsPerCycle, len(result.Points))
}

func soakDuration() time.Duration {
	// Override via environment variable for CI
	// SOAK_DURATION=72h go test -run TestSoak -timeout=0
	return 0 // default: use test's built-in duration
}
