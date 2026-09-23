package chronicle

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// TestLargeDataset_1MPoints_QueryCorrectness implements Bounty #8:
// "Add large dataset test: Write 1M+ points, verify query correctness".
func TestLargeDataset_1MPoints_QueryCorrectness(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/large_dataset.db"

	cfg := DefaultConfig(path)
	cfg.Storage.MaxMemory = 512 * 1024 * 1024 // 512MB
	cfg.Storage.BufferSize = 50_000
	cfg.WAL.SyncInterval = 100 * time.Millisecond

	db, err := Open(path, cfg)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	numSeries := 20
	pointsPerSeries := 50000 // 20 * 50,000 = 1,000,000 points total
	if testing.Short() {
		numSeries = 10
		pointsPerSeries = 5000 // 10 * 5,000 = 50,000 points in short mode
		t.Logf("Running short mode: %d series × %d points (%d points total)",
			numSeries, pointsPerSeries, numSeries*pointsPerSeries)
	} else {
		t.Logf("Running full 1M+ dataset test: %d series × %d points (%d points total)",
			numSeries, pointsPerSeries, numSeries*pointsPerSeries)
	}

	totalExpectedPoints := numSeries * pointsPerSeries
	baseTime := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC).UnixNano()
	interval := int64(time.Millisecond) // 1 millisecond between points per series

	// 1. Ingestion Phase
	ingestStart := time.Now()
	batchSize := 5000
	batch := make([]Point, 0, batchSize)

	for s := 0; s < numSeries; s++ {
		hostTag := fmt.Sprintf("node-%03d", s)
		regionTag := "us-west"
		if s%2 != 0 {
			regionTag = "us-east"
		}
		baseVal := float64(s * 1000)

		for i := 0; i < pointsPerSeries; i++ {
			p := Point{
				Metric: "system_metrics",
				Tags: map[string]string{
					"host":   hostTag,
					"region": regionTag,
				},
				Value:     baseVal + float64(i),
				Timestamp: baseTime + int64(i)*interval,
			}
			batch = append(batch, p)

			if len(batch) >= batchSize {
				if err := db.WriteBatch(batch); err != nil {
					t.Fatalf("failed writing batch: %v", err)
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		if err := db.WriteBatch(batch); err != nil {
			t.Fatalf("failed writing final batch: %v", err)
		}
	}

	ingestElapsed := time.Since(ingestStart)
	t.Logf("Ingested %d points in %v (%.0f points/sec)",
		totalExpectedPoints, ingestElapsed, float64(totalExpectedPoints)/ingestElapsed.Seconds())

	// Flush write buffer to partition storage
	if err := db.Flush(); err != nil {
		t.Fatalf("db.Flush() failed: %v", err)
	}

	// 2. Query Correctness Verification
	verifyStart := time.Now()

	// 2a. Per-Series Point Count and Boundary Verification
	for s := 0; s < numSeries; s++ {
		hostTag := fmt.Sprintf("node-%03d", s)
		res, err := db.Execute(&Query{
			Metric: "system_metrics",
			Tags:   map[string]string{"host": hostTag},
		})
		if err != nil {
			t.Fatalf("query for %s failed: %v", hostTag, err)
		}

		if len(res.Points) != pointsPerSeries {
			t.Fatalf("series %s count mismatch: expected %d, got %d",
				hostTag, pointsPerSeries, len(res.Points))
		}

		// Verify chronological monotonicity
		for i := 1; i < len(res.Points); i++ {
			if res.Points[i].Timestamp <= res.Points[i-1].Timestamp {
				t.Fatalf("series %s timestamp order violation at index %d: %d <= %d",
					hostTag, i, res.Points[i].Timestamp, res.Points[i-1].Timestamp)
			}
		}

		// Verify first and last point values
		baseVal := float64(s * 1000)
		expectedFirst := baseVal
		expectedLast := baseVal + float64(pointsPerSeries-1)
		if res.Points[0].Value != expectedFirst {
			t.Errorf("series %s first point mismatch: expected %g, got %g",
				hostTag, expectedFirst, res.Points[0].Value)
		}
		if res.Points[len(res.Points)-1].Value != expectedLast {
			t.Errorf("series %s last point mismatch: expected %g, got %g",
				hostTag, expectedLast, res.Points[len(res.Points)-1].Value)
		}
	}

	// 2b. Time-Range Slice Correctness (Prefix Slicing via End Timestamp)
	// Query the first 50% time window for series 0: [0..50%)
	midIdx := pointsPerSeries / 2
	sliceEnd := baseTime + int64(midIdx)*interval
	expectedSliceCount := midIdx

	resSlice, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Start:  0,
		End:    sliceEnd,
	})
	if err != nil {
		t.Fatalf("time-range slice query failed: %v", err)
	}

	if len(resSlice.Points) != expectedSliceCount {
		t.Fatalf("time-range slice count mismatch: expected %d, got %d",
			expectedSliceCount, len(resSlice.Points))
	}
	if resSlice.Points[0].Timestamp < baseTime || resSlice.Points[len(resSlice.Points)-1].Timestamp >= sliceEnd {
		t.Errorf("time-range boundary violation: baseTime=%d, end=%d, first=%d, last=%d",
			baseTime, sliceEnd, resSlice.Points[0].Timestamp, resSlice.Points[len(resSlice.Points)-1].Timestamp)
	}

	// 2c. Tag Filter Correctness
	// Half of the series belong to "us-west"
	expectedWestSeries := (numSeries + 1) / 2
	expectedWestPoints := expectedWestSeries * pointsPerSeries
	resWest, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"region": "us-west"},
	})
	if err != nil {
		t.Fatalf("tag query for region=us-west failed: %v", err)
	}
	if len(resWest.Points) != expectedWestPoints {
		t.Fatalf("tag filter region=us-west count mismatch: expected %d, got %d",
			expectedWestPoints, len(resWest.Points))
	}

	// 2d. Mathematical Aggregation Correctness (Sum, Mean, Min, Max, Count)
	// Total time span fits inside 1 hour window aligned to baseTime
	aggWindow := time.Hour

	// AggCount
	resCount, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Start:  0,
		End:    baseTime + int64(aggWindow),
		Aggregation: &Aggregation{
			Function: AggCount,
			Window:   aggWindow,
		},
	})
	if err != nil {
		t.Fatalf("AggCount query failed: %v", err)
	}
	if len(resCount.Points) != 1 || resCount.Points[0].Value != float64(pointsPerSeries) {
		t.Errorf("AggCount mismatch: expected %d, got %+v", pointsPerSeries, resCount.Points)
	}

	// AggSum: sum(0 .. N-1) = N*(N-1)/2
	expectedSum := float64(pointsPerSeries) * float64(pointsPerSeries-1) / 2.0
	resSum, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Start:  0,
		End:    baseTime + int64(aggWindow),
		Aggregation: &Aggregation{
			Function: AggSum,
			Window:   aggWindow,
		},
	})
	if err != nil {
		t.Fatalf("AggSum query failed: %v", err)
	}
	if len(resSum.Points) != 1 || math.Abs(resSum.Points[0].Value-expectedSum) > 0.001 {
		t.Errorf("AggSum mismatch: expected %g, got %+v", expectedSum, resSum.Points)
	}

	// AggMean: mean(0 .. N-1) = (N-1)/2
	expectedMean := float64(pointsPerSeries-1) / 2.0
	resMean, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Start:  0,
		End:    baseTime + int64(aggWindow),
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   aggWindow,
		},
	})
	if err != nil {
		t.Fatalf("AggMean query failed: %v", err)
	}
	if len(resMean.Points) != 1 || math.Abs(resMean.Points[0].Value-expectedMean) > 0.001 {
		t.Errorf("AggMean mismatch: expected %g, got %+v", expectedMean, resMean.Points)
	}

	// AggMin & AggMax
	resMin, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Start:  0,
		End:    baseTime + int64(aggWindow),
		Aggregation: &Aggregation{
			Function: AggMin,
			Window:   aggWindow,
		},
	})
	if err != nil || len(resMin.Points) != 1 || resMin.Points[0].Value != 0.0 {
		t.Errorf("AggMin mismatch: expected 0, got %+v (err: %v)", resMin.Points, err)
	}

	expectedMax := float64(pointsPerSeries - 1)
	resMax, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Start:  0,
		End:    baseTime + int64(aggWindow),
		Aggregation: &Aggregation{
			Function: AggMax,
			Window:   aggWindow,
		},
	})
	if err != nil || len(resMax.Points) != 1 || resMax.Points[0].Value != expectedMax {
		t.Errorf("AggMax mismatch: expected %g, got %+v (err: %v)", expectedMax, resMax.Points, err)
	}

	// 2e. Multi-bucket Window Downsampling
	// Resample into 10 equal buckets
	numBuckets := 10
	bucketDuration := time.Duration(pointsPerSeries/numBuckets) * time.Millisecond
	resBuckets, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Start:  0,
		End:    baseTime + int64(time.Duration(pointsPerSeries)*time.Millisecond),
		Aggregation: &Aggregation{
			Function: AggCount,
			Window:   bucketDuration,
		},
	})
	if err != nil {
		t.Fatalf("multi-bucket query failed: %v", err)
	}
	expectedPointsPerBucket := float64(pointsPerSeries / numBuckets)
	if len(resBuckets.Points) != numBuckets {
		t.Fatalf("expected %d downsample buckets, got %d", numBuckets, len(resBuckets.Points))
	}
	for bIdx, p := range resBuckets.Points {
		if p.Value != expectedPointsPerBucket {
			t.Errorf("bucket %d count mismatch: expected %g, got %g",
				bIdx, expectedPointsPerBucket, p.Value)
		}
	}

	// 2f. Query Limit
	limit := 100
	resLimit, err := db.Execute(&Query{
		Metric: "system_metrics",
		Tags:   map[string]string{"host": "node-000"},
		Limit:  limit,
	})
	if err != nil {
		t.Fatalf("limit query failed: %v", err)
	}
	if len(resLimit.Points) != limit {
		t.Errorf("expected limit of %d points, got %d", limit, len(resLimit.Points))
	}

	t.Logf("Query verification completed successfully in %v across all points!",
		time.Since(verifyStart))
}

// TestLargeDataset_MultiMetric verifies large datasets spanning multiple distinct metric names.
func TestLargeDataset_MultiMetric(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/multi_metric.db"

	cfg := DefaultConfig(path)
	cfg.Storage.MaxMemory = 256 * 1024 * 1024
	cfg.Storage.BufferSize = 20_000

	db, err := Open(path, cfg)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	metrics := []string{"cpu_pct", "memory_bytes", "disk_ops"}
	pointsPerMetric := 20000
	if testing.Short() {
		pointsPerMetric = 2000
	}

	baseTime := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC).UnixNano()

	for _, m := range metrics {
		batch := make([]Point, pointsPerMetric)
		for i := 0; i < pointsPerMetric; i++ {
			batch[i] = Point{
				Metric:    m,
				Tags:      map[string]string{"env": "production", "tier": "backend"},
				Value:     float64(i * 10),
				Timestamp: baseTime + int64(i)*int64(time.Millisecond*100),
			}
		}
		if err := db.WriteBatch(batch); err != nil {
			t.Fatalf("failed to write batch for %s: %v", m, err)
		}
	}
	_ = db.Flush()

	// Verify each metric returns exactly its own points without cross-metric contamination
	for _, m := range metrics {
		res, err := db.Execute(&Query{
			Metric: m,
		})
		if err != nil {
			t.Fatalf("query for metric %s failed: %v", m, err)
		}
		if len(res.Points) != pointsPerMetric {
			t.Fatalf("metric %s point count mismatch: expected %d, got %d",
				m, pointsPerMetric, len(res.Points))
		}
		if res.Points[0].Value != 0 || res.Points[pointsPerMetric-1].Value != float64((pointsPerMetric-1)*10) {
			t.Errorf("metric %s boundary values corrupted", m)
		}
	}
	t.Logf("Multi-metric dataset verification passed: %d points per metric", pointsPerMetric)
}
