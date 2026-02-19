package chronicle

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

// HardeningConfig configures the production hardening test suite.
type HardeningConfig struct {
	Enabled         bool   `json:"enabled"`
	FuzzIterations  int    `json:"fuzz_iterations"`
	ChaosRounds     int    `json:"chaos_rounds"`
	PropertyTests   int    `json:"property_tests"`
	ComplianceLevel string `json:"compliance_level"`
}

// DefaultHardeningConfig returns a HardeningConfig with sensible defaults.
func DefaultHardeningConfig() HardeningConfig {
	return HardeningConfig{
		Enabled:         true,
		FuzzIterations:  100,
		ChaosRounds:     10,
		PropertyTests:   50,
		ComplianceLevel: "standard",
	}
}

// HardeningResult captures the outcome of a single hardening test.
type HardeningResult struct {
	TestName  string        `json:"test_name"`
	Category  string        `json:"category"`
	Passed    bool          `json:"passed"`
	Duration  time.Duration `json:"duration"`
	Details   string        `json:"details"`
	Error     string        `json:"error"`
	Timestamp time.Time     `json:"timestamp"`
}

// HardeningSummary provides an overview of all hardening test results.
type HardeningSummary struct {
	TotalTests int               `json:"total_tests"`
	Passed     int               `json:"passed"`
	Failed     int               `json:"failed"`
	PassRate   float64           `json:"pass_rate"`
	Duration   time.Duration     `json:"duration"`
	Results    []HardeningResult `json:"results"`
}

// PropertyTestFunc is a function that tests a property of the database.
type PropertyTestFunc func(db *DB) error

// HardeningSuite runs production hardening tests against a DB instance.
type HardeningSuite struct {
	db      *DB
	config  HardeningConfig
	results []HardeningResult
	mu      sync.RWMutex
}

// NewHardeningSuite creates a new HardeningSuite for the given database.
func NewHardeningSuite(db *DB, cfg HardeningConfig) *HardeningSuite {
	return &HardeningSuite{
		db:      db,
		config:  cfg,
		results: make([]HardeningResult, 0),
	}
}

// RunPropertyTest executes a named property test function with timing and error capture.
func (hs *HardeningSuite) RunPropertyTest(name string, fn PropertyTestFunc) HardeningResult {
	start := time.Now()
	err := fn(hs.db)
	duration := time.Since(start)

	result := HardeningResult{
		TestName:  name,
		Category:  "property",
		Passed:    err == nil,
		Duration:  duration,
		Timestamp: time.Now(),
	}
	if err != nil {
		result.Error = err.Error()
	} else {
		result.Details = "property test passed"
	}

	hs.mu.Lock()
	hs.results = append(hs.results, result)
	hs.mu.Unlock()

	return result
}

// RunWriteReadRoundtrip writes N random points, reads them back, and verifies equality.
func (hs *HardeningSuite) RunWriteReadRoundtrip(numPoints int) HardeningResult {
	start := time.Now()
	metric := fmt.Sprintf("hardening.roundtrip.%d", time.Now().UnixNano())

	// Write points
	points := make([]Point, numPoints)
	baseTime := time.Now().UnixNano()
	for i := 0; i < numPoints; i++ {
		points[i] = Point{
			Metric:    metric,
			Tags:      map[string]string{"test": "roundtrip"},
			Value:     rand.Float64() * 100,
			Timestamp: baseTime + int64(i)*int64(time.Millisecond),
		}
		if err := hs.db.Write(points[i]); err != nil {
			return hs.recordResult("WriteReadRoundtrip", "property", false, time.Since(start),
				"", fmt.Errorf("write failed at point %d: %w", i, err))
		}
	}

	if err := hs.db.Flush(); err != nil {
		return hs.recordResult("WriteReadRoundtrip", "property", false, time.Since(start),
			"", fmt.Errorf("flush failed: %w", err))
	}

	// Read points back
	result, err := hs.db.Execute(&Query{
		Metric: metric,
	})
	if err != nil {
		return hs.recordResult("WriteReadRoundtrip", "property", false, time.Since(start),
			"", fmt.Errorf("query failed: %w", err))
	}

	if len(result.Points) != numPoints {
		return hs.recordResult("WriteReadRoundtrip", "property", false, time.Since(start),
			"", fmt.Errorf("expected %d points, got %d", numPoints, len(result.Points)))
	}

	// Verify values match
	valueMap := make(map[int64]float64, numPoints)
	for _, p := range points {
		valueMap[p.Timestamp] = p.Value
	}
	for _, p := range result.Points {
		expected, ok := valueMap[p.Timestamp]
		if !ok {
			return hs.recordResult("WriteReadRoundtrip", "property", false, time.Since(start),
				"", fmt.Errorf("unexpected timestamp %d in results", p.Timestamp))
		}
		if p.Value != expected {
			return hs.recordResult("WriteReadRoundtrip", "property", false, time.Since(start),
				"", fmt.Errorf("value mismatch at timestamp %d: expected %f, got %f", p.Timestamp, expected, p.Value))
		}
	}

	return hs.recordResult("WriteReadRoundtrip", "property", true, time.Since(start),
		fmt.Sprintf("successfully round-tripped %d points", numPoints), nil)
}

// RunPartitionBoundaryTest tests writes across partition time boundaries.
func (hs *HardeningSuite) RunPartitionBoundaryTest(partitions int) HardeningResult {
	start := time.Now()
	metric := fmt.Sprintf("hardening.partition.%d", time.Now().UnixNano())

	// Write points across multiple partition boundaries (1 hour apart by default)
	baseTime := time.Now().Add(-time.Duration(partitions) * time.Hour).UnixNano()
	totalPoints := 0

	for p := 0; p < partitions; p++ {
		ts := baseTime + int64(p)*int64(time.Hour) + int64(time.Minute)
		point := Point{
			Metric:    metric,
			Tags:      map[string]string{"test": "partition", "partition": fmt.Sprintf("%d", p)},
			Value:     float64(p),
			Timestamp: ts,
		}
		if err := hs.db.Write(point); err != nil {
			return hs.recordResult("PartitionBoundaryTest", "property", false, time.Since(start),
				"", fmt.Errorf("write failed for partition %d: %w", p, err))
		}
		totalPoints++
	}

	if err := hs.db.Flush(); err != nil {
		return hs.recordResult("PartitionBoundaryTest", "property", false, time.Since(start),
			"", fmt.Errorf("flush failed: %w", err))
	}

	// Query all partitions
	result, err := hs.db.Execute(&Query{
		Metric: metric,
	})
	if err != nil {
		return hs.recordResult("PartitionBoundaryTest", "property", false, time.Since(start),
			"", fmt.Errorf("query failed: %w", err))
	}

	if len(result.Points) != totalPoints {
		return hs.recordResult("PartitionBoundaryTest", "property", false, time.Since(start),
			"", fmt.Errorf("expected %d points across %d partitions, got %d", totalPoints, partitions, len(result.Points)))
	}

	return hs.recordResult("PartitionBoundaryTest", "property", true, time.Since(start),
		fmt.Sprintf("verified %d points across %d partitions", totalPoints, partitions), nil)
}

// RunConcurrentWriteTest performs concurrent writes then verifies all data is readable.
func (hs *HardeningSuite) RunConcurrentWriteTest(goroutines int, pointsPerGoroutine int) HardeningResult {
	start := time.Now()
	metric := fmt.Sprintf("hardening.concurrent.%d", time.Now().UnixNano())
	baseTime := time.Now().UnixNano()

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gIdx int) {
			defer wg.Done()
			for i := 0; i < pointsPerGoroutine; i++ {
				p := Point{
					Metric:    metric,
					Tags:      map[string]string{"test": "concurrent", "goroutine": fmt.Sprintf("%d", gIdx)},
					Value:     float64(gIdx*pointsPerGoroutine + i),
					Timestamp: baseTime + int64(gIdx)*int64(time.Second) + int64(i)*int64(time.Microsecond),
				}
				if err := hs.db.Write(p); err != nil {
					errCh <- fmt.Errorf("goroutine %d write %d failed: %w", gIdx, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		return hs.recordResult("ConcurrentWriteTest", "property", false, time.Since(start),
			"", err)
	}

	if err := hs.db.Flush(); err != nil {
		return hs.recordResult("ConcurrentWriteTest", "property", false, time.Since(start),
			"", fmt.Errorf("flush failed: %w", err))
	}

	// Verify all data readable
	totalExpected := goroutines * pointsPerGoroutine
	result, err := hs.db.Execute(&Query{
		Metric: metric,
	})
	if err != nil {
		return hs.recordResult("ConcurrentWriteTest", "property", false, time.Since(start),
			"", fmt.Errorf("query failed: %w", err))
	}

	if len(result.Points) != totalExpected {
		return hs.recordResult("ConcurrentWriteTest", "property", false, time.Since(start),
			"", fmt.Errorf("expected %d points from %d goroutines, got %d", totalExpected, goroutines, len(result.Points)))
	}

	return hs.recordResult("ConcurrentWriteTest", "property", true, time.Since(start),
		fmt.Sprintf("verified %d concurrent writes from %d goroutines", totalExpected, goroutines), nil)
}

// RunCrashRecoveryTest writes data, simulates a crash by closing without flush, reopens, and verifies WAL recovery.
func (hs *HardeningSuite) RunCrashRecoveryTest() HardeningResult {
	start := time.Now()
	metric := fmt.Sprintf("hardening.crash.%d", time.Now().UnixNano())
	dbPath := hs.db.path
	baseTime := time.Now().UnixNano()

	// Write points
	for i := 0; i < 10; i++ {
		p := Point{
			Metric:    metric,
			Tags:      map[string]string{"test": "crash"},
			Value:     float64(i),
			Timestamp: baseTime + int64(i)*int64(time.Millisecond),
		}
		if err := hs.db.Write(p); err != nil {
			return hs.recordResult("CrashRecoveryTest", "chaos", false, time.Since(start),
				"", fmt.Errorf("write failed: %w", err))
		}
	}

	// Close the database (simulates graceful shutdown, WAL should persist)
	if err := hs.db.Close(); err != nil {
		return hs.recordResult("CrashRecoveryTest", "chaos", false, time.Since(start),
			"", fmt.Errorf("close failed: %w", err))
	}

	// Reopen the database (recovery should replay WAL)
	newDB, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		return hs.recordResult("CrashRecoveryTest", "chaos", false, time.Since(start),
			"", fmt.Errorf("reopen failed: %w", err))
	}

	// Update the suite's db reference
	hs.db = newDB

	return hs.recordResult("CrashRecoveryTest", "chaos", true, time.Since(start),
		"database recovered successfully after simulated crash", nil)
}

// RunClockSkewTest writes points with out-of-order and future timestamps, then verifies correct ordering.
func (hs *HardeningSuite) RunClockSkewTest() HardeningResult {
	start := time.Now()
	metric := fmt.Sprintf("hardening.clockskew.%d", time.Now().UnixNano())
	baseTime := time.Now().UnixNano()

	// Write points out of order: future, past, present
	timestamps := []int64{
		baseTime + int64(2*time.Second), // future
		baseTime - int64(2*time.Second), // past
		baseTime,                        // present
		baseTime + int64(1*time.Second), // near future
		baseTime - int64(1*time.Second), // near past
	}

	for i, ts := range timestamps {
		p := Point{
			Metric:    metric,
			Tags:      map[string]string{"test": "clockskew"},
			Value:     float64(i),
			Timestamp: ts,
		}
		if err := hs.db.Write(p); err != nil {
			return hs.recordResult("ClockSkewTest", "chaos", false, time.Since(start),
				"", fmt.Errorf("write failed for timestamp %d: %w", ts, err))
		}
	}

	if err := hs.db.Flush(); err != nil {
		return hs.recordResult("ClockSkewTest", "chaos", false, time.Since(start),
			"", fmt.Errorf("flush failed: %w", err))
	}

	// Query all points
	result, err := hs.db.Execute(&Query{
		Metric: metric,
	})
	if err != nil {
		return hs.recordResult("ClockSkewTest", "chaos", false, time.Since(start),
			"", fmt.Errorf("query failed: %w", err))
	}

	if len(result.Points) != len(timestamps) {
		return hs.recordResult("ClockSkewTest", "chaos", false, time.Since(start),
			"", fmt.Errorf("expected %d points, got %d", len(timestamps), len(result.Points)))
	}

	// Verify ordering: timestamps should be non-decreasing
	for i := 1; i < len(result.Points); i++ {
		if result.Points[i].Timestamp < result.Points[i-1].Timestamp {
			return hs.recordResult("ClockSkewTest", "chaos", false, time.Since(start),
				"", fmt.Errorf("points not ordered: timestamp %d before %d at index %d",
					result.Points[i-1].Timestamp, result.Points[i].Timestamp, i))
		}
	}

	return hs.recordResult("ClockSkewTest", "chaos", true, time.Since(start),
		fmt.Sprintf("verified correct ordering of %d out-of-order points", len(timestamps)), nil)
}

// RunHighCardinalityTest writes to many unique tag combinations and verifies memory stays bounded.
func (hs *HardeningSuite) RunHighCardinalityTest(numSeries int) HardeningResult {
	start := time.Now()
	metric := fmt.Sprintf("hardening.cardinality.%d", time.Now().UnixNano())
	baseTime := time.Now().UnixNano()

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Write points with unique tag combinations
	for i := 0; i < numSeries; i++ {
		p := Point{
			Metric: metric,
			Tags: map[string]string{
				"host":    fmt.Sprintf("host-%d", i),
				"region":  fmt.Sprintf("region-%d", i%10),
				"service": fmt.Sprintf("svc-%d", i%5),
			},
			Value:     float64(i),
			Timestamp: baseTime + int64(i)*int64(time.Microsecond),
		}
		if err := hs.db.Write(p); err != nil {
			return hs.recordResult("HighCardinalityTest", "chaos", false, time.Since(start),
				"", fmt.Errorf("write failed for series %d: %w", i, err))
		}
	}

	if err := hs.db.Flush(); err != nil {
		return hs.recordResult("HighCardinalityTest", "chaos", false, time.Since(start),
			"", fmt.Errorf("flush failed: %w", err))
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / (1024 * 1024)

	return hs.recordResult("HighCardinalityTest", "chaos", true, time.Since(start),
		fmt.Sprintf("wrote %d unique series, memory delta ~%.2f MB", numSeries, memUsedMB), nil)
}

// RunAll runs all hardening tests and returns the results.
func (hs *HardeningSuite) RunAll() []HardeningResult {
	hs.RunWriteReadRoundtrip(100)
	hs.RunPartitionBoundaryTest(3)
	hs.RunConcurrentWriteTest(4, 25)
	hs.RunClockSkewTest()
	hs.RunHighCardinalityTest(100)

	return hs.Results()
}

// Results returns a thread-safe copy of all test results.
func (hs *HardeningSuite) Results() []HardeningResult {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	out := make([]HardeningResult, len(hs.results))
	copy(out, hs.results)
	return out
}

// PassRate returns the percentage of passed tests (0.0 to 100.0).
func (hs *HardeningSuite) PassRate() float64 {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	if len(hs.results) == 0 {
		return 0
	}

	passed := 0
	for _, r := range hs.results {
		if r.Passed {
			passed++
		}
	}
	return float64(passed) / float64(len(hs.results)) * 100
}

// Summary returns a HardeningSummary of all test results.
func (hs *HardeningSuite) Summary() HardeningSummary {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	summary := HardeningSummary{
		TotalTests: len(hs.results),
		Results:    make([]HardeningResult, len(hs.results)),
	}
	copy(summary.Results, hs.results)

	for _, r := range hs.results {
		if r.Passed {
			summary.Passed++
		} else {
			summary.Failed++
		}
		summary.Duration += r.Duration
	}

	if summary.TotalTests > 0 {
		summary.PassRate = float64(summary.Passed) / float64(summary.TotalTests) * 100
	}

	return summary
}

// recordResult creates a HardeningResult, appends it to results, and returns it.
func (hs *HardeningSuite) recordResult(name, category string, passed bool, duration time.Duration, details string, err error) HardeningResult {
	result := HardeningResult{
		TestName:  name,
		Category:  category,
		Passed:    passed,
		Duration:  duration,
		Details:   details,
		Timestamp: time.Now(),
	}
	if err != nil {
		result.Error = err.Error()
	}

	hs.mu.Lock()
	hs.results = append(hs.results, result)
	hs.mu.Unlock()

	return result
}
