package chronicle

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

// BenchmarkSuite runs reproducible performance benchmarks.
type BenchmarkSuite struct {
	results []BenchmarkResult
	mu      sync.Mutex
}

// BenchmarkResult captures the outcome of a single benchmark run.
type BenchmarkResult struct {
	Name          string        `json:"name"`
	Category      string        `json:"category"`
	Operations    int64         `json:"operations"`
	Duration      time.Duration `json:"duration"`
	OpsPerSec     float64       `json:"ops_per_sec"`
	AvgLatencyUs  float64       `json:"avg_latency_us"`
	P50LatencyUs  float64       `json:"p50_latency_us"`
	P99LatencyUs  float64       `json:"p99_latency_us"`
	BytesTotal    int64         `json:"bytes_total,omitempty"`
	ThroughputMBs float64       `json:"throughput_mbs,omitempty"`
	MemAllocBytes int64         `json:"mem_alloc_bytes,omitempty"`
	MemAllocCount int64         `json:"mem_alloc_count,omitempty"`
}

// NewBenchmarkSuite creates a new benchmark suite.
func NewBenchmarkSuite() *BenchmarkSuite {
	return &BenchmarkSuite{}
}

// RunWriteBenchmark benchmarks point write throughput.
func (bs *BenchmarkSuite) RunWriteBenchmark(db *DB, points int, batchSize int) (*BenchmarkResult, error) {
	if db == nil {
		return nil, fmt.Errorf("benchmark: database is nil")
	}
	if points <= 0 {
		points = 100000
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	rng := rand.New(rand.NewSource(42))
	batches := make([][]Point, 0, points/batchSize+1)
	for i := 0; i < points; i += batchSize {
		end := i + batchSize
		if end > points {
			end = points
		}
		batch := make([]Point, 0, end-i)
		for j := i; j < end; j++ {
			batch = append(batch, Point{
				Metric:    fmt.Sprintf("bench.metric.%d", j%100),
				Value:     rng.Float64() * 100,
				Timestamp: time.Now().Add(time.Duration(j) * time.Millisecond).UnixNano(),
				Tags: map[string]string{
					"host":   fmt.Sprintf("host-%d", j%10),
					"region": fmt.Sprintf("region-%d", j%3),
				},
			})
		}
		batches = append(batches, batch)
	}

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	latencies := make([]time.Duration, 0, len(batches))
	start := time.Now()

	for _, batch := range batches {
		batchStart := time.Now()
		if err := db.WriteBatch(batch); err != nil {
			return nil, fmt.Errorf("benchmark: write failed: %w", err)
		}
		latencies = append(latencies, time.Since(batchStart))
	}

	duration := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	result := &BenchmarkResult{
		Name:          "write_throughput",
		Category:      "write",
		Operations:    int64(points),
		Duration:      duration,
		OpsPerSec:     float64(points) / duration.Seconds(),
		AvgLatencyUs:  avgDurationUs(latencies),
		P50LatencyUs:  percentileDurationUs(latencies, 0.50),
		P99LatencyUs:  percentileDurationUs(latencies, 0.99),
		MemAllocBytes: int64(memAfter.TotalAlloc - memBefore.TotalAlloc),
		MemAllocCount: int64(memAfter.Mallocs - memBefore.Mallocs),
	}

	bs.mu.Lock()
	bs.results = append(bs.results, *result)
	bs.mu.Unlock()
	return result, nil
}

// RunQueryBenchmark benchmarks query execution latency.
func (bs *BenchmarkSuite) RunQueryBenchmark(db *DB, iterations int) (*BenchmarkResult, error) {
	if db == nil {
		return nil, fmt.Errorf("benchmark: database is nil")
	}
	if iterations <= 0 {
		iterations = 1000
	}

	now := time.Now()
	query := &Query{
		Metric: "bench.metric.0",
		Start:  now.Add(-1 * time.Hour).UnixNano(),
		End:    now.UnixNano(),
	}

	// Warm up
	for i := 0; i < 10; i++ {
		db.Execute(query)
	}

	latencies := make([]time.Duration, 0, iterations)
	start := time.Now()

	for i := 0; i < iterations; i++ {
		qStart := time.Now()
		_, err := db.Execute(query)
		latencies = append(latencies, time.Since(qStart))
		if err != nil {
			continue
		}
	}

	duration := time.Since(start)

	result := &BenchmarkResult{
		Name:         "query_latency",
		Category:     "query",
		Operations:   int64(iterations),
		Duration:     duration,
		OpsPerSec:    float64(iterations) / duration.Seconds(),
		AvgLatencyUs: avgDurationUs(latencies),
		P50LatencyUs: percentileDurationUs(latencies, 0.50),
		P99LatencyUs: percentileDurationUs(latencies, 0.99),
	}

	bs.mu.Lock()
	bs.results = append(bs.results, *result)
	bs.mu.Unlock()
	return result, nil
}

// RunCompressionBenchmark benchmarks compression ratio and speed.
func (bs *BenchmarkSuite) RunCompressionBenchmark(pointCount int) (*BenchmarkResult, error) {
	if pointCount <= 0 {
		pointCount = 100000
	}

	rng := rand.New(rand.NewSource(42))
	points := make([]Point, pointCount)
	var rawBytes int64
	for i := range points {
		points[i] = Point{
			Metric:    "bench.compress",
			Value:     rng.Float64() * 100,
			Timestamp: time.Now().Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": fmt.Sprintf("h%d", i%5)},
		}
		rawBytes += 8 + 8 + int64(len(points[i].Metric)) + 20 // approximate
	}

	start := time.Now()
	// Simulate compression — encode timestamps using delta-of-delta
	var compressed int64
	prevTs := int64(0)
	prevDelta := int64(0)
	for _, p := range points {
		delta := p.Timestamp - prevTs
		dd := delta - prevDelta
		compressed += int64(varIntSize(dd))
		prevDelta = delta
		prevTs = p.Timestamp
	}

	// Float values via gorilla-style XOR encoding
	prevBits := uint64(0)
	for _, p := range points {
		bits := math.Float64bits(p.Value)
		xor := bits ^ prevBits
		if xor == 0 {
			compressed += 1
		} else {
			compressed += 8
		}
		prevBits = bits
	}
	duration := time.Since(start)

	ratio := float64(rawBytes) / float64(compressed)

	result := &BenchmarkResult{
		Name:          "compression_ratio",
		Category:      "compression",
		Operations:    int64(pointCount),
		Duration:      duration,
		OpsPerSec:     float64(pointCount) / duration.Seconds(),
		BytesTotal:    rawBytes,
		ThroughputMBs: float64(rawBytes) / duration.Seconds() / (1024 * 1024),
		AvgLatencyUs:  ratio, // repurpose for compression ratio
	}

	bs.mu.Lock()
	bs.results = append(bs.results, *result)
	bs.mu.Unlock()
	return result, nil
}

// RunColdStartBenchmark benchmarks database open/close time.
func (bs *BenchmarkSuite) RunColdStartBenchmark(path string, iterations int) (*BenchmarkResult, error) {
	if iterations <= 0 {
		iterations = 10
	}

	latencies := make([]time.Duration, 0, iterations)
	start := time.Now()

	for i := 0; i < iterations; i++ {
		openStart := time.Now()
		db, err := Open(path, DefaultConfig(path))
		if err != nil {
			return nil, fmt.Errorf("benchmark: open failed: %w", err)
		}
		latencies = append(latencies, time.Since(openStart))
		db.Close()
	}

	duration := time.Since(start)

	result := &BenchmarkResult{
		Name:         "cold_start",
		Category:     "startup",
		Operations:   int64(iterations),
		Duration:     duration,
		OpsPerSec:    float64(iterations) / duration.Seconds(),
		AvgLatencyUs: avgDurationUs(latencies),
		P50LatencyUs: percentileDurationUs(latencies, 0.50),
		P99LatencyUs: percentileDurationUs(latencies, 0.99),
	}

	bs.mu.Lock()
	bs.results = append(bs.results, *result)
	bs.mu.Unlock()
	return result, nil
}

// RunMemoryBenchmark benchmarks memory footprint per point.
func (bs *BenchmarkSuite) RunMemoryBenchmark(pointCount int) *BenchmarkResult {
	if pointCount <= 0 {
		pointCount = 100000
	}

	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	points := make([]Point, pointCount)
	for i := range points {
		points[i] = Point{
			Metric:    fmt.Sprintf("mem.bench.%d", i%50),
			Value:     float64(i),
			Timestamp: int64(i),
			Tags:      map[string]string{"k": "v"},
		}
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	allocBytes := int64(memAfter.TotalAlloc - memBefore.TotalAlloc)
	bytesPerPoint := float64(allocBytes) / float64(pointCount)

	result := &BenchmarkResult{
		Name:          "memory_footprint",
		Category:      "memory",
		Operations:    int64(pointCount),
		MemAllocBytes: allocBytes,
		AvgLatencyUs:  bytesPerPoint, // bytes per point
	}

	bs.mu.Lock()
	bs.results = append(bs.results, *result)
	bs.mu.Unlock()
	return result
}

// RunConcurrentWriteBenchmark measures write throughput under concurrent goroutine contention.
func (bs *BenchmarkSuite) RunConcurrentWriteBenchmark(db *DB, concurrency int, pointsPerWriter int) (*BenchmarkResult, error) {
	if concurrency <= 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}
	if pointsPerWriter <= 0 {
		pointsPerWriter = 10000
	}

	totalPoints := int64(concurrency * pointsPerWriter)
	latencies := make([]time.Duration, totalPoints)
	var latIdx int64
	var mu sync.Mutex

	start := time.Now()
	var wg sync.WaitGroup
	var writeErr error

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			base := time.Now().UnixNano()
			for i := 0; i < pointsPerWriter; i++ {
				p := Point{
					Metric:    fmt.Sprintf("conc.bench.%d", goroutineID%10),
					Value:     rand.Float64() * 100,
					Timestamp: base + int64(i),
					Tags:      map[string]string{"writer": fmt.Sprintf("w%d", goroutineID)},
				}
				t0 := time.Now()
				if err := db.Write(p); err != nil {
					mu.Lock()
					writeErr = err
					mu.Unlock()
					return
				}
				dur := time.Since(t0)
				mu.Lock()
				if latIdx < totalPoints {
					latencies[latIdx] = dur
					latIdx++
				}
				mu.Unlock()
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	if writeErr != nil {
		return nil, writeErr
	}

	// Calculate percentiles from collected latencies
	collected := latencies[:latIdx]
	sort.Slice(collected, func(i, j int) bool { return collected[i] < collected[j] })

	var p50, p99 float64
	if len(collected) > 0 {
		p50 = float64(collected[len(collected)*50/100].Microseconds())
		p99 = float64(collected[len(collected)*99/100].Microseconds())
	}

	result := &BenchmarkResult{
		Name:         fmt.Sprintf("concurrent_write_%d_writers", concurrency),
		Category:     "write",
		Operations:   totalPoints,
		Duration:     elapsed,
		OpsPerSec:    float64(totalPoints) / elapsed.Seconds(),
		AvgLatencyUs: float64(elapsed.Microseconds()) / float64(totalPoints),
		P50LatencyUs: p50,
		P99LatencyUs: p99,
	}

	bs.mu.Lock()
	bs.results = append(bs.results, *result)
	bs.mu.Unlock()
	return result, nil
}

// RunCardinalityBenchmark measures query latency as series cardinality scales.
func (bs *BenchmarkSuite) RunCardinalityBenchmark(db *DB, seriesCounts []int) ([]BenchmarkResult, error) {
	if len(seriesCounts) == 0 {
		seriesCounts = []int{100, 1000, 10000}
	}

	var results []BenchmarkResult
	for _, count := range seriesCounts {
		// Write data with N distinct series
		now := time.Now().UnixNano()
		for i := 0; i < count; i++ {
			p := Point{
				Metric:    fmt.Sprintf("card.bench.%d", i),
				Value:     rand.Float64() * 100,
				Timestamp: now + int64(i),
				Tags:      map[string]string{"series": fmt.Sprintf("s%d", i)},
			}
			if err := db.Write(p); err != nil {
				return nil, err
			}
		}
		_ = db.Flush()

		// Benchmark query across all series
		iterations := 50
		latencies := make([]time.Duration, iterations)
		for i := 0; i < iterations; i++ {
			t0 := time.Now()
			_, err := db.Execute(&Query{Metric: fmt.Sprintf("card.bench.%d", rand.Intn(count))})
			latencies[i] = time.Since(t0)
			if err != nil {
				return nil, err
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		r := BenchmarkResult{
			Name:         fmt.Sprintf("cardinality_%d_series", count),
			Category:     "cardinality",
			Operations:   int64(iterations),
			Duration:     latencies[len(latencies)-1],
			P50LatencyUs: float64(latencies[iterations*50/100].Microseconds()),
			P99LatencyUs: float64(latencies[iterations*99/100].Microseconds()),
		}
		bs.mu.Lock()
		bs.results = append(bs.results, r)
		bs.mu.Unlock()
		results = append(results, r)
	}
	return results, nil
}

// RunAggregationBenchmark measures latency of different aggregation functions.
func (bs *BenchmarkSuite) RunAggregationBenchmark(db *DB, pointCount int) ([]BenchmarkResult, error) {
	if pointCount <= 0 {
		pointCount = 10000
	}

	// Seed data
	now := time.Now().UnixNano()
	pts := make([]Point, pointCount)
	for i := range pts {
		pts[i] = Point{
			Metric:    "agg_bench",
			Value:     rand.Float64()*100 + float64(i%10),
			Timestamp: now + int64(i)*int64(time.Second),
			Tags:      map[string]string{"host": fmt.Sprintf("h%d", i%5)},
		}
	}
	if err := db.WriteBatch(pts); err != nil {
		return nil, err
	}
	_ = db.Flush()

	aggs := []struct {
		name string
		fn   AggFunc
	}{
		{"sum", AggSum},
		{"mean", AggMean},
		{"min", AggMin},
		{"max", AggMax},
		{"count", AggCount},
		{"stddev", AggStddev},
	}

	iterations := 100
	var results []BenchmarkResult
	for _, ag := range aggs {
		latencies := make([]time.Duration, iterations)
		for i := 0; i < iterations; i++ {
			q := &Query{
				Metric:      "agg_bench",
				Aggregation: &Aggregation{Function: ag.fn, Window: time.Minute},
			}
			t0 := time.Now()
			_, err := db.Execute(q)
			latencies[i] = time.Since(t0)
			if err != nil {
				return nil, err
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		var total time.Duration
		for _, l := range latencies {
			total += l
		}

		r := BenchmarkResult{
			Name:         fmt.Sprintf("aggregation_%s", ag.name),
			Category:     "aggregation",
			Operations:   int64(iterations),
			Duration:     total,
			OpsPerSec:    float64(iterations) / total.Seconds(),
			AvgLatencyUs: float64(total.Microseconds()) / float64(iterations),
			P50LatencyUs: float64(latencies[iterations*50/100].Microseconds()),
			P99LatencyUs: float64(latencies[iterations*99/100].Microseconds()),
		}
		bs.mu.Lock()
		bs.results = append(bs.results, r)
		bs.mu.Unlock()
		results = append(results, r)
	}
	return results, nil
}

// RunRetentionBenchmark measures the impact of retention eviction on write throughput.
func (bs *BenchmarkSuite) RunRetentionBenchmark(db *DB, pointCount int) (*BenchmarkResult, error) {
	if pointCount <= 0 {
		pointCount = 50000
	}

	// Write a mix of old and new data; old data triggers retention eviction.
	now := time.Now().UnixNano()
	start := time.Now()

	for i := 0; i < pointCount; i++ {
		// Alternate between recent and aged-out timestamps
		ts := now + int64(i)
		if i%5 == 0 {
			ts = now - int64(30*24*time.Hour) + int64(i) // 30 days old
		}
		p := Point{
			Metric:    "retention_bench",
			Value:     rand.Float64() * 100,
			Timestamp: ts,
			Tags:      map[string]string{"src": "bench"},
		}
		if err := db.Write(p); err != nil {
			return nil, err
		}
	}
	_ = db.Flush()
	elapsed := time.Since(start)

	result := &BenchmarkResult{
		Name:         "retention_mixed_write",
		Category:     "retention",
		Operations:   int64(pointCount),
		Duration:     elapsed,
		OpsPerSec:    float64(pointCount) / elapsed.Seconds(),
		AvgLatencyUs: float64(elapsed.Microseconds()) / float64(pointCount),
	}

	bs.mu.Lock()
	bs.results = append(bs.results, *result)
	bs.mu.Unlock()
	return result, nil
}

// Results returns all benchmark results.
func (bs *BenchmarkSuite) Results() []BenchmarkResult {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	out := make([]BenchmarkResult, len(bs.results))
	copy(out, bs.results)
	return out
}

// FormatReport generates a markdown benchmark report.
func (bs *BenchmarkSuite) FormatReport() string {
	bs.mu.Lock()
	results := make([]BenchmarkResult, len(bs.results))
	copy(results, bs.results)
	bs.mu.Unlock()

	report := "# Chronicle Benchmark Report\n\n"
	report += fmt.Sprintf("**Date**: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	report += fmt.Sprintf("**Go**: %s\n", runtime.Version())
	report += fmt.Sprintf("**OS/Arch**: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	report += fmt.Sprintf("**CPUs**: %d\n\n", runtime.NumCPU())

	report += "| Benchmark | Ops | Duration | Ops/sec | Avg Latency | P50 | P99 |\n"
	report += "|-----------|-----|----------|---------|-------------|-----|-----|\n"

	for _, r := range results {
		report += fmt.Sprintf("| %s | %d | %v | %.0f | %.1fµs | %.1fµs | %.1fµs |\n",
			r.Name, r.Operations, r.Duration.Round(time.Millisecond),
			r.OpsPerSec, r.AvgLatencyUs, r.P50LatencyUs, r.P99LatencyUs)
	}

	return report
}

func avgDurationUs(durations []time.Duration) float64 {
	if len(durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return float64(total.Microseconds()) / float64(len(durations))
}

func percentileDurationUs(durations []time.Duration, percentile float64) float64 {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := int(float64(len(sorted)-1) * percentile)
	return float64(sorted[idx].Microseconds())
}

func varIntSize(v int64) int {
	uv := uint64(v)
	if v < 0 {
		uv = ^uint64(v)<<1 | 1
	} else {
		uv = uint64(v) << 1
	}
	size := 1
	for uv >= 0x80 {
		size++
		uv >>= 7
	}
	return size
}
