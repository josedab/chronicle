package chronicle

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// BenchRunnerConfig configures the built-in benchmark runner.
type BenchRunnerConfig struct {
	WriteCount     int `json:"write_count"`
	QueryCount     int `json:"query_count"`
	PointsPerBatch int `json:"points_per_batch"`
	Concurrency    int `json:"concurrency"`
}

// DefaultBenchRunnerConfig returns sensible defaults.
func DefaultBenchRunnerConfig() BenchRunnerConfig {
	return BenchRunnerConfig{
		WriteCount:     1000,
		QueryCount:     100,
		PointsPerBatch: 100,
		Concurrency:    4,
	}
}

// BenchRunResult holds the result of a single benchmark operation.
type BenchRunResult struct {
	Operation  string        `json:"operation"`
	Duration   time.Duration `json:"duration"`
	Throughput float64       `json:"throughput"`
	AvgLatency time.Duration `json:"avg_latency"`
	P95Latency time.Duration `json:"p95_latency"`
	P99Latency time.Duration `json:"p99_latency"`
	PointCount int           `json:"point_count"`
}

// BenchRunComparison compares two benchmark results.
type BenchRunComparison struct {
	Operation        string  `json:"operation"`
	ThroughputChange float64 `json:"throughput_change_pct"`
	LatencyChange    float64 `json:"latency_change_pct"`
}

// BenchRunSuite executes write and query benchmarks against a DB.
type BenchRunSuite struct {
	db     *DB
	config BenchRunnerConfig
}

// NewBenchRunSuite creates a new benchmark suite.
func NewBenchRunSuite(db *DB, cfg BenchRunnerConfig) *BenchRunSuite {
	return &BenchRunSuite{db: db, config: cfg}
}

// RunWrite benchmarks write operations and returns a result.
func (s *BenchRunSuite) RunWrite() BenchRunResult {
	var latencies []time.Duration
	totalPoints := 0
	start := time.Now()

	for i := 0; i < s.config.WriteCount; i++ {
		opStart := time.Now()
		_ = s.db.Write(Point{ //nolint:errcheck // benchmark cleanup
			Metric:    "bench_write",
			Value:     float64(i),
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Tags:      map[string]string{"bench": "true"},
		})
		latencies = append(latencies, time.Since(opStart))
		totalPoints++
	}
	_ = s.db.Flush() //nolint:errcheck // benchmark cleanup

	elapsed := time.Since(start)
	return BenchRunResult{
		Operation:  "write",
		Duration:   elapsed,
		Throughput: float64(totalPoints) / elapsed.Seconds(),
		AvgLatency: avgDuration(latencies),
		P95Latency: percentileDuration(latencies, 0.95),
		P99Latency: percentileDuration(latencies, 0.99),
		PointCount: totalPoints,
	}
}

// RunQuery benchmarks query operations and returns a result.
func (s *BenchRunSuite) RunQuery() BenchRunResult {
	// Seed data for querying
	for i := 0; i < 100; i++ {
		_ = s.db.Write(Point{ //nolint:errcheck // benchmark cleanup
			Metric:    "bench_query",
			Value:     float64(i),
			Timestamp: time.Now().Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"bench": "true"},
		})
	}
	_ = s.db.Flush() //nolint:errcheck // benchmark cleanup

	var latencies []time.Duration
	totalPoints := 0
	start := time.Now()

	for i := 0; i < s.config.QueryCount; i++ {
		opStart := time.Now()
		result, err := s.db.Execute(&Query{Metric: "bench_query"})
		if err == nil && result != nil {
			totalPoints += len(result.Points)
		}
		latencies = append(latencies, time.Since(opStart))
	}

	elapsed := time.Since(start)
	return BenchRunResult{
		Operation:  "query",
		Duration:   elapsed,
		Throughput: float64(s.config.QueryCount) / elapsed.Seconds(),
		AvgLatency: avgDuration(latencies),
		P95Latency: percentileDuration(latencies, 0.95),
		P99Latency: percentileDuration(latencies, 0.99),
		PointCount: totalPoints,
	}
}

// RunAll executes all benchmarks and returns results.
func (s *BenchRunSuite) RunAll() []BenchRunResult {
	return []BenchRunResult{s.RunWrite(), s.RunQuery()}
}

// BenchRunnerEngine manages benchmark execution and result tracking.
type BenchRunnerEngine struct {
	db          *DB
	config      BenchRunnerConfig
	mu          sync.RWMutex
	lastResults []BenchRunResult
	stopCh      chan struct{}
	running     bool
}

// NewBenchRunnerEngine creates a new BenchRunnerEngine.
func NewBenchRunnerEngine(db *DB, cfg BenchRunnerConfig) *BenchRunnerEngine {
	return &BenchRunnerEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// Start begins the benchmark runner engine.
func (e *BenchRunnerEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop halts the benchmark runner engine.
func (e *BenchRunnerEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// RunSuite executes the full benchmark suite and stores results.
func (e *BenchRunnerEngine) RunSuite() []BenchRunResult {
	suite := NewBenchRunSuite(e.db, e.config)
	results := suite.RunAll()
	e.mu.Lock()
	e.lastResults = results
	e.mu.Unlock()
	return results
}

// LastResults returns the results from the last benchmark run.
func (e *BenchRunnerEngine) LastResults() []BenchRunResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := make([]BenchRunResult, len(e.lastResults))
	copy(cp, e.lastResults)
	return cp
}

// CompareResults compares two sets of benchmark results.
func (e *BenchRunnerEngine) CompareResults(a, b []BenchRunResult) []BenchRunComparison {
	bMap := make(map[string]BenchRunResult)
	for _, r := range b {
		bMap[r.Operation] = r
	}
	var comparisons []BenchRunComparison
	for _, ra := range a {
		rb, ok := bMap[ra.Operation]
		if !ok {
			continue
		}
		comp := BenchRunComparison{
			Operation: ra.Operation,
		}
		if ra.Throughput > 0 {
			comp.ThroughputChange = ((rb.Throughput - ra.Throughput) / ra.Throughput) * 100
		}
		if ra.AvgLatency > 0 {
			comp.LatencyChange = ((float64(rb.AvgLatency) - float64(ra.AvgLatency)) / float64(ra.AvgLatency)) * 100
		}
		comparisons = append(comparisons, comp)
	}
	return comparisons
}

func avgDuration(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range ds {
		total += d
	}
	return total / time.Duration(len(ds))
}

func percentileDuration(ds []time.Duration, pct float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(ds))
	copy(sorted, ds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * pct)
	return sorted[idx]
}

// RegisterHTTPHandlers registers benchmark runner HTTP endpoints.
func (e *BenchRunnerEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/bench/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		results := e.RunSuite()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})
	mux.HandleFunc("/api/v1/bench/results", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.LastResults())
	})
}
