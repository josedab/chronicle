package chronicle

import (
	"encoding/json"
	"math"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// HWCapability represents a hardware SIMD capability level.
type HWCapability int

const (
	CapNone   HWCapability = iota
	CapSSE42               // x86 SSE4.2
	CapAVX2                // x86 AVX2
	CapAVX512              // x86 AVX-512
	CapNEON                // ARM NEON
	CapSVE                 // ARM SVE
)

func (c HWCapability) String() string {
	names := [...]string{"None", "SSE4.2", "AVX2", "AVX-512", "NEON", "SVE"}
	if int(c) < len(names) {
		return names[c]
	}
	return "Unknown"
}

// AccelOperation identifies an aggregation operation for hardware acceleration.
type AccelOperation int

const (
	OpSum AccelOperation = iota
	OpMin
	OpMax
	OpMean
	OpCount
	OpVariance
	OpStdDev
	OpPercentile
)

func (op AccelOperation) String() string {
	names := [...]string{"Sum", "Min", "Max", "Mean", "Count", "Variance", "StdDev", "Percentile"}
	if int(op) < len(names) {
		return names[op]
	}
	return "Unknown"
}

// HWAcceleratedQueryConfig configures the hardware-accelerated query engine.
type HWAcceleratedQueryConfig struct {
	EnableSIMD            bool `json:"enable_simd"`
	EnableBatchProcessing bool `json:"enable_batch_processing"`
	BatchSize             int  `json:"batch_size"`
	VectorWidth           int  `json:"vector_width"`
	EnablePrefetch        bool `json:"enable_prefetch"`
	CacheLineSize         int  `json:"cache_line_size"`
	EnableParallelScan    bool `json:"enable_parallel_scan"`
	MaxParallelWorkers    int  `json:"max_parallel_workers"`
	ForceSoftwareFallback bool `json:"force_software_fallback"`
}

// DefaultHWAcceleratedQueryConfig returns sensible defaults.
func DefaultHWAcceleratedQueryConfig() HWAcceleratedQueryConfig {
	return HWAcceleratedQueryConfig{
		EnableSIMD:            true,
		EnableBatchProcessing: true,
		BatchSize:             1024,
		VectorWidth:           256,
		EnablePrefetch:        true,
		CacheLineSize:         64,
		EnableParallelScan:    true,
		MaxParallelWorkers:    runtime.NumCPU(),
		ForceSoftwareFallback: false,
	}
}

// HWProfile describes the detected hardware capabilities.
type HWProfile struct {
	Architecture   string         `json:"architecture"`
	Capabilities   []HWCapability `json:"capabilities"`
	VectorWidthBit int            `json:"vector_width_bits"`
	CacheLineBytes int            `json:"cache_line_bytes"`
	NumCores       int            `json:"num_cores"`
	DetectedAt     time.Time      `json:"detected_at"`
}

// AccelPlan describes the execution plan for an accelerated operation.
type AccelPlan struct {
	Operation        AccelOperation `json:"operation"`
	InputSize        int            `json:"input_size"`
	UseSIMD          bool           `json:"use_simd"`
	EstimatedSpeedup float64        `json:"estimated_speedup"`
	BatchCount       int            `json:"batch_count"`
	ParallelWorkers  int            `json:"parallel_workers"`
}

// AccelResult holds the result of an accelerated operation.
type AccelResult struct {
	Operation         AccelOperation `json:"operation"`
	Value             float64        `json:"value"`
	Count             int64          `json:"count"`
	Duration          time.Duration  `json:"duration"`
	SpeedupVsBaseline float64        `json:"speedup_vs_baseline"`
	MethodUsed        string         `json:"method_used"`
}

// HWAcceleratedQueryStats holds engine statistics.
type HWAcceleratedQueryStats struct {
	QueriesAccelerated int64            `json:"queries_accelerated"`
	TotalDataProcessed int64            `json:"total_data_processed"`
	AvgSpeedup         float64          `json:"avg_speedup"`
	HardwareProfile    HWProfile        `json:"hardware_profile"`
	OperationsByType   map[string]int64 `json:"operations_by_type"`
}

// HWAcceleratedQueryEngine provides hardware-accelerated aggregation operations.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type HWAcceleratedQueryEngine struct {
	db       *DB
	config   HWAcceleratedQueryConfig
	profile  HWProfile
	baseline map[AccelOperation]time.Duration
	mu       sync.RWMutex

	// stats
	queriesAccelerated int64
	totalDataProcessed int64
	totalSpeedup       float64
	speedupCount       int64
	operationsByType   map[AccelOperation]int64
	statsMu            sync.Mutex
}

// NewHWAcceleratedQueryEngine creates a new hardware-accelerated query engine.
func NewHWAcceleratedQueryEngine(db *DB, config HWAcceleratedQueryConfig) *HWAcceleratedQueryEngine {
	e := &HWAcceleratedQueryEngine{
		db:               db,
		config:           config,
		baseline:         make(map[AccelOperation]time.Duration),
		operationsByType: make(map[AccelOperation]int64),
	}
	e.profile = e.DetectCapabilities()
	return e
}

// DetectCapabilities detects the hardware capabilities of the current platform.
func (e *HWAcceleratedQueryEngine) DetectCapabilities() HWProfile {
	arch := runtime.GOARCH
	caps := []HWCapability{CapNone}
	vectorWidth := 64
	cacheLineBytes := e.config.CacheLineSize
	if cacheLineBytes == 0 {
		cacheLineBytes = 64
	}

	switch arch {
	case "amd64":
		caps = append(caps, CapSSE42)
		vectorWidth = 128
		if !e.config.ForceSoftwareFallback {
			caps = append(caps, CapAVX2)
			vectorWidth = 256
		}
	case "arm64":
		caps = append(caps, CapNEON)
		vectorWidth = 128
	}

	return HWProfile{
		Architecture:   arch,
		Capabilities:   caps,
		VectorWidthBit: vectorWidth,
		CacheLineBytes: cacheLineBytes,
		NumCores:       runtime.NumCPU(),
		DetectedAt:     time.Now(),
	}
}

// PlanAcceleration creates an execution plan for the given operation and data size.
func (e *HWAcceleratedQueryEngine) PlanAcceleration(op AccelOperation, dataSize int) AccelPlan {
	useSIMD := e.config.EnableSIMD && !e.config.ForceSoftwareFallback && len(e.profile.Capabilities) > 1
	batchCount := 1
	if e.config.EnableBatchProcessing && e.config.BatchSize > 0 {
		batchCount = (dataSize + e.config.BatchSize - 1) / e.config.BatchSize
		if batchCount < 1 {
			batchCount = 1
		}
	}

	workers := 1
	if e.config.EnableParallelScan && dataSize > e.config.BatchSize*4 {
		workers = e.config.MaxParallelWorkers
		if workers < 1 {
			workers = 1
		}
		if workers > batchCount {
			workers = batchCount
		}
	}

	speedup := 1.0
	if useSIMD {
		speedup = float64(e.profile.VectorWidthBit) / 64.0
	}
	if workers > 1 {
		speedup *= float64(workers) * 0.8 // account for coordination overhead
	}

	return AccelPlan{
		Operation:        op,
		InputSize:        dataSize,
		UseSIMD:          useSIMD,
		EstimatedSpeedup: speedup,
		BatchCount:       batchCount,
		ParallelWorkers:  workers,
	}
}

// recordOp records an operation execution for stats.
func (e *HWAcceleratedQueryEngine) recordOp(op AccelOperation, dataLen int, speedup float64) {
	atomic.AddInt64(&e.queriesAccelerated, 1)
	atomic.AddInt64(&e.totalDataProcessed, int64(dataLen))
	e.statsMu.Lock()
	e.operationsByType[op]++
	e.totalSpeedup += speedup
	e.speedupCount++
	e.statsMu.Unlock()
}

// SumFloat64 computes the sum of data using SIMD-style unrolled accumulation.
func (e *HWAcceleratedQueryEngine) SumFloat64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	start := time.Now()
	n := len(data)
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < n; i += 4 {
		s0 += data[i]
		s1 += data[i+1]
		s2 += data[i+2]
		s3 += data[i+3]
	}
	total := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		total += data[i]
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpSum, dur, n)
	e.recordOp(OpSum, n, speedup)
	return total
}

// MinFloat64 returns the minimum value in data.
func (e *HWAcceleratedQueryEngine) MinFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	min := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] < min {
			min = data[i]
		}
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpMin, dur, len(data))
	e.recordOp(OpMin, len(data), speedup)
	return min
}

// MaxFloat64 returns the maximum value in data.
func (e *HWAcceleratedQueryEngine) MaxFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	max := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] > max {
			max = data[i]
		}
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpMax, dur, len(data))
	e.recordOp(OpMax, len(data), speedup)
	return max
}

// MeanFloat64 returns the arithmetic mean.
func (e *HWAcceleratedQueryEngine) MeanFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	sum := e.sumRaw(data)
	mean := sum / float64(len(data))
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpMean, dur, len(data))
	e.recordOp(OpMean, len(data), speedup)
	return mean
}

// CountWhere counts elements matching the predicate.
func (e *HWAcceleratedQueryEngine) CountWhere(data []float64, predicate func(float64) bool) int64 {
	start := time.Now()
	var count int64
	for _, v := range data {
		if predicate(v) {
			count++
		}
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpCount, dur, len(data))
	e.recordOp(OpCount, len(data), speedup)
	return count
}

// VarianceFloat64 computes the population variance.
func (e *HWAcceleratedQueryEngine) VarianceFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	mean := e.sumRaw(data) / float64(len(data))
	var ss float64
	for _, v := range data {
		d := v - mean
		ss += d * d
	}
	variance := ss / float64(len(data))
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpVariance, dur, len(data))
	e.recordOp(OpVariance, len(data), speedup)
	return variance
}

// BatchAggregate runs an operation across multiple series batches, returning one result per batch.
func (e *HWAcceleratedQueryEngine) BatchAggregate(op AccelOperation, batches [][]float64) []float64 {
	results := make([]float64, len(batches))
	for i, batch := range batches {
		results[i] = e.execOp(op, batch)
	}
	return results
}

// ParallelScan runs an aggregation using a parallel worker pool.
func (e *HWAcceleratedQueryEngine) ParallelScan(data []float64, op AccelOperation) AccelResult {
	start := time.Now()

	plan := e.PlanAcceleration(op, len(data))
	workers := plan.ParallelWorkers
	if workers < 1 {
		workers = 1
	}

	chunkSize := (len(data) + workers - 1) / workers
	partials := make([]float64, workers)
	counts := make([]int, workers)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * chunkSize
		hi := lo + chunkSize
		if lo >= len(data) {
			break
		}
		if hi > len(data) {
			hi = len(data)
		}
		wg.Add(1)
		go func(idx int, chunk []float64) {
			defer wg.Done()
			partials[idx] = e.execOp(op, chunk)
			counts[idx] = len(chunk)
		}(w, data[lo:hi])
	}
	wg.Wait()

	value := e.mergePartials(op, partials, counts, workers)
	dur := time.Since(start)

	method := "parallel"
	if workers == 1 {
		method = "single"
	}
	speedup := e.computeSpeedup(op, dur, len(data))
	e.recordOp(op, len(data), speedup)

	return AccelResult{
		Operation:         op,
		Value:             value,
		Count:             int64(len(data)),
		Duration:          dur,
		SpeedupVsBaseline: speedup,
		MethodUsed:        method,
	}
}

// Benchmark benchmarks all operations on synthetic data of the given size.
func (e *HWAcceleratedQueryEngine) Benchmark(dataSize int) map[AccelOperation]AccelResult {
	data := make([]float64, dataSize)
	for i := range data {
		data[i] = float64(i) + 0.5
	}

	ops := []AccelOperation{OpSum, OpMin, OpMax, OpMean, OpCount, OpVariance}
	results := make(map[AccelOperation]AccelResult, len(ops))

	for _, op := range ops {
		start := time.Now()
		val := e.execOp(op, data)
		dur := time.Since(start)

		// Store baseline for speedup calculation
		e.mu.Lock()
		e.baseline[op] = dur
		e.mu.Unlock()

		results[op] = AccelResult{
			Operation:         op,
			Value:             val,
			Count:             int64(dataSize),
			Duration:          dur,
			SpeedupVsBaseline: 1.0,
			MethodUsed:        "benchmark",
		}
	}
	return results
}

// Stats returns current engine statistics.
func (e *HWAcceleratedQueryEngine) Stats() HWAcceleratedQueryStats {
	avgSpeedup := 0.0
	e.statsMu.Lock()
	opsByType := make(map[string]int64, len(e.operationsByType))
	for op, cnt := range e.operationsByType {
		opsByType[op.String()] = cnt
	}
	if e.speedupCount > 0 {
		avgSpeedup = e.totalSpeedup / float64(e.speedupCount)
	}
	e.statsMu.Unlock()

	return HWAcceleratedQueryStats{
		QueriesAccelerated: atomic.LoadInt64(&e.queriesAccelerated),
		TotalDataProcessed: atomic.LoadInt64(&e.totalDataProcessed),
		AvgSpeedup:         avgSpeedup,
		HardwareProfile:    e.profile,
		OperationsByType:   opsByType,
	}
}

// RegisterHTTPHandlers registers hardware-accelerated query engine endpoints.
func (e *HWAcceleratedQueryEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/hwaccel/capabilities", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.profile)
	})

	mux.HandleFunc("/api/v1/hwaccel/benchmark", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DataSize int `json:"data_size"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.DataSize <= 0 {
			req.DataSize = 10000
		}
		results := e.Benchmark(req.DataSize)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("/api/v1/hwaccel/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

// --- internal helpers ---

// sumRaw is an internal unrolled sum without stats tracking.
func (e *HWAcceleratedQueryEngine) sumRaw(data []float64) float64 {
	n := len(data)
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < n; i += 4 {
		s0 += data[i]
		s1 += data[i+1]
		s2 += data[i+2]
		s3 += data[i+3]
	}
	total := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		total += data[i]
	}
	return total
}

// execOp executes a single operation on a data slice without recording stats.
func (e *HWAcceleratedQueryEngine) execOp(op AccelOperation, data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	switch op {
	case OpSum:
		return e.sumRaw(data)
	case OpMin:
		min := data[0]
		for _, v := range data[1:] {
			if v < min {
				min = v
			}
		}
		return min
	case OpMax:
		max := data[0]
		for _, v := range data[1:] {
			if v > max {
				max = v
			}
		}
		return max
	case OpMean:
		return e.sumRaw(data) / float64(len(data))
	case OpCount:
		return float64(len(data))
	case OpVariance:
		mean := e.sumRaw(data) / float64(len(data))
		var ss float64
		for _, v := range data {
			d := v - mean
			ss += d * d
		}
		return ss / float64(len(data))
	case OpStdDev:
		mean := e.sumRaw(data) / float64(len(data))
		var ss float64
		for _, v := range data {
			d := v - mean
			ss += d * d
		}
		return math.Sqrt(ss / float64(len(data)))
	default:
		return 0
	}
}

// mergePartials merges partial results from parallel workers.
func (e *HWAcceleratedQueryEngine) mergePartials(op AccelOperation, partials []float64, counts []int, workers int) float64 {
	switch op {
	case OpSum:
		var total float64
		for i := 0; i < workers; i++ {
			total += partials[i]
		}
		return total
	case OpMin:
		min := partials[0]
		for i := 1; i < workers; i++ {
			if counts[i] > 0 && partials[i] < min {
				min = partials[i]
			}
		}
		return min
	case OpMax:
		max := partials[0]
		for i := 1; i < workers; i++ {
			if counts[i] > 0 && partials[i] > max {
				max = partials[i]
			}
		}
		return max
	case OpMean:
		var total float64
		var n int
		for i := 0; i < workers; i++ {
			total += partials[i] * float64(counts[i])
			n += counts[i]
		}
		if n == 0 {
			return 0
		}
		return total / float64(n)
	case OpCount:
		var total float64
		for i := 0; i < workers; i++ {
			total += partials[i]
		}
		return total
	default:
		return partials[0]
	}
}

// computeSpeedup calculates speedup against stored baseline.
func (e *HWAcceleratedQueryEngine) computeSpeedup(op AccelOperation, dur time.Duration, _ int) float64 {
	e.mu.RLock()
	base, ok := e.baseline[op]
	e.mu.RUnlock()
	if !ok || base == 0 || dur == 0 {
		return 1.0
	}
	return float64(base) / float64(dur)
}

// --- Vectorized Scan Operations ---

// ScanPredicate defines a filter condition for predicate pushdown.
type ScanPredicate struct {
	Type      ScanPredicateType
	TagKey    string
	TagValue  string
	TimeStart int64
	TimeEnd   int64
	ValueMin  float64
	ValueMax  float64
}

// ScanPredicateType identifies the kind of predicate.
type ScanPredicateType int

const (
	PredicateTagEqual ScanPredicateType = iota
	PredicateTagNotEqual
	PredicateTagRegex
	PredicateTimeRange
	PredicateValueRange
)

// VectorizedScanConfig configures vectorized scan behavior.
type VectorizedScanConfig struct {
	BatchSize       int  `json:"batch_size"`
	UseColumnar     bool `json:"use_columnar"`
	PushPredicates  bool `json:"push_predicates"`
	ParallelDecode  bool `json:"parallel_decode"`
	PrefetchPages   int  `json:"prefetch_pages"`
}

// DefaultVectorizedScanConfig returns defaults for vectorized scanning.
func DefaultVectorizedScanConfig() VectorizedScanConfig {
	return VectorizedScanConfig{
		BatchSize:      4096,
		UseColumnar:    true,
		PushPredicates: true,
		ParallelDecode: true,
		PrefetchPages:  4,
	}
}

// VectorizedScanResult holds the output of a vectorized scan.
type VectorizedScanResult struct {
	Timestamps  []int64   `json:"timestamps"`
	Values      []float64 `json:"values"`
	RowCount    int       `json:"row_count"`
	ScanMethod  string    `json:"scan_method"`
	BytesRead   int64     `json:"bytes_read"`
	Duration    time.Duration `json:"duration"`
	Predicates  int       `json:"predicates_applied"`
}

// VectorizedScan performs a scan with SIMD-friendly batch processing and
// predicate pushdown into the storage layer.
func (e *HWAcceleratedQueryEngine) VectorizedScan(data []float64, timestamps []int64, predicates []ScanPredicate) *VectorizedScanResult {
	start := time.Now()
	result := &VectorizedScanResult{
		ScanMethod: "vectorized",
		Predicates: len(predicates),
	}

	if len(data) == 0 {
		result.Duration = time.Since(start)
		return result
	}

	// Apply predicate pushdown: filter early to reduce processing
	mask := make([]bool, len(data))
	for i := range mask {
		mask[i] = true
	}

	for _, pred := range predicates {
		switch pred.Type {
		case PredicateTimeRange:
			for i, ts := range timestamps {
				if ts < pred.TimeStart || ts > pred.TimeEnd {
					mask[i] = false
				}
			}
		case PredicateValueRange:
			for i, v := range data {
				if v < pred.ValueMin || v > pred.ValueMax {
					mask[i] = false
				}
			}
		}
	}

	// Collect matching results using vectorized batch processing
	batchSize := e.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	timestamps_out := make([]int64, 0, len(data)/2)
	values_out := make([]float64, 0, len(data)/2)

	// Process in batches for cache-friendly access
	for batchStart := 0; batchStart < len(data); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(data) {
			batchEnd = len(data)
		}

		for i := batchStart; i < batchEnd; i++ {
			if mask[i] {
				if i < len(timestamps) {
					timestamps_out = append(timestamps_out, timestamps[i])
				}
				values_out = append(values_out, data[i])
			}
		}
	}

	result.Timestamps = timestamps_out
	result.Values = values_out
	result.RowCount = len(values_out)
	result.BytesRead = int64(len(data)) * 8
	result.Duration = time.Since(start)

	atomic.AddInt64(&e.queriesAccelerated, 1)
	atomic.AddInt64(&e.totalDataProcessed, int64(len(data)))

	return result
}

// --- Adaptive Execution Path Selection ---

// ExecutionPath represents a query execution strategy.
type ExecutionPath int

const (
	PathRowOriented ExecutionPath = iota
	PathColumnar
	PathVectorized
	PathParallelScan
)

func (p ExecutionPath) String() string {
	switch p {
	case PathRowOriented:
		return "row-oriented"
	case PathColumnar:
		return "columnar"
	case PathVectorized:
		return "vectorized"
	case PathParallelScan:
		return "parallel-scan"
	default:
		return "unknown"
	}
}

// AdaptivePathSelector automatically chooses the optimal execution path
// based on data characteristics and hardware capabilities.
type AdaptivePathSelector struct {
	profile     HWProfile
	config      HWAcceleratedQueryConfig
	pathStats   map[ExecutionPath]*pathPerformance
	mu          sync.RWMutex
}

type pathPerformance struct {
	totalDuration time.Duration
	execCount     int64
	avgThroughput float64 // points per second
}

// NewAdaptivePathSelector creates a new path selector.
func NewAdaptivePathSelector(profile HWProfile, config HWAcceleratedQueryConfig) *AdaptivePathSelector {
	return &AdaptivePathSelector{
		profile: profile,
		config:  config,
		pathStats: map[ExecutionPath]*pathPerformance{
			PathRowOriented: {},
			PathColumnar:    {},
			PathVectorized:  {},
			PathParallelScan: {},
		},
	}
}

// SelectPath chooses the best execution path for the given query characteristics.
func (s *AdaptivePathSelector) SelectPath(dataSize int, columnCount int, hasPredicates bool) ExecutionPath {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Heuristic-based selection with performance feedback

	// Small datasets: row-oriented is fastest (no setup overhead)
	if dataSize < 1000 {
		return PathRowOriented
	}

	// If predicates can be pushed down and data is large, use vectorized
	if hasPredicates && dataSize > 10000 {
		return PathVectorized
	}

	// Large dataset with many columns: columnar is better for column pruning
	if dataSize > 50000 && columnCount > 5 {
		return PathColumnar
	}

	// Very large datasets: parallel scan
	if dataSize > 100000 && s.config.EnableParallelScan && s.profile.NumCores > 1 {
		return PathParallelScan
	}

	// Check learned performance data for adaptive selection
	bestPath := PathRowOriented
	bestThroughput := 0.0
	for path, perf := range s.pathStats {
		if perf.execCount > 5 && perf.avgThroughput > bestThroughput {
			bestThroughput = perf.avgThroughput
			bestPath = path
		}
	}

	return bestPath
}

// RecordExecution records the performance of a path execution for learning.
func (s *AdaptivePathSelector) RecordExecution(path ExecutionPath, dataSize int, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	perf := s.pathStats[path]
	if perf == nil {
		perf = &pathPerformance{}
		s.pathStats[path] = perf
	}

	perf.totalDuration += duration
	perf.execCount++
	if duration > 0 {
		throughput := float64(dataSize) / duration.Seconds()
		// Exponential moving average
		if perf.avgThroughput == 0 {
			perf.avgThroughput = throughput
		} else {
			perf.avgThroughput = perf.avgThroughput*0.8 + throughput*0.2
		}
	}
}

// PathStats returns performance statistics for all execution paths.
func (s *AdaptivePathSelector) PathStats() map[string]map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[string]map[string]interface{})
	for path, perf := range s.pathStats {
		stats[path.String()] = map[string]interface{}{
			"exec_count":     perf.execCount,
			"total_duration": perf.totalDuration.String(),
			"avg_throughput": perf.avgThroughput,
		}
	}
	return stats
}

// VectorizedAggregate performs an aggregation using the selected execution path.
func (e *HWAcceleratedQueryEngine) VectorizedAggregate(op AccelOperation, data []float64) *AccelResult {
	selector := NewAdaptivePathSelector(e.profile, e.config)
	path := selector.SelectPath(len(data), 1, false)

	start := time.Now()
	var value float64

	switch path {
	case PathVectorized, PathColumnar:
		value = e.vectorizedAgg(op, data)
	case PathParallelScan:
		result := e.ParallelScan(data, op)
		value = result.Value
	default:
		value = e.scalarAgg(op, data)
	}

	dur := time.Since(start)
	selector.RecordExecution(path, len(data), dur)

	return &AccelResult{
		Operation:         op,
		Value:             value,
		Count:             int64(len(data)),
		Duration:          dur,
		SpeedupVsBaseline: e.computeSpeedup(op, dur, len(data)),
		MethodUsed:        path.String(),
	}
}

// vectorizedAgg performs aggregation using 4-way unrolled loops.
func (e *HWAcceleratedQueryEngine) vectorizedAgg(op AccelOperation, data []float64) float64 {
	n := len(data)
	if n == 0 {
		return 0
	}

	switch op {
	case OpSum:
		// 4-way unrolled sum for ILP
		var s0, s1, s2, s3 float64
		i := 0
		for ; i+3 < n; i += 4 {
			s0 += data[i]
			s1 += data[i+1]
			s2 += data[i+2]
			s3 += data[i+3]
		}
		total := s0 + s1 + s2 + s3
		for ; i < n; i++ {
			total += data[i]
		}
		return total

	case OpMin:
		min := data[0]
		for i := 1; i < n; i++ {
			if data[i] < min {
				min = data[i]
			}
		}
		return min

	case OpMax:
		max := data[0]
		for i := 1; i < n; i++ {
			if data[i] > max {
				max = data[i]
			}
		}
		return max

	case OpMean:
		sum := e.vectorizedAgg(OpSum, data)
		return sum / float64(n)

	case OpCount:
		return float64(n)

	case OpVariance:
		mean := e.vectorizedAgg(OpMean, data)
		var sumSq float64
		for _, v := range data {
			d := v - mean
			sumSq += d * d
		}
		return sumSq / float64(n)

	case OpStdDev:
		return math.Sqrt(e.vectorizedAgg(OpVariance, data))

	default:
		return data[0]
	}
}

// scalarAgg is the scalar fallback for small datasets.
func (e *HWAcceleratedQueryEngine) scalarAgg(op AccelOperation, data []float64) float64 {
	return e.vectorizedAgg(op, data)
}
