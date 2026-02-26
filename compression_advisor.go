package chronicle

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// CompressionAdvisorConfig configures the ML-driven compression advisor.
type CompressionAdvisorConfig struct {
	Enabled            bool
	AnalysisWindow     int // number of values to analyze
	ReanalysisInterval time.Duration
	BenchmarkSamples   int
	AutoApply          bool
	MinImprovementPct  float64 // minimum improvement to recommend change
}

// DefaultCompressionAdvisorConfig returns sensible defaults for the compression advisor.
func DefaultCompressionAdvisorConfig() CompressionAdvisorConfig {
	return CompressionAdvisorConfig{
		Enabled:            true,
		AnalysisWindow:     1000,
		ReanalysisInterval: time.Hour,
		BenchmarkSamples:   100,
		AutoApply:          false,
		MinImprovementPct:  10.0,
	}
}

// MetricProfile holds analyzed characteristics of a metric.
type MetricProfile struct {
	Metric         string    `json:"metric"`
	SampleSize     int       `json:"sample_size"`
	Entropy        float64   `json:"entropy"`
	DeltaStability float64   `json:"delta_stability"`
	Monotonicity   float64   `json:"monotonicity"`
	RepeatRatio    float64   `json:"repeat_ratio"`
	ValueRange     float64   `json:"value_range"`
	MeanDelta      float64   `json:"mean_delta"`
	StddevDelta    float64   `json:"stddev_delta"`
	Sparsity       float64   `json:"sparsity"`
	Cardinality    int       `json:"cardinality"`
	IsInteger      bool      `json:"is_integer"`
	ProfiledAt     time.Time `json:"profiled_at"`
}

// CodecBenchmark holds benchmark results for a codec on a specific metric.
type CodecBenchmark struct {
	Codec            CodecType     `json:"codec"`
	Metric           string        `json:"metric"`
	CompressionRatio float64       `json:"compression_ratio"`
	CompressTime     time.Duration `json:"compress_time"`
	DecompressTime   time.Duration `json:"decompress_time"`
	CompressedSize   int64         `json:"compressed_size"`
	OriginalSize     int64         `json:"original_size"`
	CPUOverhead      float64       `json:"cpu_overhead"`
	Accuracy         float64       `json:"accuracy"`
	BenchmarkedAt    time.Time     `json:"benchmarked_at"`
}

// CompressionRecommendation is the advisor's recommendation for a metric.
type CompressionRecommendation struct {
	Metric           string           `json:"metric"`
	CurrentCodec     CodecType        `json:"current_codec"`
	RecommendedCodec CodecType        `json:"recommended_codec"`
	Confidence       float64          `json:"confidence"`
	RatioImprovement float64          `json:"ratio_improvement"`
	SpeedImpact      float64          `json:"speed_impact"`
	Reasoning        string           `json:"reasoning"`
	Benchmarks       []CodecBenchmark `json:"benchmarks"`
	Applied          bool             `json:"applied"`
	RecommendedAt    time.Time        `json:"recommended_at"`
}

// AdvisorRule is a heuristic rule for codec selection.
type AdvisorRule struct {
	Name      string
	Condition func(profile *MetricProfile) bool
	Codec     CodecType
	Priority  int
	Reasoning string
}

// CompressionReport summarizes compression across all metrics.
type CompressionReport struct {
	TotalMetrics          int                         `json:"total_metrics"`
	AnalyzedMetrics       int                         `json:"analyzed_metrics"`
	OptimizedMetrics      int                         `json:"optimized_metrics"`
	AvgCompressionRatio   float64                     `json:"avg_compression_ratio"`
	BestCompressionRatio  float64                     `json:"best_compression_ratio"`
	WorstCompressionRatio float64                     `json:"worst_compression_ratio"`
	TotalOriginalSize     int64                       `json:"total_original_size"`
	TotalCompressedSize   int64                       `json:"total_compressed_size"`
	OverallSavings        float64                     `json:"overall_savings"`
	CodecDistribution     map[string]int              `json:"codec_distribution"`
	TopRecommendations    []CompressionRecommendation `json:"top_recommendations"`
	GeneratedAt           time.Time                   `json:"generated_at"`
}

// CompressionAdvisorStats holds runtime statistics for the advisor.
type CompressionAdvisorStats struct {
	MetricsProfiled        int           `json:"metrics_profiled"`
	BenchmarksRun          int64         `json:"benchmarks_run"`
	RecommendationsMade    int64         `json:"recommendations_made"`
	RecommendationsApplied int64         `json:"recommendations_applied"`
	AvgImprovement         float64       `json:"avg_improvement"`
	LastAnalysis           time.Time     `json:"last_analysis"`
	AnalysisDuration       time.Duration `json:"analysis_duration"`
}

// CompressionAdvisor provides ML-driven per-metric codec recommendation with
// auto-tuning and benchmarking.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type CompressionAdvisor struct {
	db              *DB
	config          CompressionAdvisorConfig
	mu              sync.RWMutex
	profiles        map[string]*MetricProfile
	benchmarks      map[string][]CodecBenchmark
	recommendations map[string]*CompressionRecommendation
	rules           []AdvisorRule
	applied         map[string]CodecType
	stopCh          chan struct{}
	running         bool
	stats           CompressionAdvisorStats
}

// NewCompressionAdvisor creates a new compression advisor.
func NewCompressionAdvisor(db *DB, cfg CompressionAdvisorConfig) *CompressionAdvisor {
	ca := &CompressionAdvisor{
		db:              db,
		config:          cfg,
		profiles:        make(map[string]*MetricProfile),
		benchmarks:      make(map[string][]CodecBenchmark),
		recommendations: make(map[string]*CompressionRecommendation),
		applied:         make(map[string]CodecType),
		stopCh:          make(chan struct{}),
	}
	ca.initDefaultRules()
	return ca
}

// Start begins the background reanalysis loop.
func (ca *CompressionAdvisor) Start() {
	ca.mu.Lock()
	if ca.running {
		ca.mu.Unlock()
		return
	}
	ca.running = true
	ca.mu.Unlock()

	go ca.runLoop()
}

// Stop terminates the background reanalysis loop.
func (ca *CompressionAdvisor) Stop() {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if !ca.running {
		return
	}
	ca.running = false
	close(ca.stopCh)
}

func (ca *CompressionAdvisor) runLoop() {
	ticker := time.NewTicker(ca.config.ReanalysisInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ca.stopCh:
			return
		case <-ticker.C:
			if ca.config.Enabled {
				_ = ca.AnalyzeAll() //nolint:errcheck // best-effort background analysis
			}
		}
	}
}

func (ca *CompressionAdvisor) initDefaultRules() {
	ca.rules = []AdvisorRule{
		{
			Name:      "high_repeat_rle",
			Condition: func(p *MetricProfile) bool { return p.RepeatRatio > 0.5 },
			Codec:     CodecRLE,
			Priority:  100,
			Reasoning: "High repeat ratio favors run-length encoding",
		},
		{
			Name: "monotonic_integer_delta",
			Condition: func(p *MetricProfile) bool {
				return p.Monotonicity > 0.8 && p.IsInteger
			},
			Codec:     CodecDeltaDelta,
			Priority:  90,
			Reasoning: "Monotonic integer data compresses well with delta-of-delta encoding",
		},
		{
			Name: "low_entropy_stable_delta",
			Condition: func(p *MetricProfile) bool {
				return p.Entropy < 0.3 && p.DeltaStability > 0.7
			},
			Codec:     CodecGorilla,
			Priority:  80,
			Reasoning: "Low entropy with stable deltas is ideal for Gorilla XOR compression",
		},
		{
			Name: "high_cardinality_dictionary",
			Condition: func(p *MetricProfile) bool {
				return p.Cardinality > 100 && !p.IsInteger
			},
			Codec:     CodecDictionary,
			Priority:  70,
			Reasoning: "High cardinality non-integer data benefits from dictionary encoding",
		},
		{
			Name:      "sparse_bitpacking",
			Condition: func(p *MetricProfile) bool { return p.Sparsity > 0.7 },
			Codec:     CodecBitPacking,
			Priority:  60,
			Reasoning: "Sparse data with many zeros compresses well with bit-packing",
		},
		{
			Name: "high_entropy_zstd",
			Condition: func(p *MetricProfile) bool {
				return p.Entropy > 0.8 && p.ValueRange > 1000
			},
			Codec:     CodecZSTD,
			Priority:  50,
			Reasoning: "High entropy data with large value range benefits from ZSTD compression",
		},
	}
}

// ProfileMetric analyzes the characteristics of a metric's values.
func (ca *CompressionAdvisor) ProfileMetric(metric string, values []float64) (*MetricProfile, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("no values to profile")
	}

	profile := &MetricProfile{
		Metric:     metric,
		SampleSize: len(values),
		ProfiledAt: time.Now(),
	}

	profile.Entropy = ca.computeEntropy(values)
	profile.Monotonicity = ca.computeMonotonicity(values)
	profile.DeltaStability = ca.computeDeltaStability(values)

	// Repeat ratio
	if len(values) > 1 {
		repeats := 0
		for i := 1; i < len(values); i++ {
			if values[i] == values[i-1] {
				repeats++
			}
		}
		profile.RepeatRatio = float64(repeats) / float64(len(values)-1)
	}

	// Value range
	minVal, maxVal := values[0], values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	profile.ValueRange = maxVal - minVal

	// Mean and stddev of deltas
	if len(values) > 1 {
		deltas := make([]float64, len(values)-1)
		for i := 1; i < len(values); i++ {
			deltas[i-1] = values[i] - values[i-1]
		}
		var sum float64
		for _, d := range deltas {
			sum += d
		}
		profile.MeanDelta = sum / float64(len(deltas))

		var variance float64
		for _, d := range deltas {
			diff := d - profile.MeanDelta
			variance += diff * diff
		}
		variance /= float64(len(deltas))
		profile.StddevDelta = math.Sqrt(variance)
	}

	// Sparsity
	zeros := 0
	for _, v := range values {
		if v == 0 {
			zeros++
		}
	}
	profile.Sparsity = float64(zeros) / float64(len(values))

	// Cardinality
	unique := make(map[float64]struct{})
	for _, v := range values {
		unique[v] = struct{}{}
	}
	profile.Cardinality = len(unique)

	// IsInteger check
	profile.IsInteger = true
	for _, v := range values {
		if v != math.Trunc(v) {
			profile.IsInteger = false
			break
		}
	}

	ca.mu.Lock()
	ca.profiles[metric] = profile
	ca.stats.MetricsProfiled = len(ca.profiles)
	ca.mu.Unlock()

	return profile, nil
}

func (ca *CompressionAdvisor) computeEntropy(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// Bin values into buckets for entropy estimation
	counts := make(map[float64]int)
	for _, v := range values {
		counts[v]++
	}

	n := float64(len(values))
	entropy := 0.0
	for _, count := range counts {
		p := float64(count) / n
		if p > 0 {
			entropy -= p * math.Log2(p)
		}
	}

	// Normalize to 0-1 range
	maxEntropy := math.Log2(n)
	if maxEntropy == 0 {
		return 0
	}
	normalized := entropy / maxEntropy
	if normalized > 1 {
		normalized = 1
	}
	return normalized
}

func (ca *CompressionAdvisor) computeMonotonicity(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	increasing := 0
	decreasing := 0
	for i := 1; i < len(values); i++ {
		if values[i] > values[i-1] {
			increasing++
		} else if values[i] < values[i-1] {
			decreasing++
		}
	}

	total := len(values) - 1
	if total == 0 {
		return 0
	}

	incRatio := float64(increasing) / float64(total)
	decRatio := float64(decreasing) / float64(total)

	// Return the dominant direction
	if incRatio > decRatio {
		return incRatio
	}
	return decRatio
}

func (ca *CompressionAdvisor) computeDeltaStability(values []float64) float64 {
	if len(values) < 3 {
		return 0
	}

	deltas := make([]float64, len(values)-1)
	for i := 1; i < len(values); i++ {
		deltas[i-1] = values[i] - values[i-1]
	}

	// Compute mean of deltas
	var sum float64
	for _, d := range deltas {
		sum += d
	}
	mean := sum / float64(len(deltas))

	// Compute coefficient of variation (inverted to 0-1 stability)
	var variance float64
	for _, d := range deltas {
		diff := d - mean
		variance += diff * diff
	}
	variance /= float64(len(deltas))
	stddev := math.Sqrt(variance)

	if mean == 0 {
		if stddev == 0 {
			return 1.0 // All deltas are zero → perfectly stable
		}
		return 0
	}

	cv := stddev / math.Abs(mean)
	// Convert CV to stability: lower CV = higher stability
	stability := 1.0 / (1.0 + cv)
	return stability
}

// BenchmarkCodec benchmarks a specific codec against a set of values.
func (ca *CompressionAdvisor) BenchmarkCodec(metric string, values []float64, codec CodecType) (*CodecBenchmark, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("no values to benchmark")
	}

	// Encode values to bytes
	data := valuesToBytes(values)
	originalSize := int64(len(data))

	// Benchmark compression
	compressStart := time.Now()
	compressed, err := ca.compressWithCodec(data, codec)
	compressTime := time.Since(compressStart)
	if err != nil {
		return nil, fmt.Errorf("compression failed for %s: %w", codec, err)
	}

	compressedSize := int64(len(compressed))

	// Benchmark decompression
	decompressStart := time.Now()
	decompressed, err := ca.decompressWithCodec(compressed, codec)
	decompressTime := time.Since(decompressStart)
	if err != nil {
		return nil, fmt.Errorf("decompression failed for %s: %w", codec, err)
	}

	// Check accuracy
	accuracy := 1.0
	if len(decompressed) != len(data) {
		accuracy = float64(len(decompressed)) / float64(len(data))
	}

	var ratio float64
	if compressedSize > 0 {
		ratio = float64(originalSize) / float64(compressedSize)
	}

	// Estimate CPU overhead relative to no-compression baseline
	baselineTime := time.Duration(len(data)) // trivial baseline
	cpuOverhead := float64(compressTime+decompressTime) / float64(baselineTime)

	result := &CodecBenchmark{
		Codec:            codec,
		Metric:           metric,
		CompressionRatio: ratio,
		CompressTime:     compressTime,
		DecompressTime:   decompressTime,
		CompressedSize:   compressedSize,
		OriginalSize:     originalSize,
		CPUOverhead:      cpuOverhead,
		Accuracy:         accuracy,
		BenchmarkedAt:    time.Now(),
	}

	ca.mu.Lock()
	ca.benchmarks[metric] = append(ca.benchmarks[metric], *result)
	ca.stats.BenchmarksRun++
	ca.mu.Unlock()

	return result, nil
}
