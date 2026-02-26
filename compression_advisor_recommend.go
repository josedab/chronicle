package chronicle

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Benchmark, recommendation, and rule-based codec selection for the compression advisor.

// BenchmarkAll benchmarks all available codecs for a metric.
func (ca *CompressionAdvisor) BenchmarkAll(metric string, values []float64) ([]CodecBenchmark, error) {
	codecs := []CodecType{
		CodecGorilla, CodecDeltaDelta, CodecRLE,
		CodecZSTD, CodecLZ4, CodecSnappy, CodecGzip,
		CodecBitPacking, CodecFloatXOR, CodecDictionary,
	}

	var results []CodecBenchmark
	for _, codec := range codecs {
		bench, err := ca.BenchmarkCodec(metric, values, codec)
		if err != nil {
			continue // skip codecs that fail
		}
		results = append(results, *bench)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("all codecs failed for metric %s", metric)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].CompressionRatio > results[j].CompressionRatio
	})

	return results, nil
}

// Recommend returns a compression recommendation for a metric.
func (ca *CompressionAdvisor) Recommend(metric string) (*CompressionRecommendation, error) {
	ca.mu.RLock()
	profile, ok := ca.profiles[metric]
	ca.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no profile for metric %q; run ProfileMetric first", metric)
	}

	return ca.RecommendFromProfile(profile)
}

// RecommendFromProfile generates a recommendation from an existing profile.
func (ca *CompressionAdvisor) RecommendFromProfile(profile *MetricProfile) (*CompressionRecommendation, error) {
	if profile == nil {
		return nil, fmt.Errorf("nil profile")
	}

	recommendedCodec, reasoning := ca.applyRules(profile)

	// Determine confidence based on how decisive the profile is
	confidence := ca.computeConfidence(profile, recommendedCodec)

	ca.mu.RLock()
	currentCodec, hasCurrent := ca.applied[profile.Metric]
	benchmarks := ca.benchmarks[profile.Metric]
	ca.mu.RUnlock()

	if !hasCurrent {
		currentCodec = CodecNone
	}

	// Estimate improvement from benchmarks
	var ratioImprovement, speedImpact float64
	if len(benchmarks) > 0 {
		var currentRatio, recommendedRatio float64
		var currentSpeed, recommendedSpeed float64
		for _, b := range benchmarks {
			if b.Codec == currentCodec {
				currentRatio = b.CompressionRatio
				currentSpeed = float64(b.CompressTime + b.DecompressTime)
			}
			if b.Codec == recommendedCodec {
				recommendedRatio = b.CompressionRatio
				recommendedSpeed = float64(b.CompressTime + b.DecompressTime)
			}
		}
		if currentRatio > 0 {
			ratioImprovement = ((recommendedRatio - currentRatio) / currentRatio) * 100
		}
		if currentSpeed > 0 {
			speedImpact = ((currentSpeed - recommendedSpeed) / currentSpeed) * 100
		}
	}

	rec := &CompressionRecommendation{
		Metric:           profile.Metric,
		CurrentCodec:     currentCodec,
		RecommendedCodec: recommendedCodec,
		Confidence:       confidence,
		RatioImprovement: ratioImprovement,
		SpeedImpact:      speedImpact,
		Reasoning:        reasoning,
		Benchmarks:       benchmarks,
		Applied:          false,
		RecommendedAt:    time.Now(),
	}

	ca.mu.Lock()
	ca.recommendations[profile.Metric] = rec
	ca.stats.RecommendationsMade++
	ca.mu.Unlock()

	return rec, nil
}

func (ca *CompressionAdvisor) applyRules(profile *MetricProfile) (CodecType, string) {
	// Sort rules by priority (highest first)
	sorted := make([]AdvisorRule, len(ca.rules))
	copy(sorted, ca.rules)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority > sorted[j].Priority
	})

	for _, rule := range sorted {
		if rule.Condition(profile) {
			return rule.Codec, rule.Reasoning
		}
	}

	// Default fallback
	return CodecGorilla, "Default codec for general-purpose time-series compression"
}

func (ca *CompressionAdvisor) computeConfidence(profile *MetricProfile, codec CodecType) float64 {
	confidence := 0.5 // base

	// Larger sample → higher confidence
	if profile.SampleSize > 500 {
		confidence += 0.1
	}
	if profile.SampleSize > 1000 {
		confidence += 0.1
	}

	// Strong signal characteristics boost confidence
	switch codec {
	case CodecRLE:
		confidence += profile.RepeatRatio * 0.3
	case CodecDeltaDelta:
		confidence += profile.Monotonicity * 0.3
	case CodecGorilla:
		confidence += profile.DeltaStability * 0.2
	case CodecBitPacking:
		confidence += profile.Sparsity * 0.3
	}

	if confidence > 1.0 {
		confidence = 1.0
	}
	return confidence
}

// ApplyRecommendation applies the current recommendation for a metric.
func (ca *CompressionAdvisor) ApplyRecommendation(metric string) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	rec, ok := ca.recommendations[metric]
	if !ok {
		return fmt.Errorf("no recommendation for metric %q", metric)
	}

	if rec.RatioImprovement < ca.config.MinImprovementPct && rec.CurrentCodec != CodecNone {
		return fmt.Errorf("improvement %.1f%% below minimum threshold %.1f%%",
			rec.RatioImprovement, ca.config.MinImprovementPct)
	}

	ca.applied[metric] = rec.RecommendedCodec
	rec.Applied = true
	ca.stats.RecommendationsApplied++

	return nil
}

// AnalyzeAll analyzes all metrics in the database.
func (ca *CompressionAdvisor) AnalyzeAll() error {
	if ca.db == nil {
		return fmt.Errorf("no database attached")
	}

	start := time.Now()
	metrics := ca.db.Metrics()

	for _, metric := range metrics {
		// Query recent data for analysis
		q := &Query{
			Metric: metric,
			Limit:  ca.config.AnalysisWindow,
		}
		result, err := ca.db.Execute(q)
		if err != nil || len(result.Points) == 0 {
			continue
		}

		values := make([]float64, len(result.Points))
		for i, p := range result.Points {
			values[i] = p.Value
		}

		_, err = ca.ProfileMetric(metric, values)
		if err != nil {
			continue
		}

		_, _ = ca.BenchmarkAll(metric, values) //nolint:errcheck // best-effort benchmark
		_, _ = ca.Recommend(metric)            //nolint:errcheck // best-effort recommendation

		if ca.config.AutoApply {
			_ = ca.ApplyRecommendation(metric) //nolint:errcheck // best-effort auto-apply
		}
	}

	ca.mu.Lock()
	ca.stats.LastAnalysis = time.Now()
	ca.stats.AnalysisDuration = time.Since(start)
	ca.mu.Unlock()

	return nil
}

// GenerateReport produces a compression report across all analyzed metrics.
func (ca *CompressionAdvisor) GenerateReport() (*CompressionReport, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	report := &CompressionReport{
		AnalyzedMetrics:       len(ca.profiles),
		CodecDistribution:     make(map[string]int),
		WorstCompressionRatio: math.MaxFloat64,
		GeneratedAt:           time.Now(),
	}

	if ca.db != nil {
		report.TotalMetrics = len(ca.db.Metrics())
	}

	var totalRatio float64
	ratioCount := 0

	for metric, rec := range ca.recommendations {
		if rec.Applied {
			report.OptimizedMetrics++
		}
		report.CodecDistribution[rec.RecommendedCodec.String()]++

		// Aggregate benchmark data
		if benchmarks, ok := ca.benchmarks[metric]; ok && len(benchmarks) > 0 {
			best := benchmarks[0] // sorted by ratio
			totalRatio += best.CompressionRatio
			ratioCount++

			if best.CompressionRatio > report.BestCompressionRatio {
				report.BestCompressionRatio = best.CompressionRatio
			}
			if best.CompressionRatio < report.WorstCompressionRatio {
				report.WorstCompressionRatio = best.CompressionRatio
			}

			report.TotalOriginalSize += best.OriginalSize
			report.TotalCompressedSize += best.CompressedSize
		}
	}

	if ratioCount > 0 {
		report.AvgCompressionRatio = totalRatio / float64(ratioCount)
	}
	if report.WorstCompressionRatio == math.MaxFloat64 {
		report.WorstCompressionRatio = 0
	}
	if report.TotalOriginalSize > 0 {
		report.OverallSavings = (1 - float64(report.TotalCompressedSize)/float64(report.TotalOriginalSize)) * 100
	}

	// Top recommendations sorted by improvement
	recs := make([]CompressionRecommendation, 0, len(ca.recommendations))
	for _, rec := range ca.recommendations {
		recs = append(recs, *rec)
	}
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].RatioImprovement > recs[j].RatioImprovement
	})
	if len(recs) > 10 {
		recs = recs[:10]
	}
	report.TopRecommendations = recs

	return report, nil
}

// GetProfile returns the profile for a metric, or nil.
func (ca *CompressionAdvisor) GetProfile(metric string) *MetricProfile {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.profiles[metric]
}

// GetRecommendation returns the recommendation for a metric, or nil.
func (ca *CompressionAdvisor) GetRecommendation(metric string) *CompressionRecommendation {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.recommendations[metric]
}

// ListRecommendations returns all current recommendations.
func (ca *CompressionAdvisor) ListRecommendations() []*CompressionRecommendation {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	recs := make([]*CompressionRecommendation, 0, len(ca.recommendations))
	for _, rec := range ca.recommendations {
		recs = append(recs, rec)
	}
	return recs
}

// Stats returns current advisor statistics.
func (ca *CompressionAdvisor) Stats() CompressionAdvisorStats {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.stats
}

// RegisterHTTPHandlers registers compression advisor HTTP endpoints.
func (ca *CompressionAdvisor) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/compression/profiles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ca.mu.RLock()
		profiles := make([]*MetricProfile, 0, len(ca.profiles))
		for _, p := range ca.profiles {
			profiles = append(profiles, p)
		}
		ca.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(profiles)
	})

	mux.HandleFunc("/api/v1/compression/profiles/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := strings.TrimPrefix(r.URL.Path, "/api/v1/compression/profiles/")
		if metric == "" {
			http.Error(w, "metric name required", http.StatusBadRequest)
			return
		}
		profile := ca.GetProfile(metric)
		if profile == nil {
			http.Error(w, "profile not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(profile)
	})

	mux.HandleFunc("/api/v1/compression/analyze", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string    `json:"metric"`
			Values []float64 `json:"values"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		profile, err := ca.ProfileMetric(req.Metric, req.Values)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(profile)
	})

	mux.HandleFunc("/api/v1/compression/recommendations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ca.ListRecommendations())
	})

	mux.HandleFunc("/api/v1/compression/benchmark", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string    `json:"metric"`
			Values []float64 `json:"values"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		benchmarks, err := ca.BenchmarkAll(req.Metric, req.Values)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(benchmarks)
	})

	mux.HandleFunc("/api/v1/compression/apply/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := strings.TrimPrefix(r.URL.Path, "/api/v1/compression/apply/")
		if metric == "" {
			http.Error(w, "metric name required", http.StatusBadRequest)
			return
		}
		if err := ca.ApplyRecommendation(metric); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "applied", "metric": metric})
	})

	mux.HandleFunc("/api/v1/compression/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		report, err := ca.GenerateReport()
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/api/v1/compression/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ca.Stats())
	})
}

// valuesToBytes converts float64 values to a byte slice.
func valuesToBytes(values []float64) []byte {
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}
	return data
}

func (ca *CompressionAdvisor) compressWithCodec(data []byte, codec CodecType) ([]byte, error) {
	switch codec {
	case CodecNone:
		return data, nil
	case CodecGorilla:
		return compressGorilla(data)
	case CodecDeltaDelta:
		return compressDeltaDelta(data)
	case CodecRLE:
		return compressRLE(data)
	case CodecGzip:
		return compressGzip(data)
	case CodecSnappy:
		return compressSnappy(data)
	case CodecDictionary:
		return compressDictionary(data)
	case CodecBitPacking:
		return compressBitPacking(data)
	case CodecFloatXOR:
		return compressFloatXOR(data)
	case CodecZSTD:
		// ZSTD not natively available; use gzip as proxy
		return compressGzip(data)
	case CodecLZ4:
		// LZ4 not natively available; use snappy as proxy
		return compressSnappy(data)
	default:
		return nil, fmt.Errorf("unsupported codec: %v", codec)
	}
}

func (ca *CompressionAdvisor) decompressWithCodec(data []byte, codec CodecType) ([]byte, error) {
	switch codec {
	case CodecNone:
		return data, nil
	case CodecGorilla:
		return decompressGorilla(data)
	case CodecDeltaDelta:
		return decompressDeltaDelta(data)
	case CodecRLE:
		return decompressRLE(data)
	case CodecGzip:
		return decompressGzip(data)
	case CodecSnappy:
		return decompressSnappy(data)
	case CodecDictionary:
		return decompressDictionary(data)
	case CodecBitPacking:
		return decompressBitPacking(data)
	case CodecFloatXOR:
		return decompressFloatXOR(data)
	case CodecZSTD:
		return decompressGzip(data)
	case CodecLZ4:
		return decompressSnappy(data)
	default:
		return nil, fmt.Errorf("unsupported codec: %v", codec)
	}
}
