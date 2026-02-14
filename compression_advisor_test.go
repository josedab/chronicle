package chronicle

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDefaultCompressionAdvisorConfig(t *testing.T) {
	cfg := DefaultCompressionAdvisorConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.AnalysisWindow != 1000 {
		t.Errorf("expected AnalysisWindow=1000, got %d", cfg.AnalysisWindow)
	}
	if cfg.MinImprovementPct != 10.0 {
		t.Errorf("expected MinImprovementPct=10.0, got %f", cfg.MinImprovementPct)
	}
	if cfg.BenchmarkSamples != 100 {
		t.Errorf("expected BenchmarkSamples=100, got %d", cfg.BenchmarkSamples)
	}
	if cfg.AutoApply {
		t.Error("expected AutoApply to be false")
	}
}

func TestNewCompressionAdvisor(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	if ca == nil {
		t.Fatal("expected non-nil advisor")
	}
	if len(ca.rules) == 0 {
		t.Error("expected default rules to be initialized")
	}
}

func TestProfileMetricEmpty(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	_, err := ca.ProfileMetric("test", nil)
	if err == nil {
		t.Fatal("expected error for empty values")
	}
}

func TestProfileMetricBasic(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	profile, err := ca.ProfileMetric("monotonic", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if profile.Metric != "monotonic" {
		t.Errorf("expected metric 'monotonic', got %q", profile.Metric)
	}
	if profile.SampleSize != 10 {
		t.Errorf("expected sample size 10, got %d", profile.SampleSize)
	}
	if profile.Monotonicity < 0.9 {
		t.Errorf("expected high monotonicity for sequential data, got %f", profile.Monotonicity)
	}
	if !profile.IsInteger {
		t.Error("expected IsInteger=true for whole numbers")
	}
	if profile.ValueRange != 9 {
		t.Errorf("expected value range 9, got %f", profile.ValueRange)
	}
	if profile.Cardinality != 10 {
		t.Errorf("expected cardinality 10, got %d", profile.Cardinality)
	}
}

func TestProfileMetricRepeatRatio(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	values := []float64{5, 5, 5, 5, 5, 5, 5, 5, 5, 5}
	profile, err := ca.ProfileMetric("constant", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if profile.RepeatRatio != 1.0 {
		t.Errorf("expected repeat ratio 1.0, got %f", profile.RepeatRatio)
	}
	if profile.Cardinality != 1 {
		t.Errorf("expected cardinality 1, got %d", profile.Cardinality)
	}
	if profile.ValueRange != 0 {
		t.Errorf("expected value range 0, got %f", profile.ValueRange)
	}
}

func TestProfileMetricSparsity(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	values := []float64{0, 0, 0, 0, 0, 0, 0, 1, 0, 0}
	profile, err := ca.ProfileMetric("sparse", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if profile.Sparsity != 0.9 {
		t.Errorf("expected sparsity 0.9, got %f", profile.Sparsity)
	}
}

func TestComputeEntropyUniform(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	// All same value → zero entropy
	entropy := ca.computeEntropy([]float64{1, 1, 1, 1})
	if entropy != 0 {
		t.Errorf("expected zero entropy for constant values, got %f", entropy)
	}
}

func TestComputeEntropyHigh(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	// All different values → high entropy
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i)
	}
	entropy := ca.computeEntropy(values)
	if entropy < 0.5 {
		t.Errorf("expected high entropy for all-unique values, got %f", entropy)
	}
}

func TestComputeMonotonicity(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	// Strictly increasing
	mono := ca.computeMonotonicity([]float64{1, 2, 3, 4, 5})
	if mono != 1.0 {
		t.Errorf("expected monotonicity 1.0, got %f", mono)
	}

	// Strictly decreasing
	mono = ca.computeMonotonicity([]float64{5, 4, 3, 2, 1})
	if mono != 1.0 {
		t.Errorf("expected monotonicity 1.0, got %f", mono)
	}

	// Random
	mono = ca.computeMonotonicity([]float64{1, 3, 2, 4, 3})
	if mono > 0.8 {
		t.Errorf("expected lower monotonicity for non-monotonic data, got %f", mono)
	}
}

func TestComputeDeltaStability(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	// Constant deltas → high stability
	stability := ca.computeDeltaStability([]float64{1, 2, 3, 4, 5})
	if stability < 0.9 {
		t.Errorf("expected high delta stability for linear data, got %f", stability)
	}

	// Unstable deltas
	stability = ca.computeDeltaStability([]float64{1, 100, 2, 200, 3})
	if stability > 0.5 {
		t.Errorf("expected low delta stability for erratic data, got %f", stability)
	}
}

func TestRuleMatchingRLE(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{RepeatRatio: 0.8}
	codec, reasoning := ca.applyRules(profile)

	if codec != CodecRLE {
		t.Errorf("expected CodecRLE for high repeat ratio, got %v", codec)
	}
	if reasoning == "" {
		t.Error("expected non-empty reasoning")
	}
}

func TestRuleMatchingDeltaDelta(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{Monotonicity: 0.95, IsInteger: true, RepeatRatio: 0.1}
	codec, _ := ca.applyRules(profile)

	if codec != CodecDeltaDelta {
		t.Errorf("expected CodecDeltaDelta for monotonic integers, got %v", codec)
	}
}

func TestRuleMatchingGorilla(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{Entropy: 0.1, DeltaStability: 0.9, RepeatRatio: 0.1}
	codec, _ := ca.applyRules(profile)

	if codec != CodecGorilla {
		t.Errorf("expected CodecGorilla for low entropy + stable deltas, got %v", codec)
	}
}

func TestRuleMatchingBitPacking(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{Sparsity: 0.8, RepeatRatio: 0.1}
	codec, _ := ca.applyRules(profile)

	if codec != CodecBitPacking {
		t.Errorf("expected CodecBitPacking for sparse data, got %v", codec)
	}
}

func TestRuleMatchingZSTD(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{Entropy: 0.9, ValueRange: 5000, RepeatRatio: 0.1}
	codec, _ := ca.applyRules(profile)

	if codec != CodecZSTD {
		t.Errorf("expected CodecZSTD for high entropy + large range, got %v", codec)
	}
}

func TestRuleFallbackGorilla(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	// Profile that doesn't match any specific rule
	profile := &MetricProfile{
		Entropy:        0.5,
		DeltaStability: 0.3,
		Monotonicity:   0.4,
		RepeatRatio:    0.1,
		Sparsity:       0.1,
		ValueRange:     100,
	}
	codec, _ := ca.applyRules(profile)

	if codec != CodecGorilla {
		t.Errorf("expected CodecGorilla as fallback, got %v", codec)
	}
}

func TestBenchmarkCodecEmpty(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	_, err := ca.BenchmarkCodec("test", nil, CodecGorilla)
	if err == nil {
		t.Fatal("expected error for empty values")
	}
}

func TestBenchmarkCodecGorilla(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i) * 1.5
	}

	bench, err := ca.BenchmarkCodec("test_metric", values, CodecGorilla)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if bench.Codec != CodecGorilla {
		t.Errorf("expected CodecGorilla, got %v", bench.Codec)
	}
	if bench.Metric != "test_metric" {
		t.Errorf("expected metric 'test_metric', got %q", bench.Metric)
	}
	if bench.OriginalSize != int64(len(values)*8) {
		t.Errorf("expected original size %d, got %d", len(values)*8, bench.OriginalSize)
	}
	if bench.CompressionRatio <= 0 {
		t.Error("expected positive compression ratio")
	}
	if bench.Accuracy <= 0 {
		t.Error("expected positive accuracy")
	}
}

func TestBenchmarkAll(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	values := make([]float64, 50)
	for i := range values {
		values[i] = float64(i)
	}

	benchmarks, err := ca.BenchmarkAll("bench_metric", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(benchmarks) == 0 {
		t.Fatal("expected at least one benchmark result")
	}

	// Should be sorted by compression ratio descending
	for i := 1; i < len(benchmarks); i++ {
		if benchmarks[i].CompressionRatio > benchmarks[i-1].CompressionRatio {
			t.Error("benchmarks should be sorted by compression ratio descending")
		}
	}
}

func TestRecommendNoProfile(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	_, err := ca.Recommend("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing profile")
	}
}

func TestRecommendFromProfile(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{
		Metric:         "cpu.usage",
		SampleSize:     1000,
		RepeatRatio:    0.8,
		Monotonicity:   0.3,
		DeltaStability: 0.5,
		Entropy:        0.2,
	}

	rec, err := ca.RecommendFromProfile(profile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rec.Metric != "cpu.usage" {
		t.Errorf("expected metric 'cpu.usage', got %q", rec.Metric)
	}
	if rec.RecommendedCodec != CodecRLE {
		t.Errorf("expected CodecRLE for high repeat ratio, got %v", rec.RecommendedCodec)
	}
	if rec.Confidence <= 0 || rec.Confidence > 1 {
		t.Errorf("expected confidence in (0,1], got %f", rec.Confidence)
	}
	if rec.Reasoning == "" {
		t.Error("expected non-empty reasoning")
	}
}

func TestRecommendEndToEnd(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	// Profile → Benchmark → Recommend
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	_, err := ca.ProfileMetric("counter", values)
	if err != nil {
		t.Fatalf("profile error: %v", err)
	}

	rec, err := ca.Recommend("counter")
	if err != nil {
		t.Fatalf("recommend error: %v", err)
	}

	if rec.RecommendedCodec == CodecNone {
		t.Error("expected a non-none codec recommendation")
	}
}

func TestApplyRecommendation(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{
		Metric:      "mem.used",
		SampleSize:  500,
		RepeatRatio: 0.9,
	}

	_, err := ca.RecommendFromProfile(profile)
	if err != nil {
		t.Fatalf("recommend error: %v", err)
	}

	err = ca.ApplyRecommendation("mem.used")
	if err != nil {
		t.Fatalf("apply error: %v", err)
	}

	rec := ca.GetRecommendation("mem.used")
	if !rec.Applied {
		t.Error("expected recommendation to be marked as applied")
	}

	stats := ca.Stats()
	if stats.RecommendationsApplied != 1 {
		t.Errorf("expected 1 applied recommendation, got %d", stats.RecommendationsApplied)
	}
}

func TestApplyRecommendationMissing(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	err := ca.ApplyRecommendation("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing recommendation")
	}
}

func TestGetProfileNil(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	p := ca.GetProfile("nonexistent")
	if p != nil {
		t.Error("expected nil profile for unknown metric")
	}
}

func TestGetRecommendationNil(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	r := ca.GetRecommendation("nonexistent")
	if r != nil {
		t.Error("expected nil recommendation for unknown metric")
	}
}

func TestListRecommendations(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profiles := []*MetricProfile{
		{Metric: "a", RepeatRatio: 0.8, SampleSize: 100},
		{Metric: "b", Monotonicity: 0.9, IsInteger: true, SampleSize: 100},
	}

	for _, p := range profiles {
		ca.RecommendFromProfile(p)
	}

	recs := ca.ListRecommendations()
	if len(recs) != 2 {
		t.Errorf("expected 2 recommendations, got %d", len(recs))
	}
}

func TestGenerateReport(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ca.ProfileMetric("cpu", values)
	ca.BenchmarkAll("cpu", values)
	ca.Recommend("cpu")

	report, err := ca.GenerateReport()
	if err != nil {
		t.Fatalf("report error: %v", err)
	}

	if report.AnalyzedMetrics != 1 {
		t.Errorf("expected 1 analyzed metric, got %d", report.AnalyzedMetrics)
	}
	if len(report.CodecDistribution) == 0 {
		t.Error("expected non-empty codec distribution")
	}
	if len(report.TopRecommendations) == 0 {
		t.Error("expected at least one top recommendation")
	}
	if report.GeneratedAt.IsZero() {
		t.Error("expected non-zero generated timestamp")
	}
}

func TestGenerateReportEmpty(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	report, err := ca.GenerateReport()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.AnalyzedMetrics != 0 {
		t.Errorf("expected 0 analyzed metrics, got %d", report.AnalyzedMetrics)
	}
}

func TestCompressionAdvisorStatsTracking(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	values := make([]float64, 50)
	for i := range values {
		values[i] = float64(i)
	}

	ca.ProfileMetric("s1", values)
	ca.ProfileMetric("s2", values)

	stats := ca.Stats()
	if stats.MetricsProfiled != 2 {
		t.Errorf("expected 2 profiled metrics, got %d", stats.MetricsProfiled)
	}

	ca.BenchmarkCodec("s1", values, CodecGorilla)
	stats = ca.Stats()
	if stats.BenchmarksRun != 1 {
		t.Errorf("expected 1 benchmark, got %d", stats.BenchmarksRun)
	}
}

func TestStartStop(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	ca.Start()

	ca.mu.RLock()
	running := ca.running
	ca.mu.RUnlock()
	if !running {
		t.Error("expected advisor to be running")
	}

	ca.Stop()
	ca.mu.RLock()
	running = ca.running
	ca.mu.RUnlock()
	if running {
		t.Error("expected advisor to be stopped")
	}
}

func TestComputeConfidence(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())

	profile := &MetricProfile{SampleSize: 2000, RepeatRatio: 0.9}
	confidence := ca.computeConfidence(profile, CodecRLE)

	if confidence < 0.7 {
		t.Errorf("expected high confidence for large sample + high repeat, got %f", confidence)
	}
	if confidence > 1.0 {
		t.Errorf("confidence should not exceed 1.0, got %f", confidence)
	}
}

func TestRecommendFromProfileNil(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	_, err := ca.RecommendFromProfile(nil)
	if err == nil {
		t.Fatal("expected error for nil profile")
	}
}

func TestProfileMetricFloat(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	values := []float64{1.5, 2.7, 3.14, 4.1}
	profile, err := ca.ProfileMetric("floats", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if profile.IsInteger {
		t.Error("expected IsInteger=false for float values")
	}
}

func TestProfileMetricMeanDelta(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	// Constant increment of 2
	values := []float64{0, 2, 4, 6, 8}
	profile, err := ca.ProfileMetric("linear", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if math.Abs(profile.MeanDelta-2.0) > 0.001 {
		t.Errorf("expected mean delta 2.0, got %f", profile.MeanDelta)
	}
	if profile.StddevDelta > 0.001 {
		t.Errorf("expected near-zero stddev delta for linear data, got %f", profile.StddevDelta)
	}
}

func TestHTTPStats(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/compression/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var stats CompressionAdvisorStats
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}
}

func TestHTTPProfiles(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	ca.ProfileMetric("cpu", []float64{1, 2, 3})

	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/compression/profiles", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHTTPProfileByMetric(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	ca.ProfileMetric("cpu", []float64{1, 2, 3})

	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/compression/profiles/cpu", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHTTPProfileNotFound(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/compression/profiles/missing", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHTTPAnalyze(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	body := `{"metric":"cpu","values":[1,2,3,4,5]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/compression/analyze", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHTTPRecommendations(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/compression/recommendations", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHTTPReport(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/compression/report", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHTTPMethodNotAllowed(t *testing.T) {
	ca := NewCompressionAdvisor(nil, DefaultCompressionAdvisorConfig())
	mux := http.NewServeMux()
	ca.RegisterHTTPHandlers(mux)

	// POST to a GET-only endpoint
	req := httptest.NewRequest(http.MethodPost, "/api/v1/compression/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestValuesToBytes(t *testing.T) {
	values := []float64{1.0, 2.0, 3.0}
	data := valuesToBytes(values)
	if len(data) != 24 {
		t.Errorf("expected 24 bytes, got %d", len(data))
	}
}
