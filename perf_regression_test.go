package chronicle

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestDefaultPerfRegressionConfig(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.BaselineDir != ".perf-baselines" {
		t.Errorf("BaselineDir = %q, want %q", cfg.BaselineDir, ".perf-baselines")
	}
	if cfg.RegressionThresholdPct != 10.0 {
		t.Errorf("RegressionThresholdPct = %v, want 10.0", cfg.RegressionThresholdPct)
	}
	if cfg.MinSampleSize != 5 {
		t.Errorf("MinSampleSize = %d, want 5", cfg.MinSampleSize)
	}
	if cfg.ConfidenceLevel != 0.95 {
		t.Errorf("ConfidenceLevel = %v, want 0.95", cfg.ConfidenceLevel)
	}
	if !cfg.EnableTrending {
		t.Error("expected EnableTrending to be true")
	}
	if cfg.TrendWindowDays != 30 {
		t.Errorf("TrendWindowDays = %d, want 30", cfg.TrendWindowDays)
	}
	if cfg.BlockPROnRegression {
		t.Error("expected BlockPROnRegression to be false")
	}
}

func TestStatisticalAnalyzer_CompareResults(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()
	sa := NewStatisticalAnalyzer(cfg)

	rng := rand.New(rand.NewSource(42))
	baseline := make([]float64, 30)
	current := make([]float64, 30)
	for i := range baseline {
		baseline[i] = 100.0 + rng.NormFloat64()*2.0
		current[i] = 100.0 + rng.NormFloat64()*2.0
	}

	result := sa.CompareResults(baseline, current)

	if result.Verdict == VerdictFail {
		t.Errorf("expected no regression for similar distributions, got verdict=%s change=%.2f%%", result.Verdict, result.ChangePercent)
	}
	if math.Abs(result.ChangePercent) > cfg.RegressionThresholdPct {
		t.Errorf("change percent %.2f exceeds threshold for identical distributions", result.ChangePercent)
	}
}

func TestStatisticalAnalyzer_DetectRegression(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()
	cfg.RegressionThresholdPct = 5.0
	sa := NewStatisticalAnalyzer(cfg)

	rng := rand.New(rand.NewSource(99))
	baseline := make([]float64, 50)
	current := make([]float64, 50)
	for i := range baseline {
		baseline[i] = 100.0 + rng.NormFloat64()*1.0
		current[i] = 120.0 + rng.NormFloat64()*1.0 // 20% higher
	}

	result := sa.CompareResults(baseline, current)

	if !result.IsSignificant {
		t.Error("expected result to be statistically significant")
	}
	if !result.IsRegression {
		t.Error("expected regression to be detected")
	}
	if result.Verdict != VerdictFail {
		t.Errorf("verdict = %s, want %s", result.Verdict, VerdictFail)
	}
	if result.ChangePercent < 15.0 {
		t.Errorf("change percent = %.2f, expected > 15%%", result.ChangePercent)
	}
}

func TestStatisticalAnalyzer_SmallSample(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()
	sa := NewStatisticalAnalyzer(cfg)

	// Single element each — not enough for t-test
	result := sa.CompareResults([]float64{100.0}, []float64{200.0})

	if result.Verdict != VerdictWarn {
		t.Errorf("verdict = %s, want %s for small sample", result.Verdict, VerdictWarn)
	}

	// Empty slices
	result2 := sa.CompareResults([]float64{}, []float64{})
	if result2.Verdict != VerdictWarn {
		t.Errorf("verdict = %s, want %s for empty samples", result2.Verdict, VerdictWarn)
	}
}

func TestStatisticalAnalyzer_WelchTTest(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()
	sa := NewStatisticalAnalyzer(cfg)

	// Two clearly different distributions
	baseline := []float64{10, 10, 10, 10, 10}
	current := []float64{20, 20, 20, 20, 20}

	result := sa.CompareResults(baseline, current)

	// With zero variance in both, seDiff=0, so should get VerdictPass (special case)
	if result.Verdict != VerdictPass {
		t.Errorf("verdict = %s, want %s for zero-variance distributions", result.Verdict, VerdictPass)
	}

	// Now with some variance
	baseline2 := []float64{10, 11, 9, 10, 11, 9, 10, 11}
	current2 := []float64{30, 31, 29, 30, 31, 29, 30, 31}

	result2 := sa.CompareResults(baseline2, current2)
	if result2.TStatistic <= 0 {
		t.Errorf("t-statistic = %v, expected positive value", result2.TStatistic)
	}
	if result2.PValue >= 1.0 {
		t.Errorf("p-value = %v, expected < 1.0", result2.PValue)
	}
	if result2.EffectSize <= 0 {
		t.Errorf("effect size = %v, expected positive", result2.EffectSize)
	}
	if result2.ConfidenceInterval[0] >= result2.ConfidenceInterval[1] {
		t.Errorf("CI lower (%.2f) >= upper (%.2f)", result2.ConfidenceInterval[0], result2.ConfidenceInterval[1])
	}
}

func TestTrendTracker_RecordAndGet(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()
	tt := NewTrendTracker(cfg)

	benchName := "BenchmarkWrite"
	for i := 0; i < 10; i++ {
		tt.RecordResult(fmt.Sprintf("sha-%d", i), benchName, 100.0+float64(i))
	}

	td := tt.GetTrend(benchName, 30)
	if td.BenchmarkName != benchName {
		t.Errorf("BenchmarkName = %q, want %q", td.BenchmarkName, benchName)
	}
	if len(td.Points) != 10 {
		t.Errorf("len(Points) = %d, want 10", len(td.Points))
	}
	if len(td.MovingAverage) == 0 {
		t.Error("expected non-empty MovingAverage")
	}

	// Verify points have correct values
	for i, p := range td.Points {
		expected := 100.0 + float64(i)
		if p.Value != expected {
			t.Errorf("point[%d].Value = %v, want %v", i, p.Value, expected)
		}
	}

	// Non-existent benchmark returns empty trend
	empty := tt.GetTrend("nonexistent", 30)
	if len(empty.Points) != 0 {
		t.Errorf("expected 0 points for nonexistent benchmark, got %d", len(empty.Points))
	}
}

func TestTrendTracker_DetectDrift(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()
	cfg.RegressionThresholdPct = 5.0
	tt := NewTrendTracker(cfg)

	benchName := "BenchmarkDrift"
	// Record points with a strong upward trend (degrading)
	for i := 0; i < 20; i++ {
		tt.RecordResult(fmt.Sprintf("sha-%d", i), benchName, 100.0+float64(i)*10.0)
	}

	drifting, direction := tt.DetectDrift(benchName)
	if !drifting {
		t.Error("expected drift to be detected for strongly increasing values")
	}
	if direction != "degrading" {
		t.Errorf("direction = %q, want %q", direction, "degrading")
	}

	// Record points with a strong downward trend (improving)
	benchName2 := "BenchmarkImproving"
	for i := 0; i < 20; i++ {
		tt.RecordResult(fmt.Sprintf("sha-%d", i), benchName2, 200.0-float64(i)*10.0)
	}

	drifting2, direction2 := tt.DetectDrift(benchName2)
	if !drifting2 {
		t.Error("expected drift to be detected for strongly decreasing values")
	}
	if direction2 != "improving" {
		t.Errorf("direction = %q, want %q", direction2, "improving")
	}

	// Flat trend: no drift
	benchName3 := "BenchmarkStable"
	for i := 0; i < 20; i++ {
		tt.RecordResult(fmt.Sprintf("sha-%d", i), benchName3, 100.0)
	}
	drifting3, _ := tt.DetectDrift(benchName3)
	if drifting3 {
		t.Error("expected no drift for constant values")
	}
}

func TestRegressionResult_Verdicts(t *testing.T) {
	cfg := DefaultPerfRegressionConfig()
	sa := NewStatisticalAnalyzer(cfg)

	tests := []struct {
		name            string
		baseline        []float64
		current         []float64
		expectedVerdict PerfRegressionVerdict
	}{
		{
			name:            "pass_similar",
			baseline:        []float64{100, 101, 99, 100, 101, 99, 100, 101},
			current:         []float64{100, 101, 99, 100, 101, 99, 100, 101},
			expectedVerdict: VerdictPass,
		},
		{
			name:            "warn_small_sample",
			baseline:        []float64{100},
			current:         []float64{200},
			expectedVerdict: VerdictWarn,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sa.CompareResults(tc.baseline, tc.current)
			if result.Verdict != tc.expectedVerdict {
				t.Errorf("verdict = %s, want %s (change=%.2f%%, sig=%v)",
					result.Verdict, tc.expectedVerdict, result.ChangePercent, result.IsSignificant)
			}
		})
	}

	// Verify verdict constants
	if VerdictPass != "pass" {
		t.Errorf("VerdictPass = %q, want %q", VerdictPass, "pass")
	}
	if VerdictWarn != "warn" {
		t.Errorf("VerdictWarn = %q, want %q", VerdictWarn, "warn")
	}
	if VerdictFail != "fail" {
		t.Errorf("VerdictFail = %q, want %q", VerdictFail, "fail")
	}
}

func TestNewPerfRegressionEngine(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	perfCfg := DefaultPerfRegressionConfig()
	engine := NewPerfRegressionEngine(db, perfCfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
}

func TestPerfRegressionEngine_StartStop(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	perfCfg := DefaultPerfRegressionConfig()
	engine := NewPerfRegressionEngine(db, perfCfg)

	// Start and Stop should not panic
	engine.Start()
	engine.Stop()
}

func TestPerfRegressionEngine_UpdateBaseline(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	perfCfg := DefaultPerfRegressionConfig()
	engine := NewPerfRegressionEngine(db, perfCfg)
	engine.Start()
	defer engine.Stop()

	ctx := context.Background()
	results := []BenchRunResult{
		{Operation: "write", Throughput: 1000.0},
		{Operation: "read", Throughput: 5000.0},
	}

	if err := engine.UpdateBaseline(ctx, "abc123", results); err != nil {
		t.Fatalf("UpdateBaseline: %v", err)
	}

	// Verify baselines were stored
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	bl, ok := engine.baselines["write"]
	if !ok {
		t.Fatal("expected baseline for 'write'")
	}
	if bl.CommitSHA != "abc123" {
		t.Errorf("CommitSHA = %q, want %q", bl.CommitSHA, "abc123")
	}
	if bl.Results.Mean != 1000.0 {
		t.Errorf("Mean = %v, want 1000.0", bl.Results.Mean)
	}

	bl2, ok := engine.baselines["read"]
	if !ok {
		t.Fatal("expected baseline for 'read'")
	}
	if bl2.Results.Mean != 5000.0 {
		t.Errorf("Mean = %v, want 5000.0", bl2.Results.Mean)
	}
}

func TestPerfRegressionEngine_CompareToBaseline(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	perfCfg := DefaultPerfRegressionConfig()
	perfCfg.MinSampleSize = 3
	engine := NewPerfRegressionEngine(db, perfCfg)
	engine.Start()
	defer engine.Stop()

	ctx := context.Background()

	// Manually set a baseline with enough samples
	engine.mu.Lock()
	engine.baselines["write"] = BenchmarkBaseline{
		CommitSHA:     "base123",
		BenchmarkName: "write",
		Results: BaselineStats{
			Mean:    100.0,
			Samples: []float64{98, 100, 102, 99, 101},
		},
	}
	engine.mu.Unlock()

	// Compare with a similar result
	results := []BenchRunResult{
		{Operation: "write", Throughput: 101.0},
	}

	regressions, err := engine.CompareToBaseline(ctx, results)
	if err != nil {
		t.Fatalf("CompareToBaseline: %v", err)
	}
	if len(regressions) != 1 {
		t.Fatalf("expected 1 regression result, got %d", len(regressions))
	}
	if regressions[0].BenchmarkName != "write" {
		t.Errorf("BenchmarkName = %q, want %q", regressions[0].BenchmarkName, "write")
	}

	// Operation not in baselines should be skipped
	results2 := []BenchRunResult{
		{Operation: "delete", Throughput: 50.0},
	}
	regressions2, err := engine.CompareToBaseline(ctx, results2)
	if err != nil {
		t.Fatalf("CompareToBaseline: %v", err)
	}
	if len(regressions2) != 0 {
		t.Errorf("expected 0 results for unknown operation, got %d", len(regressions2))
	}
}

func TestPerfRegressionEngine_ShouldBlockPR(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// With blocking disabled
	perfCfg := DefaultPerfRegressionConfig()
	perfCfg.BlockPROnRegression = false
	engine := NewPerfRegressionEngine(db, perfCfg)

	failResults := []RegressionResult{
		{Verdict: VerdictFail, BenchmarkName: "write"},
	}
	if engine.ShouldBlockPR(failResults) {
		t.Error("should not block PR when BlockPROnRegression is false")
	}

	// With blocking enabled
	perfCfg2 := DefaultPerfRegressionConfig()
	perfCfg2.BlockPROnRegression = true
	engine2 := NewPerfRegressionEngine(db, perfCfg2)

	if !engine2.ShouldBlockPR(failResults) {
		t.Error("should block PR when BlockPROnRegression is true and there is a failure")
	}

	passResults := []RegressionResult{
		{Verdict: VerdictPass, BenchmarkName: "write"},
		{Verdict: VerdictWarn, BenchmarkName: "read"},
	}
	if engine2.ShouldBlockPR(passResults) {
		t.Error("should not block PR when no failures exist")
	}
}
