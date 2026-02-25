package chronicle

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"
)

// PerfRegressionVerdict represents the outcome of a regression check.
type PerfRegressionVerdict string

const (
	VerdictPass PerfRegressionVerdict = "pass"
	VerdictWarn PerfRegressionVerdict = "warn"
	VerdictFail PerfRegressionVerdict = "fail"
)

// PerfRegressionConfig configures the performance regression detection engine.
type PerfRegressionConfig struct {
	Enabled                bool    `json:"enabled"`
	BaselineDir            string  `json:"baseline_dir"`
	RegressionThresholdPct float64 `json:"regression_threshold_pct"`
	MinSampleSize          int     `json:"min_sample_size"`
	ConfidenceLevel        float64 `json:"confidence_level"`
	EnableTrending         bool    `json:"enable_trending"`
	TrendWindowDays        int     `json:"trend_window_days"`
	BlockPROnRegression    bool    `json:"block_pr_on_regression"`
}

// DefaultPerfRegressionConfig returns sensible defaults for regression detection.
func DefaultPerfRegressionConfig() PerfRegressionConfig {
	return PerfRegressionConfig{
		Enabled:                true,
		BaselineDir:            ".perf-baselines",
		RegressionThresholdPct: 10.0,
		MinSampleSize:          5,
		ConfidenceLevel:        0.95,
		EnableTrending:         true,
		TrendWindowDays:        30,
		BlockPROnRegression:    false,
	}
}

// BenchmarkBaseline stores a baseline benchmark result for a specific commit.
type BenchmarkBaseline struct {
	CommitSHA     string               `json:"commit_sha"`
	Timestamp     time.Time            `json:"timestamp"`
	BenchmarkName string               `json:"benchmark_name"`
	Results       BaselineStats        `json:"results"`
	Environment   BenchmarkEnvironment `json:"environment"`
}

// BaselineStats holds statistical summary of benchmark samples.
type BaselineStats struct {
	Mean    float64   `json:"mean"`
	Median  float64   `json:"median"`
	StdDev  float64   `json:"stddev"`
	P95     float64   `json:"p95"`
	P99     float64   `json:"p99"`
	Samples []float64 `json:"samples"`
}

// BenchmarkEnvironment captures the environment in which a benchmark was run.
type BenchmarkEnvironment struct {
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

// RegressionResult holds the outcome of a statistical comparison between baseline and current.
type RegressionResult struct {
	BenchmarkName      string                `json:"benchmark_name"`
	BaselineMean       float64               `json:"baseline_mean"`
	CurrentMean        float64               `json:"current_mean"`
	ChangePercent      float64               `json:"change_percent"`
	PValue             float64               `json:"p_value"`
	TStatistic         float64               `json:"t_statistic"`
	EffectSize         float64               `json:"effect_size"`
	ConfidenceInterval [2]float64            `json:"confidence_interval"`
	IsSignificant      bool                  `json:"is_significant"`
	IsRegression       bool                  `json:"is_regression"`
	Verdict            PerfRegressionVerdict `json:"verdict"`
}

// TrendPoint represents a single data point in a performance trend.
type TrendPoint struct {
	CommitSHA string    `json:"commit_sha"`
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// TrendData holds trend analysis for a benchmark over time.
type TrendData struct {
	BenchmarkName  string       `json:"benchmark_name"`
	Points         []TrendPoint `json:"points"`
	MovingAverage  []float64    `json:"moving_average"`
	Slope          float64      `json:"slope"`
	IsDrifting     bool         `json:"is_drifting"`
	DriftDirection string       `json:"drift_direction,omitempty"`
}

// PerfRegressionReport is the full regression analysis report.
type PerfRegressionReport struct {
	Timestamp   time.Time          `json:"timestamp"`
	CommitSHA   string             `json:"commit_sha"`
	Results     []RegressionResult `json:"results"`
	Trends      []TrendData        `json:"trends,omitempty"`
	Summary     string             `json:"summary"`
	ShouldBlock bool               `json:"should_block"`
}

// ---------------------------------------------------------------------------
// StatisticalAnalyzer
// ---------------------------------------------------------------------------

// StatisticalAnalyzer provides statistical comparison of benchmark results.
type StatisticalAnalyzer struct {
	config PerfRegressionConfig
}

// NewStatisticalAnalyzer creates a new StatisticalAnalyzer.
func NewStatisticalAnalyzer(cfg PerfRegressionConfig) *StatisticalAnalyzer {
	return &StatisticalAnalyzer{config: cfg}
}

// CompareResults performs a statistical comparison between baseline and current samples.
func (sa *StatisticalAnalyzer) CompareResults(baseline, current []float64) RegressionResult {
	var result RegressionResult

	bMean := perfMean(baseline)
	cMean := perfMean(current)
	result.BaselineMean = bMean
	result.CurrentMean = cMean

	if bMean != 0 {
		result.ChangePercent = ((cMean - bMean) / math.Abs(bMean)) * 100
	}

	bStd := perfStddev(baseline)
	cStd := perfStddev(current)

	// Welch's t-test
	nB := float64(len(baseline))
	nC := float64(len(current))

	if nB < 2 || nC < 2 {
		result.Verdict = VerdictWarn
		return result
	}

	seBSq := (bStd * bStd) / nB
	seCSq := (cStd * cStd) / nC
	seDiff := math.Sqrt(seBSq + seCSq)

	if seDiff == 0 {
		result.Verdict = VerdictPass
		return result
	}

	result.TStatistic = (cMean - bMean) / seDiff

	// Welch-Satterthwaite degrees of freedom
	num := (seBSq + seCSq) * (seBSq + seCSq)
	denom := (seBSq*seBSq)/(nB-1) + (seCSq*seCSq)/(nC-1)
	df := num / denom
	if df < 1 {
		df = 1
	}

	result.PValue = welchTTestPValue(result.TStatistic, df)

	// Cohen's d effect size
	pooledStd := math.Sqrt(((nB-1)*bStd*bStd + (nC-1)*cStd*cStd) / (nB + nC - 2))
	if pooledStd > 0 {
		result.EffectSize = (cMean - bMean) / pooledStd
	}

	// Confidence interval for the difference of means
	alpha := 1.0 - sa.config.ConfidenceLevel
	tCrit := tCriticalValue(alpha, df)
	diff := cMean - bMean
	result.ConfidenceInterval = [2]float64{
		diff - tCrit*seDiff,
		diff + tCrit*seDiff,
	}

	result.IsSignificant = result.PValue < (1.0 - sa.config.ConfidenceLevel)
	result.IsRegression = sa.IsRegression(result)

	if result.IsRegression {
		result.Verdict = VerdictFail
	} else if result.IsSignificant && result.ChangePercent > 0 {
		result.Verdict = VerdictWarn
	} else {
		result.Verdict = VerdictPass
	}

	return result
}

// IsRegression returns true when the result indicates a statistically significant regression
// exceeding the configured threshold.
func (sa *StatisticalAnalyzer) IsRegression(result RegressionResult) bool {
	return result.IsSignificant && result.ChangePercent > sa.config.RegressionThresholdPct
}

// ---------------------------------------------------------------------------
// TrendTracker
// ---------------------------------------------------------------------------

// TrendTracker records benchmark results over time and detects performance drift.
type TrendTracker struct {
	mu     sync.RWMutex
	points map[string][]TrendPoint // benchmarkName -> points
	config PerfRegressionConfig
}

// NewTrendTracker creates a new TrendTracker.
func NewTrendTracker(cfg PerfRegressionConfig) *TrendTracker {
	return &TrendTracker{
		points: make(map[string][]TrendPoint),
		config: cfg,
	}
}

// RecordResult adds a data point for the given benchmark.
func (tt *TrendTracker) RecordResult(commitSHA, benchName string, value float64) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.points[benchName] = append(tt.points[benchName], TrendPoint{
		CommitSHA: commitSHA,
		Timestamp: time.Now(),
		Value:     value,
	})
}

// GetTrend returns trend data for a benchmark within the given number of days.
func (tt *TrendTracker) GetTrend(benchName string, days int) TrendData {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	td := TrendData{BenchmarkName: benchName}
	cutoff := time.Now().AddDate(0, 0, -days)

	pts, ok := tt.points[benchName]
	if !ok {
		return td
	}

	for _, p := range pts {
		if p.Timestamp.After(cutoff) {
			td.Points = append(td.Points, p)
		}
	}

	if len(td.Points) < 2 {
		return td
	}

	// Moving average (window of 5)
	td.MovingAverage = movingAverage(extractValues(td.Points), 5)

	// Linear regression slope
	td.Slope = linearRegressionSlope(td.Points)

	td.IsDrifting, td.DriftDirection = tt.detectDriftFromSlope(td.Slope, td.Points)
	return td
}

// DetectDrift checks whether the given benchmark shows gradual performance drift.
func (tt *TrendTracker) DetectDrift(benchName string) (bool, string) {
	td := tt.GetTrend(benchName, tt.config.TrendWindowDays)
	return td.IsDrifting, td.DriftDirection
}

func (tt *TrendTracker) detectDriftFromSlope(slope float64, points []TrendPoint) (bool, string) {
	if len(points) < 3 {
		return false, ""
	}
	avgVal := 0.0
	for _, p := range points {
		avgVal += p.Value
	}
	avgVal /= float64(len(points))
	if avgVal == 0 {
		return false, ""
	}
	// Relative slope per point as percentage of mean
	relSlope := (slope / avgVal) * 100
	threshold := tt.config.RegressionThresholdPct / 2 // use half threshold for drift
	if relSlope > threshold {
		return true, "degrading"
	}
	if relSlope < -threshold {
		return true, "improving"
	}
	return false, ""
}

func (tt *TrendTracker) allTrends() []TrendData {
	tt.mu.RLock()
	names := make([]string, 0, len(tt.points))
	for n := range tt.points {
		names = append(names, n)
	}
	tt.mu.RUnlock()

	sort.Strings(names)
	trends := make([]TrendData, 0, len(names))
	for _, n := range names {
		trends = append(trends, tt.GetTrend(n, tt.config.TrendWindowDays))
	}
	return trends
}

// ---------------------------------------------------------------------------
// PerfRegressionEngine
// ---------------------------------------------------------------------------

// PerfRegressionEngine orchestrates benchmark execution, comparison, trending and reporting.
type PerfRegressionEngine struct {
	db       *DB
	config   PerfRegressionConfig
	analyzer *StatisticalAnalyzer
	tracker  *TrendTracker
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc

	baselines  map[string]BenchmarkBaseline // benchmarkName -> baseline
	lastReport *PerfRegressionReport
}

// NewPerfRegressionEngine creates a new PerfRegressionEngine.
func NewPerfRegressionEngine(db *DB, cfg PerfRegressionConfig) *PerfRegressionEngine {
	ctx, cancel := context.WithCancel(context.Background())
	return &PerfRegressionEngine{
		db:        db,
		config:    cfg,
		analyzer:  NewStatisticalAnalyzer(cfg),
		tracker:   NewTrendTracker(cfg),
		ctx:       ctx,
		cancel:    cancel,
		baselines: make(map[string]BenchmarkBaseline),
	}
}

// Start begins the engine.
func (e *PerfRegressionEngine) Start() {
	// No-op for now; engine runs on-demand.
}

// Stop halts the engine and cancels the context.
func (e *PerfRegressionEngine) Stop() {
	e.cancel()
}

// RunBenchmarks executes the benchmark suite and returns results.
func (e *PerfRegressionEngine) RunBenchmarks(ctx context.Context) ([]BenchRunResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	suite := NewBenchRunSuite(e.db, DefaultBenchRunnerConfig())
	results := suite.RunAll()

	return results, nil
}

// CompareToBaseline compares the given benchmark results to stored baselines.
func (e *PerfRegressionEngine) CompareToBaseline(ctx context.Context, results []BenchRunResult) ([]RegressionResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	var regressions []RegressionResult
	for _, r := range results {
		bl, ok := e.baselines[r.Operation]
		if !ok || len(bl.Results.Samples) < e.config.MinSampleSize {
			continue
		}

		// Build current samples from throughput (single run → single-element slice
		// unless caller provides multi-run results).
		currentSamples := []float64{r.Throughput}
		rr := e.analyzer.CompareResults(bl.Results.Samples, currentSamples)
		rr.BenchmarkName = r.Operation
		regressions = append(regressions, rr)
	}
	return regressions, nil
}

// UpdateBaseline stores the provided results as the new baseline for the given commit.
