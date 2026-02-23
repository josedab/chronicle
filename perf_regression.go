package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
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
func (e *PerfRegressionEngine) UpdateBaseline(ctx context.Context, commitSHA string, results []BenchRunResult) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, r := range results {
		samples := []float64{r.Throughput}
		stats := computeBaselineStats(samples)

		e.baselines[r.Operation] = BenchmarkBaseline{
			CommitSHA:     commitSHA,
			Timestamp:     time.Now(),
			BenchmarkName: r.Operation,
			Results:       stats,
			Environment:   BenchmarkEnvironment{},
		}

		if e.config.EnableTrending {
			e.tracker.RecordResult(commitSHA, r.Operation, r.Throughput)
		}
	}
	return nil
}

// GetTrends returns performance trend data for all tracked benchmarks.
func (e *PerfRegressionEngine) GetTrends() []TrendData {
	return e.tracker.allTrends()
}

// GenerateReport produces a full regression report in text and structured form.
func (e *PerfRegressionEngine) GenerateReport(commitSHA string, regressions []RegressionResult) *PerfRegressionReport {
	report := &PerfRegressionReport{
		Timestamp: time.Now(),
		CommitSHA: commitSHA,
		Results:   regressions,
	}

	if e.config.EnableTrending {
		report.Trends = e.GetTrends()
	}

	// Build summary
	var fails, warns int
	for _, r := range regressions {
		switch r.Verdict {
		case VerdictFail:
			fails++
		case VerdictWarn:
			warns++
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Performance Regression Report — %s\n", commitSHA[:perfMinInt(8, len(commitSHA))])
	fmt.Fprintf(&sb, "Benchmarks checked: %d | Failures: %d | Warnings: %d\n", len(regressions), fails, warns)
	for _, r := range regressions {
		fmt.Fprintf(&sb, "  [%s] %s: %.2f → %.2f (%+.1f%%) p=%.4f\n",
			r.Verdict, r.BenchmarkName, r.BaselineMean, r.CurrentMean, r.ChangePercent, r.PValue)
	}

	report.Summary = sb.String()
	report.ShouldBlock = e.ShouldBlockPR(regressions)

	e.mu.Lock()
	e.lastReport = report
	e.mu.Unlock()

	return report
}

// ShouldBlockPR returns true if the PR should be blocked based on regression results.
func (e *PerfRegressionEngine) ShouldBlockPR(results []RegressionResult) bool {
	if !e.config.BlockPROnRegression {
		return false
	}
	for _, r := range results {
		if r.Verdict == VerdictFail {
			return true
		}
	}
	return false
}

// RegisterHTTPHandlers registers performance regression HTTP endpoints.
func (e *PerfRegressionEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/perf/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		results, err := e.RunBenchmarks(r.Context())
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("/api/v1/perf/baseline", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			e.mu.RLock()
			bl := make([]BenchmarkBaseline, 0, len(e.baselines))
			for _, b := range e.baselines {
				bl = append(bl, b)
			}
			e.mu.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(bl)

		case http.MethodPost:
			var req struct {
				CommitSHA string           `json:"commit_sha"`
				Results   []BenchRunResult `json:"results"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := e.UpdateBaseline(r.Context(), req.CommitSHA, req.Results); err != nil {
				internalError(w, err, "internal error")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "updated"})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/perf/trends", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetTrends())
	})

	mux.HandleFunc("/api/v1/perf/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		e.mu.RLock()
		rpt := e.lastReport
		e.mu.RUnlock()
		if rpt == nil {
			http.Error(w, "no report available", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rpt)
	})

	mux.HandleFunc("/api/v1/perf/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		e.mu.RLock()
		rpt := e.lastReport
		e.mu.RUnlock()

		status := map[string]any{
			"enabled":      e.config.Enabled,
			"block_on_reg": e.config.BlockPROnRegression,
		}
		if rpt != nil {
			status["last_run"] = rpt.Timestamp
			status["should_block"] = rpt.ShouldBlock
			status["commit"] = rpt.CommitSHA
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})
}

// ---------------------------------------------------------------------------
// Statistical helpers
// ---------------------------------------------------------------------------

func perfMean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := 0.0
	for _, v := range vals {
		s += v
	}
	return s / float64(len(vals))
}

func perfStddev(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	m := perfMean(vals)
	ss := 0.0
	for _, v := range vals {
		d := v - m
		ss += d * d
	}
	return math.Sqrt(ss / float64(len(vals)-1))
}

func perfMedian(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := make([]float64, len(vals))
	copy(s, vals)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 0 {
		return (s[n/2-1] + s[n/2]) / 2
	}
	return s[n/2]
}

func perfPercentile(vals []float64, pct float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := make([]float64, len(vals))
	copy(s, vals)
	sort.Float64s(s)
	idx := int(float64(len(s)-1) * pct)
	return s[idx]
}

func computeBaselineStats(samples []float64) BaselineStats {
	return BaselineStats{
		Mean:    perfMean(samples),
		Median:  perfMedian(samples),
		StdDev:  perfStddev(samples),
		P95:     perfPercentile(samples, 0.95),
		P99:     perfPercentile(samples, 0.99),
		Samples: samples,
	}
}

// welchTTestPValue approximates a two-tailed p-value using a normal approximation
// for large df and a simple t-distribution approximation otherwise.
func welchTTestPValue(t, df float64) float64 {
	if df <= 0 {
		return 1.0
	}
	// Use regularised incomplete beta function approximation.
	x := df / (df + t*t)
	p := regIncBeta(df/2, 0.5, x)
	return p
}

// tCriticalValue returns an approximate critical t-value for the given alpha (two-tailed) and df.
func tCriticalValue(alpha, df float64) float64 {
	// For large df, use a normal approximation via the rational approximation of the
	// inverse normal CDF.  For small df fall back to a conservative estimate.
	if df > 120 {
		return invNormApprox(1 - alpha/2)
	}
	// Simple Cornish-Fisher-style expansion
	z := invNormApprox(1 - alpha/2)
	g1 := (z*z*z + z) / 4
	g2 := (5*z*z*z*z*z + 16*z*z*z + 3*z) / 96
	return z + g1/df + g2/(df*df)
}

// invNormApprox returns an approximation of the inverse standard normal CDF.
func invNormApprox(p float64) float64 {
	// Rational approximation (Abramowitz & Stegun 26.2.23)
	if p <= 0 {
		return -4
	}
	if p >= 1 {
		return 4
	}
	t := math.Sqrt(-2 * math.Log(1-p))
	c0, c1, c2 := 2.515517, 0.802853, 0.010328
	d1, d2, d3 := 1.432788, 0.189269, 0.001308
	return t - (c0+c1*t+c2*t*t)/(1+d1*t+d2*t*t+d3*t*t*t)
}

// regIncBeta computes the regularised incomplete beta function I_x(a,b) using a
// continued-fraction expansion (Lentz's method). This is sufficient for computing
// t-distribution p-values.
func regIncBeta(a, b, x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}

	lnBeta := lgamma(a) + lgamma(b) - lgamma(a+b)
	front := math.Exp(math.Log(x)*a+math.Log(1-x)*b-lnBeta) / a

	// Lentz's continued fraction
	const maxIter = 200
	const epsilon = 1e-14

	f := 1.0
	c := 1.0
	d := 1.0 - (a+b)*x/(a+1)
	if math.Abs(d) < epsilon {
		d = epsilon
	}
	d = 1 / d
	f = d

	for i := 1; i <= maxIter; i++ {
		m := float64(i)

		// Even step
		num := m * (b - m) * x / ((a + 2*m - 1) * (a + 2*m))
		d = 1 + num*d
		if math.Abs(d) < epsilon {
			d = epsilon
		}
		c = 1 + num/c
		if math.Abs(c) < epsilon {
			c = epsilon
		}
		d = 1 / d
		f *= d * c

		// Odd step
		num = -(a + m) * (a + b + m) * x / ((a + 2*m) * (a + 2*m + 1))
		d = 1 + num*d
		if math.Abs(d) < epsilon {
			d = epsilon
		}
		c = 1 + num/c
		if math.Abs(c) < epsilon {
			c = epsilon
		}
		d = 1 / d
		delta := d * c
		f *= delta

		if math.Abs(delta-1) < epsilon {
			break
		}
	}

	return front * f
}

func lgamma(x float64) float64 {
	v, _ := math.Lgamma(x)
	return v
}

// movingAverage computes a simple moving average with the given window size.
func movingAverage(vals []float64, window int) []float64 {
	if window <= 0 || len(vals) == 0 {
		return nil
	}
	if window > len(vals) {
		window = len(vals)
	}
	out := make([]float64, 0, len(vals)-window+1)
	sum := 0.0
	for i := 0; i < window; i++ {
		sum += vals[i]
	}
	out = append(out, sum/float64(window))
	for i := window; i < len(vals); i++ {
		sum += vals[i] - vals[i-window]
		out = append(out, sum/float64(window))
	}
	return out
}

// linearRegressionSlope computes the slope of a simple linear regression on
// sequentially indexed trend points.
func linearRegressionSlope(points []TrendPoint) float64 {
	n := float64(len(points))
	if n < 2 {
		return 0
	}
	var sumX, sumY, sumXY, sumX2 float64
	for i, p := range points {
		x := float64(i)
		sumX += x
		sumY += p.Value
		sumXY += x * p.Value
		sumX2 += x * x
	}
	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0
	}
	return (n*sumXY - sumX*sumY) / denom
}

func extractValues(points []TrendPoint) []float64 {
	vals := make([]float64, len(points))
	for i, p := range points {
		vals[i] = p.Value
	}
	return vals
}

func perfMinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
