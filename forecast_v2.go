package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// ForecastV2Config configures prophet-style forecasting.
type ForecastV2Config struct {
	Enabled              bool    `json:"enabled"`
	DefaultHorizon       int     `json:"default_horizon"`
	SeasonalityMode      string  `json:"seasonality_mode"`
	ChangepointThreshold float64 `json:"changepoint_threshold"`
	ConfidenceLevel      float64 `json:"confidence_level"`
}

// DefaultForecastV2Config returns sensible defaults.
func DefaultForecastV2Config() ForecastV2Config {
	return ForecastV2Config{
		Enabled:              true,
		DefaultHorizon:       24,
		SeasonalityMode:      "additive",
		ChangepointThreshold: 0.05,
		ConfidenceLevel:      0.95,
	}
}

// ForecastV2Request describes a forecast request.
type ForecastV2Request struct {
	Metric              string `json:"metric"`
	Horizon             int    `json:"horizon"`
	SeasonalityMode     string `json:"seasonality_mode"`
	IncludeChangepoints bool   `json:"include_changepoints"`
}

// ForecastV2Point represents a single forecast data point.
type ForecastV2Point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
	Lower     float64 `json:"lower"`
	Upper     float64 `json:"upper"`
}

// ForecastV2Changepoint represents a detected changepoint.
type ForecastV2Changepoint struct {
	Timestamp   int64   `json:"timestamp"`
	TrendBefore float64 `json:"trend_before"`
	TrendAfter  float64 `json:"trend_after"`
	Magnitude   float64 `json:"magnitude"`
}

// ForecastV2Result contains forecast predictions and analysis.
type ForecastV2Result struct {
	Metric          string                  `json:"metric"`
	Predictions     []ForecastV2Point       `json:"predictions"`
	Changepoints    []ForecastV2Changepoint `json:"changepoints"`
	SeasonalPattern []float64               `json:"seasonal_pattern"`
	TrendSlope      float64                 `json:"trend_slope"`
	ModelFit        float64                 `json:"model_fit"`
}

// ForecastV2Stats tracks forecasting statistics.
type ForecastV2Stats struct {
	TotalForecasts int     `json:"total_forecasts"`
	AvgHorizon     float64 `json:"avg_horizon"`
	AvgModelFit    float64 `json:"avg_model_fit"`
}

// ForecastV2Engine performs prophet-style forecasting.
type ForecastV2Engine struct {
	db     *DB
	config ForecastV2Config

	totalForecasts int
	totalHorizon   int
	totalModelFit  float64
	running        bool
	stopCh         chan struct{}

	mu sync.RWMutex
}

// NewForecastV2Engine creates a new forecast engine.
func NewForecastV2Engine(db *DB, cfg ForecastV2Config) *ForecastV2Engine {
	return &ForecastV2Engine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// Start begins the forecast engine.
func (e *ForecastV2Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop halts the forecast engine.
func (e *ForecastV2Engine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// Predict generates a forecast for the requested metric using trend-seasonal decomposition.
func (e *ForecastV2Engine) Predict(req ForecastV2Request) (*ForecastV2Result, error) {
	if req.Metric == "" {
		return nil, fmt.Errorf("metric name is required")
	}

	horizon := req.Horizon
	if horizon <= 0 {
		horizon = e.config.DefaultHorizon
	}

	// Query historical data
	result, err := e.db.Execute(&Query{Metric: req.Metric})
	if err != nil {
		return nil, fmt.Errorf("query metric %q: %w", req.Metric, err)
	}

	if len(result.Points) == 0 {
		return nil, fmt.Errorf("no data for metric %q", req.Metric)
	}

	values := make([]float64, len(result.Points))
	for i, p := range result.Points {
		values[i] = p.Value
	}

	// Step 1: Detect seasonality period
	seasonalPeriod := detectSeasonalPeriod(values)

	// Step 2: Extract seasonal component via averaging
	seasonal := extractSeasonalComponent(values, seasonalPeriod)

	// Step 3: Deseasonalize
	deseasonalized := make([]float64, len(values))
	mode := req.SeasonalityMode
	if mode == "" {
		mode = e.config.SeasonalityMode
	}
	for i, v := range values {
		si := seasonal[i%len(seasonal)]
		if mode == "multiplicative" && si != 0 {
			deseasonalized[i] = v / si
		} else {
			deseasonalized[i] = v - si
		}
	}

	// Step 4: Linear regression on deseasonalized data for trend
	slope, intercept := linearRegression(deseasonalized)

	// Step 5: Compute residuals for confidence intervals
	residualStd := computeResidualStd(values, slope, intercept, seasonal, mode)

	// Detect changepoints
	var changepoints []ForecastV2Changepoint
	if req.IncludeChangepoints {
		changepoints = e.DetectChangepoints(values)
	}

	// Step 6: Generate predictions with seasonal + trend + statistical confidence
	n := len(values)
	lastTS := result.Points[n-1].Timestamp
	step := int64(time.Hour)
	if n > 1 {
		step = result.Points[n-1].Timestamp - result.Points[n-2].Timestamp
		if step <= 0 {
			step = int64(time.Hour)
		}
	}

	// z-value for confidence level (approximation)
	z := 1.96 // 95% default
	if e.config.ConfidenceLevel >= 0.99 {
		z = 2.576
	} else if e.config.ConfidenceLevel >= 0.95 {
		z = 1.96
	} else if e.config.ConfidenceLevel >= 0.90 {
		z = 1.645
	}

	predictions := make([]ForecastV2Point, horizon)
	for i := 0; i < horizon; i++ {
		idx := float64(n + i)
		trend := slope*idx + intercept

		// Add seasonal component
		si := 0.0
		if len(seasonal) > 0 {
			si = seasonal[(n+i)%len(seasonal)]
		}

		var predicted float64
		if mode == "multiplicative" {
			predicted = trend * si
		} else {
			predicted = trend + si
		}

		// Confidence interval widens with forecast horizon
		width := z * residualStd * math.Sqrt(1+float64(i+1)/float64(n))

		predictions[i] = ForecastV2Point{
			Timestamp: lastTS + int64(i+1)*step,
			Value:     predicted,
			Lower:     predicted - width,
			Upper:     predicted + width,
		}
	}

	// Compute model fit (R²) on the full decomposition
	modelFit := computeDecomposedRSquared(values, slope, intercept, seasonal, mode)

	e.mu.Lock()
	e.totalForecasts++
	e.totalHorizon += horizon
	e.totalModelFit += modelFit
	e.mu.Unlock()

	return &ForecastV2Result{
		Metric:          req.Metric,
		Predictions:     predictions,
		Changepoints:    changepoints,
		SeasonalPattern: seasonal,
		TrendSlope:      slope,
		ModelFit:        modelFit,
	}, nil
}

// DetectChangepoints identifies significant trend changes in the data.
func (e *ForecastV2Engine) DetectChangepoints(values []float64) []ForecastV2Changepoint {
	if len(values) < 4 {
		return nil
	}

	threshold := e.config.ChangepointThreshold
	var changepoints []ForecastV2Changepoint
	windowSize := len(values) / 4
	if windowSize < 2 {
		windowSize = 2
	}

	for i := windowSize; i < len(values)-windowSize; i++ {
		beforeSlope, _ := linearRegression(values[i-windowSize : i])
		afterSlope, _ := linearRegression(values[i : i+windowSize])
		magnitude := math.Abs(afterSlope - beforeSlope)

		if magnitude > threshold {
			changepoints = append(changepoints, ForecastV2Changepoint{
				Timestamp:   int64(i),
				TrendBefore: beforeSlope,
				TrendAfter:  afterSlope,
				Magnitude:   magnitude,
			})
		}
	}

	return changepoints
}

// Stats returns current forecasting statistics.
func (e *ForecastV2Engine) Stats() ForecastV2Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := ForecastV2Stats{
		TotalForecasts: e.totalForecasts,
	}
	if e.totalForecasts > 0 {
		stats.AvgHorizon = float64(e.totalHorizon) / float64(e.totalForecasts)
		stats.AvgModelFit = e.totalModelFit / float64(e.totalForecasts)
	}
	return stats
}

// RegisterHTTPHandlers registers forecast HTTP endpoints.
func (e *ForecastV2Engine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/forecast/v2/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

func linearRegression(values []float64) (slope, intercept float64) {
	n := float64(len(values))
	if n <= 1 {
		if n == 1 {
			return 0, values[0]
		}
		return 0, 0
	}

	var sumX, sumY, sumXY, sumX2 float64
	for i, v := range values {
		x := float64(i)
		sumX += x
		sumY += v
		sumXY += x * v
		sumX2 += x * x
	}

	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0, sumY / n
	}

	slope = (n*sumXY - sumX*sumY) / denom
	intercept = (sumY - slope*sumX) / n
	return
}

func computeRSquared(values []float64, slope, intercept float64) float64 {
	if len(values) <= 1 {
		return 1.0
	}

	var mean float64
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	var ssTot, ssRes float64
	for i, v := range values {
		predicted := slope*float64(i) + intercept
		ssTot += (v - mean) * (v - mean)
		ssRes += (v - predicted) * (v - predicted)
	}

	if ssTot == 0 {
		return 1.0
	}
	return 1.0 - ssRes/ssTot
}

func computeSeasonalPattern(values []float64) []float64 {
	period := detectSeasonalPeriod(values)
	return extractSeasonalComponent(values, period)
}

// detectSeasonalPeriod uses autocorrelation to find the dominant seasonal period.
// Falls back to reasonable defaults when the series is too short.
func detectSeasonalPeriod(values []float64) int {
	n := len(values)
	if n < 8 {
		return minInt(n, 4)
	}

	// Compute mean
	var mean float64
	for _, v := range values {
		mean += v
	}
	mean /= float64(n)

	// Compute autocorrelation for lags 2..n/2
	maxLag := n / 2
	if maxLag > 168 { // cap at 1 week of hourly data
		maxLag = 168
	}

	var var0 float64
	for _, v := range values {
		d := v - mean
		var0 += d * d
	}
	if var0 == 0 {
		return 4
	}

	bestLag := 4
	bestCorr := -2.0
	for lag := 2; lag <= maxLag; lag++ {
		var corr float64
		for i := 0; i < n-lag; i++ {
			corr += (values[i] - mean) * (values[i+lag] - mean)
		}
		corr /= var0
		if corr > bestCorr {
			bestCorr = corr
			bestLag = lag
		}
	}

	if bestCorr < 0.1 {
		return minInt(n, 4) // no strong seasonality detected
	}
	return bestLag
}

// extractSeasonalComponent averages values at each seasonal position to get the pattern.
func extractSeasonalComponent(values []float64, period int) []float64 {
	if period <= 0 || len(values) < period {
		return nil
	}

	seasonal := make([]float64, period)
	counts := make([]int, period)

	// Compute mean
	var mean float64
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	for i, v := range values {
		idx := i % period
		seasonal[idx] += v - mean
		counts[idx]++
	}

	for i := range seasonal {
		if counts[i] > 0 {
			seasonal[i] /= float64(counts[i])
		}
	}

	return seasonal
}

// computeResidualStd computes the standard deviation of residuals from the decomposed model.
func computeResidualStd(values []float64, slope, intercept float64, seasonal []float64, mode string) float64 {
	n := len(values)
	if n <= 2 {
		return 0
	}

	var sumSqResiduals float64
	for i, v := range values {
		trend := slope*float64(i) + intercept
		si := 0.0
		if len(seasonal) > 0 {
			si = seasonal[i%len(seasonal)]
		}
		var predicted float64
		if mode == "multiplicative" {
			predicted = trend * si
		} else {
			predicted = trend + si
		}
		r := v - predicted
		sumSqResiduals += r * r
	}

	return math.Sqrt(sumSqResiduals / float64(n-2))
}

// computeDecomposedRSquared computes R² for the trend+seasonal model.
func computeDecomposedRSquared(values []float64, slope, intercept float64, seasonal []float64, mode string) float64 {
	if len(values) <= 1 {
		return 1.0
	}

	var mean float64
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	var ssTot, ssRes float64
	for i, v := range values {
		trend := slope*float64(i) + intercept
		si := 0.0
		if len(seasonal) > 0 {
			si = seasonal[i%len(seasonal)]
		}
		var predicted float64
		if mode == "multiplicative" {
			predicted = trend * si
		} else {
			predicted = trend + si
		}
		ssTot += (v - mean) * (v - mean)
		ssRes += (v - predicted) * (v - predicted)
	}

	if ssTot == 0 {
		return 1.0
	}
	return 1.0 - ssRes/ssTot
}
