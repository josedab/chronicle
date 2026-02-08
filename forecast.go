package chronicle

import (
	"errors"
	"math"
	"sort"
	"time"
)

// ForecastConfig configures the forecasting engine.
type ForecastConfig struct {
	// Method is the forecasting algorithm to use.
	Method ForecastMethod

	// SeasonalPeriods is the number of points in a season (for Holt-Winters).
	SeasonalPeriods int

	// Alpha is the smoothing parameter for level (0-1).
	Alpha float64

	// Beta is the smoothing parameter for trend (0-1).
	Beta float64

	// Gamma is the smoothing parameter for seasonality (0-1).
	Gamma float64

	// AnomalyThreshold is the number of standard deviations for anomaly detection.
	AnomalyThreshold float64
}

// ForecastMethod identifies the forecasting algorithm.
type ForecastMethod int

const (
	// ForecastMethodSimpleExponential uses simple exponential smoothing.
	ForecastMethodSimpleExponential ForecastMethod = iota
	// ForecastMethodDoubleExponential uses double exponential (Holt's) smoothing.
	ForecastMethodDoubleExponential
	// ForecastMethodHoltWinters uses triple exponential (Holt-Winters) smoothing.
	ForecastMethodHoltWinters
	// ForecastMethodMovingAverage uses moving average.
	ForecastMethodMovingAverage
)

// DefaultForecastConfig returns default forecasting configuration.
func DefaultForecastConfig() ForecastConfig {
	return ForecastConfig{
		Method:           ForecastMethodHoltWinters,
		SeasonalPeriods:  12,
		Alpha:            0.5,
		Beta:             0.1,
		Gamma:            0.1,
		AnomalyThreshold: 3.0,
	}
}

// Forecaster provides time-series forecasting and anomaly detection.
//
// 🔬 BETA: API may evolve between minor versions with migration guidance.
// See api_stability.go for stability classifications.
type Forecaster struct {
	config ForecastConfig
}

// NewForecaster creates a new forecaster.
func NewForecaster(config ForecastConfig) *Forecaster {
	if config.Alpha <= 0 || config.Alpha >= 1 {
		config.Alpha = 0.5
	}
	if config.Beta < 0 || config.Beta >= 1 {
		config.Beta = 0.1
	}
	if config.Gamma < 0 || config.Gamma >= 1 {
		config.Gamma = 0.1
	}
	if config.SeasonalPeriods <= 0 {
		config.SeasonalPeriods = 12
	}
	if config.AnomalyThreshold <= 0 {
		config.AnomalyThreshold = 3.0
	}

	return &Forecaster{config: config}
}

// ForecastResult contains forecasting output.
type ForecastResult struct {
	// Predictions contains forecasted values.
	Predictions []ForecastPoint

	// Anomalies contains detected anomalies.
	Anomalies []AnomalyPoint

	// Model contains fitted model parameters.
	Model ForecastModel

	// RMSE is the root mean squared error of the fit.
	RMSE float64

	// MAE is the mean absolute error of the fit.
	MAE float64
}

// ForecastPoint is a predicted data point.
type ForecastPoint struct {
	Timestamp   int64
	Value       float64
	LowerBound  float64
	UpperBound  float64
	Confidence  float64
}

// AnomalyPoint is a detected anomaly.
type AnomalyPoint struct {
	Timestamp     int64
	ActualValue   float64
	ExpectedValue float64
	Deviation     float64
	Score         float64
}

// ForecastModel contains fitted model parameters.
type ForecastModel struct {
	Level       float64
	Trend       float64
	Seasonality []float64
}

// TimeSeriesData represents input data for forecasting.
type TimeSeriesData struct {
	Timestamps []int64
	Values     []float64
}

// Forecast generates predictions based on historical data.
func (f *Forecaster) Forecast(data TimeSeriesData, periods int) (*ForecastResult, error) {
	if len(data.Values) < 2 {
		return nil, errors.New("insufficient data for forecasting")
	}
	if periods <= 0 {
		return nil, errors.New("periods must be positive")
	}

	switch f.config.Method {
	case ForecastMethodSimpleExponential:
		return f.simpleExponentialSmoothing(data, periods)
	case ForecastMethodDoubleExponential:
		return f.doubleExponentialSmoothing(data, periods)
	case ForecastMethodHoltWinters:
		return f.holtWinters(data, periods)
	case ForecastMethodMovingAverage:
		return f.movingAverage(data, periods)
	default:
		return f.holtWinters(data, periods)
	}
}

// simpleExponentialSmoothing performs simple exponential smoothing.
func (f *Forecaster) simpleExponentialSmoothing(data TimeSeriesData, periods int) (*ForecastResult, error) {
	values := data.Values
	alpha := f.config.Alpha

	// Initialize
	level := values[0]
	fitted := make([]float64, len(values))
	fitted[0] = level

	// Fit
	for i := 1; i < len(values); i++ {
		level = alpha*values[i] + (1-alpha)*level
		fitted[i] = level
	}

	// Forecast
	predictions := make([]ForecastPoint, periods)
	lastTs := data.Timestamps[len(data.Timestamps)-1]
	interval := estimateInterval(data.Timestamps)
	std := stdDev(values, fitted)

	for i := 0; i < periods; i++ {
		ts := lastTs + int64(i+1)*interval
		predictions[i] = ForecastPoint{
			Timestamp:  ts,
			Value:      level,
			LowerBound: level - f.config.AnomalyThreshold*std,
			UpperBound: level + f.config.AnomalyThreshold*std,
			Confidence: 0.95,
		}
	}

	return &ForecastResult{
		Predictions: predictions,
		Anomalies:   f.detectAnomalies(data, fitted),
		Model:       ForecastModel{Level: level},
		RMSE:        rmse(values, fitted),
		MAE:         mae(values, fitted),
	}, nil
}

// doubleExponentialSmoothing performs Holt's double exponential smoothing.
func (f *Forecaster) doubleExponentialSmoothing(data TimeSeriesData, periods int) (*ForecastResult, error) {
	values := data.Values
	alpha := f.config.Alpha
	beta := f.config.Beta

	// Initialize
	level := values[0]
	trend := values[1] - values[0]
	fitted := make([]float64, len(values))
	fitted[0] = level

	// Fit
	for i := 1; i < len(values); i++ {
		prevLevel := level
		level = alpha*values[i] + (1-alpha)*(prevLevel+trend)
		trend = beta*(level-prevLevel) + (1-beta)*trend
		fitted[i] = level + trend
	}

	// Forecast
	predictions := make([]ForecastPoint, periods)
	lastTs := data.Timestamps[len(data.Timestamps)-1]
	interval := estimateInterval(data.Timestamps)
	std := stdDev(values, fitted)

	for i := 0; i < periods; i++ {
		ts := lastTs + int64(i+1)*interval
		forecast := level + float64(i+1)*trend
		uncertainty := std * math.Sqrt(float64(i+1))
		predictions[i] = ForecastPoint{
			Timestamp:  ts,
			Value:      forecast,
			LowerBound: forecast - f.config.AnomalyThreshold*uncertainty,
			UpperBound: forecast + f.config.AnomalyThreshold*uncertainty,
			Confidence: 0.95,
		}
	}

	return &ForecastResult{
		Predictions: predictions,
		Anomalies:   f.detectAnomalies(data, fitted),
		Model:       ForecastModel{Level: level, Trend: trend},
		RMSE:        rmse(values, fitted),
		MAE:         mae(values, fitted),
	}, nil
}

// holtWinters performs Holt-Winters triple exponential smoothing.
// This algorithm decomposes time series into three components:
//   - Level (L): The baseline value of the series
//   - Trend (T): The rate of change of the series
//   - Seasonality (S): Repeating patterns at fixed intervals
//
// The update equations are:
//   L_t = α(Y_t/S_{t-m}) + (1-α)(L_{t-1} + T_{t-1})
//   T_t = β(L_t - L_{t-1}) + (1-β)T_{t-1}
//   S_t = γ(Y_t/L_t) + (1-γ)S_{t-m}
//
// Where m is the seasonal period (e.g., 12 for monthly data with yearly seasonality).
func (f *Forecaster) holtWinters(data TimeSeriesData, periods int) (*ForecastResult, error) {
	values := data.Values
	m := f.config.SeasonalPeriods

	// Need at least 2 complete seasons to estimate initial seasonality
	if len(values) < 2*m {
		// Fall back to double exponential
		return f.doubleExponentialSmoothing(data, periods)
	}

	alpha := f.config.Alpha // Level smoothing (higher = more weight to recent)
	beta := f.config.Beta   // Trend smoothing
	gamma := f.config.Gamma // Seasonal smoothing

	// Initialize level as mean of first season
	// Initialize trend as average change between first two seasons
	level := mean(values[:m])
	trend := (mean(values[m:2*m]) - mean(values[:m])) / float64(m)

	// Initialize seasonality indices as ratio of actual to level
	seasonality := make([]float64, m)
	for i := 0; i < m; i++ {
		seasonality[i] = values[i] / level
	}

	// Fit model: update level, trend, and seasonality for each observation
	fitted := make([]float64, len(values))
	for i := 0; i < m; i++ {
		fitted[i] = (level + float64(i)*trend) * seasonality[i%m]
	}

	for i := m; i < len(values); i++ {
		prevLevel := level
		seasonIdx := i % m

		// Update equations with multiplicative seasonality
		level = alpha*(values[i]/seasonality[seasonIdx]) + (1-alpha)*(prevLevel+trend)
		trend = beta*(level-prevLevel) + (1-beta)*trend
		seasonality[seasonIdx] = gamma*(values[i]/level) + (1-gamma)*seasonality[seasonIdx]
		fitted[i] = (level + trend) * seasonality[seasonIdx]
	}

	// Generate forecasts with expanding confidence intervals
	predictions := make([]ForecastPoint, periods)
	lastTs := data.Timestamps[len(data.Timestamps)-1]
	interval := estimateInterval(data.Timestamps)
	std := stdDev(values, fitted)

	for i := 0; i < periods; i++ {
		ts := lastTs + int64(i+1)*interval
		seasonIdx := (len(values) + i) % m
		forecast := (level + float64(i+1)*trend) * seasonality[seasonIdx]
		// Uncertainty grows with forecast horizon (sqrt of periods ahead)
		uncertainty := std * math.Sqrt(float64(i+1))
		predictions[i] = ForecastPoint{
			Timestamp:  ts,
			Value:      forecast,
			LowerBound: forecast - f.config.AnomalyThreshold*uncertainty,
			UpperBound: forecast + f.config.AnomalyThreshold*uncertainty,
			Confidence: 0.95,
		}
	}

	return &ForecastResult{
		Predictions: predictions,
		Anomalies:   f.detectAnomalies(data, fitted),
		Model: ForecastModel{
			Level:       level,
			Trend:       trend,
			Seasonality: seasonality,
		},
		RMSE: rmse(values, fitted),
		MAE:  mae(values, fitted),
	}, nil
}

// movingAverage performs simple moving average forecasting.
func (f *Forecaster) movingAverage(data TimeSeriesData, periods int) (*ForecastResult, error) {
	values := data.Values
	window := f.config.SeasonalPeriods
	if window > len(values) {
		window = len(values)
	}

	// Calculate moving average for fitting
	fitted := make([]float64, len(values))
	for i := 0; i < len(values); i++ {
		start := i - window + 1
		if start < 0 {
			start = 0
		}
		fitted[i] = mean(values[start : i+1])
	}

	// Forecast using last window
	avgValue := mean(values[len(values)-window:])
	predictions := make([]ForecastPoint, periods)
	lastTs := data.Timestamps[len(data.Timestamps)-1]
	interval := estimateInterval(data.Timestamps)
	std := stdDev(values, fitted)

	for i := 0; i < periods; i++ {
		ts := lastTs + int64(i+1)*interval
		predictions[i] = ForecastPoint{
			Timestamp:  ts,
			Value:      avgValue,
			LowerBound: avgValue - f.config.AnomalyThreshold*std,
			UpperBound: avgValue + f.config.AnomalyThreshold*std,
			Confidence: 0.95,
		}
	}

	return &ForecastResult{
		Predictions: predictions,
		Anomalies:   f.detectAnomalies(data, fitted),
		Model:       ForecastModel{Level: avgValue},
		RMSE:        rmse(values, fitted),
		MAE:         mae(values, fitted),
	}, nil
}

// detectAnomalies identifies points that deviate significantly from expected values.
func (f *Forecaster) detectAnomalies(data TimeSeriesData, fitted []float64) []AnomalyPoint {
	var anomalies []AnomalyPoint
	std := stdDev(data.Values, fitted)
	threshold := f.config.AnomalyThreshold * std

	for i, actual := range data.Values {
		expected := fitted[i]
		deviation := actual - expected
		if math.Abs(deviation) > threshold {
			score := math.Abs(deviation) / std
			anomalies = append(anomalies, AnomalyPoint{
				Timestamp:     data.Timestamps[i],
				ActualValue:   actual,
				ExpectedValue: expected,
				Deviation:     deviation,
				Score:         score,
			})
		}
	}

	return anomalies
}

// DetectAnomalies performs standalone anomaly detection on data.
func (f *Forecaster) DetectAnomalies(data TimeSeriesData) ([]AnomalyPoint, error) {
	if len(data.Values) < 2 {
		return nil, errors.New("insufficient data for anomaly detection")
	}

	// Fit the model
	result, err := f.Forecast(data, 1)
	if err != nil {
		return nil, err
	}

	return result.Anomalies, nil
}

// Helper functions

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func stdDev(actual, fitted []float64) float64 {
	if len(actual) == 0 {
		return 0
	}
	sumSq := 0.0
	for i, a := range actual {
		diff := a - fitted[i]
		sumSq += diff * diff
	}
	return math.Sqrt(sumSq / float64(len(actual)))
}

func rmse(actual, fitted []float64) float64 {
	return stdDev(actual, fitted)
}

func mae(actual, fitted []float64) float64 {
	if len(actual) == 0 {
		return 0
	}
	sum := 0.0
	for i, a := range actual {
		sum += math.Abs(a - fitted[i])
	}
	return sum / float64(len(actual))
}

func estimateInterval(timestamps []int64) int64 {
	if len(timestamps) < 2 {
		return int64(time.Minute)
	}

	// Sort timestamps
	sorted := make([]int64, len(timestamps))
	copy(sorted, timestamps)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	// Calculate median interval
	intervals := make([]int64, len(sorted)-1)
	for i := 1; i < len(sorted); i++ {
		intervals[i-1] = sorted[i] - sorted[i-1]
	}

	sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })
	return intervals[len(intervals)/2]
}
