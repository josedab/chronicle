// Bridge: forecast.go
//
// Re-exports types from internal/forecast/ into the public chronicle package.
// Pattern: internal/forecast/ (implementation) → forecast.go (public API)

package chronicle

import "github.com/chronicle-db/chronicle/internal/forecast"

// Type aliases from internal/forecast.
type ForecastConfig = forecast.ForecastConfig
type ForecastMethod = forecast.ForecastMethod
type Forecaster = forecast.Forecaster
type ForecastResult = forecast.ForecastResult
type ForecastPoint = forecast.ForecastPoint
type AnomalyPoint = forecast.AnomalyPoint
type ForecastModel = forecast.ForecastModel
type TimeSeriesData = forecast.TimeSeriesData

// Forecast method constants from internal/forecast.
const (
ForecastMethodSimpleExponential = forecast.ForecastMethodSimpleExponential
ForecastMethodDoubleExponential = forecast.ForecastMethodDoubleExponential
ForecastMethodHoltWinters       = forecast.ForecastMethodHoltWinters
ForecastMethodMovingAverage     = forecast.ForecastMethodMovingAverage
)

// Constructor wrappers from internal/forecast.
var (
DefaultForecastConfig = forecast.DefaultForecastConfig
NewForecaster         = forecast.NewForecaster
)

// Internal helpers re-exported for root package use.
var (
	mean             = forecast.Mean
	StdDev           = forecast.StdDev
	EstimateInterval = forecast.EstimateInterval
)
