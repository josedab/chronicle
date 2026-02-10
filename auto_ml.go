package chronicle

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// AutoMLConfig configures the Auto-ML forecasting engine.
type AutoMLConfig struct {
	// EnableAutoSelection enables automatic model selection based on data characteristics
	EnableAutoSelection bool

	// EnableEnsemble enables ensemble methods that combine multiple models
	EnableEnsemble bool

	// CrossValidationFolds is the number of folds for cross-validation (0 = no CV)
	CrossValidationFolds int

	// HoldoutRatio is the fraction of data reserved for model evaluation (0.1-0.3)
	HoldoutRatio float64

	// MaxModelsToEvaluate limits how many model variants to test
	MaxModelsToEvaluate int

	// MinDataPoints is the minimum data points required for forecasting
	MinDataPoints int

	// CacheModels enables caching of trained models
	CacheModels bool

	// CacheTTL is how long cached models remain valid
	CacheTTL time.Duration

	// SeasonalityDetection enables automatic seasonality period detection
	SeasonalityDetection bool

	// TrendDetection enables automatic trend detection
	TrendDetection bool
}

// DefaultAutoMLConfig returns sensible defaults for Auto-ML forecasting.
func DefaultAutoMLConfig() AutoMLConfig {
	return AutoMLConfig{
		EnableAutoSelection:  true,
		EnableEnsemble:       false,
		CrossValidationFolds: 3,
		HoldoutRatio:         0.2,
		MaxModelsToEvaluate:  10,
		MinDataPoints:        15,
		CacheModels:          true,
		CacheTTL:             time.Hour,
		SeasonalityDetection: true,
		TrendDetection:       true,
	}
}

// AutoMLModel represents a model candidate for Auto-ML selection.
type AutoMLModel struct {
	// Name is the model identifier
	Name string

	// Method is the forecasting method
	Method ForecastMethod

	// Config contains model-specific parameters
	Config ForecastConfig

	// Score is the evaluation score (lower is better for error metrics)
	Score float64

	// Metrics contains detailed evaluation metrics
	Metrics AutoMLMetrics
}

// AutoMLMetrics contains evaluation metrics for model selection.
type AutoMLMetrics struct {
	// RMSE is Root Mean Squared Error
	RMSE float64

	// MAE is Mean Absolute Error
	MAE float64

	// MAPE is Mean Absolute Percentage Error (0-100)
	MAPE float64

	// R2 is coefficient of determination
	R2 float64

	// AIC is Akaike Information Criterion
	AIC float64

	// BIC is Bayesian Information Criterion
	BIC float64

	// CrossValidationScore is the average CV score
	CrossValidationScore float64
}

// AutoMLResult contains the results of Auto-ML model selection and forecasting.
type AutoMLResult struct {
	// BestModel is the selected model
	BestModel AutoMLModel

	// CandidateModels lists all evaluated models with scores
	CandidateModels []AutoMLModel

	// Forecast contains the generated predictions
	Forecast *ForecastResult

	// TimeSeriesCharacteristics describes detected data properties
	TimeSeriesCharacteristics TimeSeriesCharacteristics

	// TrainingTime is how long model selection took
	TrainingTime time.Duration

	// DataPoints is the number of points used
	DataPoints int
}

// TimeSeriesCharacteristics describes properties detected in the time series for Auto-ML.
type TimeSeriesCharacteristics struct {
	// HasTrend indicates if a significant trend was detected
	HasTrend bool

	// TrendDirection is 1 (up), -1 (down), or 0 (none)
	TrendDirection int

	// TrendStrength is the R² of the linear fit (0-1)
	TrendStrength float64

	// HasSeasonality indicates if seasonality was detected
	HasSeasonality bool

	// SeasonalPeriod is the detected seasonal period
	SeasonalPeriod int

	// SeasonalStrength measures seasonality prominence (0-1)
	SeasonalStrength float64

	// IsStationary indicates if the series appears stationary
	IsStationary bool

	// Volatility is the coefficient of variation
	Volatility float64

	// SeriesMean is the average value
	SeriesMean float64

	// SeriesStd is the standard deviation
	SeriesStd float64
}

// AutoMLForecaster provides automatic model selection and forecasting.
type AutoMLForecaster struct {
	db         *DB
	config     AutoMLConfig
	modelCache map[string]*cachedAutoMLModel
	cacheMu    sync.RWMutex
}

type cachedAutoMLModel struct {
	model     *AutoMLModel
	trainedAt time.Time
	dataHash  uint64
}

// NewAutoMLForecaster creates a new Auto-ML forecasting engine.
func NewAutoMLForecaster(db *DB, config AutoMLConfig) *AutoMLForecaster {
	return &AutoMLForecaster{
		db:         db,
		config:     config,
		modelCache: make(map[string]*cachedAutoMLModel),
	}
}

// AutoForecast performs automatic model selection and forecasting.
func (a *AutoMLForecaster) AutoForecast(ctx context.Context, metric string, periods int, tags map[string]string) (*AutoMLResult, error) {
	startTime := time.Now()

	if metric == "" {
		return nil, errors.New("metric name is required")
	}
	if periods <= 0 {
		periods = 24 // Default horizon
	}

	// Fetch data
	data, err := a.fetchData(ctx, metric, tags)
	if err != nil {
		return nil, err
	}

	if len(data.Values) < a.config.MinDataPoints {
		return nil, errors.New("insufficient data for forecasting")
	}

	// Analyze data characteristics
	chars := a.analyzeTimeSeriesCharacteristics(data)

	// Generate candidate models
	candidates := a.generateCandidateModels(data, chars)

	// Evaluate candidates
	evaluatedModels := a.evaluateModels(data, candidates)

	// Sort by score (ascending - lower is better)
	sort.Slice(evaluatedModels, func(i, j int) bool {
		return evaluatedModels[i].Score < evaluatedModels[j].Score
	})

	if len(evaluatedModels) == 0 {
		return nil, errors.New("no valid models found")
	}

	// Select best model
	bestModel := evaluatedModels[0]

	// Generate forecast with best model
	forecaster := NewForecaster(bestModel.Config)
	forecast, err := forecaster.Forecast(data, periods)
	if err != nil {
		return nil, err
	}

	return &AutoMLResult{
		BestModel:                 bestModel,
		CandidateModels:           evaluatedModels,
		Forecast:                  forecast,
		TimeSeriesCharacteristics: chars,
		TrainingTime:              time.Since(startTime),
		DataPoints:                len(data.Values),
	}, nil
}

func (a *AutoMLForecaster) fetchData(ctx context.Context, metric string, tags map[string]string) (TimeSeriesData, error) {
	q := &Query{
		Metric: metric,
		Tags:   tags,
		Limit:  10000, // Reasonable limit
	}

	result, err := a.db.ExecuteContext(ctx, q)
	if err != nil {
		return TimeSeriesData{}, err
	}

	if result == nil || len(result.Points) == 0 {
		return TimeSeriesData{}, errors.New("no data found for metric")
	}

	points := result.Points

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	data := TimeSeriesData{
		Timestamps: make([]int64, len(points)),
		Values:     make([]float64, len(points)),
	}

	for i, p := range points {
		data.Timestamps[i] = p.Timestamp
		data.Values[i] = p.Value
	}

	return data, nil
}

func (a *AutoMLForecaster) analyzeTimeSeriesCharacteristics(data TimeSeriesData) TimeSeriesCharacteristics {
	n := len(data.Values)
	if n == 0 {
		return TimeSeriesCharacteristics{}
	}

	// Calculate basic statistics
	dataMean := mean(data.Values)
	dataStd := a.calculateStd(data.Values, dataMean)
	volatility := 0.0
	if dataMean != 0 {
		volatility = dataStd / math.Abs(dataMean)
	}

	chars := TimeSeriesCharacteristics{
		SeriesMean: dataMean,
		SeriesStd:  dataStd,
		Volatility: volatility,
	}

	// Detect trend
	if a.config.TrendDetection && n >= 10 {
		chars.HasTrend, chars.TrendDirection, chars.TrendStrength = a.detectTrend(data)
	}

	// Detect seasonality
	if a.config.SeasonalityDetection && n >= 20 {
		chars.HasSeasonality, chars.SeasonalPeriod, chars.SeasonalStrength = a.detectSeasonality(data)
	}

	// Check stationarity
	chars.IsStationary = a.checkStationarity(data)

	return chars
}

func (a *AutoMLForecaster) calculateStd(values []float64, mean float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sumSq float64
	for _, v := range values {
		diff := v - mean
		sumSq += diff * diff
	}
	return math.Sqrt(sumSq / float64(len(values)))
}

func (a *AutoMLForecaster) detectTrend(data TimeSeriesData) (hasTrend bool, direction int, strength float64) {
	n := len(data.Values)
	if n < 3 {
		return false, 0, 0
	}

	// Linear regression
	var sumX, sumY, sumXY, sumX2 float64
	for i, y := range data.Values {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	nf := float64(n)
	slope := (nf*sumXY - sumX*sumY) / (nf*sumX2 - sumX*sumX)
	intercept := (sumY - slope*sumX) / nf

	// Calculate R²
	var ssTot, ssRes float64
	yMean := sumY / nf
	for i, y := range data.Values {
		pred := intercept + slope*float64(i)
		ssRes += (y - pred) * (y - pred)
		ssTot += (y - yMean) * (y - yMean)
	}

	r2 := 0.0
	if ssTot > 0 {
		r2 = 1 - ssRes/ssTot
	}

	// Determine trend significance
	hasTrend = r2 > 0.3 && math.Abs(slope) > 0.001*yMean

	direction = 0
	if hasTrend {
		if slope > 0 {
			direction = 1
		} else {
			direction = -1
		}
	}

	return hasTrend, direction, r2
}

func (a *AutoMLForecaster) detectSeasonality(data TimeSeriesData) (hasSeason bool, period int, strength float64) {
	n := len(data.Values)
	if n < 10 {
		return false, 0, 0
	}

	values := data.Values
	dataMean := mean(values)
	dataVar := a.calculateVariance(values, dataMean)

	if dataVar < 1e-10 {
		return false, 0, 0
	}

	// Compute autocorrelation for various lags
	maxLag := minInt(n/2, 50)
	autocorr := make([]float64, maxLag)

	for lag := 1; lag < maxLag; lag++ {
		var sum float64
		for i := 0; i < n-lag; i++ {
			sum += (values[i] - dataMean) * (values[i+lag] - dataMean)
		}
		autocorr[lag] = sum / (float64(n-lag) * dataVar)
	}

	// Find peaks in autocorrelation
	bestPeriod := 0
	bestCorr := 0.0
	threshold := 0.3

	for lag := 2; lag < maxLag-1; lag++ {
		// Check if it's a peak
		if autocorr[lag] > autocorr[lag-1] && autocorr[lag] > autocorr[lag+1] {
			if autocorr[lag] > bestCorr && autocorr[lag] > threshold {
				bestCorr = autocorr[lag]
				bestPeriod = lag
			}
		}
	}

	if bestPeriod > 0 {
		return true, bestPeriod, bestCorr
	}

	return false, 0, 0
}

func (a *AutoMLForecaster) calculateVariance(values []float64, mean float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sumSq float64
	for _, v := range values {
		diff := v - mean
		sumSq += diff * diff
	}
	return sumSq / float64(len(values))
}

func (a *AutoMLForecaster) checkStationarity(data TimeSeriesData) bool {
	n := len(data.Values)
	if n < 30 {
		return true // Insufficient data to determine
	}

	// Split into thirds and compare means/variances
	third := n / 3
	values := data.Values

	mean1 := mean(values[:third])
	mean2 := mean(values[third : 2*third])
	mean3 := mean(values[2*third:])

	var1 := a.calculateVariance(values[:third], mean1)
	var2 := a.calculateVariance(values[third:2*third], mean2)
	var3 := a.calculateVariance(values[2*third:], mean3)

	// Check if means are within 20% of each other
	avgMean := (mean1 + mean2 + mean3) / 3
	meanTolerance := 0.2 * math.Abs(avgMean)
	if meanTolerance < 1e-10 {
		meanTolerance = 0.1
	}

	meanStable := math.Abs(mean1-avgMean) < meanTolerance &&
		math.Abs(mean2-avgMean) < meanTolerance &&
		math.Abs(mean3-avgMean) < meanTolerance

	// Check if variances are within factor of 2
	avgVar := (var1 + var2 + var3) / 3
	varStable := true
	if avgVar > 1e-10 {
		varStable = var1 < 2*avgVar && var2 < 2*avgVar && var3 < 2*avgVar &&
			var1 > avgVar/2 && var2 > avgVar/2 && var3 > avgVar/2
	}

	return meanStable && varStable
}

func (a *AutoMLForecaster) generateCandidateModels(data TimeSeriesData, chars TimeSeriesCharacteristics) []AutoMLModel {
	candidates := []AutoMLModel{}
	n := len(data.Values)

	// Simple Exponential Smoothing - always a candidate
	for _, alpha := range []float64{0.2, 0.5, 0.8} {
		candidates = append(candidates, AutoMLModel{
			Name:   "SimpleExponential",
			Method: ForecastMethodSimpleExponential,
			Config: ForecastConfig{
				Method: ForecastMethodSimpleExponential,
				Alpha:  alpha,
			},
		})
	}

	// Double Exponential (Holt's) - if trend detected
	if chars.HasTrend || n >= 20 {
		for _, alpha := range []float64{0.3, 0.6} {
			for _, beta := range []float64{0.1, 0.3} {
				candidates = append(candidates, AutoMLModel{
					Name:   "DoubleExponential",
					Method: ForecastMethodDoubleExponential,
					Config: ForecastConfig{
						Method: ForecastMethodDoubleExponential,
						Alpha:  alpha,
						Beta:   beta,
					},
				})
			}
		}
	}

	// Holt-Winters - if seasonality detected or enough data
	if chars.HasSeasonality || n >= 40 {
		period := chars.SeasonalPeriod
		if period == 0 {
			period = 12 // Default
		}

		// Only if we have enough data for the seasonal period
		if n >= 2*period {
			for _, alpha := range []float64{0.3, 0.5} {
				candidates = append(candidates, AutoMLModel{
					Name:   "HoltWinters",
					Method: ForecastMethodHoltWinters,
					Config: ForecastConfig{
						Method:          ForecastMethodHoltWinters,
						Alpha:           alpha,
						Beta:            0.1,
						Gamma:           0.1,
						SeasonalPeriods: period,
					},
				})
			}
		}
	}

	// Moving Average - good baseline
	for _, window := range []int{5, 10, 20} {
		if window < n {
			candidates = append(candidates, AutoMLModel{
				Name:   "MovingAverage",
				Method: ForecastMethodMovingAverage,
				Config: ForecastConfig{
					Method:          ForecastMethodMovingAverage,
					SeasonalPeriods: window,
				},
			})
		}
	}

	// Limit candidates
	if len(candidates) > a.config.MaxModelsToEvaluate {
		candidates = candidates[:a.config.MaxModelsToEvaluate]
	}

	return candidates
}

func (a *AutoMLForecaster) evaluateModels(data TimeSeriesData, candidates []AutoMLModel) []AutoMLModel {
	n := len(data.Values)
	if n < 10 {
		return candidates
	}

	// Split data for holdout validation
	holdoutSize := int(float64(n) * a.config.HoldoutRatio)
	if holdoutSize < 2 {
		holdoutSize = 2
	}
	trainSize := n - holdoutSize

	trainData := TimeSeriesData{
		Timestamps: data.Timestamps[:trainSize],
		Values:     data.Values[:trainSize],
	}
	testData := data.Values[trainSize:]

	evaluated := make([]AutoMLModel, 0, len(candidates))

	for _, candidate := range candidates {
		forecaster := NewForecaster(candidate.Config)

		// Train and forecast
		result, err := forecaster.Forecast(trainData, holdoutSize)
		if err != nil {
			continue
		}

		// Calculate metrics
		metrics := a.calculateMetrics(testData, result.Predictions)
		candidate.Metrics = metrics
		candidate.Score = metrics.RMSE // Use RMSE as primary score

		// Cross-validation if enabled
		if a.config.CrossValidationFolds > 1 && n >= a.config.CrossValidationFolds*5 {
			cvScore := a.crossValidate(data, candidate.Config, a.config.CrossValidationFolds)
			candidate.Metrics.CrossValidationScore = cvScore
			// Blend holdout and CV scores
			candidate.Score = 0.7*metrics.RMSE + 0.3*cvScore
		}

		evaluated = append(evaluated, candidate)
	}

	return evaluated
}

func (a *AutoMLForecaster) calculateMetrics(actual []float64, predictions []ForecastPoint) AutoMLMetrics {
	n := len(actual)
	if n == 0 || len(predictions) == 0 {
		return AutoMLMetrics{}
	}

	useN := minInt(n, len(predictions))

	var sumSqErr, sumAbsErr, sumAbsPctErr float64
	var ssTot, ssRes float64
	actMean := mean(actual[:useN])

	for i := 0; i < useN; i++ {
		act := actual[i]
		pred := predictions[i].Value
		err := act - pred

		sumSqErr += err * err
		sumAbsErr += math.Abs(err)
		if act != 0 {
			sumAbsPctErr += math.Abs(err/act) * 100
		}

		ssTot += (act - actMean) * (act - actMean)
		ssRes += err * err
	}

	useNf := float64(useN)
	mse := sumSqErr / useNf

	r2 := 0.0
	if ssTot > 0 {
		r2 = 1 - ssRes/ssTot
	}

	// AIC and BIC approximations
	k := 2.0 // Simplified: assume 2 parameters
	aic := useNf*math.Log(mse) + 2*k
	bic := useNf*math.Log(mse) + k*math.Log(useNf)

	return AutoMLMetrics{
		RMSE: math.Sqrt(mse),
		MAE:  sumAbsErr / useNf,
		MAPE: sumAbsPctErr / useNf,
		R2:   r2,
		AIC:  aic,
		BIC:  bic,
	}
}

func (a *AutoMLForecaster) crossValidate(data TimeSeriesData, config ForecastConfig, folds int) float64 {
	n := len(data.Values)
	foldSize := n / folds

	if foldSize < 5 {
		return math.MaxFloat64
	}

	var totalScore float64
	validFolds := 0

	for fold := 0; fold < folds-1; fold++ {
		// Time series cross-validation: train on past, test on future
		trainEnd := (fold + 1) * foldSize
		testEnd := trainEnd + foldSize
		if testEnd > n {
			testEnd = n
		}

		trainData := TimeSeriesData{
			Timestamps: data.Timestamps[:trainEnd],
			Values:     data.Values[:trainEnd],
		}
		testData := data.Values[trainEnd:testEnd]

		forecaster := NewForecaster(config)
		result, err := forecaster.Forecast(trainData, len(testData))
		if err != nil {
			continue
		}

		metrics := a.calculateMetrics(testData, result.Predictions)
		totalScore += metrics.RMSE
		validFolds++
	}

	if validFolds == 0 {
		return math.MaxFloat64
	}

	return totalScore / float64(validFolds)
}

// SelectBestModel chooses the optimal model without generating forecasts.
func (a *AutoMLForecaster) SelectBestModel(ctx context.Context, metric string, tags map[string]string) (*AutoMLModel, *TimeSeriesCharacteristics, error) {
	data, err := a.fetchData(ctx, metric, tags)
	if err != nil {
		return nil, nil, err
	}

	chars := a.analyzeTimeSeriesCharacteristics(data)
	candidates := a.generateCandidateModels(data, chars)
	evaluated := a.evaluateModels(data, candidates)

	if len(evaluated) == 0 {
		return nil, nil, errors.New("no valid models found")
	}

	sort.Slice(evaluated, func(i, j int) bool {
		return evaluated[i].Score < evaluated[j].Score
	})

	return &evaluated[0], &chars, nil
}

// AnalyzeTimeSeries returns characteristics without forecasting.
func (a *AutoMLForecaster) AnalyzeTimeSeries(ctx context.Context, metric string, tags map[string]string) (*TimeSeriesCharacteristics, error) {
	data, err := a.fetchData(ctx, metric, tags)
	if err != nil {
		return nil, err
	}

	chars := a.analyzeTimeSeriesCharacteristics(data)
	return &chars, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
