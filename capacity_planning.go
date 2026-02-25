package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// CapacityPlanningConfig configures the predictive capacity planning system.
type CapacityPlanningConfig struct {
	// Enabled turns on capacity planning
	Enabled bool

	// MetricsCollectionInterval for gathering usage metrics
	MetricsCollectionInterval time.Duration

	// ForecastHorizon for predictions
	ForecastHorizon time.Duration

	// HistoryWindow for training forecasts
	HistoryWindow time.Duration

	// RecommendationInterval for generating recommendations
	RecommendationInterval time.Duration

	// AutoTuneEnabled allows automatic configuration adjustments
	AutoTuneEnabled bool

	// SafetyMargin percentage for recommendations (e.g., 0.2 = 20%)
	SafetyMargin float64

	// AlertThreshold for capacity alerts (0-1, percentage of limit)
	AlertThreshold float64

	// MinDataPoints required for forecasting
	MinDataPoints int
}

// DefaultCapacityPlanningConfig returns default configuration.
func DefaultCapacityPlanningConfig() CapacityPlanningConfig {
	return CapacityPlanningConfig{
		Enabled:                   true,
		MetricsCollectionInterval: 1 * time.Minute,
		ForecastHorizon:           7 * 24 * time.Hour,  // 7 days
		HistoryWindow:             30 * 24 * time.Hour, // 30 days
		RecommendationInterval:    1 * time.Hour,
		AutoTuneEnabled:           false,
		SafetyMargin:              0.2,
		AlertThreshold:            0.8,
		MinDataPoints:             100,
	}
}

// CapacityMetric represents a capacity-related metric.
type CapacityMetric struct {
	Name      string    `json:"name"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit"`
	Timestamp time.Time `json:"timestamp"`
	Limit     float64   `json:"limit,omitempty"`
}

// ResourceUsage represents current resource utilization.
type ResourceUsage struct {
	Timestamp      time.Time          `json:"timestamp"`
	StorageBytes   int64              `json:"storage_bytes"`
	StorageLimit   int64              `json:"storage_limit,omitempty"`
	MemoryBytes    int64              `json:"memory_bytes"`
	MemoryLimit    int64              `json:"memory_limit,omitempty"`
	PointsCount    int64              `json:"points_count"`
	PointsLimit    int64              `json:"points_limit,omitempty"`
	SeriesCount    int64              `json:"series_count"`
	SeriesLimit    int64              `json:"series_limit,omitempty"`
	QueryRate      float64            `json:"query_rate"` // queries/sec
	WriteRate      float64            `json:"write_rate"` // points/sec
	PartitionCount int                `json:"partition_count"`
	CustomMetrics  map[string]float64 `json:"custom_metrics,omitempty"`
}

// CapacityForecast represents a predicted future state.
type CapacityForecast struct {
	Metric           string         `json:"metric"`
	ForecastTime     time.Time      `json:"forecast_time"`
	PredictedValue   float64        `json:"predicted_value"`
	LowerBound       float64        `json:"lower_bound"`
	UpperBound       float64        `json:"upper_bound"`
	Confidence       float64        `json:"confidence"`
	Trend            TrendDirection `json:"trend"`
	TimeToExhaustion *time.Duration `json:"time_to_exhaustion,omitempty"`
}

// TrendDirection indicates the direction of a trend.
type TrendDirection string

const (
	TrendUp      TrendDirection = "up"
	TrendDown    TrendDirection = "down"
	TrendStable  TrendDirection = "stable"
	TrendUnknown TrendDirection = "unknown"
)

// CapacityRecommendation represents a suggested action.
type CapacityRecommendation struct {
	ID             string                 `json:"id"`
	Priority       RecommendationPriority `json:"priority"`
	Category       RecommendationCategory `json:"category"`
	Title          string                 `json:"title"`
	Description    string                 `json:"description"`
	Metric         string                 `json:"metric"`
	CurrentValue   float64                `json:"current_value"`
	SuggestedValue float64                `json:"suggested_value"`
	Impact         string                 `json:"impact"`
	Effort         string                 `json:"effort"`
	AutoApply      bool                   `json:"auto_apply"`
	AppliedAt      *time.Time             `json:"applied_at,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
}

// RecommendationPriority indicates urgency.
type RecommendationPriority string

const (
	PriorityCritical RecommendationPriority = "critical"
	PriorityHigh     RecommendationPriority = "high"
	PriorityMedium   RecommendationPriority = "medium"
	PriorityLow      RecommendationPriority = "low"
)

// RecommendationCategory categorizes recommendations.
type RecommendationCategory string

const (
	CategoryStorage     RecommendationCategory = "storage"
	CategoryRetention   RecommendationCategory = "retention"
	CategoryPartition   RecommendationCategory = "partition"
	CategoryDownsample  RecommendationCategory = "downsample"
	CategoryCompression RecommendationCategory = "compression"
	CategoryMemory      RecommendationCategory = "memory"
	CategoryQuery       RecommendationCategory = "query"
)

// CapacityAlert represents a capacity-related alert.
type CapacityAlert struct {
	ID           string     `json:"id"`
	Severity     string     `json:"severity"`
	Metric       string     `json:"metric"`
	Message      string     `json:"message"`
	Value        float64    `json:"value"`
	Threshold    float64    `json:"threshold"`
	Triggered    time.Time  `json:"triggered"`
	Resolved     *time.Time `json:"resolved,omitempty"`
	Acknowledged bool       `json:"acknowledged"`
}

// CapacityPlanningEngine provides predictive capacity planning.
type CapacityPlanningEngine struct {
	db     *DB
	config CapacityPlanningConfig

	// Usage history
	usageHistory []ResourceUsage
	historyMu    sync.RWMutex

	// Forecasts
	forecasts  map[string]*CapacityForecast
	forecastMu sync.RWMutex

	// Recommendations
	recommendations   []*CapacityRecommendation
	recommendationsMu sync.RWMutex

	// Active alerts
	alerts   map[string]*CapacityAlert
	alertsMu sync.RWMutex

	// Callbacks
	onAlert          func(*CapacityAlert)
	onRecommendation func(*CapacityRecommendation)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	collectionsRun           int64
	forecastsGenerated       int64
	recommendationsGenerated int64
	autoTunesApplied         int64
}

// NewCapacityPlanningEngine creates a new capacity planning engine.
func NewCapacityPlanningEngine(db *DB, config CapacityPlanningConfig) *CapacityPlanningEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &CapacityPlanningEngine{
		db:              db,
		config:          config,
		usageHistory:    make([]ResourceUsage, 0),
		forecasts:       make(map[string]*CapacityForecast),
		recommendations: make([]*CapacityRecommendation, 0),
		alerts:          make(map[string]*CapacityAlert),
		ctx:             ctx,
		cancel:          cancel,
	}

	if config.Enabled {
		engine.wg.Add(2)
		go engine.metricsCollectionLoop()
		go engine.recommendationLoop()
	}

	return engine
}

// CollectMetrics collects current resource usage.
func (e *CapacityPlanningEngine) CollectMetrics() (*ResourceUsage, error) {
	usage := &ResourceUsage{
		Timestamp:     time.Now(),
		CustomMetrics: make(map[string]float64),
	}

	// Collect storage metrics from datastore
	if e.db != nil && e.db.dataStore != nil {
		size, err := e.db.dataStore.Stat()
		if err == nil {
			usage.StorageBytes = size
		}
	}

	// Use estimates for other metrics based on collected history
	e.historyMu.RLock()
	if len(e.usageHistory) > 0 {
		last := e.usageHistory[len(e.usageHistory)-1]
		if usage.StorageBytes == 0 {
			usage.StorageBytes = last.StorageBytes
		}
		usage.PointsCount = last.PointsCount
		usage.SeriesCount = last.SeriesCount
		usage.PartitionCount = last.PartitionCount
		usage.QueryRate = last.QueryRate
		usage.WriteRate = last.WriteRate
		usage.MemoryBytes = last.MemoryBytes
	}
	e.historyMu.RUnlock()

	// Store in history
	e.historyMu.Lock()
	e.usageHistory = append(e.usageHistory, *usage)

	// Trim history to window
	cutoff := time.Now().Add(-e.config.HistoryWindow)
	for len(e.usageHistory) > 0 && e.usageHistory[0].Timestamp.Before(cutoff) {
		e.usageHistory = e.usageHistory[1:]
	}
	e.historyMu.Unlock()

	atomic.AddInt64(&e.collectionsRun, 1)

	// Check for alerts
	e.checkAlerts(usage)

	return usage, nil
}

// GenerateForecasts generates capacity forecasts.
func (e *CapacityPlanningEngine) GenerateForecasts() (map[string]*CapacityForecast, error) {
	e.historyMu.RLock()
	history := make([]ResourceUsage, len(e.usageHistory))
	copy(history, e.usageHistory)
	e.historyMu.RUnlock()

	if len(history) < e.config.MinDataPoints {
		return nil, fmt.Errorf("insufficient data points: %d < %d", len(history), e.config.MinDataPoints)
	}

	forecasts := make(map[string]*CapacityForecast)

	// Forecast storage
	storageValues := make([]float64, len(history))
	timestamps := make([]time.Time, len(history))
	for i, h := range history {
		storageValues[i] = float64(h.StorageBytes)
		timestamps[i] = h.Timestamp
	}
	forecasts["storage"] = e.forecastMetric("storage", storageValues, timestamps, float64(history[len(history)-1].StorageLimit))

	// Forecast points count
	pointsValues := make([]float64, len(history))
	for i, h := range history {
		pointsValues[i] = float64(h.PointsCount)
	}
	forecasts["points"] = e.forecastMetric("points", pointsValues, timestamps, float64(history[len(history)-1].PointsLimit))

	// Forecast series count
	seriesValues := make([]float64, len(history))
	for i, h := range history {
		seriesValues[i] = float64(h.SeriesCount)
	}
	forecasts["series"] = e.forecastMetric("series", seriesValues, timestamps, float64(history[len(history)-1].SeriesLimit))

	// Forecast memory
	memoryValues := make([]float64, len(history))
	for i, h := range history {
		memoryValues[i] = float64(h.MemoryBytes)
	}
	forecasts["memory"] = e.forecastMetric("memory", memoryValues, timestamps, float64(history[len(history)-1].MemoryLimit))

	// Forecast query rate
	queryValues := make([]float64, len(history))
	for i, h := range history {
		queryValues[i] = h.QueryRate
	}
	forecasts["query_rate"] = e.forecastMetric("query_rate", queryValues, timestamps, 0)

	// Store forecasts
	e.forecastMu.Lock()
	e.forecasts = forecasts
	e.forecastMu.Unlock()

	atomic.AddInt64(&e.forecastsGenerated, 1)

	return forecasts, nil
}

func (e *CapacityPlanningEngine) forecastMetric(name string, values []float64, timestamps []time.Time, limit float64) *CapacityForecast {
	if len(values) < 2 {
		return nil
	}

	// Use simple linear regression for forecasting
	n := float64(len(values))

	// Convert timestamps to hours from start
	start := timestamps[0]
	x := make([]float64, len(timestamps))
	for i, t := range timestamps {
		x[i] = t.Sub(start).Hours()
	}

	// Calculate regression
	sumX, sumY, sumXY, sumXX := 0.0, 0.0, 0.0, 0.0
	for i := range values {
		sumX += x[i]
		sumY += values[i]
		sumXY += x[i] * values[i]
		sumXX += x[i] * x[i]
	}

	slope := (n*sumXY - sumX*sumY) / (n*sumXX - sumX*sumX)
	intercept := (sumY - slope*sumX) / n

	// Calculate R-squared for confidence
	meanY := sumY / n
	ssTot, ssRes := 0.0, 0.0
	for i := range values {
		predicted := slope*x[i] + intercept
		ssTot += (values[i] - meanY) * (values[i] - meanY)
		ssRes += (values[i] - predicted) * (values[i] - predicted)
	}
	rSquared := 1 - (ssRes / ssTot)
	if math.IsNaN(rSquared) || rSquared < 0 {
		rSquared = 0
	}

	// Forecast future value
	forecastTime := timestamps[len(timestamps)-1].Add(e.config.ForecastHorizon)
	forecastX := forecastTime.Sub(start).Hours()
	predictedValue := slope*forecastX + intercept

	// Calculate prediction interval
	stdError := math.Sqrt(ssRes / (n - 2))
	marginOfError := 1.96 * stdError * math.Sqrt(1+1/n+math.Pow(forecastX-sumX/n, 2)/(sumXX-sumX*sumX/n))

	// Determine trend
	var trend TrendDirection
	slopePercent := slope / (sumY / n)
	if slopePercent > 0.01 {
		trend = TrendUp
	} else if slopePercent < -0.01 {
		trend = TrendDown
	} else {
		trend = TrendStable
	}

	forecast := &CapacityForecast{
		Metric:         name,
		ForecastTime:   forecastTime,
		PredictedValue: predictedValue,
		LowerBound:     predictedValue - marginOfError,
		UpperBound:     predictedValue + marginOfError,
		Confidence:     rSquared,
		Trend:          trend,
	}

	// Calculate time to exhaustion if limit is set
	if limit > 0 && slope > 0 {
		currentValue := values[len(values)-1]
		remaining := limit - currentValue
		hoursToExhaustion := remaining / slope
		if hoursToExhaustion > 0 {
			duration := time.Duration(hoursToExhaustion) * time.Hour
			forecast.TimeToExhaustion = &duration
		}
	}

	return forecast
}

// GenerateRecommendations generates capacity recommendations.
