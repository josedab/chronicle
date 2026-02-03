package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
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
		ForecastHorizon:          7 * 24 * time.Hour, // 7 days
		HistoryWindow:            30 * 24 * time.Hour, // 30 days
		RecommendationInterval:   1 * time.Hour,
		AutoTuneEnabled:          false,
		SafetyMargin:             0.2,
		AlertThreshold:           0.8,
		MinDataPoints:            100,
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
	Timestamp      time.Time            `json:"timestamp"`
	StorageBytes   int64                `json:"storage_bytes"`
	StorageLimit   int64                `json:"storage_limit,omitempty"`
	MemoryBytes    int64                `json:"memory_bytes"`
	MemoryLimit    int64                `json:"memory_limit,omitempty"`
	PointsCount    int64                `json:"points_count"`
	PointsLimit    int64                `json:"points_limit,omitempty"`
	SeriesCount    int64                `json:"series_count"`
	SeriesLimit    int64                `json:"series_limit,omitempty"`
	QueryRate      float64              `json:"query_rate"`      // queries/sec
	WriteRate      float64              `json:"write_rate"`      // points/sec
	PartitionCount int                  `json:"partition_count"`
	CustomMetrics  map[string]float64   `json:"custom_metrics,omitempty"`
}

// CapacityForecast represents a predicted future state.
type CapacityForecast struct {
	Metric          string          `json:"metric"`
	ForecastTime    time.Time       `json:"forecast_time"`
	PredictedValue  float64         `json:"predicted_value"`
	LowerBound      float64         `json:"lower_bound"`
	UpperBound      float64         `json:"upper_bound"`
	Confidence      float64         `json:"confidence"`
	Trend           TrendDirection  `json:"trend"`
	TimeToExhaustion *time.Duration `json:"time_to_exhaustion,omitempty"`
}

// TrendDirection indicates the direction of a trend.
type TrendDirection string

const (
	TrendUp       TrendDirection = "up"
	TrendDown     TrendDirection = "down"
	TrendStable   TrendDirection = "stable"
	TrendUnknown  TrendDirection = "unknown"
)

// CapacityRecommendation represents a suggested action.
type CapacityRecommendation struct {
	ID          string                `json:"id"`
	Priority    RecommendationPriority `json:"priority"`
	Category    RecommendationCategory `json:"category"`
	Title       string                `json:"title"`
	Description string                `json:"description"`
	Metric      string                `json:"metric"`
	CurrentValue float64              `json:"current_value"`
	SuggestedValue float64            `json:"suggested_value"`
	Impact      string                `json:"impact"`
	Effort      string                `json:"effort"`
	AutoApply   bool                  `json:"auto_apply"`
	AppliedAt   *time.Time            `json:"applied_at,omitempty"`
	CreatedAt   time.Time             `json:"created_at"`
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
	CategoryStorage    RecommendationCategory = "storage"
	CategoryRetention  RecommendationCategory = "retention"
	CategoryPartition  RecommendationCategory = "partition"
	CategoryDownsample RecommendationCategory = "downsample"
	CategoryCompression RecommendationCategory = "compression"
	CategoryMemory     RecommendationCategory = "memory"
	CategoryQuery      RecommendationCategory = "query"
)

// CapacityAlert represents a capacity-related alert.
type CapacityAlert struct {
	ID          string         `json:"id"`
	Severity    string         `json:"severity"`
	Metric      string         `json:"metric"`
	Message     string         `json:"message"`
	Value       float64        `json:"value"`
	Threshold   float64        `json:"threshold"`
	Triggered   time.Time      `json:"triggered"`
	Resolved    *time.Time     `json:"resolved,omitempty"`
	Acknowledged bool          `json:"acknowledged"`
}

// CapacityPlanningEngine provides predictive capacity planning.
type CapacityPlanningEngine struct {
	db       *DB
	config   CapacityPlanningConfig

	// Usage history
	usageHistory   []ResourceUsage
	historyMu      sync.RWMutex

	// Forecasts
	forecasts   map[string]*CapacityForecast
	forecastMu  sync.RWMutex

	// Recommendations
	recommendations   []*CapacityRecommendation
	recommendationsMu sync.RWMutex

	// Active alerts
	alerts   map[string]*CapacityAlert
	alertsMu sync.RWMutex

	// Callbacks
	onAlert func(*CapacityAlert)
	onRecommendation func(*CapacityRecommendation)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	collectionsRun int64
	forecastsGenerated int64
	recommendationsGenerated int64
	autoTunesApplied int64
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
	marginOfError := 1.96 * stdError * math.Sqrt(1 + 1/n + math.Pow(forecastX-sumX/n, 2)/(sumXX-sumX*sumX/n))

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
func (e *CapacityPlanningEngine) GenerateRecommendations() ([]*CapacityRecommendation, error) {
	e.historyMu.RLock()
	history := e.usageHistory
	e.historyMu.RUnlock()

	if len(history) == 0 {
		return nil, nil
	}

	current := history[len(history)-1]
	recommendations := make([]*CapacityRecommendation, 0)

	// Get forecasts
	e.forecastMu.RLock()
	forecasts := e.forecasts
	e.forecastMu.RUnlock()

	// Storage recommendations
	if forecast, ok := forecasts["storage"]; ok && forecast != nil {
		if forecast.TimeToExhaustion != nil && *forecast.TimeToExhaustion < 7*24*time.Hour {
			rec := &CapacityRecommendation{
				ID:          generateID(),
				Priority:    PriorityHigh,
				Category:    CategoryStorage,
				Title:       "Storage Exhaustion Warning",
				Description: fmt.Sprintf("Storage projected to be exhausted in %v. Consider increasing storage limit or adjusting retention.", forecast.TimeToExhaustion),
				Metric:      "storage",
				CurrentValue: float64(current.StorageBytes),
				SuggestedValue: float64(current.StorageBytes) * (1 + e.config.SafetyMargin),
				Impact:      "Prevents data loss and write failures",
				Effort:      "low",
				CreatedAt:   time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Retention policy recommendations
	if current.StorageLimit > 0 {
		utilization := float64(current.StorageBytes) / float64(current.StorageLimit)
		if utilization > 0.8 {
			rec := &CapacityRecommendation{
				ID:          generateID(),
				Priority:    PriorityMedium,
				Category:    CategoryRetention,
				Title:       "Adjust Retention Policy",
				Description: fmt.Sprintf("Storage utilization is %.1f%%. Consider reducing retention period or enabling more aggressive downsampling.", utilization*100),
				Metric:      "storage_utilization",
				CurrentValue: utilization,
				SuggestedValue: 0.7,
				Impact:      "Reduces storage usage while maintaining data availability",
				Effort:      "medium",
				AutoApply:   e.config.AutoTuneEnabled,
				CreatedAt:   time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Series cardinality recommendations
	if current.SeriesLimit > 0 {
		utilization := float64(current.SeriesCount) / float64(current.SeriesLimit)
		if utilization > 0.7 {
			rec := &CapacityRecommendation{
				ID:          generateID(),
				Priority:    PriorityMedium,
				Category:    CategoryQuery,
				Title:       "High Series Cardinality",
				Description: fmt.Sprintf("Series count is %.1f%% of limit. Consider reviewing tag usage or increasing cardinality limits.", utilization*100),
				Metric:      "series_utilization",
				CurrentValue: float64(current.SeriesCount),
				SuggestedValue: float64(current.SeriesLimit) * 0.5,
				Impact:      "Improves query performance and reduces memory usage",
				Effort:      "high",
				CreatedAt:   time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Partition recommendations
	avgPointsPerPartition := float64(current.PointsCount) / float64(max(current.PartitionCount, 1))
	if avgPointsPerPartition > 10000000 { // 10M points per partition
		rec := &CapacityRecommendation{
			ID:          generateID(),
			Priority:    PriorityLow,
			Category:    CategoryPartition,
			Title:       "Consider Partition Splitting",
			Description: fmt.Sprintf("Average %.0f points per partition. Consider reducing partition time window for better query performance.", avgPointsPerPartition),
			Metric:      "points_per_partition",
			CurrentValue: avgPointsPerPartition,
			SuggestedValue: 5000000,
			Impact:      "Improves query latency for time-bounded queries",
			Effort:      "medium",
			AutoApply:   e.config.AutoTuneEnabled,
			CreatedAt:   time.Now(),
		}
		recommendations = append(recommendations, rec)
	}

	// Memory recommendations
	if current.MemoryLimit > 0 {
		utilization := float64(current.MemoryBytes) / float64(current.MemoryLimit)
		if utilization > 0.85 {
			rec := &CapacityRecommendation{
				ID:          generateID(),
				Priority:    PriorityCritical,
				Category:    CategoryMemory,
				Title:       "Memory Pressure Warning",
				Description: fmt.Sprintf("Memory utilization is %.1f%%. Risk of OOM. Increase memory limit or reduce cache sizes.", utilization*100),
				Metric:      "memory_utilization",
				CurrentValue: float64(current.MemoryBytes),
				SuggestedValue: float64(current.MemoryLimit) * 0.7,
				Impact:      "Prevents OOM crashes and performance degradation",
				Effort:      "low",
				CreatedAt:   time.Now(),
			}
			recommendations = append(recommendations, rec)
		}
	}

	// Downsampling recommendations based on query patterns
	if forecast, ok := forecasts["query_rate"]; ok && forecast != nil && forecast.Trend == TrendUp {
		rec := &CapacityRecommendation{
			ID:          generateID(),
			Priority:    PriorityLow,
			Category:    CategoryDownsample,
			Title:       "Enable Additional Downsampling",
			Description: "Query rate is increasing. Consider enabling additional downsampling tiers for faster dashboard queries.",
			Metric:      "query_rate",
			CurrentValue: current.QueryRate,
			SuggestedValue: current.QueryRate * 0.8,
			Impact:      "Reduces query latency and database load",
			Effort:      "medium",
			AutoApply:   e.config.AutoTuneEnabled,
			CreatedAt:   time.Now(),
		}
		recommendations = append(recommendations, rec)
	}

	// Sort by priority
	sort.Slice(recommendations, func(i, j int) bool {
		return priorityWeight(recommendations[i].Priority) > priorityWeight(recommendations[j].Priority)
	})

	// Store recommendations
	e.recommendationsMu.Lock()
	e.recommendations = recommendations
	e.recommendationsMu.Unlock()

	atomic.AddInt64(&e.recommendationsGenerated, 1)

	// Trigger callbacks for new recommendations
	if e.onRecommendation != nil {
		for _, rec := range recommendations {
			e.onRecommendation(rec)
		}
	}

	// Auto-apply if enabled
	if e.config.AutoTuneEnabled {
		for _, rec := range recommendations {
			if rec.AutoApply {
				e.applyRecommendation(rec)
			}
		}
	}

	return recommendations, nil
}

func priorityWeight(p RecommendationPriority) int {
	switch p {
	case PriorityCritical:
		return 4
	case PriorityHigh:
		return 3
	case PriorityMedium:
		return 2
	case PriorityLow:
		return 1
	default:
		return 0
	}
}

func (e *CapacityPlanningEngine) applyRecommendation(rec *CapacityRecommendation) error {
	// Implementation would apply the recommendation to the database configuration
	// This is a placeholder that logs the action
	now := time.Now()
	rec.AppliedAt = &now
	atomic.AddInt64(&e.autoTunesApplied, 1)
	return nil
}

func (e *CapacityPlanningEngine) checkAlerts(usage *ResourceUsage) {
	e.alertsMu.Lock()
	defer e.alertsMu.Unlock()

	// Check storage alert
	if usage.StorageLimit > 0 {
		utilization := float64(usage.StorageBytes) / float64(usage.StorageLimit)
		alertID := "storage_threshold"
		if utilization > e.config.AlertThreshold {
			if _, exists := e.alerts[alertID]; !exists {
				alert := &CapacityAlert{
					ID:        alertID,
					Severity:  "warning",
					Metric:    "storage_utilization",
					Message:   fmt.Sprintf("Storage utilization %.1f%% exceeds threshold %.1f%%", utilization*100, e.config.AlertThreshold*100),
					Value:     utilization,
					Threshold: e.config.AlertThreshold,
					Triggered: time.Now(),
				}
				e.alerts[alertID] = alert
				if e.onAlert != nil {
					e.onAlert(alert)
				}
			}
		} else if alert, exists := e.alerts[alertID]; exists && alert.Resolved == nil {
			now := time.Now()
			alert.Resolved = &now
		}
	}

	// Check memory alert
	if usage.MemoryLimit > 0 {
		utilization := float64(usage.MemoryBytes) / float64(usage.MemoryLimit)
		alertID := "memory_threshold"
		if utilization > e.config.AlertThreshold {
			if _, exists := e.alerts[alertID]; !exists {
				alert := &CapacityAlert{
					ID:        alertID,
					Severity:  "critical",
					Metric:    "memory_utilization",
					Message:   fmt.Sprintf("Memory utilization %.1f%% exceeds threshold %.1f%%", utilization*100, e.config.AlertThreshold*100),
					Value:     utilization,
					Threshold: e.config.AlertThreshold,
					Triggered: time.Now(),
				}
				e.alerts[alertID] = alert
				if e.onAlert != nil {
					e.onAlert(alert)
				}
			}
		} else if alert, exists := e.alerts[alertID]; exists && alert.Resolved == nil {
			now := time.Now()
			alert.Resolved = &now
		}
	}

	// Check series limit alert
	if usage.SeriesLimit > 0 {
		utilization := float64(usage.SeriesCount) / float64(usage.SeriesLimit)
		alertID := "series_threshold"
		if utilization > e.config.AlertThreshold {
			if _, exists := e.alerts[alertID]; !exists {
				alert := &CapacityAlert{
					ID:        alertID,
					Severity:  "warning",
					Metric:    "series_utilization",
					Message:   fmt.Sprintf("Series count %.1f%% of limit", utilization*100),
					Value:     utilization,
					Threshold: e.config.AlertThreshold,
					Triggered: time.Now(),
				}
				e.alerts[alertID] = alert
				if e.onAlert != nil {
					e.onAlert(alert)
				}
			}
		} else if alert, exists := e.alerts[alertID]; exists && alert.Resolved == nil {
			now := time.Now()
			alert.Resolved = &now
		}
	}
}

func (e *CapacityPlanningEngine) metricsCollectionLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.MetricsCollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.CollectMetrics()
		}
	}
}

func (e *CapacityPlanningEngine) recommendationLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.RecommendationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.GenerateForecasts()
			e.GenerateRecommendations()
		}
	}
}

// GetCurrentUsage returns current resource usage.
func (e *CapacityPlanningEngine) GetCurrentUsage() (*ResourceUsage, error) {
	e.historyMu.RLock()
	defer e.historyMu.RUnlock()

	if len(e.usageHistory) == 0 {
		return nil, fmt.Errorf("no usage data collected")
	}

	latest := e.usageHistory[len(e.usageHistory)-1]
	return &latest, nil
}

// GetUsageHistory returns usage history.
func (e *CapacityPlanningEngine) GetUsageHistory(since time.Time) []ResourceUsage {
	e.historyMu.RLock()
	defer e.historyMu.RUnlock()

	result := make([]ResourceUsage, 0)
	for _, u := range e.usageHistory {
		if u.Timestamp.After(since) {
			result = append(result, u)
		}
	}
	return result
}

// GetForecasts returns current forecasts.
func (e *CapacityPlanningEngine) GetForecasts() map[string]*CapacityForecast {
	e.forecastMu.RLock()
	defer e.forecastMu.RUnlock()

	result := make(map[string]*CapacityForecast)
	for k, v := range e.forecasts {
		result[k] = v
	}
	return result
}

// GetRecommendations returns current recommendations.
func (e *CapacityPlanningEngine) GetRecommendations() []*CapacityRecommendation {
	e.recommendationsMu.RLock()
	defer e.recommendationsMu.RUnlock()

	result := make([]*CapacityRecommendation, len(e.recommendations))
	copy(result, e.recommendations)
	return result
}

// GetAlerts returns active alerts.
func (e *CapacityPlanningEngine) GetAlerts(includeResolved bool) []*CapacityAlert {
	e.alertsMu.RLock()
	defer e.alertsMu.RUnlock()

	result := make([]*CapacityAlert, 0)
	for _, a := range e.alerts {
		if includeResolved || a.Resolved == nil {
			result = append(result, a)
		}
	}
	return result
}

// AcknowledgeAlert acknowledges an alert.
func (e *CapacityPlanningEngine) AcknowledgeAlert(alertID string) error {
	e.alertsMu.Lock()
	defer e.alertsMu.Unlock()

	alert, ok := e.alerts[alertID]
	if !ok {
		return fmt.Errorf("alert not found: %s", alertID)
	}

	alert.Acknowledged = true
	return nil
}

// ApplyRecommendation applies a recommendation.
func (e *CapacityPlanningEngine) ApplyRecommendation(recID string) error {
	e.recommendationsMu.Lock()
	defer e.recommendationsMu.Unlock()

	for _, rec := range e.recommendations {
		if rec.ID == recID {
			return e.applyRecommendation(rec)
		}
	}

	return fmt.Errorf("recommendation not found: %s", recID)
}

// DismissRecommendation dismisses a recommendation.
func (e *CapacityPlanningEngine) DismissRecommendation(recID string) error {
	e.recommendationsMu.Lock()
	defer e.recommendationsMu.Unlock()

	for i, rec := range e.recommendations {
		if rec.ID == recID {
			e.recommendations = append(e.recommendations[:i], e.recommendations[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("recommendation not found: %s", recID)
}

// OnAlert sets the alert callback.
func (e *CapacityPlanningEngine) OnAlert(callback func(*CapacityAlert)) {
	e.onAlert = callback
}

// OnRecommendation sets the recommendation callback.
func (e *CapacityPlanningEngine) OnRecommendation(callback func(*CapacityRecommendation)) {
	e.onRecommendation = callback
}

// Stats returns engine statistics.
func (e *CapacityPlanningEngine) Stats() CapacityPlanningStats {
	e.alertsMu.RLock()
	activeAlerts := 0
	for _, a := range e.alerts {
		if a.Resolved == nil {
			activeAlerts++
		}
	}
	e.alertsMu.RUnlock()

	e.recommendationsMu.RLock()
	recCount := len(e.recommendations)
	e.recommendationsMu.RUnlock()

	return CapacityPlanningStats{
		CollectionsRun:           atomic.LoadInt64(&e.collectionsRun),
		ForecastsGenerated:       atomic.LoadInt64(&e.forecastsGenerated),
		RecommendationsGenerated: atomic.LoadInt64(&e.recommendationsGenerated),
		AutoTunesApplied:         atomic.LoadInt64(&e.autoTunesApplied),
		ActiveAlerts:             activeAlerts,
		PendingRecommendations:   recCount,
	}
}

// CapacityPlanningStats contains engine statistics.
type CapacityPlanningStats struct {
	CollectionsRun           int64 `json:"collections_run"`
	ForecastsGenerated       int64 `json:"forecasts_generated"`
	RecommendationsGenerated int64 `json:"recommendations_generated"`
	AutoTunesApplied         int64 `json:"auto_tunes_applied"`
	ActiveAlerts             int   `json:"active_alerts"`
	PendingRecommendations   int   `json:"pending_recommendations"`
}

// Close shuts down the capacity planning engine.
func (e *CapacityPlanningEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// ExportReport exports a capacity planning report.
func (e *CapacityPlanningEngine) ExportReport() ([]byte, error) {
	report := struct {
		GeneratedAt      time.Time                 `json:"generated_at"`
		CurrentUsage     *ResourceUsage            `json:"current_usage,omitempty"`
		Forecasts        map[string]*CapacityForecast `json:"forecasts"`
		Recommendations  []*CapacityRecommendation `json:"recommendations"`
		ActiveAlerts     []*CapacityAlert          `json:"active_alerts"`
		Stats            CapacityPlanningStats     `json:"stats"`
	}{
		GeneratedAt:     time.Now(),
		Forecasts:       e.GetForecasts(),
		Recommendations: e.GetRecommendations(),
		ActiveAlerts:    e.GetAlerts(false),
		Stats:           e.Stats(),
	}

	if usage, err := e.GetCurrentUsage(); err == nil {
		report.CurrentUsage = usage
	}

	return json.MarshalIndent(report, "", "  ")
}
