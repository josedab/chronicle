package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// ScalingDirection represents whether to scale up or down.
type ScalingDirection string

const (
	ScaleUp   ScalingDirection = "scale_up"
	ScaleDown ScalingDirection = "scale_down"
	ScaleNone ScalingDirection = "none"
)

// PredictiveAutoscalingConfig configures the predictive auto-scaling engine.
type PredictiveAutoscalingConfig struct {
	Enabled            bool          `json:"enabled"`
	EvaluationInterval time.Duration `json:"evaluation_interval"`
	ForecastHorizon    time.Duration `json:"forecast_horizon"`
	HistoryWindow      time.Duration `json:"history_window"`
	ScaleUpThreshold   float64       `json:"scale_up_threshold"`
	ScaleDownThreshold float64       `json:"scale_down_threshold"`
	CooldownPeriod     time.Duration `json:"cooldown_period"`
	MinReplicas        int           `json:"min_replicas"`
	MaxReplicas        int           `json:"max_replicas"`
	TargetUtilization  float64       `json:"target_utilization"`
	SafetyMargin       float64       `json:"safety_margin"`
	LeadTimeMinutes    int           `json:"lead_time_minutes"`
	EnableHPAMetrics   bool          `json:"enable_hpa_metrics"`
}

// DefaultPredictiveAutoscalingConfig returns sensible defaults.
func DefaultPredictiveAutoscalingConfig() PredictiveAutoscalingConfig {
	return PredictiveAutoscalingConfig{
		Enabled:            false,
		EvaluationInterval: time.Minute,
		ForecastHorizon:    time.Hour,
		HistoryWindow:      24 * time.Hour,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 0.3,
		CooldownPeriod:     5 * time.Minute,
		MinReplicas:        1,
		MaxReplicas:        10,
		TargetUtilization:  0.7,
		SafetyMargin:       0.15,
		LeadTimeMinutes:    15,
		EnableHPAMetrics:   true,
	}
}

// LoadSample represents a single load measurement.
type LoadSample struct {
	Timestamp   time.Time `json:"timestamp"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryUsage float64   `json:"memory_usage"`
	WriteRate   float64   `json:"write_rate"`
	QueryRate   float64   `json:"query_rate"`
	QueueDepth  int       `json:"queue_depth"`
}

// ScalingRecommendation describes a recommended scaling action.
type PredictiveScalingRecommendation struct {
	Direction       ScalingDirection `json:"direction"`
	CurrentReplicas int              `json:"current_replicas"`
	TargetReplicas  int              `json:"target_replicas"`
	Reason          string           `json:"reason"`
	Confidence      float64          `json:"confidence"`
	PredictedLoad   float64          `json:"predicted_load"`
	Timestamp       time.Time        `json:"timestamp"`
	LeadTimeMinutes int              `json:"lead_time_minutes"`
}

// ForecastResult holds the output of load prediction.
type LoadForecastResult struct {
	PredictedValues []float64 `json:"predicted_values"`
	UpperBound      []float64 `json:"upper_bound"`
	LowerBound      []float64 `json:"lower_bound"`
	Horizon         string    `json:"horizon"`
	Model           string    `json:"model"`
	MAPE            float64   `json:"mape"`
}

// AutoscalingStats tracks engine statistics.
type AutoscalingStats struct {
	EvaluationsRun    int       `json:"evaluations_run"`
	ScaleUpEvents     int       `json:"scale_up_events"`
	ScaleDownEvents   int       `json:"scale_down_events"`
	LastEvaluation    time.Time `json:"last_evaluation"`
	LastScalingAction time.Time `json:"last_scaling_action"`
	CurrentReplicas   int       `json:"current_replicas"`
	SamplesCollected  int       `json:"samples_collected"`
}

// ScalingCallback is invoked when a scaling recommendation is made.
type ScalingCallback func(rec PredictiveScalingRecommendation)

// PredictiveAutoscaler uses ML-based load forecasting for proactive scaling.
type PredictiveAutoscaler struct {
	config    PredictiveAutoscalingConfig
	db        *DB
	samples   []LoadSample
	history   []PredictiveScalingRecommendation
	stats     AutoscalingStats
	callbacks []ScalingCallback
	replicas  int

	mu   sync.RWMutex
	done chan struct{}
}

// NewPredictiveAutoscaler creates a new predictive auto-scaling engine.
func NewPredictiveAutoscaler(db *DB, config PredictiveAutoscalingConfig) *PredictiveAutoscaler {
	return &PredictiveAutoscaler{
		config:   config,
		db:       db,
		samples:  make([]LoadSample, 0, 1440), // 24h at 1-minute intervals
		history:  make([]PredictiveScalingRecommendation, 0),
		replicas: config.MinReplicas,
		done:     make(chan struct{}),
	}
}

// Start begins the background evaluation loop.
func (a *PredictiveAutoscaler) Start() {
	if !a.config.Enabled {
		return
	}
	a.stats.CurrentReplicas = a.replicas
	go a.evaluationLoop()
}

// Stop halts the evaluation loop.
func (a *PredictiveAutoscaler) Stop() {
	select {
	case <-a.done:
	default:
		close(a.done)
	}
}

// RecordSample adds a load measurement for forecasting.
func (a *PredictiveAutoscaler) RecordSample(sample LoadSample) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now()
	}
	a.samples = append(a.samples, sample)
	a.stats.SamplesCollected++

	// Trim old samples
	cutoff := time.Now().Add(-a.config.HistoryWindow)
	idx := 0
	for _, s := range a.samples {
		if s.Timestamp.After(cutoff) {
			a.samples[idx] = s
			idx++
		}
	}
	a.samples = a.samples[:idx]
}

// Evaluate runs a single scaling evaluation and returns a recommendation.
func (a *PredictiveAutoscaler) Evaluate(ctx context.Context) (*PredictiveScalingRecommendation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.stats.EvaluationsRun++
	a.stats.LastEvaluation = time.Now()

	if len(a.samples) < 3 {
		return &PredictiveScalingRecommendation{
			Direction:       ScaleNone,
			CurrentReplicas: a.replicas,
			TargetReplicas:  a.replicas,
			Reason:          "insufficient data for prediction",
			Timestamp:       time.Now(),
		}, nil
	}

	// Check cooldown
	if !a.stats.LastScalingAction.IsZero() && time.Since(a.stats.LastScalingAction) < a.config.CooldownPeriod {
		return &PredictiveScalingRecommendation{
			Direction:       ScaleNone,
			CurrentReplicas: a.replicas,
			TargetReplicas:  a.replicas,
			Reason:          "in cooldown period",
			Timestamp:       time.Now(),
		}, nil
	}

	// Forecast using exponential smoothing
	forecast := a.forecastLoad()
	predictedPeak := a.findPeakLoad(forecast)

	rec := a.computeRecommendation(predictedPeak, forecast)

	if rec.Direction != ScaleNone {
		a.stats.LastScalingAction = time.Now()
		if rec.Direction == ScaleUp {
			a.stats.ScaleUpEvents++
		} else {
			a.stats.ScaleDownEvents++
		}
		a.replicas = rec.TargetReplicas
		a.stats.CurrentReplicas = a.replicas

		for _, cb := range a.callbacks {
			cb(*rec)
		}
	}

	a.history = append(a.history, *rec)
	if len(a.history) > 100 {
		a.history = a.history[len(a.history)-100:]
	}

	return rec, nil
}

// OnScalingEvent registers a callback for scaling recommendations.
func (a *PredictiveAutoscaler) OnScalingEvent(cb ScalingCallback) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.callbacks = append(a.callbacks, cb)
}

// GetForecast returns the current load forecast.
func (a *PredictiveAutoscaler) GetForecast() *LoadForecastResult {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.samples) < 3 {
		return nil
	}
	return a.forecastLoad()
}

// Stats returns engine statistics.
func (a *PredictiveAutoscaler) Stats() AutoscalingStats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.stats
}

// History returns recent scaling recommendations.
func (a *PredictiveAutoscaler) History() []PredictiveScalingRecommendation {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]PredictiveScalingRecommendation, len(a.history))
	copy(result, a.history)
	return result
}

// HPAMetrics returns current metrics formatted for Kubernetes HPA.
func (a *PredictiveAutoscaler) HPAMetrics() map[string]any {
	a.mu.RLock()
	defer a.mu.RUnlock()

	forecast := a.forecastLoad()
	predictedPeak := 0.0
	if forecast != nil && len(forecast.PredictedValues) > 0 {
		predictedPeak = a.findPeakLoad(forecast)
	}

	return map[string]any{
		"chronicle_predicted_load":     predictedPeak,
		"chronicle_current_replicas":   a.replicas,
		"chronicle_target_utilization": a.config.TargetUtilization,
		"chronicle_samples_count":      len(a.samples),
	}
}

func (a *PredictiveAutoscaler) evaluationLoop() {
	ticker := time.NewTicker(a.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.done:
			return
		case <-ticker.C:
			a.Evaluate(context.Background())
		}
	}
}

// forecastLoad uses exponential smoothing on the combined load metric.
func (a *PredictiveAutoscaler) forecastLoad() *LoadForecastResult {
	if len(a.samples) < 3 {
		return nil
	}

	// Combine metrics into a single load value
	values := make([]float64, len(a.samples))
	for i, s := range a.samples {
		values[i] = (s.CPUUsage + s.MemoryUsage) / 2.0
		if s.WriteRate > 0 {
			values[i] = values[i]*0.6 + math.Min(1.0, s.WriteRate/10000)*0.4
		}
	}

	// Simple exponential smoothing
	alpha := 0.3
	smoothed := make([]float64, len(values))
	smoothed[0] = values[0]
	for i := 1; i < len(values); i++ {
		smoothed[i] = alpha*values[i] + (1-alpha)*smoothed[i-1]
	}

	// Forecast: extend the trend
	steps := 6 // forecast 6 periods ahead
	last := smoothed[len(smoothed)-1]
	trend := 0.0
	if len(smoothed) > 1 {
		trend = smoothed[len(smoothed)-1] - smoothed[len(smoothed)-2]
	}

	predicted := make([]float64, steps)
	upper := make([]float64, steps)
	lower := make([]float64, steps)
	for i := 0; i < steps; i++ {
		predicted[i] = last + trend*float64(i+1)
		margin := a.config.SafetyMargin * math.Abs(predicted[i])
		upper[i] = predicted[i] + margin
		lower[i] = math.Max(0, predicted[i]-margin)
	}

	// Compute MAPE on training data
	var mape float64
	for i := 1; i < len(values); i++ {
		if values[i] > 0 {
			mape += math.Abs(values[i]-smoothed[i]) / values[i]
		}
	}
	if len(values) > 1 {
		mape /= float64(len(values) - 1)
	}

	return &LoadForecastResult{
		PredictedValues: predicted,
		UpperBound:      upper,
		LowerBound:      lower,
		Horizon:         a.config.ForecastHorizon.String(),
		Model:           "exponential_smoothing",
		MAPE:            mape,
	}
}

func (a *PredictiveAutoscaler) findPeakLoad(forecast *LoadForecastResult) float64 {
	if forecast == nil || len(forecast.UpperBound) == 0 {
		return 0
	}
	peak := 0.0
	for _, v := range forecast.UpperBound {
		if v > peak {
			peak = v
		}
	}
	return peak
}

func (a *PredictiveAutoscaler) computeRecommendation(predictedPeak float64, forecast *LoadForecastResult) *PredictiveScalingRecommendation {
	rec := &PredictiveScalingRecommendation{
		Direction:       ScaleNone,
		CurrentReplicas: a.replicas,
		TargetReplicas:  a.replicas,
		PredictedLoad:   predictedPeak,
		Timestamp:       time.Now(),
		LeadTimeMinutes: a.config.LeadTimeMinutes,
	}

	perReplicaLoad := predictedPeak / math.Max(1, float64(a.replicas))

	if perReplicaLoad > a.config.ScaleUpThreshold {
		// Need more replicas
		needed := int(math.Ceil(predictedPeak / a.config.TargetUtilization))
		if needed > a.config.MaxReplicas {
			needed = a.config.MaxReplicas
		}
		if needed > a.replicas {
			rec.Direction = ScaleUp
			rec.TargetReplicas = needed
			rec.Reason = fmt.Sprintf("predicted load %.2f exceeds threshold %.2f per replica", perReplicaLoad, a.config.ScaleUpThreshold)
			rec.Confidence = math.Min(0.95, 0.5+0.45*(1.0-forecast.MAPE))
		}
	} else if perReplicaLoad < a.config.ScaleDownThreshold && a.replicas > a.config.MinReplicas {
		// Can reduce replicas
		needed := int(math.Ceil(predictedPeak / a.config.TargetUtilization))
		if needed < a.config.MinReplicas {
			needed = a.config.MinReplicas
		}
		if needed < a.replicas {
			rec.Direction = ScaleDown
			rec.TargetReplicas = needed
			rec.Reason = fmt.Sprintf("predicted load %.2f below threshold %.2f per replica", perReplicaLoad, a.config.ScaleDownThreshold)
			rec.Confidence = math.Min(0.9, 0.4+0.5*(1.0-forecast.MAPE))
		}
	} else {
		rec.Reason = "load within acceptable range"
	}

	return rec
}
