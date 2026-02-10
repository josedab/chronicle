package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// AutoScalingConfig configures the auto-scaling hints engine.
type AutoScalingConfig struct {
	// Enabled enables auto-scaling hints
	Enabled bool `json:"enabled"`

	// PredictionHorizon how far ahead to predict
	PredictionHorizon time.Duration `json:"prediction_horizon"`

	// HistoryWindow historical data to analyze
	HistoryWindow time.Duration `json:"history_window"`

	// SampleInterval for metric sampling
	SampleInterval time.Duration `json:"sample_interval"`

	// MinDataPoints minimum data points for prediction
	MinDataPoints int `json:"min_data_points"`

	// ScaleUpThreshold CPU/memory percentage to trigger scale up
	ScaleUpThreshold float64 `json:"scale_up_threshold"`

	// ScaleDownThreshold CPU/memory percentage to trigger scale down
	ScaleDownThreshold float64 `json:"scale_down_threshold"`

	// CooldownPeriod between scaling actions
	CooldownPeriod time.Duration `json:"cooldown_period"`

	// TargetUtilization for optimal resource usage
	TargetUtilization float64 `json:"target_utilization"`
}

// DefaultAutoScalingConfig returns default configuration.
func DefaultAutoScalingConfig() AutoScalingConfig {
	return AutoScalingConfig{
		Enabled:            true,
		PredictionHorizon:  15 * time.Minute,
		HistoryWindow:      time.Hour,
		SampleInterval:     time.Minute,
		MinDataPoints:      10,
		ScaleUpThreshold:   80.0,
		ScaleDownThreshold: 30.0,
		CooldownPeriod:     5 * time.Minute,
		TargetUtilization:  70.0,
	}
}

// AutoScalingEngine provides predictive auto-scaling hints for Kubernetes HPA/VPA.
type AutoScalingEngine struct {
	db     *DB
	config AutoScalingConfig
	mu     sync.RWMutex

	// Workload profiles
	workloads map[string]*WorkloadProfile

	// Scaling recommendations
	recommendations map[string]*ScalingRecommendation

	// Historical predictions for accuracy tracking
	predictions []PredictionRecord

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// WorkloadProfile contains resource usage patterns for a workload.
type WorkloadProfile struct {
	Name        string                   `json:"name"`
	Namespace   string                   `json:"namespace"`
	Kind        WorkloadKind             `json:"kind"`
	Metrics     map[string]*MetricSeries `json:"metrics"`
	Patterns    *UsagePatterns           `json:"patterns"`
	LastUpdated time.Time                `json:"last_updated"`
}

// WorkloadKind identifies Kubernetes workload types.
type WorkloadKind string

const (
	WorkloadKindDeployment  WorkloadKind = "Deployment"
	WorkloadKindStatefulSet WorkloadKind = "StatefulSet"
	WorkloadKindDaemonSet   WorkloadKind = "DaemonSet"
	WorkloadKindReplicaSet  WorkloadKind = "ReplicaSet"
)

// MetricSeries stores time-series data for a metric.
type MetricSeries struct {
	Name       string       `json:"name"`
	Unit       string       `json:"unit"`
	Values     []float64    `json:"values"`
	Timestamps []int64      `json:"timestamps"`
	Stats      *MetricStats `json:"stats"`
}

// MetricStats contains statistical summaries.
type MetricStats struct {
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"std_dev"`
	P50    float64 `json:"p50"`
	P90    float64 `json:"p90"`
	P99    float64 `json:"p99"`
}

// UsagePatterns captures workload behavior patterns.
type UsagePatterns struct {
	DailyPattern  []float64 `json:"daily_pattern"`  // 24-hour pattern
	WeeklyPattern []float64 `json:"weekly_pattern"` // 7-day pattern
	Seasonality   float64   `json:"seasonality"`    // Seasonality strength
	Trend         float64   `json:"trend"`          // Growth/decline trend
	Spikiness     float64   `json:"spikiness"`      // Spike frequency
	PeakHours     []int     `json:"peak_hours"`     // Peak usage hours
	LowHours      []int     `json:"low_hours"`      // Low usage hours
}

// ScalingRecommendation represents a scaling recommendation.
type ScalingRecommendation struct {
	WorkloadName  string             `json:"workload_name"`
	Namespace     string             `json:"namespace"`
	Timestamp     time.Time          `json:"timestamp"`
	ValidUntil    time.Time          `json:"valid_until"`
	Action        ScalingAction      `json:"action"`
	Confidence    float64            `json:"confidence"`
	Reason        string             `json:"reason"`
	CurrentState  *ResourceState     `json:"current_state"`
	TargetState   *ResourceState     `json:"target_state"`
	HPAConfig     *HPARecommendation `json:"hpa_config,omitempty"`
	VPAConfig     *VPARecommendation `json:"vpa_config,omitempty"`
	PredictedLoad []PredictedValue   `json:"predicted_load"`
}

// ScalingAction identifies the recommended action.
type ScalingAction string

const (
	ScalingActionNone      ScalingAction = "none"
	ScalingActionScaleUp   ScalingAction = "scale_up"
	ScalingActionScaleDown ScalingAction = "scale_down"
	ScalingActionOptimize  ScalingAction = "optimize"
)

// ResourceState represents current/target resource state.
type ResourceState struct {
	Replicas      int32              `json:"replicas"`
	CPURequest    string             `json:"cpu_request"`
	CPULimit      string             `json:"cpu_limit"`
	MemoryRequest string             `json:"memory_request"`
	MemoryLimit   string             `json:"memory_limit"`
	Utilization   map[string]float64 `json:"utilization"`
}

// HPARecommendation contains HPA configuration recommendations.
type HPARecommendation struct {
	MinReplicas                  int32             `json:"min_replicas"`
	MaxReplicas                  int32             `json:"max_replicas"`
	TargetCPUUtilization         int32             `json:"target_cpu_utilization"`
	TargetMemoryUtilization      int32             `json:"target_memory_utilization,omitempty"`
	CustomMetrics                []CustomMetricHPA `json:"custom_metrics,omitempty"`
	ScaleUpStabilizationWindow   int32             `json:"scale_up_stabilization_window"`
	ScaleDownStabilizationWindow int32             `json:"scale_down_stabilization_window"`
}

// CustomMetricHPA defines custom metric for HPA.
type CustomMetricHPA struct {
	MetricName  string `json:"metric_name"`
	TargetValue string `json:"target_value"`
	TargetType  string `json:"target_type"`
}

// VPARecommendation contains VPA configuration recommendations.
type VPARecommendation struct {
	UpdateMode        string            `json:"update_mode"`
	ContainerPolicies []ContainerPolicy `json:"container_policies"`
}

// ContainerPolicy defines resource policy for a container.
type ContainerPolicy struct {
	ContainerName string       `json:"container_name"`
	MinAllowed    ResourceList `json:"min_allowed"`
	MaxAllowed    ResourceList `json:"max_allowed"`
	Recommended   ResourceList `json:"recommended"`
}

// ResourceList defines CPU and memory resources.
type ResourceList struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// PredictedValue represents a predicted future value.
type PredictedValue struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
	Lower     float64 `json:"lower"`
	Upper     float64 `json:"upper"`
}

// PredictionRecord tracks prediction accuracy.
type PredictionRecord struct {
	WorkloadName string    `json:"workload_name"`
	PredictedAt  time.Time `json:"predicted_at"`
	PredictedFor time.Time `json:"predicted_for"`
	Predicted    float64   `json:"predicted"`
	Actual       float64   `json:"actual"`
	Error        float64   `json:"error"`
}

// NewAutoScalingEngine creates a new auto-scaling hints engine.
func NewAutoScalingEngine(db *DB, config AutoScalingConfig) *AutoScalingEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &AutoScalingEngine{
		db:              db,
		config:          config,
		workloads:       make(map[string]*WorkloadProfile),
		recommendations: make(map[string]*ScalingRecommendation),
		predictions:     make([]PredictionRecord, 0),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Start starts the auto-scaling engine.
func (e *AutoScalingEngine) Start() {
	e.wg.Add(1)
	go e.analysisLoop()
}

// Stop stops the auto-scaling engine.
func (e *AutoScalingEngine) Stop() {
	e.cancel()
	e.wg.Wait()
}

func (e *AutoScalingEngine) analysisLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.SampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.analyzeWorkloads()
		}
	}
}

func (e *AutoScalingEngine) analyzeWorkloads() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for name, workload := range e.workloads {
		rec := e.generateRecommendation(workload)
		if rec != nil {
			e.recommendations[name] = rec
		}
	}
}

// RegisterWorkload registers a workload for monitoring.
func (e *AutoScalingEngine) RegisterWorkload(name, namespace string, kind WorkloadKind) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := workloadKey(namespace, name)
	e.workloads[key] = &WorkloadProfile{
		Name:        name,
		Namespace:   namespace,
		Kind:        kind,
		Metrics:     make(map[string]*MetricSeries),
		Patterns:    &UsagePatterns{},
		LastUpdated: time.Now(),
	}
}

// RecordMetric records a metric value for a workload.
func (e *AutoScalingEngine) RecordMetric(namespace, name, metric string, value float64, timestamp int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := workloadKey(namespace, name)
	workload, ok := e.workloads[key]
	if !ok {
		return
	}

	series, ok := workload.Metrics[metric]
	if !ok {
		series = &MetricSeries{
			Name:       metric,
			Values:     make([]float64, 0),
			Timestamps: make([]int64, 0),
		}
		workload.Metrics[metric] = series
	}

	series.Values = append(series.Values, value)
	series.Timestamps = append(series.Timestamps, timestamp)

	// Trim old data
	cutoff := time.Now().Add(-e.config.HistoryWindow).UnixNano()
	trimIdx := 0
	for i, ts := range series.Timestamps {
		if ts >= cutoff {
			trimIdx = i
			break
		}
	}
	if trimIdx > 0 {
		series.Values = series.Values[trimIdx:]
		series.Timestamps = series.Timestamps[trimIdx:]
	}

	// Update stats
	series.Stats = calculateStats(series.Values)
	workload.LastUpdated = time.Now()

	// Update patterns periodically
	if len(series.Values) >= e.config.MinDataPoints {
		e.updatePatterns(workload)
	}
}

func (e *AutoScalingEngine) updatePatterns(workload *WorkloadProfile) {
	cpuSeries := workload.Metrics["cpu_utilization"]
	if cpuSeries == nil || len(cpuSeries.Values) < e.config.MinDataPoints {
		return
	}

	patterns := workload.Patterns

	// Calculate daily pattern (24 hours)
	patterns.DailyPattern = calculateDailyPattern(cpuSeries.Values, cpuSeries.Timestamps)

	// Identify peak and low hours
	patterns.PeakHours, patterns.LowHours = identifyPeakHours(patterns.DailyPattern, e.config.ScaleUpThreshold, e.config.ScaleDownThreshold)

	// Calculate trend
	patterns.Trend = calculateTrend(cpuSeries.Values)

	// Calculate spikiness
	patterns.Spikiness = calculateSpikiness(cpuSeries.Values)

	// Detect seasonality
	patterns.Seasonality = detectSeasonality(cpuSeries.Values)
}

func (e *AutoScalingEngine) generateRecommendation(workload *WorkloadProfile) *ScalingRecommendation {
	cpuSeries := workload.Metrics["cpu_utilization"]
	if cpuSeries == nil || len(cpuSeries.Values) < e.config.MinDataPoints {
		return nil
	}

	memSeries := workload.Metrics["memory_utilization"]

	// Predict future load
	predictions := e.predictLoad(cpuSeries, e.config.PredictionHorizon)
	if len(predictions) == 0 {
		return nil
	}

	// Current utilization
	currentCPU := cpuSeries.Values[len(cpuSeries.Values)-1]
	var currentMem float64
	if memSeries != nil && len(memSeries.Values) > 0 {
		currentMem = memSeries.Values[len(memSeries.Values)-1]
	}

	// Max predicted utilization
	maxPredicted := 0.0
	for _, p := range predictions {
		if p.Value > maxPredicted {
			maxPredicted = p.Value
		}
	}

	// Determine action
	action := ScalingActionNone
	reason := ""
	confidence := 0.8

	if maxPredicted > e.config.ScaleUpThreshold {
		action = ScalingActionScaleUp
		reason = fmt.Sprintf("Predicted CPU utilization %.1f%% exceeds threshold %.1f%%", maxPredicted, e.config.ScaleUpThreshold)
	} else if currentCPU < e.config.ScaleDownThreshold && maxPredicted < e.config.ScaleDownThreshold {
		action = ScalingActionScaleDown
		reason = fmt.Sprintf("Current (%.1f%%) and predicted (%.1f%%) CPU below threshold %.1f%%", currentCPU, maxPredicted, e.config.ScaleDownThreshold)
	} else if math.Abs(currentCPU-e.config.TargetUtilization) > 20 {
		action = ScalingActionOptimize
		reason = fmt.Sprintf("Current utilization %.1f%% deviates from target %.1f%%", currentCPU, e.config.TargetUtilization)
	}

	// Calculate target replicas
	currentReplicas := int32(1) // Would come from actual K8s state
	targetReplicas := calculateTargetReplicas(currentReplicas, currentCPU, e.config.TargetUtilization)

	rec := &ScalingRecommendation{
		WorkloadName: workload.Name,
		Namespace:    workload.Namespace,
		Timestamp:    time.Now(),
		ValidUntil:   time.Now().Add(e.config.PredictionHorizon),
		Action:       action,
		Confidence:   confidence,
		Reason:       reason,
		CurrentState: &ResourceState{
			Replicas: currentReplicas,
			Utilization: map[string]float64{
				"cpu":    currentCPU,
				"memory": currentMem,
			},
		},
		TargetState: &ResourceState{
			Replicas: targetReplicas,
		},
		PredictedLoad: predictions,
	}

	// Generate HPA recommendation
	rec.HPAConfig = e.generateHPAConfig(workload, predictions)

	// Generate VPA recommendation
	rec.VPAConfig = e.generateVPAConfig(workload)

	return rec
}

func (e *AutoScalingEngine) predictLoad(series *MetricSeries, horizon time.Duration) []PredictedValue {
	if len(series.Values) < e.config.MinDataPoints {
		return nil
	}

	values := series.Values
	n := len(values)

	// Simple exponential smoothing with trend
	alpha := 0.3 // Smoothing factor
	beta := 0.1  // Trend factor

	// Initialize
	level := values[0]
	trend := 0.0
	if n > 1 {
		trend = values[1] - values[0]
	}

	// Smooth historical data
	for i := 1; i < n; i++ {
		prevLevel := level
		level = alpha*values[i] + (1-alpha)*(level+trend)
		trend = beta*(level-prevLevel) + (1-beta)*trend
	}

	// Generate predictions
	numSteps := int(horizon / e.config.SampleInterval)
	if numSteps > 60 {
		numSteps = 60 // Limit predictions
	}

	predictions := make([]PredictedValue, numSteps)
	lastTs := series.Timestamps[n-1]
	stdDev := series.Stats.StdDev

	for i := 0; i < numSteps; i++ {
		predicted := level + float64(i+1)*trend

		// Clamp to reasonable range
		if predicted < 0 {
			predicted = 0
		}
		if predicted > 100 {
			predicted = 100
		}

		// Confidence interval widens with horizon
		uncertainty := stdDev * math.Sqrt(float64(i+1)) * 0.5

		predictions[i] = PredictedValue{
			Timestamp: lastTs + int64((i+1)*int(e.config.SampleInterval)),
			Value:     predicted,
			Lower:     math.Max(0, predicted-uncertainty),
			Upper:     math.Min(100, predicted+uncertainty),
		}
	}

	return predictions
}

func (e *AutoScalingEngine) generateHPAConfig(workload *WorkloadProfile, predictions []PredictedValue) *HPARecommendation {
	patterns := workload.Patterns

	// Determine min/max replicas based on patterns
	minReplicas := int32(1)
	maxReplicas := int32(10)

	// If high spikiness, allow larger max
	if patterns.Spikiness > 0.5 {
		maxReplicas = 20
	}

	// Target CPU based on peak prediction
	targetCPU := int32(e.config.TargetUtilization)

	// Stabilization windows based on patterns
	scaleUpWindow := int32(60)    // 1 minute default
	scaleDownWindow := int32(300) // 5 minutes default

	// If low spikiness, can be more aggressive
	if patterns.Spikiness < 0.3 {
		scaleDownWindow = 180
	}

	return &HPARecommendation{
		MinReplicas:                  minReplicas,
		MaxReplicas:                  maxReplicas,
		TargetCPUUtilization:         targetCPU,
		ScaleUpStabilizationWindow:   scaleUpWindow,
		ScaleDownStabilizationWindow: scaleDownWindow,
	}
}

func (e *AutoScalingEngine) generateVPAConfig(workload *WorkloadProfile) *VPARecommendation {
	cpuSeries := workload.Metrics["cpu_utilization"]
	memSeries := workload.Metrics["memory_utilization"]

	if cpuSeries == nil {
		return nil
	}

	// Calculate recommended resources based on P90
	cpuP90 := cpuSeries.Stats.P90
	var memP90 float64
	if memSeries != nil {
		memP90 = memSeries.Stats.P90
	}

	// Convert percentages to resource units (simplified)
	recommendedCPU := fmt.Sprintf("%dm", int(cpuP90*10))  // millicores
	recommendedMem := fmt.Sprintf("%dMi", int(memP90*10)) // MiB

	return &VPARecommendation{
		UpdateMode: "Auto",
		ContainerPolicies: []ContainerPolicy{
			{
				ContainerName: "main",
				MinAllowed: ResourceList{
					CPU:    "100m",
					Memory: "128Mi",
				},
				MaxAllowed: ResourceList{
					CPU:    "4000m",
					Memory: "8Gi",
				},
				Recommended: ResourceList{
					CPU:    recommendedCPU,
					Memory: recommendedMem,
				},
			},
		},
	}
}

// GetRecommendation returns the current recommendation for a workload.
func (e *AutoScalingEngine) GetRecommendation(namespace, name string) (*ScalingRecommendation, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := workloadKey(namespace, name)
	rec, ok := e.recommendations[key]
	if !ok || time.Now().After(rec.ValidUntil) {
		return nil, false
	}
	return rec, true
}

// GetAllRecommendations returns all active recommendations.
func (e *AutoScalingEngine) GetAllRecommendations() []*ScalingRecommendation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	recs := make([]*ScalingRecommendation, 0)

	for _, rec := range e.recommendations {
		if now.Before(rec.ValidUntil) {
			recs = append(recs, rec)
		}
	}

	sort.Slice(recs, func(i, j int) bool {
		return recs[i].WorkloadName < recs[j].WorkloadName
	})

	return recs
}

// ExportPrometheusRules generates Prometheus alerting rules for scaling.
func (e *AutoScalingEngine) ExportPrometheusRules() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var rules []map[string]any

	for _, rec := range e.recommendations {
		if rec.Action == ScalingActionScaleUp {
			rules = append(rules, map[string]any{
				"alert": fmt.Sprintf("ScaleUpRecommended_%s", rec.WorkloadName),
				"expr":  fmt.Sprintf(`avg(container_cpu_usage_seconds_total{namespace="%s",pod=~"%s.*"}) > %.2f`, rec.Namespace, rec.WorkloadName, e.config.ScaleUpThreshold/100),
				"for":   "5m",
				"labels": map[string]string{
					"severity": "warning",
					"action":   "scale_up",
				},
				"annotations": map[string]string{
					"summary":     fmt.Sprintf("Scale up recommended for %s", rec.WorkloadName),
					"description": rec.Reason,
				},
			})
		}
	}

	data, _ := json.MarshalIndent(map[string]any{
		"groups": []map[string]any{
			{
				"name":  "chronicle-autoscaling",
				"rules": rules,
			},
		},
	}, "", "  ")

	return string(data)
}

// GetPredictionAccuracy returns prediction accuracy metrics.
func (e *AutoScalingEngine) GetPredictionAccuracy() PredictionAccuracy {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.predictions) == 0 {
		return PredictionAccuracy{}
	}

	var totalError, totalAbsError float64
	var count int

	for _, p := range e.predictions {
		if p.Actual > 0 {
			totalError += p.Error
			totalAbsError += math.Abs(p.Error)
			count++
		}
	}

	if count == 0 {
		return PredictionAccuracy{}
	}

	return PredictionAccuracy{
		MeanError:         totalError / float64(count),
		MeanAbsoluteError: totalAbsError / float64(count),
		SampleCount:       count,
	}
}

// PredictionAccuracy contains accuracy metrics.
type PredictionAccuracy struct {
	MeanError         float64 `json:"mean_error"`
	MeanAbsoluteError float64 `json:"mean_absolute_error"`
	SampleCount       int     `json:"sample_count"`
}

// --- Helper functions ---

func workloadKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func calculateStats(values []float64) *MetricStats {
	if len(values) == 0 {
		return &MetricStats{}
	}

	n := len(values)
	sorted := make([]float64, n)
	copy(sorted, values)
	sort.Float64s(sorted)

	var sum float64
	minVal := values[0]
	maxVal := values[0]

	for _, v := range values {
		sum += v
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	mean := sum / float64(n)

	var variance float64
	for _, v := range values {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(n)
	stdDev := math.Sqrt(variance)

	return &MetricStats{
		Min:    minVal,
		Max:    maxVal,
		Mean:   mean,
		StdDev: stdDev,
		P50:    autoscalingPercentile(sorted, 0.50),
		P90:    autoscalingPercentile(sorted, 0.90),
		P99:    autoscalingPercentile(sorted, 0.99),
	}
}

func autoscalingPercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func calculateDailyPattern(values []float64, timestamps []int64) []float64 {
	hourlyBuckets := make([][]float64, 24)
	for i := range hourlyBuckets {
		hourlyBuckets[i] = make([]float64, 0)
	}

	for i, ts := range timestamps {
		hour := time.Unix(0, ts).Hour()
		hourlyBuckets[hour] = append(hourlyBuckets[hour], values[i])
	}

	pattern := make([]float64, 24)
	for h := 0; h < 24; h++ {
		if len(hourlyBuckets[h]) > 0 {
			var sum float64
			for _, v := range hourlyBuckets[h] {
				sum += v
			}
			pattern[h] = sum / float64(len(hourlyBuckets[h]))
		}
	}

	return pattern
}

func identifyPeakHours(pattern []float64, highThreshold, lowThreshold float64) ([]int, []int) {
	var peakHours, lowHours []int

	for h, v := range pattern {
		if v >= highThreshold {
			peakHours = append(peakHours, h)
		} else if v <= lowThreshold && v > 0 {
			lowHours = append(lowHours, h)
		}
	}

	return peakHours, lowHours
}

func calculateTrend(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	n := float64(len(values))
	var sumX, sumY, sumXY, sumX2 float64

	for i, y := range values {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	// Linear regression slope
	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return 0
	}

	return (n*sumXY - sumX*sumY) / denominator
}

func calculateSpikiness(values []float64) float64 {
	if len(values) < 3 {
		return 0
	}

	// Calculate ratio of max to mean
	stats := calculateStats(values)
	if stats.Mean == 0 {
		return 0
	}

	return (stats.Max - stats.Mean) / stats.Mean
}

func detectSeasonality(values []float64) float64 {
	// Simple autocorrelation at lag 24 (assuming hourly data)
	if len(values) < 48 {
		return 0
	}

	lag := 24
	n := len(values) - lag

	var sum1, sum2, sumProd float64
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	for i := 0; i < n; i++ {
		diff1 := values[i] - mean
		diff2 := values[i+lag] - mean
		sumProd += diff1 * diff2
		sum1 += diff1 * diff1
		sum2 += diff2 * diff2
	}

	if sum1 == 0 || sum2 == 0 {
		return 0
	}

	return sumProd / math.Sqrt(sum1*sum2)
}

func calculateTargetReplicas(current int32, utilization, target float64) int32 {
	if target == 0 {
		return current
	}

	ratio := utilization / target
	newReplicas := int32(math.Ceil(float64(current) * ratio))

	// Clamp
	if newReplicas < 1 {
		newReplicas = 1
	}
	if newReplicas > 100 {
		newReplicas = 100
	}

	return newReplicas
}
