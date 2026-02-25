package chronicle

import (
	"context"
	"fmt"
	"math"
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
