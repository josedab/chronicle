// autoscaling_prediction.go contains extended autoscaling functionality.
package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

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
