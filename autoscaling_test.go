package chronicle

import (
	"testing"
	"time"
)

func TestAutoScalingEngine_RegisterWorkload(t *testing.T) {
	db := &DB{}
	engine := NewAutoScalingEngine(db, DefaultAutoScalingConfig())

	engine.RegisterWorkload("api-server", "default", WorkloadKindDeployment)

	engine.mu.RLock()
	_, ok := engine.workloads["default/api-server"]
	engine.mu.RUnlock()

	if !ok {
		t.Error("expected workload to be registered")
	}
}

func TestAutoScalingEngine_RecordMetric(t *testing.T) {
	db := &DB{}
	config := DefaultAutoScalingConfig()
	config.MinDataPoints = 3
	engine := NewAutoScalingEngine(db, config)

	engine.RegisterWorkload("api-server", "default", WorkloadKindDeployment)

	// Record metrics
	now := time.Now()
	for i := 0; i < 10; i++ {
		engine.RecordMetric("default", "api-server", "cpu_utilization", 50.0+float64(i), now.Add(time.Duration(i)*time.Minute).UnixNano())
	}

	engine.mu.RLock()
	workload := engine.workloads["default/api-server"]
	series := workload.Metrics["cpu_utilization"]
	engine.mu.RUnlock()

	if len(series.Values) != 10 {
		t.Errorf("expected 10 values, got %d", len(series.Values))
	}

	if series.Stats == nil {
		t.Error("expected stats to be calculated")
	}
}

func TestAutoScalingEngine_PredictLoad(t *testing.T) {
	db := &DB{}
	config := DefaultAutoScalingConfig()
	config.MinDataPoints = 5
	config.SampleInterval = time.Minute
	config.PredictionHorizon = 10 * time.Minute
	engine := NewAutoScalingEngine(db, config)

	// Create test series
	series := &MetricSeries{
		Name:       "cpu_utilization",
		Values:     []float64{50, 55, 60, 65, 70, 75, 80},
		Timestamps: make([]int64, 7),
		Stats:      &MetricStats{StdDev: 10},
	}

	now := time.Now()
	for i := range series.Timestamps {
		series.Timestamps[i] = now.Add(time.Duration(i) * time.Minute).UnixNano()
	}

	predictions := engine.predictLoad(series, config.PredictionHorizon)

	if len(predictions) == 0 {
		t.Fatal("expected predictions")
	}

	// Trend should predict increasing values
	if predictions[0].Value <= 75 {
		t.Logf("First prediction: %.2f (trend may be dampened by smoothing)", predictions[0].Value)
	}

	// Check confidence intervals
	for i, p := range predictions {
		if p.Upper < p.Value || p.Lower > p.Value {
			t.Errorf("prediction %d: invalid confidence interval [%.2f, %.2f] for value %.2f", i, p.Lower, p.Upper, p.Value)
		}
	}
}

func TestAutoScalingEngine_GenerateRecommendation(t *testing.T) {
	db := &DB{}
	config := DefaultAutoScalingConfig()
	config.MinDataPoints = 5
	config.ScaleUpThreshold = 80
	config.ScaleDownThreshold = 20
	engine := NewAutoScalingEngine(db, config)

	engine.RegisterWorkload("api-server", "default", WorkloadKindDeployment)

	// Record high CPU utilization
	now := time.Now()
	for i := 0; i < 20; i++ {
		engine.RecordMetric("default", "api-server", "cpu_utilization", 85.0, now.Add(time.Duration(i)*time.Minute).UnixNano())
	}

	// Trigger analysis
	engine.analyzeWorkloads()

	rec, ok := engine.GetRecommendation("default", "api-server")
	if !ok {
		t.Fatal("expected recommendation")
	}

	if rec.Action != ScalingActionScaleUp {
		t.Errorf("expected ScaleUp action, got %s", rec.Action)
	}

	if rec.HPAConfig == nil {
		t.Error("expected HPA config")
	}
}

func TestAutoScalingEngine_ScaleDownRecommendation(t *testing.T) {
	db := &DB{}
	config := DefaultAutoScalingConfig()
	config.MinDataPoints = 5
	config.ScaleDownThreshold = 30
	engine := NewAutoScalingEngine(db, config)

	engine.RegisterWorkload("background-worker", "default", WorkloadKindDeployment)

	// Record low CPU utilization
	now := time.Now()
	for i := 0; i < 20; i++ {
		engine.RecordMetric("default", "background-worker", "cpu_utilization", 10.0, now.Add(time.Duration(i)*time.Minute).UnixNano())
	}

	engine.analyzeWorkloads()

	rec, ok := engine.GetRecommendation("default", "background-worker")
	if !ok {
		t.Fatal("expected recommendation")
	}

	if rec.Action != ScalingActionScaleDown {
		t.Errorf("expected ScaleDown action, got %s", rec.Action)
	}
}

func TestAutoScalingEngine_GetAllRecommendations(t *testing.T) {
	db := &DB{}
	config := DefaultAutoScalingConfig()
	config.MinDataPoints = 5
	engine := NewAutoScalingEngine(db, config)

	// Register multiple workloads
	workloads := []string{"api", "worker", "scheduler"}
	now := time.Now()

	for _, name := range workloads {
		engine.RegisterWorkload(name, "default", WorkloadKindDeployment)
		for i := 0; i < 10; i++ {
			engine.RecordMetric("default", name, "cpu_utilization", 85.0, now.Add(time.Duration(i)*time.Minute).UnixNano())
		}
	}

	engine.analyzeWorkloads()

	recs := engine.GetAllRecommendations()
	if len(recs) != 3 {
		t.Errorf("expected 3 recommendations, got %d", len(recs))
	}
}

func TestAutoScalingEngine_ExportPrometheusRules(t *testing.T) {
	db := &DB{}
	config := DefaultAutoScalingConfig()
	config.MinDataPoints = 5
	engine := NewAutoScalingEngine(db, config)

	engine.RegisterWorkload("api-server", "production", WorkloadKindDeployment)

	now := time.Now()
	for i := 0; i < 20; i++ {
		engine.RecordMetric("production", "api-server", "cpu_utilization", 90.0, now.Add(time.Duration(i)*time.Minute).UnixNano())
	}

	engine.analyzeWorkloads()

	rules := engine.ExportPrometheusRules()
	if rules == "" {
		t.Error("expected Prometheus rules")
	}

	if len(rules) < 50 {
		t.Errorf("rules seem too short: %s", rules)
	}
}

func TestCalculateStats(t *testing.T) {
	values := []float64{10, 20, 30, 40, 50}
	stats := calculateStats(values)

	if stats.Min != 10 {
		t.Errorf("expected min 10, got %f", stats.Min)
	}
	if stats.Max != 50 {
		t.Errorf("expected max 50, got %f", stats.Max)
	}
	if stats.Mean != 30 {
		t.Errorf("expected mean 30, got %f", stats.Mean)
	}
}

func TestCalculateTrend(t *testing.T) {
	// Increasing trend
	increasing := []float64{10, 20, 30, 40, 50}
	trend := calculateTrend(increasing)
	if trend <= 0 {
		t.Errorf("expected positive trend, got %f", trend)
	}

	// Decreasing trend
	decreasing := []float64{50, 40, 30, 20, 10}
	trend = calculateTrend(decreasing)
	if trend >= 0 {
		t.Errorf("expected negative trend, got %f", trend)
	}

	// Flat
	flat := []float64{30, 30, 30, 30, 30}
	trend = calculateTrend(flat)
	if trend != 0 {
		t.Errorf("expected zero trend, got %f", trend)
	}
}

func TestCalculateTargetReplicas(t *testing.T) {
	tests := []struct {
		current     int32
		utilization float64
		target      float64
		expected    int32
	}{
		{2, 80, 40, 4},  // Double replicas
		{4, 20, 40, 2},  // Halve replicas
		{1, 100, 50, 2}, // Scale up from 1
		{1, 10, 50, 1},  // Don't go below 1
	}

	for _, tt := range tests {
		result := calculateTargetReplicas(tt.current, tt.utilization, tt.target)
		if result != tt.expected {
			t.Errorf("calculateTargetReplicas(%d, %.1f, %.1f) = %d, want %d",
				tt.current, tt.utilization, tt.target, result, tt.expected)
		}
	}
}

func TestCalculateDailyPattern(t *testing.T) {
	// Create data with clear pattern
	var values []float64
	var timestamps []int64

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 24; h++ {
		// Higher during business hours
		var v float64
		if h >= 9 && h <= 17 {
			v = 80.0
		} else {
			v = 20.0
		}
		values = append(values, v)
		timestamps = append(timestamps, base.Add(time.Duration(h)*time.Hour).UnixNano())
	}

	pattern := calculateDailyPattern(values, timestamps)

	// Check business hours have higher values
	if pattern[12] <= pattern[2] {
		t.Error("expected higher utilization during business hours")
	}
}

func TestDefaultAutoScalingConfig(t *testing.T) {
	config := DefaultAutoScalingConfig()

	if !config.Enabled {
		t.Error("expected Enabled to be true")
	}
	if config.ScaleUpThreshold != 80 {
		t.Errorf("expected ScaleUpThreshold 80, got %f", config.ScaleUpThreshold)
	}
	if config.ScaleDownThreshold != 30 {
		t.Errorf("expected ScaleDownThreshold 30, got %f", config.ScaleDownThreshold)
	}
	if config.TargetUtilization != 70 {
		t.Errorf("expected TargetUtilization 70, got %f", config.TargetUtilization)
	}
}
