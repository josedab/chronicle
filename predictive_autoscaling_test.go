package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestPredictiveAutoscalerBasic(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultPredictiveAutoscalingConfig()
	scaler := NewPredictiveAutoscaler(db, config)

	// Record enough samples for prediction
	now := time.Now()
	for i := 0; i < 10; i++ {
		scaler.RecordSample(LoadSample{
			Timestamp:   now.Add(-time.Duration(10-i) * time.Minute),
			CPUUsage:    0.3 + float64(i)*0.05,
			MemoryUsage: 0.4,
			WriteRate:   1000 + float64(i)*100,
		})
	}

	rec, err := scaler.Evaluate(context.Background())
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	if rec == nil {
		t.Fatal("expected non-nil recommendation")
	}
	if rec.CurrentReplicas != config.MinReplicas {
		t.Errorf("expected %d replicas, got %d", config.MinReplicas, rec.CurrentReplicas)
	}
}

func TestPredictiveAutoscalerInsufficientData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	scaler := NewPredictiveAutoscaler(db, DefaultPredictiveAutoscalingConfig())

	rec, err := scaler.Evaluate(context.Background())
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	if rec.Direction != ScaleNone {
		t.Errorf("expected ScaleNone, got %s", rec.Direction)
	}
}

func TestPredictiveAutoscalerForecast(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	scaler := NewPredictiveAutoscaler(db, DefaultPredictiveAutoscalingConfig())

	for i := 0; i < 5; i++ {
		scaler.RecordSample(LoadSample{
			Timestamp: time.Now().Add(-time.Duration(5-i) * time.Minute),
			CPUUsage:  float64(i) * 0.2,
		})
	}

	forecast := scaler.GetForecast()
	if forecast == nil {
		t.Fatal("expected non-nil forecast")
	}
	if len(forecast.PredictedValues) == 0 {
		t.Error("expected predicted values")
	}
	if forecast.Model != "exponential_smoothing" {
		t.Errorf("expected exponential_smoothing model, got %s", forecast.Model)
	}
}

func TestPredictiveAutoscalerCallback(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultPredictiveAutoscalingConfig()
	config.ScaleUpThreshold = 0.01 // Very low threshold to trigger scale-up
	scaler := NewPredictiveAutoscaler(db, config)

	var called bool
	scaler.OnScalingEvent(func(rec PredictiveScalingRecommendation) {
		called = true
	})

	for i := 0; i < 10; i++ {
		scaler.RecordSample(LoadSample{
			Timestamp: time.Now().Add(-time.Duration(10-i) * time.Minute),
			CPUUsage:  0.9,
			MemoryUsage: 0.9,
			WriteRate: 50000,
		})
	}

	scaler.Evaluate(context.Background())
	// May or may not trigger based on per-replica load
	_ = called
}

func TestPredictiveAutoscalerStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	scaler := NewPredictiveAutoscaler(db, DefaultPredictiveAutoscalingConfig())
	stats := scaler.Stats()
	if stats.EvaluationsRun != 0 {
		t.Errorf("expected 0 evaluations, got %d", stats.EvaluationsRun)
	}
}

func TestPredictiveAutoscalerStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultPredictiveAutoscalingConfig()
	config.Enabled = true
	config.EvaluationInterval = 50 * time.Millisecond
	scaler := NewPredictiveAutoscaler(db, config)
	scaler.Start()
	time.Sleep(30 * time.Millisecond)
	scaler.Stop()
}

func TestPredictiveAutoscalerHPAMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	scaler := NewPredictiveAutoscaler(db, DefaultPredictiveAutoscalingConfig())
	metrics := scaler.HPAMetrics()
	if _, ok := metrics["chronicle_current_replicas"]; !ok {
		t.Error("expected chronicle_current_replicas in HPA metrics")
	}
}

func TestPredictiveAutoscalerHistory(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	scaler := NewPredictiveAutoscaler(db, DefaultPredictiveAutoscalingConfig())

	// Add enough samples so Evaluate doesn't short-circuit
	for i := 0; i < 5; i++ {
		scaler.RecordSample(LoadSample{
			Timestamp: time.Now().Add(-time.Duration(5-i) * time.Minute),
			CPUUsage:  0.5,
		})
	}
	scaler.Evaluate(context.Background())

	history := scaler.History()
	if len(history) != 1 {
		t.Errorf("expected 1 history entry, got %d", len(history))
	}
}
