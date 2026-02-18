package chronicle

import (
	"context"
	"testing"
)

func TestFoundationModelForecast(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	input := TSModelInput{
		Task:       TSModelForecast,
		Metric:     "cpu_usage",
		Values:     []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
		Timestamps: []int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
		Horizon:    5,
	}

	result, err := fm.Forecast(context.Background(), input)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}
	if len(result.Values) != 5 {
		t.Errorf("expected 5 predictions, got %d", len(result.Values))
	}
	if len(result.Timestamps) != 5 {
		t.Errorf("expected 5 timestamps, got %d", len(result.Timestamps))
	}
	// Predictions should continue the trend
	if result.Values[0] <= 10.0 {
		t.Errorf("first prediction %f should be > 10.0 for upward trend", result.Values[0])
	}
	if result.Confidence <= 0 {
		t.Error("confidence should be positive")
	}
}

func TestFoundationModelForecastMinData(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	_, err := fm.Forecast(context.Background(), TSModelInput{Values: []float64{1.0}})
	if err == nil {
		t.Fatal("expected error for insufficient data")
	}
}

func TestFoundationModelDetectAnomalies(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	// Normal values with one extreme outlier
	input := TSModelInput{
		Metric: "latency",
		Values: []float64{10, 11, 10, 12, 10, 11, 10, 11, 10, 11, 10, 11, 100, 10, 11, 10},
	}

	result, err := fm.DetectAnomalies(context.Background(), input)
	if err != nil {
		t.Fatalf("DetectAnomalies failed: %v", err)
	}
	if len(result.Anomalies) == 0 {
		t.Error("expected at least 1 anomaly for value 100")
	}
	if result.Threshold != 3.0 {
		t.Errorf("expected threshold 3.0, got %f", result.Threshold)
	}
}

func TestFoundationModelClassify(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	// Upward trending data
	input := TSModelInput{
		Metric: "requests",
		Values: []float64{1, 2, 4, 8, 16, 32, 64, 128},
	}

	result, err := fm.Classify(context.Background(), input)
	if err != nil {
		t.Fatalf("Classify failed: %v", err)
	}
	if result.Label == "" {
		t.Error("expected a classification label")
	}
	if len(result.Scores) == 0 {
		t.Error("expected classification scores")
	}
}

func TestFoundationModelEmbed(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	input := TSModelInput{
		Metric: "cpu",
		Values: []float64{1, 2, 3, 4, 5},
	}

	result, err := fm.Embed(context.Background(), input)
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	if result.Dimension != 32 {
		t.Errorf("expected 32 dimensions, got %d", result.Dimension)
	}
	if len(result.Embedding) != 32 {
		t.Errorf("expected 32 values, got %d", len(result.Embedding))
	}
}

func TestFoundationModelFineTune(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	data := []TSModelInput{
		{Metric: "cpu", Values: []float64{1, 2, 3, 4, 5}},
	}

	if err := fm.FineTune(context.Background(), data); err != nil {
		t.Fatalf("FineTune failed: %v", err)
	}

	stats := fm.Stats()
	if stats.FineTuneCount != 1 {
		t.Errorf("expected 1 fine-tune count, got %d", stats.FineTuneCount)
	}
}

func TestFoundationModelFineTuneDisabled(t *testing.T) {
	cfg := DefaultFoundationModelConfig()
	cfg.EnableFineTuning = false
	fm := NewFoundationModel(nil, cfg)

	err := fm.FineTune(context.Background(), []TSModelInput{{Values: []float64{1, 2}}})
	if err == nil {
		t.Fatal("expected error when fine-tuning disabled")
	}
}

func TestFoundationModelRegistry(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	models := fm.Registry().List()
	if len(models) != 1 {
		t.Fatalf("expected 1 default model, got %d", len(models))
	}

	fm.Registry().Register(&TSModelInfo{ID: "custom-v1", Name: "Custom", Version: "1.0"})
	models = fm.Registry().List()
	if len(models) != 2 {
		t.Errorf("expected 2 models, got %d", len(models))
	}
}

func TestFoundationModelStats(t *testing.T) {
	fm := NewFoundationModel(nil, DefaultFoundationModelConfig())

	fm.Forecast(context.Background(), TSModelInput{Metric: "x", Values: []float64{1, 2, 3}})
	fm.DetectAnomalies(context.Background(), TSModelInput{Metric: "x", Values: []float64{1, 2, 3}})

	stats := fm.Stats()
	if stats.TotalInferences != 2 {
		t.Errorf("expected 2 inferences, got %d", stats.TotalInferences)
	}
	if stats.ForecastCount != 1 {
		t.Errorf("expected 1 forecast, got %d", stats.ForecastCount)
	}
	if stats.AnomalyCount != 1 {
		t.Errorf("expected 1 anomaly, got %d", stats.AnomalyCount)
	}
}
