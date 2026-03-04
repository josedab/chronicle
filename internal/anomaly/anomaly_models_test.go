package anomaly

import (
	"math"
	"testing"
)

func TestIsolationForestDetailed(t *testing.T) {
	forest := NewIsolationForest(10, 50)
	if forest == nil {
		t.Fatal("NewIsolationForest returned nil")
	}

	// Score before training
	score := forest.Score(5.0)
	if score != 0.5 {
		t.Errorf("untrained score = %f, want 0.5", score)
	}

	// Train with normal data
	data := make([]float64, 100)
	for i := range data {
		data[i] = float64(i) / 10.0
	}
	forest.Train(data)

	// Score a normal value
	normalScore := forest.Score(5.0)
	if normalScore < 0 || normalScore > 1 {
		t.Errorf("score out of range [0,1]: %f", normalScore)
	}
}

func TestIsolationForestSmallData(t *testing.T) {
	forest := NewIsolationForest(5, 10)
	forest.Train([]float64{1.0})

	score := forest.Score(1.0)
	if score < 0 || score > 1 {
		t.Errorf("score out of range: %f", score)
	}
}

func TestIsolationForestConstantData(t *testing.T) {
	forest := NewIsolationForest(5, 10)
	data := make([]float64, 20)
	for i := range data {
		data[i] = 42.0
	}
	forest.Train(data)

	score := forest.Score(42.0)
	if score < 0 || score > 1 {
		t.Errorf("score out of range: %f", score)
	}
}

func TestLSTMModelDetailed(t *testing.T) {
	model := NewLSTMModel(10, 8, 1)
	if model == nil {
		t.Fatal("NewLSTMModel returned nil")
	}

	// Predict before training with empty context
	pred := model.Predict(nil)
	if pred != 0 {
		t.Errorf("untrained predict(nil) = %f, want 0", pred)
	}

	// Predict before training with context
	pred = model.Predict([]float64{1, 2, 3})
	if pred != 3 {
		t.Errorf("untrained predict should return last context value, got %f", pred)
	}

	// Train
	data := make([]float64, 50)
	for i := range data {
		data[i] = math.Sin(float64(i) * 0.1)
	}
	model.Train(data)

	// Predict after training
	context := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
	pred = model.Predict(context)
	if math.IsNaN(pred) || math.IsInf(pred, 0) {
		t.Errorf("prediction is NaN or Inf: %f", pred)
	}
}

func TestAutoencoder(t *testing.T) {
	ae := NewAutoencoder(5, 3)
	if ae == nil {
		t.Fatal("NewAutoencoder returned nil")
	}

	// Reconstruct before training
	input := []float64{1, 2, 3, 4, 5}
	output := ae.Reconstruct(input)
	if len(output) != 5 {
		t.Fatalf("expected 5 outputs, got %d", len(output))
	}

	// Train with short data
	ae.Train([]float64{1, 2, 3})

	// Train with sufficient data
	data := make([]float64, 50)
	for i := range data {
		data[i] = float64(i) * 0.5
	}
	ae.Train(data)
	if !ae.trained {
		t.Error("expected trained=true after training")
	}
}

func TestTransformerModelDetailed(t *testing.T) {
	model := NewTransformerModel(10, 8, 2)
	if model == nil {
		t.Fatal("NewTransformerModel returned nil")
	}

	// Predict with empty context
	attn, pred := model.Predict(nil)
	if attn != nil {
		t.Errorf("expected nil attention for empty context, got %v", attn)
	}
	if pred != 0 {
		t.Errorf("expected 0 prediction for empty context, got %f", pred)
	}

	// Train
	model.Train([]float64{1, 2, 3, 4, 5})

	// Predict with context
	context := []float64{1, 2, 3, 4, 5}
	attn, pred = model.Predict(context)
	if len(attn) != len(context) {
		t.Errorf("expected %d attention weights, got %d", len(context), len(attn))
	}

	// Attention should sum to ~1
	attnSum := 0.0
	for _, a := range attn {
		attnSum += a
	}
	if math.Abs(attnSum-1.0) > 1e-6 {
		t.Errorf("attention weights should sum to 1, got %f", attnSum)
	}
}

func TestStatisticalModelDetailed(t *testing.T) {
	model := NewStatisticalModel(0.8)
	if model == nil {
		t.Fatal("NewStatisticalModel returned nil")
	}

	// Score before training
	score := model.Score(5.0)
	if score != 0.5 {
		t.Errorf("untrained score = %f, want 0.5", score)
	}

	// Train with empty data
	model.Train(nil)
	if model.trained {
		t.Error("should not be trained with empty data")
	}

	// Train with real data
	data := make([]float64, 100)
	for i := range data {
		data[i] = float64(i)
	}
	model.Train(data)

	// Score normal value (near mean)
	normalScore := model.Score(50.0)
	if normalScore < 0 || normalScore > 1 {
		t.Errorf("normal score out of range: %f", normalScore)
	}

	// Score extreme value (outlier)
	outlierScore := model.Score(1000.0)
	if outlierScore < 0 || outlierScore > 1 {
		t.Errorf("outlier score out of range: %f", outlierScore)
	}
	if outlierScore <= normalScore {
		t.Errorf("outlier score (%f) should be > normal score (%f)", outlierScore, normalScore)
	}
}
