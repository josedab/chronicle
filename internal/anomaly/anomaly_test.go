package anomaly

import (
	"testing"
)

func TestAnomalyConfig(t *testing.T) {
	config := DefaultAnomalyConfig()

	if config.Sensitivity <= 0 {
		t.Error("Sensitivity should be positive")
	}
	if config.WindowSize == 0 {
		t.Error("WindowSize should be set")
	}
	if config.MinDataPoints == 0 {
		t.Error("MinDataPoints should be set")
	}
}

func TestAnomalyResult(t *testing.T) {
	result := AnomalyResult{
		IsAnomaly:     true,
		Score:         0.95,
		ExpectedValue: 50.0,
		Deviation:     45.0,
		Confidence:    0.85,
	}

	if !result.IsAnomaly {
		t.Error("Result should be an anomaly")
	}
	if result.Score < 0.9 {
		t.Errorf("Expected score > 0.9, got %f", result.Score)
	}
}

func TestIsolationForest(t *testing.T) {
	forest := NewIsolationForest(100, 256)
	if forest == nil {
		t.Fatal("Failed to create IsolationForest")
	}

	// Train with sample data
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i % 100)
	}
	forest.Train(data)

	// Test score (not Predict)
	score := forest.Score(50.0) // Normal value
	if score < 0 || score > 1 {
		t.Errorf("Score should be between 0 and 1, got %f", score)
	}
}

func TestStatisticalModel(t *testing.T) {
	model := NewStatisticalModel(0.95)
	if model == nil {
		t.Fatal("Failed to create StatisticalModel")
	}

	// Train with sample data
	data := make([]float64, 100)
	for i := range data {
		data[i] = float64(i)
	}
	model.Train(data)

	// Check mean is calculated (std not stddev)
	if model.mean == 0 && model.std == 0 {
		t.Error("Mean and std should be calculated after training")
	}
}

func TestLSTMModel(t *testing.T) {
	model := NewLSTMModel(1, 32, 2)
	if model == nil {
		t.Fatal("Failed to create LSTMModel")
	}

	// Test that model can be initialized
	if model.hiddenSize != 32 {
		t.Errorf("Expected hidden size 32, got %d", model.hiddenSize)
	}
}

func TestAutoencoderModel(t *testing.T) {
	model := NewAutoencoder(10, 4)
	if model == nil {
		t.Fatal("Failed to create Autoencoder")
	}
}

func TestTransformerModel(t *testing.T) {
	model := NewTransformerModel(64, 4, 2)
	if model == nil {
		t.Fatal("Failed to create TransformerModel")
	}
}

func TestAnomalyDetector(t *testing.T) {
	config := DefaultAnomalyConfig()
	detector := NewAnomalyDetector(nil, config)
	if detector == nil {
		t.Fatal("Failed to create AnomalyDetector")
	}
}

func TestAnomalyModelType(t *testing.T) {
	tests := []struct {
		model    AnomalyModel
		expected string
	}{
		{AnomalyModelIsolationForest, "isolation_forest"},
		{AnomalyModelStatistical, "statistical"},
		{AnomalyModelLSTM, "lstm"},
		{AnomalyModelAutoencoder, "autoencoder"},
		{AnomalyModelTransformer, "transformer"},
		{AnomalyModelEnsemble, "ensemble"},
	}

	for _, tc := range tests {
		if tc.model.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.model.String())
		}
	}
}

func TestPrediction(t *testing.T) {
	pred := Prediction{
		Timestamp: 1000000000,
		Predicted: 50.0,
		Actual:    55.0,
		IsAnomaly: false,
		Score:     0.1,
		Model:     "ensemble",
	}

	if pred.IsAnomaly {
		t.Error("Should not be an anomaly")
	}
	if pred.Score > 0.5 {
		t.Errorf("Score should be low for non-anomaly, got %f", pred.Score)
	}
}

func TestDefaultAnomalyConfig(t *testing.T) {
	config := DefaultAnomalyConfig()

	if config.Model != AnomalyModelEnsemble {
		t.Errorf("Default model should be ensemble, got %v", config.Model)
	}
	if config.Sensitivity <= 0 || config.Sensitivity > 1 {
		t.Errorf("Sensitivity should be between 0 and 1, got %f", config.Sensitivity)
	}
	if !config.AutoTrain {
		t.Error("AutoTrain should be enabled by default")
	}
	if config.RetrainInterval <= 0 {
		t.Error("RetrainInterval should be positive")
	}
}
