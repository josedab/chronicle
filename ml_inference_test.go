package chronicle

import (
	"context"
	"math"
	"os"
	"testing"
	"time"
)

func TestMLInferencePipeline(t *testing.T) {
	// Create temp DB
	path := "test_ml_inference.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Create pipeline
	config := DefaultMLInferenceConfig()
	pipeline := NewMLInferencePipeline(db, config)
	pipeline.Start()
	defer pipeline.Stop()

	t.Run("RegisterModel", func(t *testing.T) {
		model := &InferenceModel{
			ID:          "test-model",
			Name:        "Test Model",
			Type:        InferenceModelTypeAnomalyDetector,
			Description: "A test model",
		}

		err := pipeline.RegisterModel(model)
		if err != nil {
			t.Fatalf("Failed to register model: %v", err)
		}

		// Verify it's registered
		retrieved, ok := pipeline.GetModel("test-model")
		if !ok {
			t.Fatal("Model not found after registration")
		}

		if retrieved.Name != "Test Model" {
			t.Errorf("Expected name 'Test Model', got '%s'", retrieved.Name)
		}
	})

	t.Run("ListModels", func(t *testing.T) {
		models := pipeline.ListModels()
		if len(models) == 0 {
			t.Error("Expected at least one model")
		}
	})

	t.Run("UnregisterModel", func(t *testing.T) {
		pipeline.UnregisterModel("test-model")
		_, ok := pipeline.GetModel("test-model")
		if ok {
			t.Error("Model should not exist after unregistration")
		}
	})
}

func TestStatisticalExtractor(t *testing.T) {
	extractor := NewStatisticalExtractor()

	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	timestamps := make([]int64, len(values))

	features, err := extractor.Extract(values, timestamps)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(features) != 9 {
		t.Errorf("Expected 9 features, got %d", len(features))
	}

	// Check mean (should be 5.5)
	if math.Abs(features[0]-5.5) > 0.001 {
		t.Errorf("Expected mean 5.5, got %f", features[0])
	}

	// Check min (should be 1)
	if features[2] != 1 {
		t.Errorf("Expected min 1, got %f", features[2])
	}

	// Check max (should be 10)
	if features[3] != 10 {
		t.Errorf("Expected max 10, got %f", features[3])
	}
}

func TestTemporalExtractor(t *testing.T) {
	extractor := NewTemporalExtractor()

	// Create linear trend data
	values := make([]float64, 100)
	timestamps := make([]int64, 100)
	for i := range values {
		values[i] = float64(i) * 2
		timestamps[i] = int64(i) * int64(time.Second)
	}

	features, err := extractor.Extract(values, timestamps)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(features) != 5 {
		t.Errorf("Expected 5 features, got %d", len(features))
	}

	// Check that trend is positive
	if features[3] <= 0 {
		t.Errorf("Expected positive trend, got %f", features[3])
	}
}

func TestRollingWindowExtractor(t *testing.T) {
	extractor := NewRollingWindowExtractor(5)

	values := []float64{1, 2, 3, 4, 5, 100, 6, 7, 8, 9}
	timestamps := make([]int64, len(values))

	features, err := extractor.Extract(values, timestamps)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(features) != len(values) {
		t.Errorf("Expected %d features, got %d", len(values), len(features))
	}

	// The 100 should have a high z-score
	if features[5] < 1.9 {
		t.Errorf("Expected anomaly index 5 to have high score, got %f", features[5])
	}
}

func TestAutoMLSelector(t *testing.T) {
	config := DefaultInferenceAutoMLConfig()
	selector := NewAutoMLSelector(config)

	// Create test data
	values := make([]float64, 200)
	for i := range values {
		values[i] = math.Sin(float64(i)*0.1) + float64(i)*0.01
	}

	model, metrics, err := selector.SelectBestModel(values, "test")
	if err != nil {
		t.Fatalf("SelectBestModel failed: %v", err)
	}

	if model == nil {
		t.Fatal("Expected a model to be selected")
	}

	if metrics["score"] <= 0 {
		t.Error("Expected positive score")
	}
}

func TestMLInferencePipeline_TrainAndScore(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test_ml_train.db"

	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write test data
	now := time.Now()
	for i := 0; i < 200; i++ {
		p := Point{
			Metric:    "test_metric",
			Value:     math.Sin(float64(i)*0.1) * 10,
			Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	db.Flush()

	// Create pipeline
	config := DefaultMLInferenceConfig()
	pipeline := NewMLInferencePipeline(db, config)
	pipeline.Start()
	defer pipeline.Stop()

	ctx := context.Background()

	queryStart := now.Add(-time.Hour).UnixNano()
	queryEnd := now.Add(time.Hour).UnixNano()

	t.Run("TrainModel", func(t *testing.T) {
		model, err := pipeline.TrainModel(
			ctx,
			"trained-model",
			"test_metric",
			queryStart,
			queryEnd,
			InferenceModelTypeAnomalyDetector,
		)
		if err != nil {
			t.Fatalf("TrainModel failed: %v", err)
		}

		if model.ID != "trained-model" {
			t.Errorf("Expected ID 'trained-model', got '%s'", model.ID)
		}

		// Verify model is registered
		_, ok := pipeline.GetModel("trained-model")
		if !ok {
			t.Error("Trained model not found in registry")
		}
	})

	t.Run("ScoreMetric", func(t *testing.T) {
		result, err := pipeline.ScoreMetric(
			ctx,
			"trained-model",
			"test_metric",
			queryStart,
			queryEnd,
		)
		if err != nil {
			t.Fatalf("ScoreMetric failed: %v", err)
		}

		if len(result.Scores) == 0 {
			t.Error("Expected scores in result")
		}
	})

	t.Run("AutoDetectAnomalies", func(t *testing.T) {
		result, err := pipeline.AutoDetectAnomalies(
			ctx,
			"test_metric",
			queryStart,
			queryEnd,
		)
		if err != nil {
			t.Fatalf("AutoDetectAnomalies failed: %v", err)
		}

		if result == nil {
			t.Error("Expected non-nil result")
		}
	})
}

func TestMLInferenceStats(t *testing.T) {
	// Create temp DB
	path := "test_ml_stats.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	config := DefaultMLInferenceConfig()
	pipeline := NewMLInferencePipeline(db, config)
	pipeline.Start()
	defer pipeline.Stop()

	stats := pipeline.GetStats()
	if stats.ModelsLoaded != 0 {
		t.Errorf("Expected 0 models loaded, got %d", stats.ModelsLoaded)
	}

	// Register a model
	model := &InferenceModel{
		ID:   "stats-test",
		Name: "Stats Test",
		Type: InferenceModelTypeAnomalyDetector,
	}
	pipeline.RegisterModel(model)

	stats = pipeline.GetStats()
	if stats.ModelsLoaded != 1 {
		t.Errorf("Expected 1 model loaded, got %d", stats.ModelsLoaded)
	}
}

func TestModelRegistry(t *testing.T) {
	registry := NewModelRegistry("/tmp/test_models")

	// Create a simple model
	model := NewStatisticalAnomalyDetector("test-detector", 2.0)
	model.Train([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	// Save
	err := registry.Save("test-detector", model)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// List
	ids := registry.List()
	if len(ids) != 1 {
		t.Errorf("Expected 1 model, got %d", len(ids))
	}

	// Load
	loaded := NewStatisticalAnomalyDetector("loaded", 2.0)
	err = registry.Load("test-detector", loaded)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
}

func TestInferenceHook(t *testing.T) {
	// Create temp DB
	path := "test_ml_hook.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	config := DefaultMLInferenceConfig()
	pipeline := NewMLInferencePipeline(db, config)
	pipeline.Start()
	defer pipeline.Stop()

	// Create a test hook
	hook := &testInferenceHook{}
	pipeline.AddHook(hook)

	// The hook should be registered
	if len(pipeline.hooks) != 1 {
		t.Errorf("Expected 1 hook, got %d", len(pipeline.hooks))
	}
}

type testInferenceHook struct {
	calls []Point
}

func (h *testInferenceHook) OnScore(point Point, score float64, anomaly bool) {
	h.calls = append(h.calls, point)
}
