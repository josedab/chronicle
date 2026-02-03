package chronicle

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultAutoMLConfig(t *testing.T) {
	cfg := DefaultAutoMLConfig()

	if !cfg.EnableAutoSelection {
		t.Error("Auto selection should be enabled by default")
	}
	if cfg.CrossValidationFolds != 3 {
		t.Errorf("Expected 3 CV folds, got %d", cfg.CrossValidationFolds)
	}
	if cfg.HoldoutRatio != 0.2 {
		t.Errorf("Expected 0.2 holdout ratio, got %f", cfg.HoldoutRatio)
	}
	if cfg.MinDataPoints != 15 {
		t.Errorf("Expected 15 min data points, got %d", cfg.MinDataPoints)
	}
	if !cfg.SeasonalityDetection {
		t.Error("Seasonality detection should be enabled by default")
	}
	if !cfg.TrendDetection {
		t.Error("Trend detection should be enabled by default")
	}
}

func TestAutoMLForecasterBasic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	// Write linear trending data
	for i := 0; i < 100; i++ {
		p := Point{
			Metric:    "test_metric",
			Value:     float64(10 + i*2),
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	result, err := forecaster.AutoForecast(ctx, "test_metric", 10, nil)
	if err != nil {
		t.Fatalf("AutoForecast failed: %v", err)
	}

	if result.DataPoints != 100 {
		t.Errorf("Expected 100 data points, got %d", result.DataPoints)
	}

	if len(result.CandidateModels) == 0 {
		t.Error("Expected at least one candidate model")
	}

	if result.Forecast == nil {
		t.Error("Expected forecast result")
	}

	if len(result.Forecast.Predictions) != 10 {
		t.Errorf("Expected 10 predictions, got %d", len(result.Forecast.Predictions))
	}

	// Trend should be detected
	if !result.TimeSeriesCharacteristics.HasTrend {
		t.Error("Expected trend to be detected in linear data")
	}
}

func TestAutoMLTrendDetection(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	// Write upward trending data
	for i := 0; i < 50; i++ {
		p := Point{
			Metric:    "uptrend",
			Value:     float64(i) * 3,
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	chars, err := forecaster.AnalyzeTimeSeries(ctx, "uptrend", nil)
	if err != nil {
		t.Fatalf("AnalyzeTimeSeries failed: %v", err)
	}

	if !chars.HasTrend {
		t.Error("Expected trend to be detected")
	}
	if chars.TrendDirection != 1 {
		t.Errorf("Expected upward trend (1), got %d", chars.TrendDirection)
	}
	if chars.TrendStrength < 0.9 {
		t.Errorf("Expected high trend strength for linear data, got %f", chars.TrendStrength)
	}
}

func TestAutoMLSeasonalityDetection(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-48 * time.Hour).UnixNano()

	// Write seasonal data with period 12
	for i := 0; i < 96; i++ {
		seasonal := []float64{10, 15, 20, 25, 30, 35, 35, 30, 25, 20, 15, 10}
		value := seasonal[i%12]
		p := Point{
			Metric:    "seasonal",
			Value:     value,
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	chars, err := forecaster.AnalyzeTimeSeries(ctx, "seasonal", nil)
	if err != nil {
		t.Fatalf("AnalyzeTimeSeries failed: %v", err)
	}

	if !chars.HasSeasonality {
		t.Error("Expected seasonality to be detected")
	}
	// Period should be detected (allow some flexibility)
	if chars.SeasonalPeriod < 10 || chars.SeasonalPeriod > 14 {
		t.Errorf("Expected seasonal period around 12, got %d", chars.SeasonalPeriod)
	}
}

func TestAutoMLStationarityCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	// Write stationary data (constant with small noise)
	for i := 0; i < 60; i++ {
		noise := float64(i%3 - 1) // Small variation: -1, 0, 1
		p := Point{
			Metric:    "stationary",
			Value:     50.0 + noise,
			Timestamp: baseTime + int64(i)*int64(time.Minute),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	chars, err := forecaster.AnalyzeTimeSeries(ctx, "stationary", nil)
	if err != nil {
		t.Fatalf("AnalyzeTimeSeries failed: %v", err)
	}

	if !chars.IsStationary {
		t.Error("Expected data to be classified as stationary")
	}
}

func TestAutoMLModelSelection(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	// Write predictable data
	for i := 0; i < 80; i++ {
		p := Point{
			Metric:    "predictable",
			Value:     float64(i % 10), // Repeating pattern
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	model, chars, err := forecaster.SelectBestModel(ctx, "predictable", nil)
	if err != nil {
		t.Fatalf("SelectBestModel failed: %v", err)
	}

	if model == nil {
		t.Fatal("Expected a model to be selected")
	}

	if model.Name == "" {
		t.Error("Expected model to have a name")
	}

	if model.Score <= 0 {
		t.Error("Expected positive score")
	}

	if chars == nil {
		t.Error("Expected data characteristics")
	}
}

func TestAutoMLInsufficientData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().UnixNano()

	// Write only 5 points (less than minimum)
	for i := 0; i < 5; i++ {
		p := Point{
			Metric:    "sparse",
			Value:     float64(i),
			Timestamp: baseTime + int64(i)*int64(time.Minute),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	_, err = forecaster.AutoForecast(ctx, "sparse", 5, nil)
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestAutoMLNoData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	_, err = forecaster.AutoForecast(context.Background(), "nonexistent", 5, nil)
	if err == nil {
		t.Error("Expected error for nonexistent metric")
	}
}

func TestAutoMLEmptyMetric(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	_, err = forecaster.AutoForecast(context.Background(), "", 5, nil)
	if err == nil {
		t.Error("Expected error for empty metric name")
	}
}

func TestAutoMLCandidateModels(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-48 * time.Hour).UnixNano()

	// Write enough data for various models
	for i := 0; i < 100; i++ {
		p := Point{
			Metric:    "full_test",
			Value:     float64(50 + i%20), // Some pattern
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	result, err := forecaster.AutoForecast(ctx, "full_test", 5, nil)
	if err != nil {
		t.Fatalf("AutoForecast failed: %v", err)
	}

	// Should have multiple candidate models
	if len(result.CandidateModels) < 3 {
		t.Errorf("Expected at least 3 candidate models, got %d", len(result.CandidateModels))
	}

	// Models should be sorted by score (ascending)
	for i := 1; i < len(result.CandidateModels); i++ {
		if result.CandidateModels[i].Score < result.CandidateModels[i-1].Score {
			t.Error("Candidate models should be sorted by score (ascending)")
		}
	}

	// Best model should be first
	if result.BestModel.Name != result.CandidateModels[0].Name {
		t.Error("BestModel should match the first candidate")
	}
}

func TestAutoMLMetricsCalculation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	// Write linear data for predictable metrics
	for i := 0; i < 50; i++ {
		p := Point{
			Metric:    "linear",
			Value:     float64(i) * 2,
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	result, err := forecaster.AutoForecast(ctx, "linear", 5, nil)
	if err != nil {
		t.Fatalf("AutoForecast failed: %v", err)
	}

	// Check metrics are populated
	metrics := result.BestModel.Metrics
	if metrics.RMSE < 0 {
		t.Error("RMSE should be non-negative")
	}
	if metrics.MAE < 0 {
		t.Error("MAE should be non-negative")
	}
	if metrics.MAPE < 0 {
		t.Error("MAPE should be non-negative")
	}

	// For linear data, R² should be reasonable
	if metrics.R2 < -1 || metrics.R2 > 1 {
		t.Errorf("R² should be between -1 and 1, got %f", metrics.R2)
	}
}

func TestAutoMLTrainingTime(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	for i := 0; i < 50; i++ {
		p := Point{
			Metric:    "timed",
			Value:     float64(i),
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	result, err := forecaster.AutoForecast(ctx, "timed", 5, nil)
	if err != nil {
		t.Fatalf("AutoForecast failed: %v", err)
	}

	// Training time should be recorded
	if result.TrainingTime <= 0 {
		t.Error("Training time should be positive")
	}
}

func TestAutoMLWithTags(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	// Write data with different tags
	for i := 0; i < 30; i++ {
		p := Point{
			Metric:    "tagged_metric",
			Value:     float64(i),
			Tags:      map[string]string{"region": "us-west"},
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}

	for i := 0; i < 30; i++ {
		p := Point{
			Metric:    "tagged_metric",
			Value:     float64(100 + i),
			Tags:      map[string]string{"region": "eu-west"},
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	// Query with specific tag
	result, err := forecaster.AutoForecast(ctx, "tagged_metric", 5, map[string]string{"region": "us-west"})
	if err != nil {
		t.Fatalf("AutoForecast failed: %v", err)
	}

	// Should only get data from us-west (lower values)
	if result.TimeSeriesCharacteristics.SeriesMean > 50 {
		t.Errorf("Mean %f seems too high for us-west data (expected < 50)", result.TimeSeriesCharacteristics.SeriesMean)
	}
}

func TestTimeSeriesCharacteristicsVolatility(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath, DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	baseTime := time.Now().Add(-24 * time.Hour).UnixNano()

	// Write volatile data
	for i := 0; i < 50; i++ {
		value := 50.0
		if i%2 == 0 {
			value = 100.0
		}
		p := Point{
			Metric:    "volatile",
			Value:     value,
			Timestamp: baseTime + int64(i)*int64(time.Minute),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("Failed to write point: %v", err)
		}
	}
	_ = db.Flush()

	forecaster := NewAutoMLForecaster(db, DefaultAutoMLConfig())

	chars, err := forecaster.AnalyzeTimeSeries(ctx, "volatile", nil)
	if err != nil {
		t.Fatalf("AnalyzeTimeSeries failed: %v", err)
	}

	// Should detect high volatility
	if chars.Volatility < 0.2 {
		t.Errorf("Expected high volatility, got %f", chars.Volatility)
	}

	// Mean should be around 75
	if math.Abs(chars.SeriesMean-75) > 5 {
		t.Errorf("Expected mean around 75, got %f", chars.SeriesMean)
	}
}

func TestMinIntHelper(t *testing.T) {
	if minInt(5, 10) != 5 {
		t.Error("minInt(5, 10) should be 5")
	}
	if minInt(10, 5) != 5 {
		t.Error("minInt(10, 5) should be 5")
	}
	if minInt(5, 5) != 5 {
		t.Error("minInt(5, 5) should be 5")
	}
}
