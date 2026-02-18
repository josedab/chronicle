package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestForecaster_SimpleExponential(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method: ForecastMethodSimpleExponential,
		Alpha:  0.5,
	})

	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		Values:     []float64{10, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}

	result, err := f.Forecast(data, 5)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if len(result.Predictions) != 5 {
		t.Errorf("expected 5 predictions, got %d", len(result.Predictions))
	}

	// Predictions should be around 20 (last smoothed level)
	for _, p := range result.Predictions {
		if p.Value < 15 || p.Value > 25 {
			t.Errorf("prediction %f out of expected range", p.Value)
		}
	}
}

func TestForecaster_DoubleExponential(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method: ForecastMethodDoubleExponential,
		Alpha:  0.5,
		Beta:   0.1,
	})

	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		Values:     []float64{10, 12, 14, 16, 18, 20, 22, 24, 26, 28},
	}

	result, err := f.Forecast(data, 5)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should capture upward trend
	if result.Model.Trend <= 0 {
		t.Error("expected positive trend")
	}

	// Predictions should continue upward
	for i := 1; i < len(result.Predictions); i++ {
		if result.Predictions[i].Value <= result.Predictions[i-1].Value {
			t.Error("expected increasing predictions")
		}
	}
}

func TestForecaster_HoltWinters(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method:          ForecastMethodHoltWinters,
		SeasonalPeriods: 4,
		Alpha:           0.5,
		Beta:            0.1,
		Gamma:           0.1,
	})

	// Create seasonal data (2 complete seasons)
	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		Values:     []float64{10, 20, 15, 25, 12, 22, 17, 27, 14, 24, 19, 29},
	}

	result, err := f.Forecast(data, 4)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Should have seasonality
	if len(result.Model.Seasonality) != 4 {
		t.Errorf("expected 4 seasonal factors, got %d", len(result.Model.Seasonality))
	}

	if len(result.Predictions) != 4 {
		t.Errorf("expected 4 predictions, got %d", len(result.Predictions))
	}
}

func TestForecaster_HoltWinters_FallbackToDouble(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method:          ForecastMethodHoltWinters,
		SeasonalPeriods: 12,
		Alpha:           0.5,
		Beta:            0.1,
		Gamma:           0.1,
	})

	// Not enough data for 2 seasons
	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5},
		Values:     []float64{10, 12, 14, 16, 18},
	}

	result, err := f.Forecast(data, 3)
	if err != nil {
		t.Fatalf("Forecast should fall back to double exponential: %v", err)
	}

	if len(result.Predictions) != 3 {
		t.Errorf("expected 3 predictions, got %d", len(result.Predictions))
	}
}

func TestForecaster_MovingAverage(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method:          ForecastMethodMovingAverage,
		SeasonalPeriods: 3, // Window size
	})

	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		Values:     []float64{10, 10, 10, 20, 20, 20, 30, 30, 30, 30},
	}

	result, err := f.Forecast(data, 5)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// All predictions should be the same (moving average of last window)
	expectedAvg := 30.0
	for _, p := range result.Predictions {
		if p.Value != expectedAvg {
			t.Errorf("expected prediction %f, got %f", expectedAvg, p.Value)
		}
	}
}

func TestForecaster_AnomalyDetection(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method:           ForecastMethodSimpleExponential,
		Alpha:            0.5,
		AnomalyThreshold: 2.0,
	})

	// Data with anomaly at index 5
	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		Values:     []float64{10, 11, 10, 11, 10, 50, 10, 11, 10, 11},
	}

	anomalies, err := f.DetectAnomalies(data)
	if err != nil {
		t.Fatalf("DetectAnomalies failed: %v", err)
	}

	if len(anomalies) == 0 {
		t.Error("expected at least one anomaly")
	}

	// The spike at 50 should be detected
	found := false
	for _, a := range anomalies {
		if a.ActualValue == 50 {
			found = true
			if a.Score <= 0 {
				t.Error("anomaly score should be positive")
			}
			break
		}
	}

	if !found {
		t.Error("expected to find anomaly at value 50")
	}
}

func TestForecaster_InsufficientData(t *testing.T) {
	f := NewForecaster(DefaultForecastConfig())

	// Only 1 data point
	data := TimeSeriesData{
		Timestamps: []int64{1},
		Values:     []float64{10},
	}

	_, err := f.Forecast(data, 5)
	if err == nil {
		t.Error("expected error for insufficient data")
	}
}

func TestForecaster_InvalidPeriods(t *testing.T) {
	f := NewForecaster(DefaultForecastConfig())

	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3},
		Values:     []float64{10, 11, 12},
	}

	_, err := f.Forecast(data, 0)
	if err == nil {
		t.Error("expected error for zero periods")
	}

	_, err = f.Forecast(data, -1)
	if err == nil {
		t.Error("expected error for negative periods")
	}
}

func TestForecaster_ConfidenceIntervals(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method:           ForecastMethodSimpleExponential,
		Alpha:            0.5,
		AnomalyThreshold: 3.0,
	})

	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		Values:     []float64{10, 12, 11, 13, 10, 14, 11, 15, 10, 16},
	}

	result, err := f.Forecast(data, 5)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	for _, p := range result.Predictions {
		if p.LowerBound >= p.Value {
			t.Error("lower bound should be less than value")
		}
		if p.UpperBound <= p.Value {
			t.Error("upper bound should be greater than value")
		}
		if p.Confidence != 0.95 {
			t.Errorf("expected 0.95 confidence, got %f", p.Confidence)
		}
	}
}

func TestForecaster_ErrorMetrics(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method: ForecastMethodSimpleExponential,
		Alpha:  0.5,
	})

	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5},
		Values:     []float64{10, 10, 10, 10, 10},
	}

	result, err := f.Forecast(data, 3)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Perfect fit should have low error
	if result.RMSE > 1 {
		t.Errorf("expected low RMSE for constant data, got %f", result.RMSE)
	}
	if result.MAE > 1 {
		t.Errorf("expected low MAE for constant data, got %f", result.MAE)
	}
}

func TestDefaultForecastConfig(t *testing.T) {
	config := DefaultForecastConfig()

	if config.Method != ForecastMethodHoltWinters {
		t.Error("default method should be HoltWinters")
	}
	if config.SeasonalPeriods != 12 {
		t.Errorf("expected 12 seasonal periods, got %d", config.SeasonalPeriods)
	}
	if config.Alpha != 0.5 {
		t.Errorf("expected alpha 0.5, got %f", config.Alpha)
	}
	if config.AnomalyThreshold != 3.0 {
		t.Errorf("expected threshold 3.0, got %f", config.AnomalyThreshold)
	}
}

func TestNewForecaster_Defaults(t *testing.T) {
	// Invalid parameters should be corrected
	f := NewForecaster(ForecastConfig{
		Alpha:            0,  // Invalid
		Beta:             -1, // Invalid
		Gamma:            2,  // Invalid
		SeasonalPeriods:  0,  // Invalid
		AnomalyThreshold: -1, // Invalid
	})

	if f.config.Alpha != 0.5 {
		t.Errorf("expected alpha to default to 0.5, got %f", f.config.Alpha)
	}
	if f.config.SeasonalPeriods != 12 {
		t.Errorf("expected seasonal periods to default to 12, got %d", f.config.SeasonalPeriods)
	}
	if f.config.AnomalyThreshold != 3.0 {
		t.Errorf("expected threshold to default to 3.0, got %f", f.config.AnomalyThreshold)
	}
}

func TestHelperFunctions(t *testing.T) {
	t.Run("mean", func(t *testing.T) {
		if mean([]float64{}) != 0 {
			t.Error("mean of empty slice should be 0")
		}
		if mean([]float64{1, 2, 3, 4, 5}) != 3 {
			t.Error("mean incorrect")
		}
	})

	t.Run("stdDev", func(t *testing.T) {
		actual := []float64{1, 2, 3, 4, 5}
		fitted := []float64{1, 2, 3, 4, 5}
		if stdDev(actual, fitted) != 0 {
			t.Error("stdDev of perfect fit should be 0")
		}
	})

	t.Run("estimateInterval", func(t *testing.T) {
		ts := []int64{1000, 2000, 3000, 4000, 5000}
		interval := estimateInterval(ts)
		if interval != 1000 {
			t.Errorf("expected interval 1000, got %d", interval)
		}

		// Single timestamp
		single := []int64{1000}
		if estimateInterval(single) != int64(time.Minute) {
			t.Error("single timestamp should default to 1 minute")
		}
	})
}

func TestForecaster_TimestampProgression(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method: ForecastMethodSimpleExponential,
		Alpha:  0.5,
	})

	interval := int64(1000)
	data := TimeSeriesData{
		Timestamps: []int64{0, 1000, 2000, 3000, 4000},
		Values:     []float64{10, 11, 12, 13, 14},
	}

	result, err := f.Forecast(data, 3)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	// Timestamps should progress correctly
	expectedTs := []int64{5000, 6000, 7000}
	for i, p := range result.Predictions {
		if p.Timestamp != expectedTs[i] {
			t.Errorf("prediction %d: expected ts %d, got %d", i, expectedTs[i], p.Timestamp)
		}
	}
	_ = interval // Used for reference
}

func TestForecaster_RMSE_MAE(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Method: ForecastMethodSimpleExponential,
		Alpha:  0.5,
	})

	data := TimeSeriesData{
		Timestamps: []int64{1, 2, 3, 4, 5},
		Values:     []float64{10, 20, 15, 25, 10},
	}

	result, err := f.Forecast(data, 1)
	if err != nil {
		t.Fatalf("Forecast failed: %v", err)
	}

	if math.IsNaN(result.RMSE) {
		t.Error("RMSE should not be NaN")
	}
	if math.IsNaN(result.MAE) {
		t.Error("MAE should not be NaN")
	}
	if result.RMSE < 0 {
		t.Error("RMSE should be non-negative")
	}
	if result.MAE < 0 {
		t.Error("MAE should be non-negative")
	}
}
