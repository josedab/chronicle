package chronicle

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestForecastV2Predict(t *testing.T) {
	db := setupTestDB(t)

	start := time.Now().Add(-24 * time.Hour)
	writeTestPoints(t, db, "forecast_v2_cpu", 20, start)

	cfg := DefaultForecastV2Config()
	engine := NewForecastV2Engine(db, cfg)

	result, err := engine.Predict(ForecastV2Request{
		Metric:              "forecast_v2_cpu",
		Horizon:             10,
		IncludeChangepoints: true,
	})
	if err != nil {
		t.Fatalf("predict: %v", err)
	}

	if result.Metric != "forecast_v2_cpu" {
		t.Errorf("metric: got %q, want %q", result.Metric, "forecast_v2_cpu")
	}
	if len(result.Predictions) != 10 {
		t.Errorf("predictions count: got %d, want 10", len(result.Predictions))
	}

	// Check confidence intervals
	for i, p := range result.Predictions {
		if p.Lower > p.Value {
			t.Errorf("prediction %d: lower (%f) > value (%f)", i, p.Lower, p.Value)
		}
		if p.Upper < p.Value {
			t.Errorf("prediction %d: upper (%f) < value (%f)", i, p.Upper, p.Value)
		}
	}
}

func TestForecastV2EmptyData(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultForecastV2Config()
	engine := NewForecastV2Engine(db, cfg)

	_, err := engine.Predict(ForecastV2Request{Metric: "nonexistent_metric", Horizon: 5})
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestForecastV2EmptyMetric(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultForecastV2Config()
	engine := NewForecastV2Engine(db, cfg)

	_, err := engine.Predict(ForecastV2Request{Metric: "", Horizon: 5})
	if err == nil {
		t.Error("expected error for empty metric name")
	}
}

func TestForecastV2DetectChangepoints(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultForecastV2Config()
	cfg.ChangepointThreshold = 0.01
	engine := NewForecastV2Engine(db, cfg)

	// Create data with a clear changepoint
	values := make([]float64, 20)
	for i := 0; i < 10; i++ {
		values[i] = float64(i)
	}
	for i := 10; i < 20; i++ {
		values[i] = float64(i) * 5
	}

	changepoints := engine.DetectChangepoints(values)
	// With such a dramatic change, we should detect at least one changepoint
	if len(changepoints) == 0 {
		t.Error("expected at least one changepoint")
	}
}

func TestForecastV2DetectChangepointsShortData(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultForecastV2Config()
	engine := NewForecastV2Engine(db, cfg)

	changepoints := engine.DetectChangepoints([]float64{1.0, 2.0})
	if changepoints != nil {
		t.Error("expected nil for short data")
	}
}

func TestForecastV2Stats(t *testing.T) {
	db := setupTestDB(t)

	start := time.Now().Add(-24 * time.Hour)
	writeTestPoints(t, db, "forecast_v2_stats_metric", 10, start)

	cfg := DefaultForecastV2Config()
	engine := NewForecastV2Engine(db, cfg)

	stats := engine.Stats()
	if stats.TotalForecasts != 0 {
		t.Errorf("initial forecasts: got %d, want 0", stats.TotalForecasts)
	}

	engine.Predict(ForecastV2Request{Metric: "forecast_v2_stats_metric", Horizon: 5})

	stats = engine.Stats()
	if stats.TotalForecasts != 1 {
		t.Errorf("forecasts after predict: got %d, want 1", stats.TotalForecasts)
	}
	if stats.AvgHorizon != 5.0 {
		t.Errorf("avg horizon: got %f, want 5.0", stats.AvgHorizon)
	}
}

func TestForecastV2StartStop(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultForecastV2Config()
	engine := NewForecastV2Engine(db, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := engine.Start(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestForecastV2DefaultConfig(t *testing.T) {
	cfg := DefaultForecastV2Config()
	if cfg.DefaultHorizon != 24 {
		t.Errorf("default horizon: got %d, want 24", cfg.DefaultHorizon)
	}
	if cfg.SeasonalityMode != "additive" {
		t.Errorf("seasonality mode: got %q, want %q", cfg.SeasonalityMode, "additive")
	}
	if cfg.ConfidenceLevel != 0.95 {
		t.Errorf("confidence level: got %f, want 0.95", cfg.ConfidenceLevel)
	}
}

func TestForecastV2HTTPHandlers(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultForecastV2Config()
	engine := NewForecastV2Engine(db, cfg)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/forecast/v2/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("stats status: got %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q, want %q", ct, "application/json")
	}
}
