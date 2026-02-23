package chronicle

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestDefaultAnomalyPipelineConfig(t *testing.T) {
	cfg := DefaultAnomalyPipelineConfig()

	if cfg.WindowSize <= 0 {
		t.Errorf("WindowSize should be positive, got %d", cfg.WindowSize)
	}
	if cfg.ZScoreThreshold <= 0 {
		t.Errorf("ZScoreThreshold should be positive, got %f", cfg.ZScoreThreshold)
	}
	if cfg.IQRMultiplier <= 0 {
		t.Errorf("IQRMultiplier should be positive, got %f", cfg.IQRMultiplier)
	}
	if cfg.MinDataPoints <= 0 {
		t.Errorf("MinDataPoints should be positive, got %d", cfg.MinDataPoints)
	}
	if cfg.EvaluationInterval <= 0 {
		t.Errorf("EvaluationInterval should be positive, got %v", cfg.EvaluationInterval)
	}
	if cfg.Sensitivity <= 0 || cfg.Sensitivity > 1 {
		t.Errorf("Sensitivity should be in (0,1], got %f", cfg.Sensitivity)
	}
	if !cfg.EnableAutoAlert {
		t.Error("EnableAutoAlert should default to true")
	}
}

func TestNewAnomalyPipeline(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pCfg := DefaultAnomalyPipelineConfig()
	p := NewAnomalyPipeline(db, pCfg)

	if p == nil {
		t.Fatal("NewAnomalyPipeline returned nil")
	}
	if p.baselines == nil {
		t.Error("baselines map not initialized")
	}
	if p.anomalies == nil {
		t.Error("anomalies slice not initialized")
	}
	if p.db != db {
		t.Error("db not set correctly")
	}

	// nil db should not panic
	p2 := NewAnomalyPipeline(nil, pCfg)
	if p2 == nil {
		t.Fatal("NewAnomalyPipeline with nil db returned nil")
	}
}

func TestAnomalyPipelineStartStop(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pCfg := DefaultAnomalyPipelineConfig()
	p := NewAnomalyPipeline(db, pCfg)

	// Start without hub should fail
	if err := p.Start(context.Background()); err == nil {
		t.Error("Start without stream hub should return error")
	}

	// Set up hub and start
	hub := NewStreamHub(db, StreamConfig{BufferSize: 100})
	p.SetStreamHub(hub)

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	stats := p.Stats()
	if !stats.Running {
		t.Error("pipeline should be running after Start")
	}

	// Double start should fail
	if err := p.Start(context.Background()); err == nil {
		t.Error("double Start should return error")
	}

	// Stop
	p.Stop()
	stats = p.Stats()
	if stats.Running {
		t.Error("pipeline should not be running after Stop")
	}

	// Double stop should not panic
	p.Stop()
}

func TestAnomalyPipelineProcessPoint(t *testing.T) {
	pCfg := DefaultAnomalyPipelineConfig()
	pCfg.MinDataPoints = 20
	p := NewAnomalyPipeline(nil, pCfg)

	// Feed 30 normal points around 100
	for i := 0; i < 30; i++ {
		pt := Point{
			Metric:    "cpu.usage",
			Value:     100.0 + float64(i%5),
			Timestamp: time.Now().UnixNano(),
		}
		p.processPoint(pt)
	}

	stats := p.Stats()
	if stats.TotalProcessed != 30 {
		t.Errorf("expected 30 processed, got %d", stats.TotalProcessed)
	}

	baseline := p.GetBaseline("cpu.usage")
	if baseline == nil {
		t.Fatal("baseline should exist after processing points")
	}

	anomaliesBefore := stats.TotalAnomalies

	// Feed extreme outlier
	outlier := Point{
		Metric:    "cpu.usage",
		Value:     1000.0,
		Timestamp: time.Now().UnixNano(),
	}
	p.processPoint(outlier)

	stats = p.Stats()
	if stats.TotalAnomalies <= anomaliesBefore {
		t.Error("extreme outlier should trigger anomaly detection")
	}

	anomalies := p.ListAnomalies("cpu.usage", time.Time{}, 10)
	if len(anomalies) == 0 {
		t.Error("expected at least one anomaly for cpu.usage")
	}
}

func TestAnomalyPipelineUpdateBaseline(t *testing.T) {
	pCfg := DefaultAnomalyPipelineConfig()
	pCfg.WindowSize = 10
	p := NewAnomalyPipeline(nil, pCfg)

	b := &metricBaseline{
		values: make([]float64, 0, pCfg.WindowSize),
	}

	// Add known values: 1..5
	for i := 1; i <= 5; i++ {
		p.updateBaseline(b, float64(i))
	}

	expectedMean := 3.0
	if math.Abs(b.mean-expectedMean) > 0.001 {
		t.Errorf("mean: expected %.1f, got %.4f", expectedMean, b.mean)
	}
	if b.stddev <= 0 {
		t.Error("stddev should be positive for varying values")
	}
	if b.count != 5 {
		t.Errorf("count: expected 5, got %d", b.count)
	}
	if b.lastUpdated.IsZero() {
		t.Error("lastUpdated should be set")
	}

	// Window overflow: add enough values to exceed window
	for i := 0; i < 20; i++ {
		p.updateBaseline(b, 50.0)
	}
	if len(b.values) > pCfg.WindowSize {
		t.Errorf("values should be bounded by WindowSize %d, got %d", pCfg.WindowSize, len(b.values))
	}
	// After 20 identical values filling the window, mean should converge to 50
	if math.Abs(b.mean-50.0) > 0.001 {
		t.Errorf("mean should converge to 50.0, got %.4f", b.mean)
	}
}

func TestAnomalyPipelineClassifyAnomaly(t *testing.T) {
	p := NewAnomalyPipeline(nil, DefaultAnomalyPipelineConfig())

	tests := []struct {
		name         string
		value        float64
		expected     float64
		score        float64
		wantType     string
		wantSeverity string
	}{
		{"high_spike", 200, 100, 0.8, "spike", "critical"},
		{"high_dip", 10, 100, 0.8, "dip", "critical"},
		{"moderate_drift_up", 120, 100, 0.5, "drift", "warning"},
		{"moderate_drift_down", 80, 100, 0.5, "drift", "warning"},
		{"low_outlier_up", 110, 100, 0.1, "outlier", "info"},
		{"low_outlier_down", 90, 100, 0.1, "outlier", "info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotSeverity := p.classifyAnomaly(tt.value, tt.expected, tt.score)
			if gotType != tt.wantType {
				t.Errorf("type: got %q, want %q", gotType, tt.wantType)
			}
			if gotSeverity != tt.wantSeverity {
				t.Errorf("severity: got %q, want %q", gotSeverity, tt.wantSeverity)
			}
		})
	}
}

func TestAnomalyPipelineListAnomalies(t *testing.T) {
	pCfg := DefaultAnomalyPipelineConfig()
	pCfg.MinDataPoints = 20
	p := NewAnomalyPipeline(nil, pCfg)

	// Build baseline for two metrics
	for i := 0; i < 30; i++ {
		p.processPoint(Point{Metric: "m1", Value: 100.0, Timestamp: time.Now().UnixNano()})
		p.processPoint(Point{Metric: "m2", Value: 50.0, Timestamp: time.Now().UnixNano()})
	}

	beforeOutlier := time.Now().Add(-time.Millisecond)

	// Inject outliers
	p.processPoint(Point{Metric: "m1", Value: 1000.0, Timestamp: time.Now().UnixNano()})
	p.processPoint(Point{Metric: "m2", Value: 1000.0, Timestamp: time.Now().UnixNano()})

	// Filter by metric
	m1Only := p.ListAnomalies("m1", time.Time{}, 0)
	for _, a := range m1Only {
		if a.Metric != "m1" {
			t.Errorf("expected metric m1, got %s", a.Metric)
		}
	}

	// Filter by since
	recent := p.ListAnomalies("", beforeOutlier, 0)
	for _, a := range recent {
		if a.Timestamp.Before(beforeOutlier) {
			t.Error("anomaly timestamp should be after since filter")
		}
	}

	// Limit
	limited := p.ListAnomalies("", time.Time{}, 1)
	if len(limited) > 1 {
		t.Errorf("expected at most 1 anomaly, got %d", len(limited))
	}

	// No match
	none := p.ListAnomalies("nonexistent", time.Time{}, 0)
	if len(none) != 0 {
		t.Errorf("expected 0 anomalies for nonexistent metric, got %d", len(none))
	}
}

func TestAnomalyPipelineStats(t *testing.T) {
	pCfg := DefaultAnomalyPipelineConfig()
	pCfg.MinDataPoints = 5
	p := NewAnomalyPipeline(nil, pCfg)

	stats := p.Stats()
	if stats.TotalProcessed != 0 || stats.TotalAnomalies != 0 || stats.ActiveMetrics != 0 {
		t.Error("initial stats should be zero")
	}
	if stats.Running {
		t.Error("pipeline should not be running initially")
	}

	// Process some points
	for i := 0; i < 10; i++ {
		p.processPoint(Point{Metric: "test.metric", Value: 100.0, Timestamp: time.Now().UnixNano()})
	}

	stats = p.Stats()
	if stats.TotalProcessed != 10 {
		t.Errorf("expected 10 processed, got %d", stats.TotalProcessed)
	}
	if stats.ActiveMetrics != 1 {
		t.Errorf("expected 1 active metric, got %d", stats.ActiveMetrics)
	}

	// Add a second metric
	p.processPoint(Point{Metric: "other.metric", Value: 50.0, Timestamp: time.Now().UnixNano()})
	stats = p.Stats()
	if stats.ActiveMetrics != 2 {
		t.Errorf("expected 2 active metrics, got %d", stats.ActiveMetrics)
	}
}

func TestAnomalyPipelineDetectPoint(t *testing.T) {
	pCfg := DefaultAnomalyPipelineConfig()
	pCfg.MinDataPoints = 20
	p := NewAnomalyPipeline(nil, pCfg)

	// Build baseline with 30 normal points
	for i := 0; i < 30; i++ {
		result := p.DetectPoint(Point{
			Metric:    "latency",
			Tags:      map[string]string{"host": "a"},
			Value:     100.0 + float64(i%3),
			Timestamp: time.Now().UnixNano(),
		})
		// First MinDataPoints should return nil
		if i < pCfg.MinDataPoints-1 && result != nil {
			t.Errorf("point %d: expected nil before MinDataPoints, got anomaly", i)
		}
	}

	// Normal value should not trigger
	normal := p.DetectPoint(Point{
		Metric:    "latency",
		Tags:      map[string]string{"host": "a"},
		Value:     101.0,
		Timestamp: time.Now().UnixNano(),
	})
	if normal != nil {
		t.Error("normal value should not be detected as anomaly")
	}

	// Extreme outlier should trigger
	anomaly := p.DetectPoint(Point{
		Metric:    "latency",
		Tags:      map[string]string{"host": "a"},
		Value:     1000.0,
		Timestamp: time.Now().UnixNano(),
	})
	if anomaly == nil {
		t.Fatal("extreme outlier should be detected as anomaly")
	}
	if anomaly.Metric != "latency" {
		t.Errorf("metric: got %q, want %q", anomaly.Metric, "latency")
	}
	if anomaly.Value != 1000.0 {
		t.Errorf("value: got %f, want 1000.0", anomaly.Value)
	}
	if anomaly.Score <= 0 {
		t.Error("anomaly score should be positive")
	}
	if anomaly.ID == "" {
		t.Error("anomaly ID should not be empty")
	}

	stats := p.Stats()
	if stats.TotalAnomalies < 1 {
		t.Error("stats should reflect at least one anomaly")
	}
}
