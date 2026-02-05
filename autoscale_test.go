package chronicle

import (
	"testing"
	"time"
)

func TestAutoscaleEngine_Basic(t *testing.T) {
	cfg := DefaultAutoscaleConfig()
	cfg.CollectionInterval = 10 * time.Millisecond
	cfg.EvaluationInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 0

	engine := NewAutoscaleEngine(nil, cfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}

	snap := engine.Snapshot()
	if snap.BufferPoolSize <= 0 {
		t.Error("expected positive buffer pool")
	}
	if snap.WriteWorkers <= 0 {
		t.Error("expected positive write workers")
	}
}

func TestAutoscaleEngine_ScaleUp(t *testing.T) {
	cfg := DefaultAutoscaleConfig()
	cfg.CooldownPeriod = 0
	cfg.ScaleUpThreshold = 0.7

	engine := NewAutoscaleEngine(nil, cfg)

	// Simulate high-load metric history
	now := time.Now()
	engine.mu.Lock()
	for i := 0; i < 20; i++ {
		engine.metricHistory["write_rate"] = append(engine.metricHistory["write_rate"],
			InternalMetricSample{Timestamp: now.Add(time.Duration(i) * time.Second), Value: 0.9})
	}
	engine.mu.Unlock()

	engine.evaluate()

	decisions := engine.Decisions()
	hasScaleUp := false
	for _, d := range decisions {
		if d.Direction == ScaleDirectionUp {
			hasScaleUp = true
			break
		}
	}
	if !hasScaleUp {
		t.Error("expected at least one scale-up decision")
	}
}

func TestAutoscaleEngine_ScaleDown(t *testing.T) {
	cfg := DefaultAutoscaleConfig()
	cfg.CooldownPeriod = 0
	cfg.ScaleDownThreshold = 0.5

	engine := NewAutoscaleEngine(nil, cfg)

	// Simulate low-load metric history
	now := time.Now()
	engine.mu.Lock()
	for i := 0; i < 20; i++ {
		engine.metricHistory["write_rate"] = append(engine.metricHistory["write_rate"],
			InternalMetricSample{Timestamp: now.Add(time.Duration(i) * time.Second), Value: 0.1})
	}
	engine.mu.Unlock()

	engine.evaluate()

	decisions := engine.Decisions()
	hasScaleDown := false
	for _, d := range decisions {
		if d.Direction == ScaleDirectionDown {
			hasScaleDown = true
			break
		}
	}
	if !hasScaleDown {
		t.Error("expected at least one scale-down decision")
	}
}

func TestAutoscaleEngine_Cooldown(t *testing.T) {
	cfg := DefaultAutoscaleConfig()
	cfg.CooldownPeriod = 1 * time.Hour
	cfg.ScaleUpThreshold = 0.7

	engine := NewAutoscaleEngine(nil, cfg)

	now := time.Now()
	engine.mu.Lock()
	for i := 0; i < 20; i++ {
		engine.metricHistory["write_rate"] = append(engine.metricHistory["write_rate"],
			InternalMetricSample{Timestamp: now.Add(time.Duration(i) * time.Second), Value: 0.9})
	}
	engine.mu.Unlock()

	engine.evaluate()
	first := len(engine.Decisions())

	engine.evaluate()
	second := len(engine.Decisions())

	if second > first {
		t.Error("cooldown should prevent additional decisions")
	}
}

func TestAutoscaleEngine_OnDecisionCallback(t *testing.T) {
	cfg := DefaultAutoscaleConfig()
	cfg.CooldownPeriod = 0
	cfg.ScaleUpThreshold = 0.7

	engine := NewAutoscaleEngine(nil, cfg)

	called := false
	engine.OnDecision(func(d *ScaleDecision) {
		called = true
	})

	now := time.Now()
	engine.mu.Lock()
	for i := 0; i < 20; i++ {
		engine.metricHistory["write_rate"] = append(engine.metricHistory["write_rate"],
			InternalMetricSample{Timestamp: now.Add(time.Duration(i) * time.Second), Value: 0.9})
	}
	engine.mu.Unlock()

	engine.evaluate()
	if !called {
		t.Error("expected callback to be called")
	}
}

func TestHistogramBuckets(t *testing.T) {
	samples := []InternalMetricSample{
		{Value: 1.0}, {Value: 2.0}, {Value: 3.0}, {Value: 4.0}, {Value: 5.0},
	}
	buckets := HistogramBuckets(samples, 2)
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}
	total := 0
	for _, b := range buckets {
		total += b.Count
	}
	if total != 5 {
		t.Errorf("expected 5 total, got %d", total)
	}
}

func TestHistogramBuckets_Empty(t *testing.T) {
	buckets := HistogramBuckets(nil, 5)
	if buckets != nil {
		t.Error("expected nil for empty input")
	}
}

func TestScaleDimension_String(t *testing.T) {
	if ScaleDimensionBufferPool.String() != "buffer_pool" {
		t.Error("unexpected string for buffer_pool")
	}
	if ScaleDirectionUp.String() != "up" {
		t.Error("unexpected string for up")
	}
	if ScaleDirectionNone.String() != "none" {
		t.Error("unexpected string for none")
	}
}

func TestDefaultAutoscaleConfig(t *testing.T) {
	cfg := DefaultAutoscaleConfig()
	if !cfg.Enabled {
		t.Error("expected enabled by default")
	}
	if cfg.ScaleUpThreshold != 0.80 {
		t.Error("expected 0.80 scale-up threshold")
	}
	if cfg.MaxBufferPool <= cfg.MinBufferPool {
		t.Error("max should exceed min for buffer pool")
	}
}
