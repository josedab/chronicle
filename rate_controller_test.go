package chronicle

import (
	"testing"
	"time"
)

func TestRateControllerConfig(t *testing.T) {
	cfg := DefaultRateControllerConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.GlobalRateLimit != 100000 {
		t.Errorf("expected 100000 global limit, got %d", cfg.GlobalRateLimit)
	}
	if cfg.PerMetricLimit != 10000 {
		t.Errorf("expected 10000 per-metric limit, got %d", cfg.PerMetricLimit)
	}
	if cfg.BurstSize != 1000 {
		t.Errorf("expected 1000 burst size, got %d", cfg.BurstSize)
	}
	if cfg.WindowDuration != time.Second {
		t.Errorf("expected 1s window, got %v", cfg.WindowDuration)
	}
}

func TestRateControllerAllow(t *testing.T) {
	db := setupTestDB(t)

	engine := NewRateControllerEngine(db, DefaultRateControllerConfig())

	t.Run("allows within limit", func(t *testing.T) {
		allowed := engine.Allow("cpu_usage")
		if !allowed {
			t.Error("expected request to be allowed")
		}

		stats := engine.Stats()
		if stats.TotalAllowed != 1 {
			t.Errorf("expected 1 allowed, got %d", stats.TotalAllowed)
		}
	})
}

func TestRateControllerThrottle(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultRateControllerConfig()
	cfg.BurstSize = 5
	cfg.PerMetricLimit = 0
	cfg.GlobalRateLimit = 0
	engine := NewRateControllerEngine(db, cfg)

	// Exhaust the per-metric bucket
	allowed := 0
	for i := 0; i < 10; i++ {
		if engine.Allow("cpu_usage") {
			allowed++
		}
	}

	if allowed != 5 {
		t.Errorf("expected 5 allowed (burst), got %d", allowed)
	}

	stats := engine.Stats()
	if stats.TotalThrottled != 5 {
		t.Errorf("expected 5 throttled, got %d", stats.TotalThrottled)
	}
	if stats.ThrottledMetrics["cpu_usage"] != 5 {
		t.Errorf("expected 5 throttled for cpu_usage, got %d", stats.ThrottledMetrics["cpu_usage"])
	}
}

func TestRateControllerBurst(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultRateControllerConfig()
	cfg.BurstSize = 10
	cfg.PerMetricLimit = 0
	cfg.GlobalRateLimit = 0
	engine := NewRateControllerEngine(db, cfg)

	// All burst tokens should be available immediately
	allowed := 0
	for i := 0; i < 10; i++ {
		if engine.Allow("burst_metric") {
			allowed++
		}
	}

	if allowed != 10 {
		t.Errorf("expected 10 burst allowed, got %d", allowed)
	}

	// Next request should be throttled
	if engine.Allow("burst_metric") {
		t.Error("expected throttle after burst exhausted")
	}
}

func TestRateControllerPerMetricLimit(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultRateControllerConfig()
	cfg.BurstSize = 100
	cfg.PerMetricLimit = 0
	cfg.GlobalRateLimit = 100000
	engine := NewRateControllerEngine(db, cfg)

	// Override metric_a to have a small bucket
	engine.mu.Lock()
	engine.metricBuckets["metric_a"] = &RateBucket{
		Tokens:     3,
		MaxTokens:  3,
		RefillRate: 0,
		LastRefill: engine.globalBucket.LastRefill,
	}
	engine.mu.Unlock()

	// Exhaust metric_a's per-metric bucket
	for i := 0; i < 3; i++ {
		engine.Allow("metric_a")
	}

	// metric_a should be throttled (per-metric bucket exhausted)
	if engine.Allow("metric_a") {
		t.Error("expected metric_a to be throttled")
	}

	// metric_b should still work (independent per-metric bucket, global has plenty)
	if !engine.Allow("metric_b") {
		t.Error("expected metric_b to be allowed")
	}
}

func TestRateControllerSetMetricLimit(t *testing.T) {
	db := setupTestDB(t)

	engine := NewRateControllerEngine(db, DefaultRateControllerConfig())

	engine.SetMetricLimit("custom_metric", 5000)

	// Verify the bucket was created with custom rate
	rate := engine.GetMetricRate("custom_metric")
	if rate < 0 {
		t.Errorf("expected non-negative rate, got %f", rate)
	}
}

func TestRateControllerStats(t *testing.T) {
	db := setupTestDB(t)

	engine := NewRateControllerEngine(db, DefaultRateControllerConfig())

	for i := 0; i < 5; i++ {
		engine.Allow("stat_metric")
	}

	stats := engine.Stats()
	if stats.TotalAllowed != 5 {
		t.Errorf("expected 5 allowed, got %d", stats.TotalAllowed)
	}
	if stats.TotalThrottled != 0 {
		t.Errorf("expected 0 throttled, got %d", stats.TotalThrottled)
	}
}

func TestRateControllerGetMetricRate(t *testing.T) {
	db := setupTestDB(t)

	engine := NewRateControllerEngine(db, DefaultRateControllerConfig())

	// Unknown metric returns 0
	rate := engine.GetMetricRate("unknown")
	if rate != 0 {
		t.Errorf("expected 0 for unknown metric, got %f", rate)
	}

	// After allow, metric has a bucket
	engine.Allow("known")
	rate = engine.GetMetricRate("known")
	if rate < 0 {
		t.Errorf("expected non-negative rate, got %f", rate)
	}
}
