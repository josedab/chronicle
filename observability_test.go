package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestMetricsCollector_CountersGauges(t *testing.T) {
	cfg := DefaultInternalMetricsConfig()
	mc := NewMetricsCollector(cfg)

	mc.IncrCounter("requests", 1)
	mc.IncrCounter("requests", 5)
	mc.SetGauge("connections", 42)

	snap := mc.Snapshot()
	if snap.Counters["requests"] != 6 {
		t.Errorf("expected requests=6, got %d", snap.Counters["requests"])
	}
	if snap.Gauges["connections"] != 42 {
		t.Errorf("expected connections=42, got %d", snap.Gauges["connections"])
	}
}

func TestMetricsCollector_Histogram(t *testing.T) {
	cfg := DefaultInternalMetricsConfig()
	mc := NewMetricsCollector(cfg)

	mc.RecordValue("latency", 0.1)
	mc.RecordValue("latency", 0.5)
	mc.RecordValue("latency", 1.0)
	mc.RecordDuration("request_time", 100*time.Millisecond)

	snap := mc.Snapshot()
	h, ok := snap.Histograms["latency"]
	if !ok {
		t.Fatal("expected 'latency' histogram")
	}
	if h.Count != 3 {
		t.Errorf("expected count=3, got %d", h.Count)
	}
	if h.Min != 0.1 {
		t.Errorf("expected min=0.1, got %f", h.Min)
	}
	if h.Max != 1.0 {
		t.Errorf("expected max=1.0, got %f", h.Max)
	}
}

func TestMetricRingBuffer_PushGetRecent(t *testing.T) {
	rb := NewMetricRingBuffer(5)

	for i := 0; i < 10; i++ {
		rb.Push(MetricEntry{
			Name:  "test",
			Value: float64(i),
		})
	}

	recent := rb.GetRecent(3)
	if len(recent) != 3 {
		t.Fatalf("expected 3 recent entries, got %d", len(recent))
	}
	// Most recent should be 9, 8, 7.
	if recent[0].Value != 9 {
		t.Errorf("expected most recent value=9, got %f", recent[0].Value)
	}
	if recent[1].Value != 8 {
		t.Errorf("expected second recent value=8, got %f", recent[1].Value)
	}

	t.Run("RequestMoreThanAvailable", func(t *testing.T) {
		rb2 := NewMetricRingBuffer(10)
		rb2.Push(MetricEntry{Name: "only", Value: 1})
		recent := rb2.GetRecent(5)
		if len(recent) != 1 {
			t.Errorf("expected 1 entry, got %d", len(recent))
		}
	})

	t.Run("EmptyBuffer", func(t *testing.T) {
		rb3 := NewMetricRingBuffer(5)
		recent := rb3.GetRecent(3)
		if len(recent) != 0 {
			t.Errorf("expected 0 entries for empty buffer, got %d", len(recent))
		}
	})
}

func TestHealthChecker_RegisterAndCheck(t *testing.T) {
	cfg := DefaultHealthCheckerConfig()
	cfg.CheckInterval = 50 * time.Millisecond
	cfg.FailureThreshold = 1
	cfg.RecoveryThreshold = 1

	hc := NewHealthChecker(cfg)

	hc.RegisterCheck("always-ok", func(ctx context.Context) *HealthCheckResult {
		return &HealthCheckResult{Status: HealthOK, Message: "all good"}
	})

	hc.Start()
	time.Sleep(100 * time.Millisecond)
	hc.Stop()

	status := hc.Status()
	if status.Overall != HealthOK {
		t.Errorf("expected overall HealthOK, got %v", status.Overall)
	}
	if len(status.Checks) != 1 {
		t.Errorf("expected 1 check, got %d", len(status.Checks))
	}
	if !hc.IsHealthy() {
		t.Errorf("expected IsHealthy=true")
	}
}

func TestHealthChecker_DegradedState(t *testing.T) {
	cfg := DefaultHealthCheckerConfig()
	cfg.CheckInterval = 50 * time.Millisecond
	cfg.FailureThreshold = 1
	cfg.RecoveryThreshold = 1

	hc := NewHealthChecker(cfg)

	hc.RegisterCheck("degraded-check", func(ctx context.Context) *HealthCheckResult {
		return &HealthCheckResult{Status: HealthDegraded, Message: "high memory"}
	})

	hc.Start()
	time.Sleep(150 * time.Millisecond)
	hc.Stop()

	status := hc.Status()
	if status.Overall != HealthDegraded {
		t.Errorf("expected overall HealthDegraded, got %v", status.Overall)
	}
}

func TestObservabilitySuite_StartStop(t *testing.T) {
	cfg := DefaultObservabilitySuiteConfig()
	cfg.Metrics.CollectionInterval = 50 * time.Millisecond
	cfg.Health.CheckInterval = 50 * time.Millisecond

	suite := NewObservabilitySuite(cfg)
	suite.Start()

	mc := suite.Metrics()
	if mc == nil {
		t.Fatal("expected non-nil Metrics()")
	}
	mc.IncrCounter("test.counter", 1)

	hc := suite.Health()
	if hc == nil {
		t.Fatal("expected non-nil Health()")
	}

	time.Sleep(100 * time.Millisecond)
	suite.Stop()

	snap := mc.Snapshot()
	if snap.Counters["test.counter"] != 1 {
		t.Errorf("expected counter=1, got %d", snap.Counters["test.counter"])
	}
}
