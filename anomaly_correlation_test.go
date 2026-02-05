package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestAnomalyCorrelationEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyCorrelationConfig()
	config.MinConfidence = 0.1
	engine := NewAnomalyCorrelationEngine(db, config)

	// Ingest correlated signals with shared tags and temporal proximity
	now := time.Now()
	engine.IngestSignal(AnomalySignal{
		ID: "s1", SignalType: CorrelationSignalMetric, Metric: "cpu_usage",
		Tags: map[string]string{"host": "web-1", "env": "prod"},
		Severity: 0.8, Timestamp: now,
	})
	engine.IngestSignal(AnomalySignal{
		ID: "s2", SignalType: CorrelationSignalLog, Metric: "error_log",
		Tags: map[string]string{"host": "web-1", "env": "prod"},
		Severity: 0.6, Timestamp: now.Add(5 * time.Second),
	})
	engine.IngestSignal(AnomalySignal{
		ID: "s3", SignalType: CorrelationSignalTrace, Metric: "latency_spike",
		Tags: map[string]string{"host": "web-1", "env": "prod"},
		Severity: 0.9, Timestamp: now.Add(10 * time.Second),
	})

	incidents, err := engine.Correlate(context.Background())
	if err != nil {
		t.Fatalf("Correlate failed: %v", err)
	}
	if len(incidents) == 0 {
		t.Fatal("expected at least one incident")
	}

	inc := incidents[0]
	if len(inc.Signals) < 2 {
		t.Errorf("expected at least 2 correlated signals, got %d", len(inc.Signals))
	}
	if inc.Confidence <= 0 {
		t.Error("expected positive confidence")
	}
	if inc.Status != "active" {
		t.Errorf("expected active status, got %s", inc.Status)
	}
}

func TestAnomalyCorrelationCausalGraph(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyCorrelationConfig()
	config.MinConfidence = 0.1
	config.EnableCausalGraph = true
	engine := NewAnomalyCorrelationEngine(db, config)

	now := time.Now()
	engine.IngestSignal(AnomalySignal{
		ID: "root", SignalType: CorrelationSignalMetric, Metric: "disk_full",
		Tags: map[string]string{"host": "db-1"}, Severity: 0.95, Timestamp: now,
	})
	engine.IngestSignal(AnomalySignal{
		ID: "effect1", SignalType: CorrelationSignalLog, Metric: "write_error",
		Tags: map[string]string{"host": "db-1"}, Severity: 0.7, Timestamp: now.Add(2 * time.Second),
	})
	engine.IngestSignal(AnomalySignal{
		ID: "effect2", SignalType: CorrelationSignalTrace, Metric: "query_timeout",
		Tags: map[string]string{"host": "db-1"}, Severity: 0.6, Timestamp: now.Add(4 * time.Second),
	})

	incidents, err := engine.Correlate(context.Background())
	if err != nil {
		t.Fatalf("Correlate failed: %v", err)
	}
	if len(incidents) == 0 {
		t.Fatal("expected at least one incident")
	}

	inc := incidents[0]
	if inc.CausalGraph == nil {
		t.Fatal("expected causal graph")
	}
	if len(inc.CausalGraph.Edges) == 0 {
		t.Error("expected at least one edge in causal graph")
	}
	if len(inc.RootCauses) == 0 {
		t.Error("expected at least one root cause")
	}
}

func TestAnomalyCorrelationTraceLinked(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyCorrelationConfig()
	config.MinConfidence = 0.1
	engine := NewAnomalyCorrelationEngine(db, config)

	now := time.Now()
	traceID := "trace-abc-123"
	engine.IngestSignal(AnomalySignal{
		ID: "t1", SignalType: CorrelationSignalMetric, Metric: "cpu",
		Tags: map[string]string{}, Severity: 0.5, Timestamp: now, TraceID: traceID,
	})
	engine.IngestSignal(AnomalySignal{
		ID: "t2", SignalType: CorrelationSignalTrace, Metric: "latency",
		Tags: map[string]string{}, Severity: 0.5, Timestamp: now.Add(time.Second), TraceID: traceID,
	})

	incidents, err := engine.Correlate(context.Background())
	if err != nil {
		t.Fatalf("Correlate failed: %v", err)
	}
	if len(incidents) == 0 {
		t.Fatal("expected trace-linked incident")
	}
}

func TestAnomalyCorrelationListAndGet(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyCorrelationConfig()
	config.MinConfidence = 0.1
	engine := NewAnomalyCorrelationEngine(db, config)

	now := time.Now()
	engine.IngestSignal(AnomalySignal{
		ID: "a1", SignalType: CorrelationSignalMetric, Metric: "m1",
		Tags: map[string]string{"x": "1"}, Severity: 0.5, Timestamp: now,
	})
	engine.IngestSignal(AnomalySignal{
		ID: "a2", SignalType: CorrelationSignalLog, Metric: "m2",
		Tags: map[string]string{"x": "1"}, Severity: 0.7, Timestamp: now.Add(time.Second),
	})

	engine.Correlate(context.Background())

	incidents := engine.ListIncidents()
	if len(incidents) == 0 {
		t.Fatal("expected incidents in list")
	}

	_, found := engine.GetIncident(incidents[0].ID)
	if !found {
		t.Error("expected to find incident by ID")
	}

	stats := engine.Stats()
	if stats["active_incidents"].(int) == 0 {
		t.Error("expected active incidents in stats")
	}
}

func TestAnomalyCorrelationCallback(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyCorrelationConfig()
	config.MinConfidence = 0.1
	engine := NewAnomalyCorrelationEngine(db, config)

	var called bool
	engine.OnIncident(func(inc *CorrelatedIncident) {
		called = true
	})

	now := time.Now()
	engine.IngestSignal(AnomalySignal{
		ID: "c1", SignalType: CorrelationSignalMetric, Metric: "cpu",
		Tags: map[string]string{"h": "1"}, Severity: 0.5, Timestamp: now,
	})
	engine.IngestSignal(AnomalySignal{
		ID: "c2", SignalType: CorrelationSignalLog, Metric: "err",
		Tags: map[string]string{"h": "1"}, Severity: 0.5, Timestamp: now.Add(time.Second),
	})
	engine.Correlate(context.Background())

	if !called {
		t.Error("expected callback to be invoked")
	}
}

func TestAnomalyCorrelationStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAnomalyCorrelationConfig()
	config.Enabled = true
	config.EvaluationInterval = 100 * time.Millisecond
	engine := NewAnomalyCorrelationEngine(db, config)

	engine.Start()
	time.Sleep(50 * time.Millisecond)
	engine.Stop()
}
