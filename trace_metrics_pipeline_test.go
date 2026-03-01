package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestTraceMetricsPipeline(t *testing.T) {
	db := setupTestDB(t)
	traceStore := NewTraceStore(DefaultTraceStoreConfig())
	sloEngine := NewSLOEngine(db, DefaultSLOEngineConfig())
	correlation := NewSignalCorrelationEngine(db, traceStore, nil)
	config := DefaultTraceMetricsPipelineConfig()
	config.CollectionInterval = 100 * time.Millisecond

	pipeline := NewTraceMetricsPipeline(db, traceStore, sloEngine, correlation, config)

	t.Run("start and stop", func(t *testing.T) {
		if err := pipeline.Start(); err != nil {
			t.Fatal(err)
		}
		if err := pipeline.Stop(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("double start", func(t *testing.T) {
		pipeline.Start()
		defer pipeline.Stop()
		if err := pipeline.Start(); err == nil {
			t.Error("expected error on double start")
		}
	})

	t.Run("process spans", func(t *testing.T) {
		p := NewTraceMetricsPipeline(db, traceStore, sloEngine, correlation, config)
		p.Start()
		defer p.Stop()

		now := time.Now()
		spans := []Span{
			{TraceID: "t1", SpanID: "s1", Service: "api", Name: "GET /users", StartTime: now, EndTime: now.Add(50 * time.Millisecond), Duration: 50 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusOK}},
			{TraceID: "t1", SpanID: "s2", Service: "api", Name: "GET /users", StartTime: now, EndTime: now.Add(100 * time.Millisecond), Duration: 100 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusOK}},
			{TraceID: "t2", SpanID: "s3", Service: "api", Name: "GET /users", StartTime: now, EndTime: now.Add(200 * time.Millisecond), Duration: 200 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusError}},
			{TraceID: "t3", SpanID: "s4", Service: "db", Name: "SELECT", StartTime: now, EndTime: now.Add(30 * time.Millisecond), Duration: 30 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusOK}},
		}

		for _, s := range spans {
			p.ProcessSpan(s)
		}

		red := p.GetREDMetrics("api", "GET /users")
		if red == nil {
			t.Fatal("expected RED metrics for api/GET /users")
		}
		if red.RequestCount != 3 {
			t.Errorf("expected 3 requests, got %d", red.RequestCount)
		}
		if red.ErrorCount != 1 {
			t.Errorf("expected 1 error, got %d", red.ErrorCount)
		}
	})

	t.Run("get all RED metrics", func(t *testing.T) {
		p := NewTraceMetricsPipeline(db, traceStore, sloEngine, correlation, config)
		p.ProcessSpan(Span{TraceID: "t1", SpanID: "s1", Service: "svc1", Name: "op1", Duration: 10 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusOK}})
		p.ProcessSpan(Span{TraceID: "t2", SpanID: "s2", Service: "svc2", Name: "op2", Duration: 20 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusOK}})

		all := p.GetAllREDMetrics()
		if len(all) != 2 {
			t.Errorf("expected 2 metrics, got %d", len(all))
		}
	})

	t.Run("topology", func(t *testing.T) {
		p := NewTraceMetricsPipeline(db, traceStore, sloEngine, correlation, config)
		p.ProcessSpan(Span{TraceID: "t1", SpanID: "s1", Service: "gateway", Name: "route", Duration: 50 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusOK}})
		p.ProcessSpan(Span{TraceID: "t1", SpanID: "s2", ParentSpanID: "s1", Service: "api", Name: "handle", Duration: 30 * time.Millisecond, Status: MMSpanStatusInfo{Code: StatusOK}})

		topo := p.GetTopology()
		if topo == nil {
			t.Fatal("expected topology")
		}
		if len(topo.Services) != 2 {
			t.Errorf("expected 2 services, got %d", len(topo.Services))
		}
	})

	t.Run("generate SLIs", func(t *testing.T) {
		p := NewTraceMetricsPipeline(db, traceStore, sloEngine, correlation, config)

		// Need enough traffic to trigger SLI generation
		for i := 0; i < 20; i++ {
			p.ProcessSpan(Span{
				TraceID:  "trace",
				SpanID:   "span",
				Service:  "orders",
				Name:     "create",
				Duration: time.Duration(50+i) * time.Millisecond,
				Status:   MMSpanStatusInfo{Code: StatusOK},
			})
		}

		slis, err := p.GenerateSLIs(context.Background())
		if err != nil {
			t.Fatalf("generate SLIs failed: %v", err)
		}
		if len(slis) < 2 {
			t.Errorf("expected at least 2 SLIs (availability+latency), got %d", len(slis))
		}
	})

	t.Run("generate SLIs no engine", func(t *testing.T) {
		p := NewTraceMetricsPipeline(db, traceStore, nil, correlation, config)
		_, err := p.GenerateSLIs(context.Background())
		if err == nil {
			t.Error("expected error when SLO engine is nil")
		}
	})
}

func TestPercentile(t *testing.T) {
	samples := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	p50 := tracePercentile(samples, 0.50)
	if p50 != 5 {
		t.Errorf("expected p50=5, got %f", p50)
	}

	p99 := tracePercentile(samples, 0.99)
	if p99 != 10 {
		t.Errorf("expected p99=10, got %f", p99)
	}

	p0 := tracePercentile(nil, 0.50)
	if p0 != 0 {
		t.Errorf("expected 0 for nil, got %f", p0)
	}
}

func TestREDMetrics(t *testing.T) {
	red := &REDMetrics{
		Service:      "test",
		Operation:    "op",
		RequestCount: 100,
		ErrorCount:   5,
		ErrorRate:    0.05,
		DurationMin:  1.0,
		DurationMax:  500.0,
		Histogram:    make(map[float64]int64),
	}

	if red.ErrorRate != 0.05 {
		t.Errorf("expected error rate 0.05, got %f", red.ErrorRate)
	}
}
