package chronicle

import (
	"testing"
	"time"
)

func TestSignalCorrelation_ExemplarLink(t *testing.T) {
	traceStore := NewTraceStore(DefaultTraceStoreConfig())
	logStore := NewLogStore(DefaultLogStoreConfig())
	engine := NewSignalCorrelationEngine(nil, traceStore, logStore)

	// Write a trace
	traceStore.WriteSpan(Span{
		TraceID: "trace-001",
		SpanID:  "span-001",
		Service: "api",
		StartTime: time.Now(),
	})

	// Link metric to trace via exemplar
	engine.LinkExemplar("http_requests_total", map[string]string{"method": "GET"}, "trace-001")

	// Correlate
	result := engine.CorrelateMetricWithTraces("http_requests_total", map[string]string{"method": "GET"})

	if len(result.Traces) != 1 {
		t.Errorf("expected 1 correlated trace, got %d", len(result.Traces))
	}
	if len(result.Links) != 1 {
		t.Errorf("expected 1 link, got %d", len(result.Links))
	}
	if result.Links[0].LinkType != "exemplar" {
		t.Errorf("expected exemplar link type, got %s", result.Links[0].LinkType)
	}
}

func TestSignalCorrelation_TraceToLogs(t *testing.T) {
	traceStore := NewTraceStore(DefaultTraceStoreConfig())
	logStore := NewLogStore(DefaultLogStoreConfig())
	engine := NewSignalCorrelationEngine(nil, traceStore, logStore)

	// Write logs with trace ID
	logStore.WriteLog(LogRecord{Body: "Request started", TraceID: "trace-001"})
	logStore.WriteLog(LogRecord{Body: "Request completed", TraceID: "trace-001"})
	logStore.WriteLog(LogRecord{Body: "Unrelated log", TraceID: "trace-002"})

	result := engine.CorrelateTraceWithLogs("trace-001")

	if len(result.Logs) != 2 {
		t.Errorf("expected 2 correlated logs, got %d", len(result.Logs))
	}
}

func TestSignalCorrelation_ByLabel(t *testing.T) {
	traceStore := NewTraceStore(DefaultTraceStoreConfig())
	logStore := NewLogStore(DefaultLogStoreConfig())
	engine := NewSignalCorrelationEngine(nil, traceStore, logStore)

	// Index labels
	engine.IndexLabels(SignalTrace, "trace-001", map[string]string{"service": "api"})

	// Write the trace
	traceStore.WriteSpan(Span{
		TraceID: "trace-001",
		SpanID:  "span-001",
		Service: "api",
		StartTime: time.Now(),
	})

	result := engine.CorrelateByLabel("service", "api")

	if len(result.Traces) != 1 {
		t.Errorf("expected 1 correlated trace, got %d", len(result.Traces))
	}
}

func TestSignalCorrelation_ServiceCorrelation(t *testing.T) {
	traceStore := NewTraceStore(DefaultTraceStoreConfig())
	logStore := NewLogStore(DefaultLogStoreConfig())
	engine := NewSignalCorrelationEngine(nil, traceStore, logStore)

	traceStore.WriteSpan(Span{TraceID: "t1", SpanID: "s1", Service: "api", StartTime: time.Now()})
	logStore.WriteLog(LogRecord{Body: "test", ServiceName: "api"})

	result := engine.CorrelateService("api")

	if len(result.Traces) != 1 {
		t.Errorf("expected 1 trace, got %d", len(result.Traces))
	}
	if len(result.Logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(result.Logs))
	}
}

func TestSignalCorrelation_Stats(t *testing.T) {
	traceStore := NewTraceStore(DefaultTraceStoreConfig())
	logStore := NewLogStore(DefaultLogStoreConfig())
	engine := NewSignalCorrelationEngine(nil, traceStore, logStore)

	engine.LinkExemplar("m1", nil, "t1")
	engine.IndexLabels(SignalTrace, "t1", map[string]string{"env": "prod"})

	stats := engine.Stats()
	if stats["exemplar_links"] != 1 {
		t.Errorf("expected 1 exemplar link, got %v", stats["exemplar_links"])
	}
}
