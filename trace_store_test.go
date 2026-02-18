package chronicle

import (
	"testing"
	"time"
)

func TestTraceStore_WriteSpan(t *testing.T) {
	store := NewTraceStore(DefaultTraceStoreConfig())

	span := Span{
		TraceID:   "trace-001",
		SpanID:    "span-001",
		Name:      "GET /api/users",
		Service:   "api-gateway",
		StartTime: time.Now(),
		EndTime:   time.Now().Add(100 * time.Millisecond),
	}

	if err := store.WriteSpan(span); err != nil {
		t.Fatalf("WriteSpan failed: %v", err)
	}

	spans, ok := store.GetTrace("trace-001")
	if !ok {
		t.Fatal("GetTrace returned false")
	}
	if len(spans) != 1 {
		t.Errorf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Service != "api-gateway" {
		t.Errorf("expected service api-gateway, got %s", spans[0].Service)
	}
}

func TestTraceStore_Validation(t *testing.T) {
	store := NewTraceStore(DefaultTraceStoreConfig())

	// Missing trace ID
	err := store.WriteSpan(Span{SpanID: "s1"})
	if err == nil {
		t.Error("expected error for missing trace_id")
	}

	// Missing span ID
	err = store.WriteSpan(Span{TraceID: "t1"})
	if err == nil {
		t.Error("expected error for missing span_id")
	}
}

func TestTraceStore_QueryByService(t *testing.T) {
	store := NewTraceStore(DefaultTraceStoreConfig())

	store.WriteSpan(Span{TraceID: "t1", SpanID: "s1", Service: "api", StartTime: time.Now()})
	store.WriteSpan(Span{TraceID: "t2", SpanID: "s2", Service: "api", StartTime: time.Now()})
	store.WriteSpan(Span{TraceID: "t3", SpanID: "s3", Service: "db", StartTime: time.Now()})

	traces := store.QueryTraces("api", 0, 0, 10)
	if len(traces) != 2 {
		t.Errorf("expected 2 traces for api, got %d", len(traces))
	}
}

func TestTraceStore_MaxSpans(t *testing.T) {
	config := TraceStoreConfig{Enabled: true, MaxSpansPerTrace: 2, RetentionDuration: time.Hour}
	store := NewTraceStore(config)

	store.WriteSpan(Span{TraceID: "t1", SpanID: "s1"})
	store.WriteSpan(Span{TraceID: "t1", SpanID: "s2"})
	err := store.WriteSpan(Span{TraceID: "t1", SpanID: "s3"})
	if err == nil {
		t.Error("expected error when exceeding max spans per trace")
	}
}

func TestTraceStore_Stats(t *testing.T) {
	store := NewTraceStore(DefaultTraceStoreConfig())
	store.WriteSpan(Span{TraceID: "t1", SpanID: "s1", Service: "api"})
	store.WriteSpan(Span{TraceID: "t1", SpanID: "s2", Service: "api"})

	stats := store.Stats()
	if stats["trace_count"] != 1 {
		t.Errorf("expected 1 trace, got %v", stats["trace_count"])
	}
	if stats["span_count"] != 2 {
		t.Errorf("expected 2 spans, got %v", stats["span_count"])
	}
}

func TestTraceStore_GetTrace_NotFound(t *testing.T) {
	store := NewTraceStore(DefaultTraceStoreConfig())

	_, ok := store.GetTrace("nonexistent")
	if ok {
		t.Error("expected false for nonexistent trace")
	}
}
