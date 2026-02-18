package chronicle

import (
	"testing"
)

func TestParseCorrelationQuery_Basic(t *testing.T) {
	q, err := ParseCorrelationQuery("CORRELATE metrics WITH traces WHERE service='api'")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if q.SourceSignal != SignalMetric {
		t.Errorf("expected source metric, got %s", q.SourceSignal)
	}
	if q.TargetSignal != SignalTrace {
		t.Errorf("expected target trace, got %s", q.TargetSignal)
	}
	if len(q.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(q.Conditions))
	}
	if q.Conditions[0].Key != "service" {
		t.Errorf("expected key 'service', got %q", q.Conditions[0].Key)
	}
	if q.Conditions[0].Value != "api" {
		t.Errorf("expected value 'api', got %q", q.Conditions[0].Value)
	}
}

func TestParseCorrelationQuery_MultipleConditions(t *testing.T) {
	q, err := ParseCorrelationQuery("CORRELATE traces WITH logs WHERE service='api' AND status='error'")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(q.Conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(q.Conditions))
	}
	if q.Conditions[0].Key != "service" {
		t.Errorf("expected first key 'service', got %q", q.Conditions[0].Key)
	}
	if q.Conditions[1].Key != "status" {
		t.Errorf("expected second key 'status', got %q", q.Conditions[1].Key)
	}
}

func TestParseCorrelationQuery_NoWhere(t *testing.T) {
	q, err := ParseCorrelationQuery("CORRELATE metrics WITH logs")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if q.SourceSignal != SignalMetric {
		t.Errorf("expected source metric, got %s", q.SourceSignal)
	}
	if q.TargetSignal != SignalLog {
		t.Errorf("expected target log, got %s", q.TargetSignal)
	}
	if len(q.Conditions) != 0 {
		t.Errorf("expected 0 conditions, got %d", len(q.Conditions))
	}
}

func TestParseCorrelationQuery_Errors(t *testing.T) {
	tests := []struct {
		input string
		desc  string
	}{
		{"SELECT * FROM metrics", "not CORRELATE"},
		{"CORRELATE metrics", "missing WITH"},
		{"CORRELATE unknown WITH logs", "invalid signal type"},
		{"CORRELATE metrics WITH invalid", "invalid target signal"},
	}

	for _, tt := range tests {
		_, err := ParseCorrelationQuery(tt.input)
		if err == nil {
			t.Errorf("%s: expected error for %q", tt.desc, tt.input)
		}
	}
}

func TestParseCorrelationQuery_NotEqual(t *testing.T) {
	q, err := ParseCorrelationQuery("CORRELATE metrics WITH traces WHERE status!='ok'")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if q.Conditions[0].Operator != "!=" {
		t.Errorf("expected operator !=, got %q", q.Conditions[0].Operator)
	}
}

func TestExecuteCorrelation(t *testing.T) {
	traceStore := NewTraceStore(DefaultTraceStoreConfig())
	logStore := NewLogStore(DefaultLogStoreConfig())
	engine := NewSignalCorrelationEngine(nil, traceStore, logStore)

	traceStore.WriteSpan(Span{TraceID: "t1", SpanID: "s1", Service: "api"})
	logStore.WriteLog(LogRecord{Body: "test", ServiceName: "api"})

	query, err := ParseCorrelationQuery("CORRELATE metrics WITH traces WHERE service='api'")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	result := engine.ExecuteCorrelation(query)
	if len(result.Traces) != 1 {
		t.Errorf("expected 1 trace, got %d", len(result.Traces))
	}
}

func TestParseCorrelationQuery_CaseInsensitive(t *testing.T) {
	q, err := ParseCorrelationQuery("correlate Metrics with Traces where service='api'")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if q.SourceSignal != SignalMetric {
		t.Errorf("expected source metric, got %s", q.SourceSignal)
	}
}
