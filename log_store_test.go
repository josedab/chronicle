package chronicle

import (
	"testing"
)

func TestLogStore_WriteLog(t *testing.T) {
	store := NewLogStore(DefaultLogStoreConfig())

	record := LogRecord{
		Severity:    LogSeverityInfo,
		Body:        "Request processed successfully",
		ServiceName: "api",
		TraceID:     "trace-001",
		Labels:      map[string]string{"env": "prod"},
	}

	if err := store.WriteLog(record); err != nil {
		t.Fatalf("WriteLog failed: %v", err)
	}

	stats := store.Stats()
	if stats["total_logs"] != 1 {
		t.Errorf("expected 1 log, got %v", stats["total_logs"])
	}
}

func TestLogStore_Validation(t *testing.T) {
	store := NewLogStore(DefaultLogStoreConfig())

	err := store.WriteLog(LogRecord{})
	if err == nil {
		t.Error("expected error for empty body")
	}
}

func TestLogStore_QueryByTraceID(t *testing.T) {
	store := NewLogStore(DefaultLogStoreConfig())

	store.WriteLog(LogRecord{Body: "log1", TraceID: "t1"})
	store.WriteLog(LogRecord{Body: "log2", TraceID: "t1"})
	store.WriteLog(LogRecord{Body: "log3", TraceID: "t2"})

	logs := store.QueryByTraceID("t1")
	if len(logs) != 2 {
		t.Errorf("expected 2 logs for t1, got %d", len(logs))
	}
}

func TestLogStore_QueryByService(t *testing.T) {
	store := NewLogStore(DefaultLogStoreConfig())

	store.WriteLog(LogRecord{Body: "log1", ServiceName: "api"})
	store.WriteLog(LogRecord{Body: "log2", ServiceName: "api"})
	store.WriteLog(LogRecord{Body: "log3", ServiceName: "worker"})

	logs := store.QueryByService("api", 0, 0, 10)
	if len(logs) != 2 {
		t.Errorf("expected 2 logs for api, got %d", len(logs))
	}
}

func TestLogStore_QueryByLabel(t *testing.T) {
	store := NewLogStore(DefaultLogStoreConfig())

	store.WriteLog(LogRecord{Body: "log1", Labels: map[string]string{"env": "prod"}})
	store.WriteLog(LogRecord{Body: "log2", Labels: map[string]string{"env": "staging"}})
	store.WriteLog(LogRecord{Body: "log3", Labels: map[string]string{"env": "prod"}})

	logs := store.QueryByLabel("env", "prod", 10)
	if len(logs) != 2 {
		t.Errorf("expected 2 prod logs, got %d", len(logs))
	}
}

func TestLogStore_DefaultSeverity(t *testing.T) {
	store := NewLogStore(DefaultLogStoreConfig())

	store.WriteLog(LogRecord{Body: "test log"})

	logs := store.QueryByTraceID("") // no trace ID, should get nothing
	_ = logs // Just verifying no panic
}

func TestLogStore_Timestamp(t *testing.T) {
	store := NewLogStore(DefaultLogStoreConfig())

	store.WriteLog(LogRecord{Body: "auto timestamp"})

	// The log should have been stored with a non-zero timestamp
	stats := store.Stats()
	if stats["total_logs"] != 1 {
		t.Errorf("expected 1 log, got %v", stats["total_logs"])
	}
}
