package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestNewMultiModalStorage(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultMultiModalConfig()
	mms, err := NewMultiModalStorage(db, config)
	if err != nil {
		t.Fatalf("NewMultiModalStorage() error = %v", err)
	}
	defer mms.Close()

	if mms.logIndex == nil {
		t.Error("logIndex should be initialized when full-text indexing enabled")
	}
}

func TestWriteLog(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	entry := &MMLogEntry{
		Level:   LogLevelInfo,
		Message: "Test log message",
		Service: "test-service",
	}

	err = mms.WriteLog(entry)
	if err != nil {
		t.Fatalf("WriteLog() error = %v", err)
	}

	if entry.ID == "" {
		t.Error("Log ID should be generated")
	}
	if entry.Timestamp.IsZero() {
		t.Error("Timestamp should be set")
	}

	stats := mms.Stats()
	if stats.TotalLogs != 1 {
		t.Errorf("TotalLogs = %d, want 1", stats.TotalLogs)
	}
}

func TestWriteLogs(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	entries := []MMLogEntry{
		{Level: LogLevelInfo, Message: "Log 1"},
		{Level: LogLevelWarn, Message: "Log 2"},
		{Level: LogLevelError, Message: "Log 3"},
	}

	err = mms.WriteLogs(entries)
	if err != nil {
		t.Fatalf("WriteLogs() error = %v", err)
	}

	stats := mms.Stats()
	if stats.TotalLogs != 3 {
		t.Errorf("TotalLogs = %d, want 3", stats.TotalLogs)
	}
}

func TestQueryLogs(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	// Write logs
	for i := 0; i < 10; i++ {
		mms.WriteLog(&MMLogEntry{
			Level:   LogLevelInfo,
			Message: "Test message " + string(rune('A'+i)),
			Service: "service-a",
		})
	}

	// Query all
	results, err := mms.QueryLogs(&MMLogQuery{})
	if err != nil {
		t.Fatalf("QueryLogs() error = %v", err)
	}

	if len(results) != 10 {
		t.Errorf("Results count = %d, want 10", len(results))
	}
}

func TestQueryLogsByLevel(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{Level: LogLevelInfo, Message: "Info"})
	mms.WriteLog(&MMLogEntry{Level: LogLevelWarn, Message: "Warn"})
	mms.WriteLog(&MMLogEntry{Level: LogLevelError, Message: "Error"})

	// Query only errors
	results, _ := mms.QueryLogs(&MMLogQuery{Level: LogLevelError})

	if len(results) != 1 {
		t.Errorf("Results count = %d, want 1", len(results))
	}
}

func TestQueryLogsLevelAndAbove(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{Level: LogLevelDebug, Message: "Debug"})
	mms.WriteLog(&MMLogEntry{Level: LogLevelInfo, Message: "Info"})
	mms.WriteLog(&MMLogEntry{Level: LogLevelWarn, Message: "Warn"})
	mms.WriteLog(&MMLogEntry{Level: LogLevelError, Message: "Error"})

	// Query WARN and above
	results, _ := mms.QueryLogs(&MMLogQuery{
		Level:         LogLevelWarn,
		LevelAndAbove: true,
	})

	if len(results) < 2 {
		t.Errorf("Results count = %d, want >= 2 (WARN and ERROR)", len(results))
	}
}

func TestQueryLogsByService(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{Service: "api", Message: "API log"})
	mms.WriteLog(&MMLogEntry{Service: "worker", Message: "Worker log"})
	mms.WriteLog(&MMLogEntry{Service: "api", Message: "Another API log"})

	results, _ := mms.QueryLogs(&MMLogQuery{Service: "api"})

	if len(results) != 2 {
		t.Errorf("Results count = %d, want 2", len(results))
	}
}

func TestQueryLogsFullTextSearch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultMultiModalConfig()
	config.FullTextIndexing = true
	mms, _ := NewMultiModalStorage(db, config)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{Message: "Database connection established"})
	mms.WriteLog(&MMLogEntry{Message: "User authentication failed"})
	mms.WriteLog(&MMLogEntry{Message: "Database query executed"})

	results, _ := mms.QueryLogs(&MMLogQuery{Search: "database"})

	if len(results) != 2 {
		t.Errorf("Results count = %d, want 2", len(results))
	}
}

func TestQueryLogsByTimeRange(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	now := time.Now()

	// Write logs at different times
	mms.WriteLog(&MMLogEntry{
		Timestamp: now.Add(-2 * time.Hour),
		Message:   "Old log",
	})
	mms.WriteLog(&MMLogEntry{
		Timestamp: now.Add(-30 * time.Minute),
		Message:   "Recent log",
	})
	mms.WriteLog(&MMLogEntry{
		Timestamp: now,
		Message:   "Current log",
	})

	// Query last hour
	results, _ := mms.QueryLogs(&MMLogQuery{
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Minute),
	})

	if len(results) != 2 {
		t.Errorf("Results count = %d, want 2", len(results))
	}
}

func TestQueryLogsByRegex(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{Message: "Error code 404"})
	mms.WriteLog(&MMLogEntry{Message: "Error code 500"})
	mms.WriteLog(&MMLogEntry{Message: "Success code 200"})

	results, _ := mms.QueryLogs(&MMLogQuery{
		MessageRegex: "Error code \\d+",
	})

	if len(results) != 2 {
		t.Errorf("Results count = %d, want 2", len(results))
	}
}

func TestWriteSpan(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	span := &Span{
		TraceID:   "trace-123",
		Name:      "operation",
		Service:   "test-service",
		Kind:      SpanKindServer,
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Second),
	}

	err = mms.WriteSpan(span)
	if err != nil {
		t.Fatalf("WriteSpan() error = %v", err)
	}

	if span.SpanID == "" {
		t.Error("SpanID should be generated")
	}
	if span.Duration == 0 {
		t.Error("Duration should be calculated")
	}

	stats := mms.Stats()
	if stats.TotalSpans != 1 {
		t.Errorf("TotalSpans = %d, want 1", stats.TotalSpans)
	}
}

func TestWriteSpans(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	spans := []*Span{
		{TraceID: "t1", Name: "op1", Service: "svc1"},
		{TraceID: "t1", Name: "op2", Service: "svc2"},
		{TraceID: "t2", Name: "op3", Service: "svc1"},
	}

	err = mms.WriteSpans(spans)
	if err != nil {
		t.Fatalf("WriteSpans() error = %v", err)
	}

	stats := mms.Stats()
	if stats.TotalSpans != 3 {
		t.Errorf("TotalSpans = %d, want 3", stats.TotalSpans)
	}
}

func TestGetTrace(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	now := time.Now()
	traceID := "trace-abc"

	// Write spans for a trace
	mms.WriteSpan(&Span{
		TraceID:   traceID,
		SpanID:    "span-1",
		Name:      "root",
		Service:   "frontend",
		StartTime: now,
		EndTime:   now.Add(100 * time.Millisecond),
	})
	mms.WriteSpan(&Span{
		TraceID:      traceID,
		SpanID:       "span-2",
		ParentSpanID: "span-1",
		Name:         "backend-call",
		Service:      "backend",
		StartTime:    now.Add(10 * time.Millisecond),
		EndTime:      now.Add(50 * time.Millisecond),
	})

	trace, err := mms.GetTrace(traceID)
	if err != nil {
		t.Fatalf("GetTrace() error = %v", err)
	}

	if trace.TraceID != traceID {
		t.Errorf("TraceID = %s, want %s", trace.TraceID, traceID)
	}
	if len(trace.Spans) != 2 {
		t.Errorf("Spans count = %d, want 2", len(trace.Spans))
	}
	if len(trace.Services) != 2 {
		t.Errorf("Services count = %d, want 2", len(trace.Services))
	}
}

func TestGetTraceNotFound(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	_, err = mms.GetTrace("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent trace")
	}
}

func TestQuerySpans(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	now := time.Now()

	// Write various spans
	mms.WriteSpan(&Span{
		TraceID:   "t1",
		Name:      "http.request",
		Service:   "api",
		Kind:      SpanKindServer,
		StartTime: now,
		EndTime:   now.Add(50 * time.Millisecond),
		Status:    MMSpanStatusInfo{Code: StatusOK},
	})
	mms.WriteSpan(&Span{
		TraceID:   "t2",
		Name:      "db.query",
		Service:   "database",
		Kind:      SpanKindClient,
		StartTime: now,
		EndTime:   now.Add(200 * time.Millisecond),
		Status:    MMSpanStatusInfo{Code: StatusError},
	})

	// Query by service
	results, _ := mms.QuerySpans(&SpanQuery{Service: "api"})
	if len(results) != 1 {
		t.Errorf("Results count = %d, want 1", len(results))
	}

	// Query by status
	results, _ = mms.QuerySpans(&SpanQuery{Status: StatusError})
	if len(results) != 1 {
		t.Errorf("Error spans = %d, want 1", len(results))
	}
}

func TestQuerySpansByDuration(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	now := time.Now()

	mms.WriteSpan(&Span{
		TraceID:   "t1",
		Name:      "fast",
		StartTime: now,
		EndTime:   now.Add(10 * time.Millisecond),
	})
	mms.WriteSpan(&Span{
		TraceID:   "t2",
		Name:      "slow",
		StartTime: now,
		EndTime:   now.Add(500 * time.Millisecond),
	})

	// Query slow spans (> 100ms)
	results, _ := mms.QuerySpans(&SpanQuery{MinDuration: 100 * time.Millisecond})
	if len(results) != 1 {
		t.Errorf("Slow spans = %d, want 1", len(results))
	}
}

func TestCorrelate(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	traceID := "trace-corr"
	spanID := "span-corr"

	// Write span
	mms.WriteSpan(&Span{
		TraceID:   traceID,
		SpanID:    spanID,
		Name:      "operation",
		Service:   "service",
		StartTime: time.Now(),
		EndTime:   time.Now().Add(100 * time.Millisecond),
	})

	// Write correlated log
	mms.WriteLog(&MMLogEntry{
		TraceID: traceID,
		SpanID:  spanID,
		Message: "Operation completed",
	})

	ctx := context.Background()
	correlated, err := mms.Correlate(ctx, traceID)
	if err != nil {
		t.Fatalf("Correlate() error = %v", err)
	}

	if correlated.TraceID != traceID {
		t.Errorf("TraceID = %s, want %s", correlated.TraceID, traceID)
	}
	if len(correlated.Spans) != 1 {
		t.Errorf("Spans count = %d, want 1", len(correlated.Spans))
	}
	if len(correlated.Logs) != 1 {
		t.Errorf("Logs count = %d, want 1", len(correlated.Logs))
	}
	if len(correlated.Correlations) != 1 {
		t.Errorf("Correlations count = %d, want 1", len(correlated.Correlations))
	}
}

func TestCorrelateByTime(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	now := time.Now()

	// Write data in time window
	mms.WriteLog(&MMLogEntry{
		Timestamp: now,
		Message:   "Log in window",
		Service:   "svc",
	})
	mms.WriteSpan(&Span{
		TraceID:   "t1",
		Service:   "svc",
		StartTime: now,
		EndTime:   now.Add(50 * time.Millisecond),
	})

	correlated, err := mms.CorrelateByTime(
		now.Add(-time.Hour),
		now.Add(time.Hour),
		[]string{"svc"},
	)
	if err != nil {
		t.Fatalf("CorrelateByTime() error = %v", err)
	}

	if len(correlated.Logs) != 1 {
		t.Errorf("Logs count = %d, want 1", len(correlated.Logs))
	}
	if len(correlated.Spans) != 1 {
		t.Errorf("Spans count = %d, want 1", len(correlated.Spans))
	}
}

func TestUnifiedQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	// Write data
	mms.WriteLog(&MMLogEntry{Service: "api", Message: "Request"})
	mms.WriteSpan(&Span{TraceID: "t1", Service: "api", Name: "op"})

	now := time.Now()
	result, err := mms.UnifiedQuery(&UnifiedQuery{
		StartTime:      now.Add(-time.Hour),
		EndTime:        now.Add(time.Hour),
		Service:        "api",
		IncludeLogs:    true,
		IncludeTraces:  true,
		IncludeMetrics: false,
	})
	if err != nil {
		t.Fatalf("UnifiedQuery() error = %v", err)
	}

	if len(result.Logs) != 1 {
		t.Errorf("Logs count = %d, want 1", len(result.Logs))
	}
	if len(result.Spans) != 1 {
		t.Errorf("Spans count = %d, want 1", len(result.Spans))
	}
}

func TestMMStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{Message: "test"})
	mms.WriteLog(&MMLogEntry{Message: "test2"})
	mms.WriteSpan(&Span{TraceID: "t1"})

	stats := mms.Stats()

	if stats.TotalLogs != 2 {
		t.Errorf("TotalLogs = %d, want 2", stats.TotalLogs)
	}
	if stats.TotalSpans != 1 {
		t.Errorf("TotalSpans = %d, want 1", stats.TotalSpans)
	}
	if stats.LogsInMemory != 2 {
		t.Errorf("LogsInMemory = %d, want 2", stats.LogsInMemory)
	}
}

func TestMaxLogEntries(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultMultiModalConfig()
	config.MaxLogEntries = 5
	mms, _ := NewMultiModalStorage(db, config)
	defer mms.Close()

	for i := 0; i < 10; i++ {
		mms.WriteLog(&MMLogEntry{Message: "Log " + string(rune('0'+i))})
	}

	stats := mms.Stats()
	if stats.LogsInMemory > 5 {
		t.Errorf("LogsInMemory = %d, should be <= 5", stats.LogsInMemory)
	}
}

func TestMaxSpans(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultMultiModalConfig()
	config.MaxSpans = 5
	mms, _ := NewMultiModalStorage(db, config)
	defer mms.Close()

	for i := 0; i < 10; i++ {
		mms.WriteSpan(&Span{TraceID: "t", SpanID: string(rune('a'+i))})
	}

	stats := mms.Stats()
	if stats.SpansInMemory > 5 {
		t.Errorf("SpansInMemory = %d, should be <= 5", stats.SpansInMemory)
	}
}

func TestInvertedIndex(t *testing.T) {
	idx := newInvertedIndex(3)

	idx.add("doc1", "hello world")
	idx.add("doc2", "hello there")
	idx.add("doc3", "goodbye world")

	// Search for "hello" - should match doc1 and doc2
	results := idx.search("hello")
	if len(results) != 2 {
		t.Errorf("Search 'hello' = %d results, want 2", len(results))
	}

	// Search for "world" - should match doc1 and doc3
	results = idx.search("world")
	if len(results) != 2 {
		t.Errorf("Search 'world' = %d results, want 2", len(results))
	}

	// Search for "hello world" - should only match doc1
	results = idx.search("hello world")
	if len(results) != 1 {
		t.Errorf("Search 'hello world' = %d results, want 1", len(results))
	}
}

func TestIsLevelAtOrAbove(t *testing.T) {
	tests := []struct {
		level     LogLevel
		threshold LogLevel
		expected  bool
	}{
		{LogLevelError, LogLevelWarn, true},
		{LogLevelWarn, LogLevelError, false},
		{LogLevelInfo, LogLevelInfo, true},
		{LogLevelDebug, LogLevelError, false},
		{LogLevelFatal, LogLevelDebug, true},
	}

	for _, tt := range tests {
		result := isLevelAtOrAbove(tt.level, tt.threshold)
		if result != tt.expected {
			t.Errorf("isLevelAtOrAbove(%s, %s) = %v, want %v",
				tt.level, tt.threshold, result, tt.expected)
		}
	}
}

func TestLogsDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultMultiModalConfig()
	config.LogsEnabled = false
	mms, _ := NewMultiModalStorage(db, config)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{Message: "test"})

	stats := mms.Stats()
	if stats.TotalLogs != 0 {
		t.Errorf("TotalLogs = %d, want 0 when logs disabled", stats.TotalLogs)
	}
}

func TestTracesDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultMultiModalConfig()
	config.TracesEnabled = false
	mms, _ := NewMultiModalStorage(db, config)
	defer mms.Close()

	mms.WriteSpan(&Span{TraceID: "t1"})

	stats := mms.Stats()
	if stats.TotalSpans != 0 {
		t.Errorf("TotalSpans = %d, want 0 when traces disabled", stats.TotalSpans)
	}
}

func TestCorrelationDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultMultiModalConfig()
	config.CorrelationEnabled = false
	mms, _ := NewMultiModalStorage(db, config)
	defer mms.Close()

	ctx := context.Background()
	_, err = mms.Correlate(ctx, "trace-id")
	if err == nil {
		t.Error("Expected error when correlation is disabled")
	}
}

func TestQueryLogsWithLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	for i := 0; i < 20; i++ {
		mms.WriteLog(&MMLogEntry{Message: "Log"})
	}

	results, _ := mms.QueryLogs(&MMLogQuery{Limit: 5})
	if len(results) != 5 {
		t.Errorf("Results count = %d, want 5", len(results))
	}
}

func TestQuerySpansWithLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	for i := 0; i < 20; i++ {
		mms.WriteSpan(&Span{TraceID: "t", SpanID: string(rune('a'+i))})
	}

	results, _ := mms.QuerySpans(&SpanQuery{Limit: 5})
	if len(results) != 5 {
		t.Errorf("Results count = %d, want 5", len(results))
	}
}

func TestSpanEvents(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	span := &Span{
		TraceID: "t1",
		Name:    "operation",
		Events: []SpanEvent{
			{Name: "event1", Timestamp: time.Now()},
			{Name: "event2", Timestamp: time.Now()},
		},
	}

	mms.WriteSpan(span)

	trace, _ := mms.GetTrace("t1")
	if len(trace.Spans[0].Events) != 2 {
		t.Errorf("Events count = %d, want 2", len(trace.Spans[0].Events))
	}
}

func TestLogAttributes(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mms, _ := NewMultiModalStorage(db, nil)
	defer mms.Close()

	mms.WriteLog(&MMLogEntry{
		Message: "Request",
		Attributes: map[string]string{
			"method": "GET",
			"path":   "/api/users",
		},
	})
	mms.WriteLog(&MMLogEntry{
		Message: "Request",
		Attributes: map[string]string{
			"method": "POST",
			"path":   "/api/users",
		},
	})

	results, _ := mms.QueryLogs(&MMLogQuery{
		Attributes: map[string]string{"method": "GET"},
	})

	if len(results) != 1 {
		t.Errorf("Results count = %d, want 1", len(results))
	}
}
