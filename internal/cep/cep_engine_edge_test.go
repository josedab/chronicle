package cep

import (
	"sync"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// --- Edge Case: Out-of-order events ---

func TestProcessEvent_OutOfOrder(t *testing.T) {
	e := newTestEngine()

	_, err := e.CreateWindow("w1", WindowConfig{
		Type:   WindowTumbling,
		Size:   time.Minute,
		Metric: "cpu",
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()

	// Process events out of order
	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 30, Timestamp: now + int64(30*time.Second)})
	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 10, Timestamp: now})
	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 20, Timestamp: now + int64(15*time.Second)})

	// No panic, all events processed
	stats := e.Stats()
	if stats.WindowCount != 1 {
		t.Errorf("expected 1 window, got %d", stats.WindowCount)
	}
}

// --- Edge Case: Window expiry and pane computation ---

func TestComputeWindowResult_EmptyPane(t *testing.T) {
	e := newTestEngine()

	w, err := e.CreateWindow("w1", WindowConfig{
		Type: WindowTumbling,
		Size: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create empty pane
	pane := &WindowPane{
		ID:        0,
		StartTime: 0,
		EndTime:   int64(time.Minute),
		Events:    []windowedEvent{},
		State:     make(map[string]float64),
	}

	result := e.computeWindowResult(w, pane)
	if result != nil {
		t.Error("Empty pane should produce nil result")
	}
}

func TestComputeWindowResult_WithEvents(t *testing.T) {
	e := newTestEngine()

	w, err := e.CreateWindow("w1", WindowConfig{
		Type: WindowTumbling,
		Size: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()
	pane := &WindowPane{
		ID:        1,
		StartTime: now,
		EndTime:   now + int64(time.Minute),
		Events: []windowedEvent{
			{Point: chronicle.Point{Metric: "cpu", Value: 10}, EventTime: now},
			{Point: chronicle.Point{Metric: "cpu", Value: 20}, EventTime: now + 1000},
		},
		State: map[string]float64{
			"count": 2,
			"sum":   30,
			"min":   10,
			"max":   20,
			"mean":  15,
		},
	}

	result := e.computeWindowResult(w, pane)
	if result == nil {
		t.Fatal("Expected non-nil result for pane with events")
	}
	if result.QueryID != "w1" {
		t.Errorf("QueryID = %q, want w1", result.QueryID)
	}
	if result.Values["count"] != 2 {
		t.Errorf("count = %f, want 2", result.Values["count"])
	}
	if result.Values["sum"] != 30 {
		t.Errorf("sum = %f, want 30", result.Values["sum"])
	}
	if result.Metadata["event_count"] != 2 {
		t.Errorf("metadata event_count = %v, want 2", result.Metadata["event_count"])
	}
}

// --- Edge Case: Pattern timeout ---

func TestPatternTimeout_WithinTime(t *testing.T) {
	e := newTestEngine()

	ch := e.Subscribe("timeout_pattern")

	pattern := &EventPattern{
		Name: "timeout_pattern",
		Sequence: []PatternStep{
			{Metric: "cpu"},
			{Metric: "memory"},
		},
		WithinTime: time.Millisecond, // Very short timeout
	}
	if err := e.RegisterPattern(pattern); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()

	// First event matches step 0 (cpu)
	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 50, Timestamp: now})

	// Non-matching event arrives late, triggering the timeout reset
	e.ProcessEvent(chronicle.Point{Metric: "disk", Value: 10, Timestamp: now + int64(time.Second)})

	// Now send the event that would match step 1, but pattern was reset to step 0
	e.ProcessEvent(chronicle.Point{Metric: "memory", Value: 60, Timestamp: now + int64(2*time.Second)})

	select {
	case <-ch:
		t.Error("Should not match pattern after timeout")
	case <-time.After(100 * time.Millisecond):
		// Expected - no match due to timeout
	}
}

// --- Edge Case: matchesStep with conditions ---

func TestMatchesStep_MetricMismatch(t *testing.T) {
	e := newTestEngine()

	step := PatternStep{Metric: "memory"}
	p := chronicle.Point{Metric: "cpu", Tags: map[string]string{}}

	if e.matchesStep(p, step) {
		t.Error("Should not match different metric")
	}
}

func TestMatchesStep_TagMismatch(t *testing.T) {
	e := newTestEngine()

	step := PatternStep{
		Metric: "cpu",
		Tags:   map[string]string{"host": "server1"},
	}
	p := chronicle.Point{
		Metric: "cpu",
		Tags:   map[string]string{"host": "server2"},
	}

	if e.matchesStep(p, step) {
		t.Error("Should not match with different tag value")
	}
}

func TestMatchesStep_ConditionFails(t *testing.T) {
	e := newTestEngine()

	step := PatternStep{
		Metric:    "cpu",
		Condition: func(p chronicle.Point) bool { return p.Value > 90 },
	}
	p := chronicle.Point{Metric: "cpu", Value: 50, Tags: map[string]string{}}

	if e.matchesStep(p, step) {
		t.Error("Should not match when condition fails")
	}
}

func TestMatchesStep_AllMatch(t *testing.T) {
	e := newTestEngine()

	step := PatternStep{
		Metric:    "cpu",
		Tags:      map[string]string{"host": "server1"},
		Condition: func(p chronicle.Point) bool { return p.Value > 90 },
	}
	p := chronicle.Point{
		Metric: "cpu",
		Value:  95,
		Tags:   map[string]string{"host": "server1"},
	}

	if !e.matchesStep(p, step) {
		t.Error("Should match when all criteria pass")
	}
}

// --- Edge Case: matchesWindow ---

func TestMatchesWindow_MetricFilter(t *testing.T) {
	e := newTestEngine()

	w := &CEPWindow{Metric: "cpu", Tags: nil}
	p := chronicle.Point{Metric: "memory", Tags: map[string]string{}}

	if e.matchesWindow(p, w) {
		t.Error("Should not match different metric")
	}
}

func TestMatchesWindow_EmptyMetricMatchesAll(t *testing.T) {
	e := newTestEngine()

	w := &CEPWindow{Metric: "", Tags: nil}
	p := chronicle.Point{Metric: "anything", Tags: map[string]string{}}

	if !e.matchesWindow(p, w) {
		t.Error("Empty metric filter should match all")
	}
}

func TestMatchesWindow_TagFilter(t *testing.T) {
	e := newTestEngine()

	w := &CEPWindow{
		Metric: "cpu",
		Tags:   map[string]string{"env": "prod"},
	}
	p := chronicle.Point{Metric: "cpu", Tags: map[string]string{"env": "dev"}}

	if e.matchesWindow(p, w) {
		t.Error("Should not match different tag value")
	}
}

// --- Edge Case: parseCEPSQL ---

func TestParseCEPSQL_TumblingWindow(t *testing.T) {
	e := newTestEngine()

	parsed, err := e.parseCEPSQL("SELECT count(*) FROM cpu WINDOW tumbling(5 minutes) GROUP BY host")
	if err != nil {
		t.Fatal(err)
	}

	if parsed.From != "cpu" {
		t.Errorf("From = %q, want cpu", parsed.From)
	}
	if parsed.Window.Type != WindowTumbling {
		t.Errorf("Window type = %v, want tumbling", parsed.Window.Type)
	}
	if parsed.Window.Size != 5*time.Minute {
		t.Errorf("Window size = %v, want 5m", parsed.Window.Size)
	}
	if len(parsed.GroupBy) != 1 || parsed.GroupBy[0] != "host" {
		t.Errorf("GroupBy = %v, want [host]", parsed.GroupBy)
	}
}

func TestParseCEPSQL_SlidingWindow(t *testing.T) {
	e := newTestEngine()

	parsed, err := e.parseCEPSQL("SELECT avg(value) FROM mem WINDOW sliding(1 hours)")
	if err != nil {
		t.Fatal(err)
	}

	if parsed.From != "mem" {
		t.Errorf("From = %q, want mem", parsed.From)
	}
	if parsed.Window.Type != WindowSliding {
		t.Errorf("Window type = %v, want sliding", parsed.Window.Type)
	}
	if parsed.Window.Size != time.Hour {
		t.Errorf("Window size = %v, want 1h", parsed.Window.Size)
	}
}

func TestParseCEPSQL_SessionWindow(t *testing.T) {
	e := newTestEngine()

	parsed, err := e.parseCEPSQL("SELECT sum(value) FROM requests WINDOW session(30 seconds)")
	if err != nil {
		t.Fatal(err)
	}

	if parsed.Window.Type != WindowSession {
		t.Errorf("Window type = %v, want session", parsed.Window.Type)
	}
	if parsed.Window.Size != 30*time.Second {
		t.Errorf("Window size = %v, want 30s", parsed.Window.Size)
	}
}

func TestParseCEPSQL_NoWindow(t *testing.T) {
	e := newTestEngine()

	parsed, err := e.parseCEPSQL("SELECT count(*) FROM cpu")
	if err != nil {
		t.Fatal(err)
	}

	if parsed.From != "cpu" {
		t.Errorf("From = %q, want cpu", parsed.From)
	}
	if parsed.Window.Size != 0 {
		t.Errorf("Window size should be 0 for query without WINDOW clause, got %v", parsed.Window.Size)
	}
}

// --- Edge Case: Concurrent window operations ---

func TestConcurrentProcessEvents(t *testing.T) {
	e := newTestEngine()

	_, err := e.CreateWindow("w1", WindowConfig{
		Type:   WindowTumbling,
		Size:   time.Minute,
		Metric: "cpu",
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	now := time.Now().UnixNano()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			e.ProcessEvent(chronicle.Point{
				Metric:    "cpu",
				Value:     float64(idx),
				Timestamp: now + int64(idx)*int64(time.Millisecond),
				Tags:      map[string]string{"host": "test"},
			})
		}(i)
	}

	wg.Wait()
	// No panic means concurrent access is safe
}

// --- Edge Case: ProcessBatch empty ---

func TestProcessBatch_Empty(t *testing.T) {
	e := newTestEngine()

	err := e.ProcessBatch(nil)
	if err != nil {
		t.Fatalf("ProcessBatch with nil should not error: %v", err)
	}

	err = e.ProcessBatch([]chronicle.Point{})
	if err != nil {
		t.Fatalf("ProcessBatch with empty slice should not error: %v", err)
	}
}

// --- Edge Case: Multiple windows for same event ---

func TestEventMatchesMultipleWindows(t *testing.T) {
	e := newTestEngine()

	_, err := e.CreateWindow("w1", WindowConfig{Type: WindowTumbling, Size: time.Minute, Metric: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.CreateWindow("w2", WindowConfig{Type: WindowTumbling, Size: 5 * time.Minute, Metric: "cpu"})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()
	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 42, Timestamp: now, Tags: map[string]string{}})

	stats := e.Stats()
	if stats.WindowCount != 2 {
		t.Errorf("Expected 2 windows, got %d", stats.WindowCount)
	}
}

// --- Edge Case: Pattern with OnMatch callback ---

func TestPatternOnMatchCallback(t *testing.T) {
	e := newTestEngine()

	callbackCalled := make(chan bool, 1)

	pattern := &EventPattern{
		Name: "callback_test",
		Sequence: []PatternStep{
			{Metric: "cpu"},
		},
		OnMatch: func(events []chronicle.Point, ctx map[string]any) {
			callbackCalled <- true
		},
	}

	if err := e.RegisterPattern(pattern); err != nil {
		t.Fatal(err)
	}

	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 50, Timestamp: time.Now().UnixNano(), Tags: map[string]string{}})

	select {
	case <-callbackCalled:
		// Success
	case <-time.After(time.Second):
		t.Error("OnMatch callback was not called")
	}
}

// --- Edge Case: Query with SQL creates window ---

func TestRegisterQuery_WithSQL_CreatesWindow(t *testing.T) {
	e := newTestEngine()

	query := &CEPQuery{
		ID:  "q-with-window",
		SQL: "SELECT count(*) FROM cpu WINDOW tumbling(1 minutes)",
	}

	err := e.RegisterQuery(query)
	if err != nil {
		t.Fatal(err)
	}

	stats := e.Stats()
	if stats.WindowCount != 1 {
		t.Errorf("Expected 1 window created by query, got %d", stats.WindowCount)
	}
	if stats.QueryCount != 1 {
		t.Errorf("Expected 1 query, got %d", stats.QueryCount)
	}

	// Unregister should clean up window too
	e.UnregisterQuery("q-with-window")
	stats = e.Stats()
	if stats.WindowCount != 0 {
		t.Errorf("Expected 0 windows after unregister, got %d", stats.WindowCount)
	}
}

// --- Edge Case: getPartitionKey ---

func TestGetPartitionKey_Empty(t *testing.T) {
	key := getPartitionKey(chronicle.Point{Tags: map[string]string{"host": "a"}}, nil)
	if key != "default" {
		t.Errorf("Expected 'default', got %q", key)
	}
}

func TestGetPartitionKey_WithFields(t *testing.T) {
	key := getPartitionKey(
		chronicle.Point{Tags: map[string]string{"host": "server1", "region": "us"}},
		[]string{"host"},
	)
	if key != "host=server1," {
		t.Errorf("Expected 'host=server1,', got %q", key)
	}
}
