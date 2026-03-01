package cep

import (
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func newTestEngine() *CEPEngine {
	config := DefaultCEPConfig()
	config.WatermarkDelay = 0
	config.CheckpointInterval = 0
	config.OutputBufferSize = 100
	return NewCEPEngine(nil, config)
}

func TestProcessEvent_SingleMatch(t *testing.T) {
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
	err = e.ProcessEvent(chronicle.Point{
		Metric:    "cpu",
		Value:     75.0,
		Timestamp: now,
		Tags:      map[string]string{"host": "server1"},
	})
	if err != nil {
		t.Fatalf("ProcessEvent error: %v", err)
	}

	stats := e.Stats()
	if stats.WindowCount != 1 {
		t.Errorf("expected 1 window, got %d", stats.WindowCount)
	}
}

func TestProcessBatch_MixedEvents(t *testing.T) {
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
	points := []chronicle.Point{
		{Metric: "cpu", Value: 50.0, Timestamp: now, Tags: map[string]string{"host": "a"}},
		{Metric: "memory", Value: 1024, Timestamp: now, Tags: map[string]string{"host": "a"}}, // non-matching
		{Metric: "cpu", Value: 90.0, Timestamp: now + 1000, Tags: map[string]string{"host": "b"}},
	}

	err = e.ProcessBatch(points)
	if err != nil {
		t.Fatalf("ProcessBatch error: %v", err)
	}
}

func TestCreateDeleteWindow_Lifecycle(t *testing.T) {
	e := newTestEngine()

	w, err := e.CreateWindow("w1", WindowConfig{
		Type: WindowTumbling,
		Size: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.ID != "w1" {
		t.Errorf("window ID = %q, want w1", w.ID)
	}

	stats := e.Stats()
	if stats.WindowCount != 1 {
		t.Errorf("expected 1 window, got %d", stats.WindowCount)
	}

	e.DeleteWindow("w1")
	stats = e.Stats()
	if stats.WindowCount != 0 {
		t.Errorf("expected 0 windows after delete, got %d", stats.WindowCount)
	}
}

func TestCreateWindow_EmptyID(t *testing.T) {
	e := newTestEngine()

	_, err := e.CreateWindow("", WindowConfig{Size: time.Minute})
	if err == nil {
		t.Error("expected error for empty window ID")
	}
}

func TestCreateWindow_ZeroDuration(t *testing.T) {
	e := newTestEngine()

	w, err := e.CreateWindow("w1", WindowConfig{
		Type: WindowTumbling,
		Size: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Slide defaults to Size when Size is 0
	if w.Slide != 0 {
		t.Errorf("expected slide=0, got %v", w.Slide)
	}
}

func TestRegisterUnregisterPattern_CRUD(t *testing.T) {
	e := newTestEngine()

	pattern := &EventPattern{
		Name: "spike",
		Sequence: []PatternStep{
			{Metric: "cpu", Name: "high"},
		},
	}

	err := e.RegisterPattern(pattern)
	if err != nil {
		t.Fatal(err)
	}

	stats := e.Stats()
	if stats.PatternCount != 1 {
		t.Errorf("expected 1 pattern, got %d", stats.PatternCount)
	}

	e.UnregisterPattern("spike")
	stats = e.Stats()
	if stats.PatternCount != 0 {
		t.Errorf("expected 0 patterns after unregister, got %d", stats.PatternCount)
	}
}

func TestRegisterPattern_EmptyName(t *testing.T) {
	e := newTestEngine()

	err := e.RegisterPattern(&EventPattern{
		Sequence: []PatternStep{{Metric: "cpu"}},
	})
	if err == nil {
		t.Error("expected error for empty pattern name")
	}
}

func TestRegisterPattern_EmptySequence(t *testing.T) {
	e := newTestEngine()

	err := e.RegisterPattern(&EventPattern{
		Name:     "test",
		Sequence: []PatternStep{},
	})
	if err == nil {
		t.Error("expected error for empty sequence")
	}
}

func TestSubscribe_ReceivesEvents(t *testing.T) {
	e := newTestEngine()

	ch := e.Subscribe("test-sub")
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
}

func TestRegisterQuery_Continuous(t *testing.T) {
	e := newTestEngine()

	query := &CEPQuery{
		ID:   "q1",
		Name: "test-query",
	}

	err := e.RegisterQuery(query)
	if err != nil {
		t.Fatal(err)
	}

	stats := e.Stats()
	if stats.QueryCount != 1 {
		t.Errorf("expected 1 query, got %d", stats.QueryCount)
	}

	e.UnregisterQuery("q1")
	stats = e.Stats()
	if stats.QueryCount != 0 {
		t.Errorf("expected 0 queries after unregister, got %d", stats.QueryCount)
	}
}

func TestRegisterQuery_EmptyID(t *testing.T) {
	e := newTestEngine()

	err := e.RegisterQuery(&CEPQuery{Name: "no-id"})
	if err == nil {
		t.Error("expected error for empty query ID")
	}
}

func TestStartStop_Lifecycle(t *testing.T) {
	e := newTestEngine()

	err := e.Start()
	if err != nil {
		t.Fatal(err)
	}

	err = e.Stop()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStartStop_WithCheckpoint(t *testing.T) {
	config := DefaultCEPConfig()
	config.CheckpointInterval = 50 * time.Millisecond
	config.WatermarkDelay = 0
	config.OutputBufferSize = 100
	e := NewCEPEngine(nil, config)

	err := e.Start()
	if err != nil {
		t.Fatal(err)
	}

	// Let checkpoint run at least once
	time.Sleep(100 * time.Millisecond)

	err = e.Stop()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPatternMatch_FullSequence(t *testing.T) {
	e := newTestEngine()

	ch := e.Subscribe("spike_detect")

	pattern := &EventPattern{
		Name: "spike_detect",
		Sequence: []PatternStep{
			{Metric: "cpu", Condition: func(p chronicle.Point) bool { return p.Value > 80 }},
			{Metric: "cpu", Condition: func(p chronicle.Point) bool { return p.Value > 90 }},
		},
		WithinTime: time.Minute,
	}

	if err := e.RegisterPattern(pattern); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()

	// First step match
	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 85, Timestamp: now})
	// Second step match - completes pattern
	e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 95, Timestamp: now + int64(time.Second)})

	// Check for pattern match
	select {
	case result := <-ch:
		if result.PatternID != "spike_detect" {
			t.Errorf("pattern ID = %q, want spike_detect", result.PatternID)
		}
		if len(result.MatchedEvents) != 2 {
			t.Errorf("matched events = %d, want 2", len(result.MatchedEvents))
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for pattern match")
	}
}

func TestPatternAfterUnregister(t *testing.T) {
	e := newTestEngine()

	pattern := &EventPattern{
		Name: "test_pattern",
		Sequence: []PatternStep{
			{Metric: "cpu"},
		},
	}

	e.RegisterPattern(pattern)
	e.UnregisterPattern("test_pattern")

	// Processing events after unregister should not panic
	err := e.ProcessEvent(chronicle.Point{Metric: "cpu", Value: 50, Timestamp: time.Now().UnixNano()})
	if err != nil {
		t.Fatal(err)
	}

	stats := e.Stats()
	if stats.PatternCount != 0 {
		t.Errorf("expected 0 patterns, got %d", stats.PatternCount)
	}
}

func TestOverlappingPatterns(t *testing.T) {
	e := newTestEngine()

	p1 := &EventPattern{
		Name:     "pattern1",
		Sequence: []PatternStep{{Metric: "cpu"}},
	}
	p2 := &EventPattern{
		Name:     "pattern2",
		Sequence: []PatternStep{{Metric: "cpu"}},
	}

	e.RegisterPattern(p1)
	e.RegisterPattern(p2)

	stats := e.Stats()
	if stats.PatternCount != 2 {
		t.Errorf("expected 2 patterns, got %d", stats.PatternCount)
	}
}

func TestNilEventTags(t *testing.T) {
	e := newTestEngine()

	_, err := e.CreateWindow("w1", WindowConfig{
		Type:   WindowTumbling,
		Size:   time.Minute,
		Metric: "cpu",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Process event with nil tags - should not panic
	err = e.ProcessEvent(chronicle.Point{
		Metric:    "cpu",
		Value:     42.0,
		Timestamp: time.Now().UnixNano(),
		Tags:      nil,
	})
	if err != nil {
		t.Fatalf("ProcessEvent with nil tags error: %v", err)
	}
}

func TestWindowSlideDefault(t *testing.T) {
	e := newTestEngine()

	w, err := e.CreateWindow("w1", WindowConfig{
		Type: WindowTumbling,
		Size: 5 * time.Minute,
		// Slide not set
	})
	if err != nil {
		t.Fatal(err)
	}

	// Slide should default to Size
	if w.Slide != 5*time.Minute {
		t.Errorf("expected slide=5m, got %v", w.Slide)
	}
}

func TestRegisterQuery_WithSQL(t *testing.T) {
	e := newTestEngine()

	query := &CEPQuery{
		ID:  "q-sql",
		SQL: "SELECT count(*) FROM cpu WINDOW tumbling(5 minutes) GROUP BY host",
	}

	err := e.RegisterQuery(query)
	if err != nil {
		t.Fatal(err)
	}

	if query.Parsed == nil {
		t.Error("expected parsed query")
	}
	if query.Parsed.From != "cpu" {
		t.Errorf("from = %q, want cpu", query.Parsed.From)
	}
}

func TestJoinWindow(t *testing.T) {
	e := newTestEngine()

	w, err := e.JoinWindow("stream1", "stream2", []string{"host"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if w.Type != WindowSliding {
		t.Errorf("join window type = %v, want sliding", w.Type)
	}
	if len(w.GroupBy) != 1 || w.GroupBy[0] != "host" {
		t.Errorf("group by = %v, want [host]", w.GroupBy)
	}
}
