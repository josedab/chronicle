package cep

import (
	"container/heap"
	"sort"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// --- PatternBuilder fluent API ---

func TestPatternBuilder_FullChain(t *testing.T) {
	callbackCalled := false
	p := NewPattern("spike_detect").
		Begin("high_cpu").
		Where("cpu", func(p chronicle.Point) bool { return p.Value > 80 }).
		WithTags(map[string]string{"env": "prod"}).
		FollowedBy("very_high_cpu").
		Where("cpu", func(p chronicle.Point) bool { return p.Value > 95 }).
		Within(5 * time.Minute).
		PartitionBy("host", "region").
		OnMatch(func(events []chronicle.Point, ctx map[string]any) {
			callbackCalled = true
		}).
		Build()
	_ = callbackCalled

	if p.Name != "spike_detect" {
		t.Errorf("Name = %q, want spike_detect", p.Name)
	}
	if len(p.Sequence) != 2 {
		t.Fatalf("Sequence length = %d, want 2", len(p.Sequence))
	}
	if p.Sequence[0].Name != "high_cpu" {
		t.Errorf("Step 0 name = %q, want high_cpu", p.Sequence[0].Name)
	}
	if p.Sequence[0].Metric != "cpu" {
		t.Errorf("Step 0 metric = %q, want cpu", p.Sequence[0].Metric)
	}
	if p.Sequence[0].Tags["env"] != "prod" {
		t.Errorf("Step 0 tags missing env=prod")
	}
	if p.Sequence[1].Name != "very_high_cpu" {
		t.Errorf("Step 1 name = %q, want very_high_cpu", p.Sequence[1].Name)
	}
	if p.WithinTime != 5*time.Minute {
		t.Errorf("WithinTime = %v, want 5m", p.WithinTime)
	}
	if len(p.PartitionBy) != 2 || p.PartitionBy[0] != "host" {
		t.Errorf("PartitionBy = %v, want [host, region]", p.PartitionBy)
	}
	if p.OnMatch == nil {
		t.Error("OnMatch should be set")
	}
	// Verify condition functions work
	if !p.Sequence[0].Condition(chronicle.Point{Value: 85}) {
		t.Error("Step 0 condition should match value=85")
	}
	if p.Sequence[0].Condition(chronicle.Point{Value: 50}) {
		t.Error("Step 0 condition should not match value=50")
	}
}

func TestPatternBuilder_MinimalPattern(t *testing.T) {
	p := NewPattern("simple").Begin("start").Build()
	if p.Name != "simple" {
		t.Errorf("Name = %q", p.Name)
	}
	if len(p.Sequence) != 1 {
		t.Errorf("Sequence len = %d", len(p.Sequence))
	}
}

// --- WindowBuilder fluent API ---

func TestWindowBuilder_Tumbling(t *testing.T) {
	cfg := NewWindowBuilder().
		Tumbling(5 * time.Minute).
		OnMetric("cpu").
		WithTags(map[string]string{"host": "server1"}).
		Aggregate(chronicle.AggMean).
		GroupBy("region").
		Build()

	if cfg.Type != WindowTumbling {
		t.Errorf("Type = %v, want tumbling", cfg.Type)
	}
	if cfg.Size != 5*time.Minute {
		t.Errorf("Size = %v, want 5m", cfg.Size)
	}
	if cfg.Metric != "cpu" {
		t.Errorf("Metric = %q, want cpu", cfg.Metric)
	}
	if cfg.Tags["host"] != "server1" {
		t.Error("Missing tag host=server1")
	}
	if cfg.Function != chronicle.AggMean {
		t.Errorf("Function = %v, want mean", cfg.Function)
	}
	if len(cfg.GroupBy) != 1 || cfg.GroupBy[0] != "region" {
		t.Errorf("GroupBy = %v", cfg.GroupBy)
	}
}

func TestWindowBuilder_Sliding(t *testing.T) {
	cfg := NewWindowBuilder().
		Sliding(10*time.Minute, 2*time.Minute).
		OnMetric("mem").
		Build()

	if cfg.Type != WindowSliding {
		t.Errorf("Type = %v, want sliding", cfg.Type)
	}
	if cfg.Size != 10*time.Minute {
		t.Errorf("Size = %v", cfg.Size)
	}
	if cfg.Slide != 2*time.Minute {
		t.Errorf("Slide = %v", cfg.Slide)
	}
}

func TestWindowBuilder_Session(t *testing.T) {
	cfg := NewWindowBuilder().Session(30 * time.Second).Build()
	if cfg.Type != WindowSession {
		t.Errorf("Type = %v, want session", cfg.Type)
	}
}

// --- byTimestamp sort.Interface ---

func TestByTimestamp_Sort(t *testing.T) {
	points := byTimestamp{
		{Timestamp: 300},
		{Timestamp: 100},
		{Timestamp: 200},
	}

	if points.Len() != 3 {
		t.Errorf("Len = %d", points.Len())
	}
	if !points.Less(1, 0) {
		t.Error("100 should be less than 300")
	}

	sort.Sort(points)
	if points[0].Timestamp != 100 || points[1].Timestamp != 200 || points[2].Timestamp != 300 {
		t.Errorf("After sort: %v, %v, %v", points[0].Timestamp, points[1].Timestamp, points[2].Timestamp)
	}
}

// --- eventHeap (container/heap) ---

func TestEventHeap(t *testing.T) {
	h := &eventHeap{}
	heap.Init(h)

	heap.Push(h, windowedEvent{EventTime: 300})
	heap.Push(h, windowedEvent{EventTime: 100})
	heap.Push(h, windowedEvent{EventTime: 200})

	if h.Len() != 3 {
		t.Errorf("Len = %d", h.Len())
	}

	// Min-heap: should pop smallest first
	e := heap.Pop(h).(windowedEvent)
	if e.EventTime != 100 {
		t.Errorf("First pop = %d, want 100", e.EventTime)
	}
	e = heap.Pop(h).(windowedEvent)
	if e.EventTime != 200 {
		t.Errorf("Second pop = %d, want 200", e.EventTime)
	}
	e = heap.Pop(h).(windowedEvent)
	if e.EventTime != 300 {
		t.Errorf("Third pop = %d, want 300", e.EventTime)
	}
}

// --- CEPDB wrapper ---
// CEPDB.Start/Stop/Write/CEP require a real *chronicle.DB.
// We test what we can without it.

func TestNewCEPDB(t *testing.T) {
	config := DefaultCEPConfig()
	cepdb, err := NewCEPDB(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	if cepdb.CEP() == nil {
		t.Error("CEP() should return non-nil engine")
	}
}

func TestCEPDB_StartStop(t *testing.T) {
	config := DefaultCEPConfig()
	config.CheckpointInterval = 0
	cepdb, err := NewCEPDB(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := cepdb.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := cepdb.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// --- advanceWatermark + evaluateWindow ---

func TestAdvanceWatermark_TriggersEvaluation(t *testing.T) {
	e := newTestEngine()

	w, _ := e.CreateWindow("w-advance", WindowConfig{
		Type:   WindowTumbling,
		Size:   time.Millisecond, // tiny window so panes close fast
		Metric: "cpu",
	})

	// Process an event in the past so its pane is closeable
	pastTime := int64(1000)
	e.ProcessEvent(chronicle.Point{
		Metric: "cpu", Value: 42.0, Timestamp: pastTime,
		Tags: map[string]string{},
	})

	// Manually set watermark way ahead
	e.waterMu.Lock()
	e.watermark = pastTime + int64(time.Minute)
	e.waterMu.Unlock()

	// Call advanceWatermark to trigger evaluateWindow
	e.advanceWatermark()

	// Check that the pane was emitted
	w.bufferMu.RLock()
	emittedCount := 0
	for _, pane := range w.panes {
		if pane.Emitted {
			emittedCount++
		}
	}
	w.bufferMu.RUnlock()

	if emittedCount == 0 {
		t.Error("Expected at least one pane to be emitted after watermark advance")
	}
}

// --- WindowType.String ---

func TestWindowType_String(t *testing.T) {
	tests := []struct {
		wt     WindowType
		expect string
	}{
		{WindowTumbling, "tumbling"},
		{WindowSliding, "sliding"},
		{WindowSession, "session"},
		{WindowCount, "count"},
		{WindowType(99), "unknown"},
	}
	for _, tc := range tests {
		if tc.wt.String() != tc.expect {
			t.Errorf("WindowType(%d).String() = %q, want %q", tc.wt, tc.wt.String(), tc.expect)
		}
	}
}

// --- NewPattern ---

func TestNewPattern(t *testing.T) {
	p := NewPattern("test-pattern")
	if p == nil {
		t.Fatal("Expected non-nil PatternBuilder")
	}
	pattern := p.Build()
	if pattern.Name != "test-pattern" {
		t.Errorf("Name = %q", pattern.Name)
	}
	if len(pattern.Sequence) != 0 {
		t.Errorf("Sequence should be empty initially, got %d", len(pattern.Sequence))
	}
}

// --- Where/WithTags on empty sequence (no-op) ---

func TestPatternBuilder_WhereOnEmpty(t *testing.T) {
	p := NewPattern("test").
		Where("cpu", nil).       // no steps yet — should be no-op
		WithTags(map[string]string{"host": "a"}).
		Build()
	if len(p.Sequence) != 0 {
		t.Error("Where/WithTags on empty sequence should be no-op")
	}
}
