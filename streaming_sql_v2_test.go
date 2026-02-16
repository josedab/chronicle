package chronicle

import (
	"testing"
	"time"
)

func TestDefaultStreamingSQLV2Config(t *testing.T) {
	cfg := DefaultStreamingSQLV2Config()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.MaxWatermarkLag != 5*time.Second {
		t.Errorf("expected MaxWatermarkLag 5s, got %v", cfg.MaxWatermarkLag)
	}
	if cfg.LateDataGracePeriod != 10*time.Second {
		t.Errorf("expected LateDataGracePeriod 10s, got %v", cfg.LateDataGracePeriod)
	}
	if !cfg.ExactlyOnceEnabled {
		t.Error("expected ExactlyOnceEnabled to be true")
	}
	if cfg.CheckpointInterval != 30*time.Second {
		t.Errorf("expected CheckpointInterval 30s, got %v", cfg.CheckpointInterval)
	}
	if cfg.CheckpointDir != "/tmp/chronicle-checkpoints" {
		t.Errorf("expected CheckpointDir /tmp/chronicle-checkpoints, got %s", cfg.CheckpointDir)
	}
	if cfg.MaxWindowsPerQuery != 10000 {
		t.Errorf("expected MaxWindowsPerQuery 10000, got %d", cfg.MaxWindowsPerQuery)
	}
	if cfg.WindowRetention != time.Hour {
		t.Errorf("expected WindowRetention 1h, got %v", cfg.WindowRetention)
	}
	if cfg.JoinBufferSize != 50000 {
		t.Errorf("expected JoinBufferSize 50000, got %d", cfg.JoinBufferSize)
	}
	if cfg.JoinTimeout != time.Minute {
		t.Errorf("expected JoinTimeout 1m, got %v", cfg.JoinTimeout)
	}
	if cfg.MaxConcurrentQueries != 100 {
		t.Errorf("expected MaxConcurrentQueries 100, got %d", cfg.MaxConcurrentQueries)
	}
}

func TestStreamWatermark(t *testing.T) {
	wm := NewStreamWatermark(2*time.Second, 5*time.Second)

	if got := wm.GetWatermark(); got != 0 {
		t.Errorf("expected initial watermark 0, got %d", got)
	}

	// Advance the watermark. The effective watermark is eventTime - maxOutOfOrderness.
	now := time.Now().UnixNano()
	wm.Advance(now)

	got := wm.GetWatermark()
	expected := now - (2 * time.Second).Nanoseconds()
	if got != expected {
		t.Errorf("expected watermark %d, got %d", expected, got)
	}

	// An event well within the watermark should not be late.
	if wm.IsLate(now) {
		t.Error("recent event should not be late")
	}

	// An event far in the past (before watermark - gracePeriod) should be late.
	veryOld := now - (20 * time.Second).Nanoseconds()
	if !wm.IsLate(veryOld) {
		t.Error("very old event should be late")
	}

	// Advancing with an older event should not move the watermark backwards.
	before := wm.GetWatermark()
	wm.Advance(now - (10 * time.Second).Nanoseconds())
	if wm.GetWatermark() != before {
		t.Error("watermark should not move backwards")
	}
}

func TestWindowedAggregator_Tumbling(t *testing.T) {
	wm := NewStreamWatermark(0, time.Hour) // no out-of-orderness, large grace
	windowSize := 10 * time.Second
	agg := NewWindowedAggregator(StreamingWindowTumbling, windowSize, 0, wm, 1000, time.Hour)

	// Use a fixed base time aligned to the window boundary.
	base := (time.Now().UnixNano() / windowSize.Nanoseconds()) * windowSize.Nanoseconds()

	events := []Point{
		{Metric: "cpu", Value: 10, Timestamp: base + 1},
		{Metric: "cpu", Value: 20, Timestamp: base + 2},
		{Metric: "cpu", Value: 30, Timestamp: base + 3},
	}
	for _, e := range events {
		agg.AddEvent(e, "q1")
	}

	// All events should land in the same tumbling window.
	agg.mu.RLock()
	if len(agg.windows) != 1 {
		t.Fatalf("expected 1 window, got %d", len(agg.windows))
	}
	var windowKey string
	for k := range agg.windows {
		windowKey = k
	}
	ws := agg.windows[windowKey]
	agg.mu.RUnlock()

	if ws.Count != 3 {
		t.Errorf("expected count 3, got %d", ws.Count)
	}
	if ws.Sum != 60 {
		t.Errorf("expected sum 60, got %f", ws.Sum)
	}
	if ws.Min != 10 {
		t.Errorf("expected min 10, got %f", ws.Min)
	}
	if ws.Max != 30 {
		t.Errorf("expected max 30, got %f", ws.Max)
	}

	// Trigger the window and check the result.
	result := agg.TriggerWindow(windowKey)
	if result == nil {
		t.Fatal("expected non-nil result from TriggerWindow")
	}
	if result.Values["count"].(int64) != 3 {
		t.Errorf("result count: expected 3, got %v", result.Values["count"])
	}
	if result.Values["sum"].(float64) != 60 {
		t.Errorf("result sum: expected 60, got %v", result.Values["sum"])
	}

	// Triggering a non-existent window should return nil.
	if agg.TriggerWindow("nonexistent") != nil {
		t.Error("expected nil for non-existent window")
	}

	// ExpireWindows should not remove windows whose retention hasn't elapsed.
	removed := agg.ExpireWindows()
	if removed != 0 {
		t.Errorf("expected 0 expired windows, got %d", removed)
	}
}

func TestWindowedAggregator_Hopping(t *testing.T) {
	wm := NewStreamWatermark(0, time.Hour)
	windowSize := 10 * time.Second
	advance := 5 * time.Second
	agg := NewWindowedAggregator(StreamingWindowHopping, windowSize, advance, wm, 1000, time.Hour)

	// Place an event in the middle of a hop boundary so it falls into multiple windows.
	base := (time.Now().UnixNano() / advance.Nanoseconds()) * advance.Nanoseconds()
	event := Point{Metric: "mem", Value: 42, Timestamp: base + advance.Nanoseconds()/2}
	agg.AddEvent(event, "q2")

	agg.mu.RLock()
	windowCount := len(agg.windows)
	agg.mu.RUnlock()

	// A hopping window with size=10s and advance=5s should assign the event to at least 1 window.
	if windowCount < 1 {
		t.Errorf("expected at least 1 hopping window, got %d", windowCount)
	}

	// Verify aggregation in each window.
	agg.mu.RLock()
	for key, ws := range agg.windows {
		if ws.Count != 1 {
			t.Errorf("window %s: expected count 1, got %d", key, ws.Count)
		}
		if ws.Sum != 42 {
			t.Errorf("window %s: expected sum 42, got %f", key, ws.Sum)
		}
	}
	agg.mu.RUnlock()
}

func TestStreamJoinEngine_InnerJoin(t *testing.T) {
	engine := NewStreamJoinEngine(1000, time.Minute)
	joinID := "join_inner_1"
	engine.RegisterJoin(joinID, "cpu", "mem", JoinTypeInner, "left.host = right.host", 5*time.Second)

	now := time.Now().UnixNano()

	// Add a left event.
	leftEvent := Point{
		Metric:    "cpu",
		Value:     80,
		Timestamp: now,
		Tags:      map[string]string{"host": "srv1"},
	}
	// No matching right event yet – inner join should produce no results.
	results := engine.ProcessLeft(joinID, leftEvent)
	if len(results) != 0 {
		t.Errorf("expected 0 results before right event, got %d", len(results))
	}

	// Add a matching right event.
	rightEvent := Point{
		Metric:    "mem",
		Value:     4096,
		Timestamp: now + int64(time.Second),
		Tags:      map[string]string{"host": "srv1"},
	}
	results = engine.ProcessRight(joinID, rightEvent)
	if len(results) != 1 {
		t.Fatalf("expected 1 join result, got %d", len(results))
	}
	if results[0].Values["left_value"].(float64) != 80 {
		t.Errorf("expected left_value 80, got %v", results[0].Values["left_value"])
	}
	if results[0].Values["right_value"].(float64) != 4096 {
		t.Errorf("expected right_value 4096, got %v", results[0].Values["right_value"])
	}

	// A right event with a different host should not join.
	noMatch := Point{
		Metric:    "mem",
		Value:     2048,
		Timestamp: now + int64(2*time.Second),
		Tags:      map[string]string{"host": "srv2"},
	}
	results = engine.ProcessRight(joinID, noMatch)
	if len(results) != 0 {
		t.Errorf("expected 0 results for non-matching host, got %d", len(results))
	}
}

func TestStreamJoinEngine_LeftJoin(t *testing.T) {
	engine := NewStreamJoinEngine(1000, time.Minute)
	joinID := "join_left_1"
	engine.RegisterJoin(joinID, "cpu", "mem", JoinTypeLeft, "left.host = right.host", 5*time.Second)

	now := time.Now().UnixNano()

	// Left event with no matching right event should still produce a result.
	leftEvent := Point{
		Metric:    "cpu",
		Value:     55,
		Timestamp: now,
		Tags:      map[string]string{"host": "srv1"},
	}
	results := engine.ProcessLeft(joinID, leftEvent)
	if len(results) != 1 {
		t.Fatalf("expected 1 result for left join with no right match, got %d", len(results))
	}
	// The right side should be a zero-value Point.
	if results[0].Values["right_metric"].(string) != "" {
		t.Errorf("expected empty right_metric for unmatched left join, got %v", results[0].Values["right_metric"])
	}

	// Now add a matching right event; subsequent left processing should join.
	rightEvent := Point{
		Metric:    "mem",
		Value:     8192,
		Timestamp: now + int64(time.Second),
		Tags:      map[string]string{"host": "srv1"},
	}
	results = engine.ProcessRight(joinID, rightEvent)
	if len(results) != 1 {
		t.Fatalf("expected 1 result after matching right event, got %d", len(results))
	}
	if results[0].Values["right_value"].(float64) != 8192 {
		t.Errorf("expected right_value 8192, got %v", results[0].Values["right_value"])
	}
}

func TestExactlyOnceProcessor(t *testing.T) {
	proc := NewExactlyOnceProcessor(100)

	// Begin and commit a transaction.
	txn1 := proc.Begin()
	if err := proc.RecordEvent(txn1, "evt-1"); err != nil {
		t.Fatalf("RecordEvent failed: %v", err)
	}
	if err := proc.RecordOutput(txn1, "out-1"); err != nil {
		t.Fatalf("RecordOutput failed: %v", err)
	}
	if err := proc.Commit(txn1); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// evt-1 should now be a duplicate.
	if !proc.IsDuplicate("evt-1") {
		t.Error("expected evt-1 to be duplicate after commit")
	}

	// An uncommitted event should not be a duplicate.
	if proc.IsDuplicate("evt-2") {
		t.Error("evt-2 should not be duplicate")
	}

	// Begin and rollback a transaction.
	txn2 := proc.Begin()
	if err := proc.RecordEvent(txn2, "evt-2"); err != nil {
		t.Fatalf("RecordEvent failed: %v", err)
	}
	if err := proc.Rollback(txn2); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// evt-2 should NOT be marked as processed after rollback.
	if proc.IsDuplicate("evt-2") {
		t.Error("rolled-back event should not be duplicate")
	}

	// Operations on a committed/rolled-back txn should fail.
	if err := proc.RecordEvent(txn1, "evt-3"); err == nil {
		t.Error("expected error recording event on committed txn")
	}
	if err := proc.Commit(txn2); err == nil {
		t.Error("expected error committing rolled-back txn")
	}
}

func TestExactlyOnceProcessor_Checkpoint(t *testing.T) {
	proc := NewExactlyOnceProcessor(100)

	// Commit some events.
	txn := proc.Begin()
	_ = proc.RecordEvent(txn, "evt-a")
	_ = proc.RecordEvent(txn, "evt-b")
	_ = proc.Commit(txn)

	// Create a checkpoint.
	cp := proc.CreateCheckpoint("query-1", 12345, []string{"w_0_100"})
	if cp.QueryID != "query-1" {
		t.Errorf("expected query ID 'query-1', got %s", cp.QueryID)
	}
	if cp.Watermark != 12345 {
		t.Errorf("expected watermark 12345, got %d", cp.Watermark)
	}
	if len(cp.WindowKeys) != 1 || cp.WindowKeys[0] != "w_0_100" {
		t.Errorf("unexpected window keys: %v", cp.WindowKeys)
	}
	if !cp.ProcessedIDs["evt-a"] || !cp.ProcessedIDs["evt-b"] {
		t.Error("checkpoint should contain processed event IDs")
	}

	// Create a fresh processor and restore from checkpoint.
	proc2 := NewExactlyOnceProcessor(100)
	if proc2.IsDuplicate("evt-a") {
		t.Error("fresh processor should not have evt-a")
	}

	proc2.RestoreCheckpoint(cp)
	if !proc2.IsDuplicate("evt-a") {
		t.Error("restored processor should recognize evt-a as duplicate")
	}
	if !proc2.IsDuplicate("evt-b") {
		t.Error("restored processor should recognize evt-b as duplicate")
	}
}

func TestNewStreamingSQLV2Engine(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	cfg := DefaultStreamingSQLV2Config()
	engine := NewStreamingSQLV2Engine(db, hub, cfg)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.joinEngine == nil {
		t.Error("expected joinEngine to be initialized")
	}
	if engine.processor == nil {
		t.Error("expected processor to be initialized")
	}
}

func TestStreamingSQLV2Engine_StartStop(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	cfg := DefaultStreamingSQLV2Config()
	engine := NewStreamingSQLV2Engine(db, hub, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Starting again should return an error.
	if err := engine.Start(); err == nil {
		t.Error("expected error on double start")
	}

	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Stop is idempotent.
	if err := engine.Stop(); err != nil {
		t.Errorf("second Stop should succeed, got: %v", err)
	}

	// A disabled engine should not start.
	cfg2 := DefaultStreamingSQLV2Config()
	cfg2.Enabled = false
	engine2 := NewStreamingSQLV2Engine(db, hub, cfg2)
	if err := engine2.Start(); err == nil {
		t.Error("expected error starting disabled engine")
	}
}

func TestStreamingSQLV2Engine_CreateWindowedQuery(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	cfg := DefaultStreamingSQLV2Config()
	engine := NewStreamingSQLV2Engine(db, hub, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine.Stop()

	q, err := engine.CreateWindowedQuery("SELECT AVG(value) FROM cpu WINDOW TUMBLING 5M")
	if err != nil {
		t.Fatalf("CreateWindowedQuery failed: %v", err)
	}
	if q.ID == "" {
		t.Error("expected non-empty query ID")
	}
	if q.SQL == "" {
		t.Error("expected non-empty SQL")
	}

	// EmitResults for the new query should return empty (no data yet).
	results := engine.EmitResults(q.ID)
	if len(results) != 0 {
		t.Errorf("expected 0 initial results, got %d", len(results))
	}

	// GetWatermark should succeed for an existing query.
	wm, ok := engine.GetWatermark(q.ID)
	if !ok {
		t.Error("expected GetWatermark to succeed for existing query")
	}
	if wm != 0 {
		t.Errorf("expected initial watermark 0, got %d", wm)
	}

	// GetWatermark for a non-existent query should return false.
	_, ok = engine.GetWatermark("nonexistent")
	if ok {
		t.Error("expected GetWatermark to fail for non-existent query")
	}

	// Respect MaxConcurrentQueries.
	cfg2 := DefaultStreamingSQLV2Config()
	cfg2.MaxConcurrentQueries = 1
	engine2 := NewStreamingSQLV2Engine(db, hub, cfg2)
	if err := engine2.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer engine2.Stop()

	_, err = engine2.CreateWindowedQuery("SELECT COUNT(*) FROM mem WINDOW TUMBLING 1M")
	if err != nil {
		t.Fatalf("first query should succeed: %v", err)
	}
	_, err = engine2.CreateWindowedQuery("SELECT SUM(value) FROM disk WINDOW TUMBLING 1M")
	if err == nil {
		t.Error("expected error when exceeding max concurrent queries")
	}
}
