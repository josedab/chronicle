package chronicle

import (
	"os"
	"testing"
	"time"
)

func openStreamTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	path := "test_stream_proc_" + time.Now().Format("20060102150405.000000000") + ".db"
	db, err := Open(path, Config{
		BufferSize:        100,
		PartitionDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return db, func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(path + ".wal")
	}
}

func TestDefaultStreamProcessingConfig(t *testing.T) {
	cfg := DefaultStreamProcessingConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.MaxPipelines <= 0 {
		t.Error("expected MaxPipelines > 0")
	}
	if cfg.CheckpointInterval <= 0 {
		t.Error("expected CheckpointInterval > 0")
	}
	if cfg.StateBackendType == "" {
		t.Error("expected StateBackendType to be set")
	}
	if cfg.MaxBufferSize <= 0 {
		t.Error("expected MaxBufferSize > 0")
	}
	if cfg.WorkerCount <= 0 {
		t.Error("expected WorkerCount > 0")
	}
}

func TestStreamPipelineLifecycle(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	// Create pipeline
	p, err := engine.CreatePipeline("test-pipeline", "cpu", "cpu_avg", &WindowSpec{
		Type: StreamTumblingWindow,
		Size: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}
	if p.ID == "" {
		t.Error("expected pipeline ID")
	}
	if p.Name != "test-pipeline" {
		t.Errorf("expected name 'test-pipeline', got %q", p.Name)
	}
	if p.State != PipelineCreated {
		t.Errorf("expected state Created, got %v", p.State)
	}

	// Start pipeline
	if err := engine.StartPipeline(p.ID); err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}
	got := engine.GetPipeline(p.ID)
	if got.State != PipelineRunning {
		t.Errorf("expected state Running, got %v", got.State)
	}

	// Pause pipeline
	if err := engine.PausePipeline(p.ID); err != nil {
		t.Fatalf("PausePipeline failed: %v", err)
	}
	got = engine.GetPipeline(p.ID)
	if got.State != PipelinePaused {
		t.Errorf("expected state Paused, got %v", got.State)
	}

	// Stop pipeline
	if err := engine.StopPipeline(p.ID); err != nil {
		t.Fatalf("StopPipeline failed: %v", err)
	}
	got = engine.GetPipeline(p.ID)
	if got.State != PipelineStopped {
		t.Errorf("expected state Stopped, got %v", got.State)
	}

	// Delete pipeline
	if err := engine.DeletePipeline(p.ID); err != nil {
		t.Fatalf("DeletePipeline failed: %v", err)
	}
	if engine.GetPipeline(p.ID) != nil {
		t.Error("expected pipeline to be deleted")
	}
}

func TestStreamPipelineListAndStats(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	// Create two pipelines
	p1, err := engine.CreatePipeline("pipe1", "cpu", "cpu_out", nil)
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}
	_, err = engine.CreatePipeline("pipe2", "mem", "mem_out", nil)
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}

	list := engine.ListPipelines()
	if len(list) != 2 {
		t.Errorf("expected 2 pipelines, got %d", len(list))
	}

	// Start one pipeline and check stats
	_ = engine.StartPipeline(p1.ID)

	stats := engine.Stats()
	if stats.TotalPipelines != 2 {
		t.Errorf("expected TotalPipelines=2, got %d", stats.TotalPipelines)
	}
	if stats.ActivePipelines != 1 {
		t.Errorf("expected ActivePipelines=1, got %d", stats.ActivePipelines)
	}
}

func TestStreamTumblingWindowAggregation(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	windowSize := 10 * time.Second
	p, err := engine.CreatePipeline("tumbling-test", "cpu", "cpu_avg", &WindowSpec{
		Type: StreamTumblingWindow,
		Size: windowSize,
	})
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}
	if err := engine.StartPipeline(p.ID); err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	baseTime := int64(1000000000)

	// Send events within the window
	for i := 0; i < 5; i++ {
		_, err := engine.ProcessEvent(p.ID, &StreamEvent{
			Key:       "host1",
			Value:     float64(i + 1),
			Timestamp: baseTime + int64(i)*int64(time.Second),
			Metric:    "cpu",
		})
		if err != nil {
			t.Fatalf("ProcessEvent failed: %v", err)
		}
	}

	// Send an event that triggers window close
	results, err := engine.ProcessEvent(p.ID, &StreamEvent{
		Key:       "host1",
		Value:     100,
		Timestamp: baseTime + windowSize.Nanoseconds() + int64(time.Second),
		Metric:    "cpu",
	})
	if err != nil {
		t.Fatalf("ProcessEvent failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected window to emit results")
	}

	// The emitted event should be the average of values 1..5 = 3.0 (and the trigger event 100)
	result := results[0]
	if result.Metric != "cpu_avg" {
		t.Errorf("expected metric 'cpu_avg', got %q", result.Metric)
	}
	if result.Key != "host1" {
		t.Errorf("expected key 'host1', got %q", result.Key)
	}

	// Check stats were updated
	stats := engine.Stats()
	if stats.TotalEventsIn != 6 {
		t.Errorf("expected TotalEventsIn=6, got %d", stats.TotalEventsIn)
	}
}

func TestStreamFilterOperator(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	p, err := engine.CreatePipeline("filter-test", "cpu", "cpu_filtered", nil)
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}

	// Add filter: only events with value > 50
	filter := NewSPFilterOperator("high-value", func(e *StreamEvent) bool {
		return e.Value > 50
	})
	if err := engine.AddOperator(p.ID, filter); err != nil {
		t.Fatalf("AddOperator failed: %v", err)
	}
	if err := engine.StartPipeline(p.ID); err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	// Low value: should be filtered out
	results, err := engine.ProcessEvent(p.ID, &StreamEvent{
		Key:       "host1",
		Value:     10,
		Timestamp: time.Now().UnixNano(),
		Metric:    "cpu",
	})
	if err != nil {
		t.Fatalf("ProcessEvent failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for filtered event, got %d", len(results))
	}

	// High value: should pass
	results, err = engine.ProcessEvent(p.ID, &StreamEvent{
		Key:       "host1",
		Value:     75,
		Timestamp: time.Now().UnixNano(),
		Metric:    "cpu",
	})
	if err != nil {
		t.Fatalf("ProcessEvent failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for passing event, got %d", len(results))
	}
}

func TestStreamMapOperator(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	p, err := engine.CreatePipeline("map-test", "temp_f", "temp_c", nil)
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}

	// Add map: Fahrenheit to Celsius
	mapper := NewMapOperator("f-to-c", func(e *StreamEvent) (*StreamEvent, error) {
		return &StreamEvent{
			Key:       e.Key,
			Value:     (e.Value - 32) * 5 / 9,
			Timestamp: e.Timestamp,
			Metric:    e.Metric,
			Tags:      e.Tags,
			Watermark: e.Watermark,
		}, nil
	})
	if err := engine.AddOperator(p.ID, mapper); err != nil {
		t.Fatalf("AddOperator failed: %v", err)
	}
	if err := engine.StartPipeline(p.ID); err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	results, err := engine.ProcessEvent(p.ID, &StreamEvent{
		Key:       "sensor1",
		Value:     212, // boiling point in F
		Timestamp: time.Now().UnixNano(),
		Metric:    "temp_f",
	})
	if err != nil {
		t.Fatalf("ProcessEvent failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// 212°F = 100°C
	if results[0].Value != 100 {
		t.Errorf("expected 100°C, got %f", results[0].Value)
	}
}

func TestStreamProcessEventTracksStats(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	p, err := engine.CreatePipeline("stats-test", "cpu", "cpu_out", nil)
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}
	if err := engine.StartPipeline(p.ID); err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, err := engine.ProcessEvent(p.ID, &StreamEvent{
			Key:       "host1",
			Value:     float64(i),
			Timestamp: time.Now().UnixNano(),
			Metric:    "cpu",
		})
		if err != nil {
			t.Fatalf("ProcessEvent failed: %v", err)
		}
	}

	stats := engine.Stats()
	if stats.TotalEventsIn != 10 {
		t.Errorf("expected TotalEventsIn=10, got %d", stats.TotalEventsIn)
	}
	if stats.TotalEventsOut != 10 {
		t.Errorf("expected TotalEventsOut=10, got %d", stats.TotalEventsOut)
	}

	// Check pipeline-level stats
	pStats := engine.GetPipeline(p.ID)
	pStats.mu.Lock()
	evIn := pStats.Stats.EventsIn
	pStats.mu.Unlock()
	if evIn != 10 {
		t.Errorf("expected pipeline EventsIn=10, got %d", evIn)
	}
}

func TestStreamPipelineValidation(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	// Empty name
	_, err := engine.CreatePipeline("", "cpu", "out", nil)
	if err == nil {
		t.Error("expected error for empty name")
	}

	// Empty source
	_, err = engine.CreatePipeline("test", "", "out", nil)
	if err == nil {
		t.Error("expected error for empty source")
	}

	// Empty sink
	_, err = engine.CreatePipeline("test", "cpu", "", nil)
	if err == nil {
		t.Error("expected error for empty sink")
	}

	// Process on non-existent pipeline
	_, err = engine.ProcessEvent("nonexistent", &StreamEvent{})
	if err == nil {
		t.Error("expected error for non-existent pipeline")
	}

	// Cannot add operator to running pipeline
	p, _ := engine.CreatePipeline("test", "cpu", "out", nil)
	_ = engine.StartPipeline(p.ID)
	err = engine.AddOperator(p.ID, NewSPFilterOperator("f", func(*StreamEvent) bool { return true }))
	if err == nil {
		t.Error("expected error adding operator to running pipeline")
	}

	// Cannot delete running pipeline
	err = engine.DeletePipeline(p.ID)
	if err == nil {
		t.Error("expected error deleting running pipeline")
	}
}

func TestStreamCountWindow(t *testing.T) {
	db, cleanup := openStreamTestDB(t)
	defer cleanup()

	engine := NewStreamProcessingEngine(db, DefaultStreamProcessingConfig())

	p, err := engine.CreatePipeline("count-window-test", "cpu", "cpu_avg", &WindowSpec{
		Type:  StreamCountWindow,
		Count: 3,
	})
	if err != nil {
		t.Fatalf("CreatePipeline failed: %v", err)
	}
	if err := engine.StartPipeline(p.ID); err != nil {
		t.Fatalf("StartPipeline failed: %v", err)
	}

	baseTime := time.Now().UnixNano()
	var allResults []*StreamEvent

	// Send 3 events to fill the count window
	for i := 0; i < 4; i++ {
		results, err := engine.ProcessEvent(p.ID, &StreamEvent{
			Key:       "host1",
			Value:     float64(10 * (i + 1)),
			Timestamp: baseTime + int64(i)*int64(time.Second),
			Metric:    "cpu",
		})
		if err != nil {
			t.Fatalf("ProcessEvent failed: %v", err)
		}
		allResults = append(allResults, results...)
	}

	// Should have emitted after the 3rd event
	if len(allResults) == 0 {
		t.Fatal("expected count window to emit results after reaching count threshold")
	}
}
