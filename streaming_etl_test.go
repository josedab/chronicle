package chronicle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestETLPipeline_TransformFilter(t *testing.T) {
	inCh := make(chan *Point, 10)
	outCh := make(chan *Point, 10)

	cfg := DefaultETLPipelineConfig()
	cfg.Name = "test-transform-filter"

	pipeline := NewETLPipeline(cfg).
		From(NewETLChannelSource(inCh)).
		Transform(func(p *Point) (*Point, error) {
			p.Value *= 2
			return p, nil
		}).
		Filter(func(p *Point) bool {
			return p.Value > 5
		}).
		To(NewETLChannelSink(outCh))

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	inCh <- &Point{Metric: "cpu", Value: 1, Timestamp: time.Now().UnixNano()}
	inCh <- &Point{Metric: "cpu", Value: 10, Timestamp: time.Now().UnixNano()}

	time.Sleep(100 * time.Millisecond)

	if err := pipeline.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Only value=10 * 2 = 20 should pass (>5). value=1 * 2 = 2 should be filtered.
	if len(outCh) != 1 {
		t.Fatalf("expected 1 output point, got %d", len(outCh))
	}
	pt := <-outCh
	if pt.Value != 20 {
		t.Errorf("expected value=20, got %f", pt.Value)
	}
}

func TestETLPipeline_FluentAPI(t *testing.T) {
	inCh := make(chan *Point, 10)
	outCh := make(chan *Point, 10)

	p := NewETLPipeline(DefaultETLPipelineConfig()).
		From(NewETLChannelSource(inCh)).
		Transform(func(pt *Point) (*Point, error) { return pt, nil }).
		Filter(func(pt *Point) bool { return true }).
		Enrich(func(pt *Point) (*Point, error) {
			pt.Tags = map[string]string{"enriched": "true"}
			return pt, nil
		}).
		Route(func(pt *Point) string { return "default" }).
		To(NewETLChannelSink(outCh))

	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
}

func TestETLChannelSourceSink(t *testing.T) {
	ch := make(chan *Point, 5)
	source := NewETLChannelSource(ch)
	if err := source.Open(context.Background()); err != nil {
		t.Fatalf("Open source: %v", err)
	}

	sinkCh := make(chan *Point, 5)
	sink := NewETLChannelSink(sinkCh)
	if err := sink.Open(context.Background()); err != nil {
		t.Fatalf("Open sink: %v", err)
	}

	pt := &Point{Metric: "test", Value: 42, Timestamp: time.Now().UnixNano()}
	if err := sink.Write(context.Background(), pt); err != nil {
		t.Fatalf("Write: %v", err)
	}

	out := <-sinkCh
	if out.Value != 42 {
		t.Errorf("expected value=42, got %f", out.Value)
	}

	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close sink: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("Close source: %v", err)
	}
}

func TestETLMultiSink(t *testing.T) {
	ch1 := make(chan *Point, 5)
	ch2 := make(chan *Point, 5)

	multi := NewETLMultiSink(NewETLChannelSink(ch1), NewETLChannelSink(ch2))
	if err := multi.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	pt := &Point{Metric: "multi", Value: 7, Timestamp: time.Now().UnixNano()}
	if err := multi.Write(context.Background(), pt); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(ch1) != 1 {
		t.Errorf("expected 1 point in sink1, got %d", len(ch1))
	}
	if len(ch2) != 1 {
		t.Errorf("expected 1 point in sink2, got %d", len(ch2))
	}

	if err := multi.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := multi.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestETLAggregateStage(t *testing.T) {
	stage := newETLAggregateStage(ETLAggregateConfig{
		Window:   time.Minute,
		Function: AggSum,
		EmitMode: "onUpdate",
	})

	if stage.Name() != "aggregate" {
		t.Errorf("expected name 'aggregate', got %q", stage.Name())
	}

	now := time.Now().UnixNano()
	pt := &Point{Metric: "cpu", Value: 10, Timestamp: now}
	results, err := stage.Process(context.Background(), pt)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one result in onUpdate mode")
	}
	if results[0].Metric != "cpu_agg" {
		t.Errorf("expected metric 'cpu_agg', got %q", results[0].Metric)
	}
}

func TestETLRegistry_RegisterList(t *testing.T) {
	registry := NewETLRegistry()

	p1 := NewETLPipeline(ETLPipelineConfig{Name: "pipeline-1"})
	p2 := NewETLPipeline(ETLPipelineConfig{Name: "pipeline-2"})

	if err := registry.Register(p1); err != nil {
		t.Fatalf("Register p1: %v", err)
	}
	if err := registry.Register(p2); err != nil {
		t.Fatalf("Register p2: %v", err)
	}

	// Duplicate registration should fail.
	if err := registry.Register(p1); err == nil {
		t.Errorf("expected error for duplicate registration")
	}

	list := registry.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 pipelines, got %d", len(list))
	}

	if registry.Get("pipeline-1") == nil {
		t.Errorf("expected to find pipeline-1")
	}
	if registry.Get("nonexistent") != nil {
		t.Errorf("expected nil for nonexistent pipeline")
	}

	if err := registry.Unregister("pipeline-1"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if len(registry.List()) != 1 {
		t.Errorf("expected 1 pipeline after unregister")
	}
}

func TestETLCheckpoint_SaveRestore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	cp := &ETLCheckpoint{
		PipelineName:    "test-pipeline",
		LastProcessed:   12345,
		PointsProcessed: 100,
		SavedAt:         time.Now(),
	}

	if err := cp.save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	_, err := os.Stat(path)
	if err != nil {
		t.Fatalf("checkpoint file not found: %v", err)
	}

	loaded, err := loadETLCheckpoint(path)
	if err != nil {
		t.Fatalf("loadETLCheckpoint: %v", err)
	}
	if loaded.PipelineName != "test-pipeline" {
		t.Errorf("expected pipeline name 'test-pipeline', got %q", loaded.PipelineName)
	}
	if loaded.LastProcessed != 12345 {
		t.Errorf("expected LastProcessed=12345, got %d", loaded.LastProcessed)
	}
	if loaded.PointsProcessed != 100 {
		t.Errorf("expected PointsProcessed=100, got %d", loaded.PointsProcessed)
	}
}

func TestETLPipelineStats(t *testing.T) {
	inCh := make(chan *Point, 10)
	outCh := make(chan *Point, 10)

	cfg := DefaultETLPipelineConfig()
	cfg.Name = "stats-test"

	pipeline := NewETLPipeline(cfg).
		From(NewETLChannelSource(inCh)).
		To(NewETLChannelSink(outCh))

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := 0; i < 5; i++ {
		inCh <- &Point{Metric: "m", Value: float64(i), Timestamp: time.Now().UnixNano()}
	}

	time.Sleep(100 * time.Millisecond)

	stats := pipeline.Stats()
	if stats.PointsRead == 0 {
		t.Errorf("expected PointsRead > 0")
	}
	if stats.Uptime <= 0 {
		t.Errorf("expected positive Uptime")
	}

	if err := pipeline.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
