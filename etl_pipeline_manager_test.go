package chronicle

import (
	"testing"
	"time"
)

func TestETLPipelineManagerLifecycle(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	pm := NewETLPipelineManager(db, DefaultETLPipelineManagerConfig())
	if err := pm.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer pm.Stop()

	// Create pipeline
	pipeline := NewETLPipeline(DefaultETLPipelineConfig())
	err := pm.CreatePipeline("test-pipeline", pipeline, map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Duplicate should fail
	err = pm.CreatePipeline("test-pipeline", pipeline, nil)
	if err == nil {
		t.Fatal("expected error for duplicate pipeline")
	}

	// List pipelines
	list := pm.ListPipelines()
	if len(list) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(list))
	}
	if list[0].Name != "test-pipeline" {
		t.Errorf("expected name 'test-pipeline', got %q", list[0].Name)
	}
	if list[0].State != PipelineStateCreated {
		t.Errorf("expected state Created, got %v", list[0].State)
	}

	// Get pipeline
	mp, err := pm.GetPipeline("test-pipeline")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if mp.Labels["env"] != "test" {
		t.Error("labels not preserved")
	}

	// Get nonexistent
	_, err = pm.GetPipeline("nope")
	if err == nil {
		t.Fatal("expected error for nonexistent pipeline")
	}

	// Stats
	stats := pm.Stats()
	if stats.TotalPipelines != 1 {
		t.Errorf("expected 1 total, got %d", stats.TotalPipelines)
	}
	if stats.TotalCreated != 1 {
		t.Errorf("expected 1 created, got %d", stats.TotalCreated)
	}
}

func TestETLPipelineManagerStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	pm := NewETLPipelineManager(db, DefaultETLPipelineManagerConfig())

	// Create with nil pipeline (no-op start/stop)
	err := pm.CreatePipeline("null-pipe", nil, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Stop non-running should fail
	err = pm.StopPipeline("null-pipe")
	if err == nil {
		t.Fatal("expected error stopping non-running pipeline")
	}

	// Start pipeline
	err = pm.StartPipeline("null-pipe")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Double start should fail
	err = pm.StartPipeline("null-pipe")
	if err == nil {
		t.Fatal("expected error for double start")
	}

	stats := pm.Stats()
	if stats.Running != 1 {
		t.Errorf("expected 1 running, got %d", stats.Running)
	}

	// Stop pipeline
	err = pm.StopPipeline("null-pipe")
	if err != nil {
		t.Fatalf("stop: %v", err)
	}

	stats = pm.Stats()
	if stats.Stopped != 1 {
		t.Errorf("expected 1 stopped, got %d", stats.Stopped)
	}
}

func TestETLPipelineManagerDelete(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	pm := NewETLPipelineManager(db, DefaultETLPipelineManagerConfig())

	pm.CreatePipeline("to-delete", nil, nil)

	// Delete stopped pipeline
	err := pm.DeletePipeline("to-delete")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify deleted
	list := pm.ListPipelines()
	if len(list) != 0 {
		t.Errorf("expected 0 pipelines, got %d", len(list))
	}

	// Delete nonexistent
	err = pm.DeletePipeline("nope")
	if err == nil {
		t.Fatal("expected error deleting nonexistent pipeline")
	}
}

func TestETLPipelineManagerMaxPipelines(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultETLPipelineManagerConfig()
	config.MaxPipelines = 2
	pm := NewETLPipelineManager(db, config)

	pm.CreatePipeline("p1", nil, nil)
	pm.CreatePipeline("p2", nil, nil)

	err := pm.CreatePipeline("p3", nil, nil)
	if err == nil {
		t.Fatal("expected error for exceeding max pipelines")
	}
}

func TestWindowedJoinInner(t *testing.T) {
	config := WindowedJoinConfig{
		LeftMetric:   "cpu",
		RightMetric:  "memory",
		WindowSize:   time.Second,
		OutputMetric: "cpu_memory",
		JoinType:     WindowedJoinInner,
	}
	wj := NewWindowedJoin(config)

	now := time.Now().UnixNano()

	// Add matching points (within window)
	wj.AddLeft(&Point{Metric: "cpu", Value: 80, Timestamp: now, Tags: map[string]string{"host": "a"}})
	wj.AddRight(&Point{Metric: "memory", Value: 4096, Timestamp: now + int64(500*time.Millisecond), Tags: map[string]string{"region": "us"}})

	// Add non-matching point (outside window)
	wj.AddLeft(&Point{Metric: "cpu", Value: 90, Timestamp: now + int64(5*time.Second)})

	results := wj.Emit()
	if len(results) != 1 {
		t.Fatalf("expected 1 joined point, got %d", len(results))
	}

	joined := results[0]
	if joined.Metric != "cpu_memory" {
		t.Errorf("expected metric 'cpu_memory', got %q", joined.Metric)
	}
	if joined.Value != 80 {
		t.Errorf("expected value 80, got %f", joined.Value)
	}
	if joined.Tags["host"] != "a" {
		t.Error("left tags not preserved")
	}
	if joined.Tags["right_region"] != "us" {
		t.Error("right tags not merged")
	}

	stats := wj.Stats()
	if stats.Processed != 3 {
		t.Errorf("expected 3 processed, got %d", stats.Processed)
	}
	if stats.Emitted != 1 {
		t.Errorf("expected 1 emitted, got %d", stats.Emitted)
	}
}

func TestWindowedJoinLeftOuter(t *testing.T) {
	wj := NewWindowedJoin(WindowedJoinConfig{
		LeftMetric:   "cpu",
		RightMetric:  "mem",
		WindowSize:   time.Second,
		OutputMetric: "result",
		JoinType:     WindowedJoinLeftOuter,
	})

	now := time.Now().UnixNano()
	wj.AddLeft(&Point{Metric: "cpu", Value: 50, Timestamp: now})
	wj.AddLeft(&Point{Metric: "cpu", Value: 60, Timestamp: now + int64(10*time.Second)})
	wj.AddRight(&Point{Metric: "mem", Value: 1024, Timestamp: now + int64(100*time.Millisecond)})

	results := wj.Emit()
	// Should have: 1 inner match + 1 unmatched left = 2
	if len(results) != 2 {
		t.Fatalf("expected 2 results for left outer, got %d", len(results))
	}
}

func TestWindowedJoinFullOuter(t *testing.T) {
	wj := NewWindowedJoin(WindowedJoinConfig{
		LeftMetric:  "a",
		RightMetric: "b",
		WindowSize:  time.Millisecond,
		JoinType:    WindowedJoinFullOuter,
	})

	now := time.Now().UnixNano()
	wj.AddLeft(&Point{Metric: "a", Value: 1, Timestamp: now})
	wj.AddRight(&Point{Metric: "b", Value: 2, Timestamp: now + int64(time.Hour)})

	results := wj.Emit()
	// No matches within 1ms window, so full outer gives both
	if len(results) != 2 {
		t.Fatalf("expected 2 results for full outer (no matches), got %d", len(results))
	}
}

func TestEnrichmentLookup(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write lookup data
	baseTime := time.Now().Truncate(time.Hour)
	db.Write(Point{Metric: "reference_metric", Value: 42.5, Timestamp: baseTime.UnixNano(), Tags: map[string]string{"type": "lookup"}})
	db.Flush()

	config := DefaultEnrichmentLookupConfig()
	config.LookupMetric = "reference_metric"
	config.ValueTag = "ref_value"
	config.CacheTTL = time.Minute
	el := NewEnrichmentLookup(db, config)

	// Enrich a point
	p := &Point{Metric: "sensor_data", Value: 100, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"host": "x"}}
	enriched, err := el.Enrich(p)
	if err != nil {
		t.Fatalf("enrich: %v", err)
	}

	if enriched.Tags["ref_value"] == "" {
		t.Error("expected enrichment tag to be set")
	}
	if enriched.Tags["host"] != "x" {
		t.Error("original tags should be preserved")
	}

	// Second call should hit cache
	_, err = el.Enrich(p)
	if err != nil {
		t.Fatalf("enrich2: %v", err)
	}

	stats := el.Stats()
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.Hits != 1 {
		t.Errorf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.CacheSize != 1 {
		t.Errorf("expected cache size 1, got %d", stats.CacheSize)
	}

	// Nil point
	_, err = el.Enrich(nil)
	if err == nil {
		t.Error("expected error for nil point")
	}
}

func TestEnrichmentLookupCacheEviction(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultEnrichmentLookupConfig()
	config.MaxCacheSize = 3
	el := NewEnrichmentLookup(db, config)

	// Fill cache beyond max
	for i := 0; i < 5; i++ {
		p := &Point{Metric: "m", Value: float64(i), Timestamp: time.Now().UnixNano(), Tags: map[string]string{"key": "unique"}}
		el.Enrich(p)
	}

	stats := el.Stats()
	if stats.CacheSize > config.MaxCacheSize {
		t.Errorf("cache size %d exceeds max %d", stats.CacheSize, config.MaxCacheSize)
	}
}

func TestPipelineSpecParsing(t *testing.T) {
	yamlData := `
name: test-pipeline
version: "1.0"
labels:
  env: production
  team: data
source:
  type: chronicle
  metric: cpu.usage
  interval: 5s
transforms:
  - type: filter
    config:
      metric: cpu.usage
  - type: scale
    config:
      factor: "0.01"
  - type: tag
    config:
      key: processed
      value: "true"
sinks:
  - type: chronicle
settings:
  workers: 4
  buffer_size: 1024
  backpressure: drop
`
	spec, err := ParsePipelineSpec([]byte(yamlData))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if spec.Name != "test-pipeline" {
		t.Errorf("expected name 'test-pipeline', got %q", spec.Name)
	}
	if spec.Version != "1.0" {
		t.Errorf("expected version '1.0', got %q", spec.Version)
	}
	if spec.Source.Type != "chronicle" {
		t.Errorf("expected source type 'chronicle', got %q", spec.Source.Type)
	}
	if spec.Source.Metric != "cpu.usage" {
		t.Errorf("expected metric 'cpu.usage', got %q", spec.Source.Metric)
	}
	if len(spec.Transforms) != 3 {
		t.Errorf("expected 3 transforms, got %d", len(spec.Transforms))
	}
	if len(spec.Sinks) != 1 {
		t.Errorf("expected 1 sink, got %d", len(spec.Sinks))
	}
	if spec.Settings.Workers != 4 {
		t.Errorf("expected 4 workers, got %d", spec.Settings.Workers)
	}
}

func TestPipelineSpecValidation(t *testing.T) {
	tests := []struct {
		name  string
		spec  PipelineSpec
		valid bool
	}{
		{
			name:  "valid",
			spec:  PipelineSpec{Name: "ok", Source: SourceSpec{Type: "chronicle"}, Sinks: []SinkSpec{{Type: "chronicle"}}},
			valid: true,
		},
		{
			name:  "missing name",
			spec:  PipelineSpec{Source: SourceSpec{Type: "chronicle"}, Sinks: []SinkSpec{{Type: "chronicle"}}},
			valid: false,
		},
		{
			name:  "missing source",
			spec:  PipelineSpec{Name: "ok", Sinks: []SinkSpec{{Type: "chronicle"}}},
			valid: false,
		},
		{
			name:  "missing sinks",
			spec:  PipelineSpec{Name: "ok", Source: SourceSpec{Type: "chronicle"}},
			valid: false,
		},
		{
			name:  "empty sink type",
			spec:  PipelineSpec{Name: "ok", Source: SourceSpec{Type: "chronicle"}, Sinks: []SinkSpec{{}}},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.Validate()
			if tt.valid && err != nil {
				t.Errorf("expected valid, got error: %v", err)
			}
			if !tt.valid && err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestPipelineSpecBuild(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	spec := &PipelineSpec{
		Name:     "build-test",
		Source:   SourceSpec{Type: "chronicle", Metric: "test", Interval: "1s"},
		Sinks:    []SinkSpec{{Type: "chronicle"}},
		Settings: SettingsSpec{Workers: 2, BufferSize: 512, Backpressure: "drop"},
	}

	pipeline, err := spec.BuildPipeline(db)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if pipeline == nil {
		t.Fatal("expected non-nil pipeline")
	}
}

func TestPipelineSpecBuildInvalidSource(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	spec := &PipelineSpec{
		Name:   "bad-source",
		Source: SourceSpec{Type: "kafka"},
		Sinks:  []SinkSpec{{Type: "chronicle"}},
	}

	_, err := spec.BuildPipeline(db)
	if err == nil {
		t.Fatal("expected error for unsupported source")
	}
}

func TestPipelineSpecToYAML(t *testing.T) {
	spec := &PipelineSpec{
		Name:   "roundtrip",
		Source: SourceSpec{Type: "chronicle", Metric: "test"},
		Sinks:  []SinkSpec{{Type: "chronicle"}},
	}

	data, err := spec.ToYAML()
	if err != nil {
		t.Fatalf("to yaml: %v", err)
	}

	parsed, err := ParsePipelineSpec(data)
	if err != nil {
		t.Fatalf("roundtrip parse: %v", err)
	}
	if parsed.Name != spec.Name {
		t.Errorf("roundtrip name mismatch: %q vs %q", parsed.Name, spec.Name)
	}
}

func TestETLDatabaseSinkWrite(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	sink := NewETLDatabaseSink(db)
	if err := sink.Open(nil); err != nil {
		t.Fatalf("open: %v", err)
	}

	now := time.Now().Truncate(time.Hour).UnixNano()
	err := sink.Write(nil, &Point{Metric: "sink_test", Value: 42, Timestamp: now, Tags: map[string]string{"a": "b"}})
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := sink.Flush(nil); err != nil {
		t.Fatalf("flush: %v", err)
	}

	db.Flush()

	// Verify data was written
	result, err := db.Execute(&Query{Metric: "sink_test"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if result == nil || len(result.Points) == 0 {
		t.Error("expected data to be written")
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestETLPipelineManagerDoubleStart(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	pm := NewETLPipelineManager(db, DefaultETLPipelineManagerConfig())
	pm.Start()
	defer pm.Stop()

	err := pm.Start()
	if err == nil {
		t.Fatal("expected error for double start")
	}
}

func TestETLPipelineManagerDeleteRunning(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	pm := NewETLPipelineManager(db, DefaultETLPipelineManagerConfig())
	pm.CreatePipeline("running-pipe", nil, nil)
	pm.StartPipeline("running-pipe")

	err := pm.DeletePipeline("running-pipe")
	if err == nil {
		t.Fatal("expected error deleting running pipeline")
	}
}
