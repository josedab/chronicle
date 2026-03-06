package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestOTelCollectorDistroConfig(t *testing.T) {
	cfg := DefaultOTelCollectorDistroConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.ServiceName == "" {
		t.Error("expected service name")
	}
	if len(cfg.Receivers) == 0 {
		t.Error("expected receivers")
	}
	if len(cfg.Processors) == 0 {
		t.Error("expected processors")
	}
	if len(cfg.Exporters) == 0 {
		t.Error("expected exporters")
	}
	if len(cfg.Pipelines) == 0 {
		t.Error("expected pipelines")
	}
}

func TestOTelCollectorDistro(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultOTelCollectorDistroConfig()
	cd := NewOTelCollectorDistro(db, cfg)

	t.Run("start and stop", func(t *testing.T) {
		if err := cd.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		if !cd.running.Load() {
			t.Error("expected running")
		}
		if err := cd.Stop(); err != nil {
			t.Fatalf("stop failed: %v", err)
		}
	})

	t.Run("double start", func(t *testing.T) {
		if err := cd.Start(); err != nil {
			t.Fatal(err)
		}
		defer cd.Stop()
		if err := cd.Start(); err == nil {
			t.Error("expected error on double start")
		}
	})

	t.Run("ingest through pipeline", func(t *testing.T) {
		cd2 := NewOTelCollectorDistro(db, cfg)
		if err := cd2.Start(); err != nil {
			t.Fatal(err)
		}
		defer cd2.Stop()

		points := []Point{
			{Metric: "otel_test", Value: 42.0, Timestamp: time.Now().UnixNano()},
			{Metric: "otel_test", Value: 43.0, Timestamp: time.Now().UnixNano()},
		}

		err := cd2.Ingest(context.Background(), "metrics/default", points)
		if err != nil {
			t.Fatalf("ingest failed: %v", err)
		}

		stats := cd2.GetStats()
		if stats["total_received"].(uint64) != 2 {
			t.Errorf("expected 2 received, got %v", stats["total_received"])
		}
	})

	t.Run("ingest unknown pipeline", func(t *testing.T) {
		cd3 := NewOTelCollectorDistro(db, cfg)
		cd3.Start()
		defer cd3.Stop()

		err := cd3.Ingest(context.Background(), "nonexistent", []Point{{Metric: "x", Value: 1}})
		if err == nil {
			t.Error("expected error for unknown pipeline")
		}
	})

	t.Run("ingest not running", func(t *testing.T) {
		cd4 := NewOTelCollectorDistro(db, cfg)
		err := cd4.Ingest(context.Background(), "metrics/default", []Point{{Metric: "x", Value: 1}})
		if err == nil {
			t.Error("expected error when not running")
		}
	})

	t.Run("get config", func(t *testing.T) {
		cfg := cd.GetConfig()
		if cfg["receivers"] == nil {
			t.Error("expected receivers in config")
		}
		if cfg["exporters"] == nil {
			t.Error("expected exporters in config")
		}
	})

	t.Run("generate dockerfile", func(t *testing.T) {
		df := cd.GenerateDockerfile()
		if df == "" {
			t.Error("expected non-empty dockerfile")
		}
	})

	t.Run("generate helm values", func(t *testing.T) {
		hv := cd.GenerateHelmValues()
		if hv["replicaCount"] == nil {
			t.Error("expected replicaCount in helm values")
		}
	})

	t.Run("generate k8s manifest", func(t *testing.T) {
		m := cd.GenerateK8sDaemonSetManifest()
		if m == "" {
			t.Error("expected non-empty manifest")
		}
	})
}

func TestFilterProcessor(t *testing.T) {
	fp := &filterProcessor{
		name:     "test-filter",
		excludes: []string{"internal_"},
	}

	points := []Point{
		{Metric: "cpu_usage"},
		{Metric: "internal_counter"},
		{Metric: "memory_usage"},
	}

	result, err := fp.Process(points)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 points after filter, got %d", len(result))
	}
}

func TestTransformProcessor(t *testing.T) {
	tp := &transformProcessor{
		name:   "test-transform",
		prefix: "chronicle.",
	}

	points := []Point{
		{Metric: "cpu"},
		{Metric: "mem"},
	}

	result, err := tp.Process(points)
	if err != nil {
		t.Fatal(err)
	}
	if result[0].Metric != "chronicle.cpu" {
		t.Errorf("expected chronicle.cpu, got %s", result[0].Metric)
	}
}

func TestCollectorDistroDisabled(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultOTelCollectorDistroConfig()
	cfg.Enabled = false
	cd := NewOTelCollectorDistro(db, cfg)
	if err := cd.Start(); err != nil {
		t.Errorf("disabled collector should start without error: %v", err)
	}
}

func TestCollectorDistro_ValidatePipelines(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultOTelCollectorDistroConfig()
	cd := NewOTelCollectorDistro(db, cfg)

	// Default config should be valid
	errs := cd.ValidatePipelines()
	if len(errs) > 0 {
		t.Errorf("default config should be valid, got errors: %v", errs)
	}

	// Add a pipeline referencing a non-existent receiver
	cd.AddCollectorPipeline(CollectorPipeline{
		Name:       "bad/pipeline",
		SignalType: "metrics",
		Receivers:  []string{"nonexistent_receiver"},
		Exporters:  []string{"chronicle"},
		Enabled:    true,
	})
	errs = cd.ValidatePipelines()
	if len(errs) == 0 {
		t.Error("expected validation errors for bad pipeline")
	}

	// Add pipeline with invalid signal type
	cd.AddCollectorPipeline(CollectorPipeline{
		Name:       "invalid/signal",
		SignalType: "unknown",
		Receivers:  []string{"otlp"},
		Exporters:  []string{"chronicle"},
		Enabled:    true,
	})
	errs = cd.ValidatePipelines()
	found := false
	for _, e := range errs {
		if len(e) > 0 {
			found = true
		}
	}
	if !found {
		t.Error("expected validation error for invalid signal type")
	}
}

func TestCollectorDistro_E2E_DataFlow(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultOTelCollectorDistroConfig()
	cd := NewOTelCollectorDistro(db, cfg)

	if err := cd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cd.Stop()

	// Ingest data through the pipeline
	now := time.Now().UnixNano()
	points := make([]Point, 100)
	for i := 0; i < 100; i++ {
		points[i] = Point{
			Metric:    "e2e_test_metric",
			Value:     float64(i),
			Tags:      map[string]string{"env": "test", "host": "localhost"},
			Timestamp: now + int64(i*1000),
		}
	}

	err := cd.Ingest(context.Background(), "metrics/default", points)
	if err != nil {
		t.Fatalf("E2E ingest failed: %v", err)
	}

	stats := cd.GetStats()
	received := stats["total_received"].(uint64)
	exported := stats["total_exported"].(uint64)
	if received != 100 {
		t.Errorf("expected 100 received, got %d", received)
	}
	if exported != 100 {
		t.Errorf("expected 100 exported, got %d", exported)
	}

	// Verify data actually reached the DB
	db.Flush()
	result, err := db.Execute(&Query{Metric: "e2e_test_metric"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Points) == 0 {
		t.Error("expected data to reach DB after E2E ingest")
	}
}

func TestCollectorDistro_PipelineManagement(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultOTelCollectorDistroConfig()
	cd := NewOTelCollectorDistro(db, cfg)

	initialCount := len(cd.config.Pipelines)

	cd.AddCollectorPipeline(CollectorPipeline{
		Name: "custom/metrics", SignalType: "metrics",
		Receivers: []string{"otlp"}, Exporters: []string{"chronicle"},
		Enabled: true,
	})
	if len(cd.config.Pipelines) != initialCount+1 {
		t.Error("pipeline not added")
	}

	removed := cd.RemoveCollectorPipeline("custom/metrics")
	if !removed {
		t.Error("pipeline not removed")
	}
	if len(cd.config.Pipelines) != initialCount {
		t.Error("pipeline count mismatch after removal")
	}

	removed = cd.RemoveCollectorPipeline("nonexistent")
	if removed {
		t.Error("should not remove nonexistent pipeline")
	}
}

func TestCollectorDistro_GenerateHelmChart(t *testing.T) {
	db := setupTestDB(t)
	cd := NewOTelCollectorDistro(db, DefaultOTelCollectorDistroConfig())

	chart := cd.GenerateHelmChart()
	if chart["Chart.yaml"] == "" {
		t.Error("expected Chart.yaml")
	}
	if chart["templates/configmap.yaml"] == "" {
		t.Error("expected templates/configmap.yaml")
	}
	if chart["templates/service.yaml"] == "" {
		t.Error("expected templates/service.yaml")
	}
}

func TestCollectorDistro_GenerateDeployment(t *testing.T) {
	db := setupTestDB(t)
	cd := NewOTelCollectorDistro(db, DefaultOTelCollectorDistroConfig())

	manifest := cd.GenerateK8sDeploymentManifest()
	if manifest == "" {
		t.Error("expected non-empty deployment manifest")
	}
	if !otelContainsStr(manifest, "replicas: 2") {
		t.Error("expected replicas in deployment manifest")
	}
}

func otelContainsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestCollectorDistro_StatsD_Config(t *testing.T) {
	cfg := DefaultOTelCollectorDistroConfig()

	// Verify StatsD receiver is in the config
	found := false
	for _, r := range cfg.Receivers {
		if r.Type == OTelReceiverStatsD {
			found = true
			if r.Endpoint != "0.0.0.0:8125" {
				t.Errorf("expected StatsD endpoint 0.0.0.0:8125, got %s", r.Endpoint)
			}
		}
	}
	if !found {
		t.Error("StatsD receiver not found in default config")
	}
}

func TestParseStatsDLine(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		wantName   string
		wantValue  float64
		wantType   string
		wantTags   map[string]string
		wantErr    bool
	}{
		{
			name: "simple_counter",
			line: "page.views:1|c",
			wantName: "page.views", wantValue: 1, wantType: "c",
		},
		{
			name: "gauge",
			line: "cpu.usage:72.5|g",
			wantName: "cpu.usage", wantValue: 72.5, wantType: "g",
		},
		{
			name: "timer",
			line: "request.latency:320|ms",
			wantName: "request.latency", wantValue: 320, wantType: "ms",
		},
		{
			name: "counter_with_sample_rate",
			line: "page.views:1|c|@0.5",
			wantName: "page.views", wantValue: 2, wantType: "c", // 1/0.5 = 2
		},
		{
			name: "gauge_with_tags",
			line: "disk.free:54321|g|#host:web01,region:us-east",
			wantName: "disk.free", wantValue: 54321, wantType: "g",
			wantTags: map[string]string{"host": "web01", "region": "us-east"},
		},
		{
			name: "counter_with_tags_and_sample",
			line: "http.requests:5|c|@0.1|#method:GET,status:200",
			wantName: "http.requests", wantValue: 50, wantType: "c", // 5/0.1 = 50
			wantTags: map[string]string{"method": "GET", "status": "200"},
		},
		{
			name: "negative_gauge",
			line: "temperature:-10|g",
			wantName: "temperature", wantValue: -10, wantType: "g",
		},
		{name: "empty_line", line: "", wantErr: true},
		{name: "no_colon", line: "justmetric", wantErr: true},
		{name: "no_type", line: "metric:42", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := ParseStatsDLine(tt.line)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if m.Name != tt.wantName {
				t.Errorf("name: got %q, want %q", m.Name, tt.wantName)
			}
			if m.Value != tt.wantValue {
				t.Errorf("value: got %f, want %f", m.Value, tt.wantValue)
			}
			if m.MetricType != tt.wantType {
				t.Errorf("type: got %q, want %q", m.MetricType, tt.wantType)
			}
			if tt.wantTags != nil {
				for k, v := range tt.wantTags {
					if m.Tags[k] != v {
						t.Errorf("tag %s: got %q, want %q", k, m.Tags[k], v)
					}
				}
			}
		})
	}
}

func TestStatsDToPoints(t *testing.T) {
	lines := []string{
		"page.views:1|c",
		"cpu.usage:72.5|g|#host:web01",
		"invalid line",
		"request.latency:320|ms|#service:api",
	}

	points, errs := StatsDToPoints(lines, "statsd.")
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
	if len(points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(points))
	}
	if points[0].Metric != "statsd.page.views" {
		t.Errorf("expected prefixed metric, got %s", points[0].Metric)
	}
	if points[1].Tags["host"] != "web01" {
		t.Error("expected tag host=web01")
	}
}
