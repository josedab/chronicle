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
