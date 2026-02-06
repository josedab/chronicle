package chronicle

import (
	"path/filepath"
	"testing"
	"time"
)

func TestOTelDistroConfig(t *testing.T) {
	cfg := DefaultOTelDistroConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.GRPCPort != 4317 {
		t.Errorf("expected GRPC port 4317, got %d", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 4318 {
		t.Errorf("expected HTTP port 4318, got %d", cfg.HTTPPort)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected batch size 1000, got %d", cfg.BatchSize)
	}
}

func TestOTelDistroStartStop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	distro := NewOTelDistro(db, DefaultOTelDistroConfig())

	if err := distro.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	// Double start should fail
	if err := distro.Start(); err == nil {
		t.Error("expected error on double start")
	}
	if err := distro.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestOTelDistroPush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	distro := NewOTelDistro(db, DefaultOTelDistroConfig())
	if err := distro.Start(); err != nil {
		t.Fatal(err)
	}
	defer distro.Stop()

	batch := &OTelMetricBatch{
		Points: []Point{
			{Metric: "cpu_usage", Value: 42.5, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"host": "test"}},
			{Metric: "mem_usage", Value: 78.2, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"host": "test"}},
		},
		Source: "test",
	}

	if err := distro.Push(batch); err != nil {
		t.Fatalf("push failed: %v", err)
	}

	// Allow time for flush
	time.Sleep(100 * time.Millisecond)

	stats := distro.Stats()
	if stats.MetricsReceived != 2 {
		t.Errorf("expected 2 metrics received, got %d", stats.MetricsReceived)
	}
}

func TestOTelDistroStats(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	distro := NewOTelDistro(db, DefaultOTelDistroConfig())
	stats := distro.Stats()
	if stats.MetricsReceived != 0 {
		t.Errorf("expected 0 metrics, got %d", stats.MetricsReceived)
	}
}

func TestOTelDistroPipelines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	distro := NewOTelDistro(db, DefaultOTelDistroConfig())
	pipelines := distro.ListPipelines()
	if len(pipelines) != 1 {
		t.Errorf("expected 1 default pipeline, got %d", len(pipelines))
	}

	distro.AddPipeline(OTelPipelineConfig{
		Name:       "custom",
		SignalType: "metrics",
		Receivers:  []string{"prometheus"},
		Exporters:  []string{"chronicle"},
		Enabled:    true,
	})
	pipelines = distro.ListPipelines()
	if len(pipelines) != 2 {
		t.Errorf("expected 2 pipelines, got %d", len(pipelines))
	}
}

func TestOTelDistroGenerateConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	distro := NewOTelDistro(db, DefaultOTelDistroConfig())
	cfg := distro.GenerateConfig()
	if cfg["receivers"] == nil {
		t.Error("expected receivers in config")
	}
	if cfg["exporters"] == nil {
		t.Error("expected exporters in config")
	}
	if cfg["service"] == nil {
		t.Error("expected service in config")
	}
}

func TestOTelDistroPushWhenNotRunning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	distro := NewOTelDistro(db, DefaultOTelDistroConfig())
	batch := &OTelMetricBatch{
		Points: []Point{{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()}},
	}
	if err := distro.Push(batch); err == nil {
		t.Error("expected error when pushing to stopped distro")
	}
}
