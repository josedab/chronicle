package chronicleexporter

import (
	"context"
	"testing"
	"time"
)

func TestNewExporter(t *testing.T) {
	cfg := DefaultConfig()
	exp, err := NewExporter(cfg)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if exp == nil {
		t.Fatal("expected non-nil exporter")
	}
}

func TestNewExporter_EmptyEndpoint(t *testing.T) {
	cfg := &Config{}
	_, err := NewExporter(cfg)
	if err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Endpoint != "http://localhost:8086" {
		t.Errorf("Endpoint = %q", cfg.Endpoint)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("BatchSize = %d", cfg.BatchSize)
	}
	if cfg.FlushInterval != 10*time.Second {
		t.Errorf("FlushInterval = %v", cfg.FlushInterval)
	}
}

func TestExporter_AddPoints_BatchFlush(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = 2
	cfg.Endpoint = "http://localhost:99999" // non-routable, flush will fail but that's OK

	exp, _ := NewExporter(cfg)
	ctx := context.Background()

	// Adding 1 point should not flush
	err := exp.AddPoints(ctx, []Point{{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()}})
	if err != nil {
		t.Logf("AddPoints (1): %v (expected, non-routable endpoint)", err)
	}

	exp.mu.Lock()
	count := len(exp.batch)
	exp.mu.Unlock()
	if count != 1 {
		t.Errorf("batch should have 1 point, got %d", count)
	}
}

func TestFactory(t *testing.T) {
	f := NewFactory()
	if f.Type() != "chronicle" {
		t.Errorf("Type = %q", f.Type())
	}
	cfg := f.CreateDefaultConfig()
	if cfg.Endpoint == "" {
		t.Error("default config has empty endpoint")
	}
	exp, err := f.CreateExporter(cfg)
	if err != nil {
		t.Fatalf("CreateExporter: %v", err)
	}
	if exp == nil {
		t.Fatal("expected non-nil exporter")
	}
}
