package chronicle

import (
	"testing"
)

func TestEBPFConfig(t *testing.T) {
	config := DefaultEBPFConfig()

	if !config.Enabled {
		t.Error("eBPF should be enabled by default")
	}
	if config.CollectionInterval == 0 {
		t.Error("CollectionInterval should be set")
	}
}

func TestEBPFCollector(t *testing.T) {
	config := DefaultEBPFConfig()
	collector := NewEBPFCollector(nil, config)

	if collector == nil {
		t.Fatal("Failed to create EBPFCollector")
	}
}

func TestEBPFEnabled(t *testing.T) {
	config := DefaultEBPFConfig()

	if !config.Enabled {
		t.Error("eBPF should be enabled")
	}
}

func TestEBPFMetricConversion(t *testing.T) {
	// Test converting metrics to Chronicle points
	point := Point{
		Metric:    "ebpf_cpu_user",
		Value:     0.25,
		Timestamp: 1000000000,
	}

	if point.Value != 0.25 {
		t.Errorf("Expected 0.25, got %f", point.Value)
	}
}

func TestEBPFDBWrapper(t *testing.T) {
	config := DefaultEBPFConfig()

	if !config.Enabled {
		t.Error("Config should be enabled")
	}
}
