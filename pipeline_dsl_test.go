package chronicle

import (
	"testing"
	"time"
)

func TestParsePipelineDSL_Valid(t *testing.T) {
	yaml := `
version: "1"
name: test-pipeline
database:
  path: /tmp/chronicle-test
  storage:
    max_memory: "256MB"
    flush_interval: "5s"
  wal:
    enabled: true
    sync_on_write: false
  retention:
    max_age: "720h"
    max_points: 1000000
inputs:
  - name: http-input
    type: http
    bind: ":8080"
  - name: file-input
    type: file
    path: /var/log/metrics.jsonl
transforms:
  - name: downsample-cpu
    type: downsample
    window: "1m"
    function: avg
outputs:
  - name: console-out
    type: stdout
alerts:
  - name: high-cpu
    metric: cpu_usage
    condition: "> 90"
    window: "5m"
`

	p, err := ParsePipelineDSL([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.Name != "test-pipeline" {
		t.Errorf("name = %q, want test-pipeline", p.Name)
	}
	if len(p.Inputs) != 2 {
		t.Errorf("inputs = %d, want 2", len(p.Inputs))
	}
	if p.Inputs[0].Type != "http" {
		t.Errorf("input[0].type = %q, want http", p.Inputs[0].Type)
	}
	if len(p.Transforms) != 1 {
		t.Errorf("transforms = %d, want 1", len(p.Transforms))
	}
	if len(p.Alerts) != 1 {
		t.Errorf("alerts = %d, want 1", len(p.Alerts))
	}
}

func TestParsePipelineDSL_ToConfig(t *testing.T) {
	yaml := `
version: "1"
name: config-test
database:
  path: /tmp/chronicle-cfg
  storage:
    max_memory: "512MB"
    max_series: 50000
    flush_interval: "10s"
  wal:
    enabled: true
    sync_on_write: true
    max_segment_size: "64MB"
  retention:
    max_age: "168h"
    max_points: 5000000
inputs:
  - name: dummy
    type: stdin
`

	p, err := ParsePipelineDSL([]byte(yaml))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	cfg, err := p.ToConfig()
	if err != nil {
		t.Fatalf("ToConfig error: %v", err)
	}

	if cfg.Storage.MaxMemory != 512*1024*1024 {
		t.Errorf("MaxMemory = %d, want %d", cfg.Storage.MaxMemory, 512*1024*1024)
	}
	if cfg.Storage.BufferSize != 50000 {
		t.Errorf("BufferSize = %d, want 50000", cfg.Storage.BufferSize)
	}
	if cfg.Storage.PartitionDuration != 10*time.Second {
		t.Errorf("PartitionDuration = %v, want 10s", cfg.Storage.PartitionDuration)
	}
	if cfg.WAL.SyncInterval != 0 {
		t.Error("WAL SyncInterval should be 0 for sync_on_write")
	}
	if cfg.WAL.WALMaxSize != 64*1024*1024 {
		t.Errorf("WAL WALMaxSize = %d, want %d", cfg.WAL.WALMaxSize, 64*1024*1024)
	}
	if cfg.Retention.RetentionDuration != 168*time.Hour {
		t.Errorf("Retention.RetentionDuration = %v, want 168h", cfg.Retention.RetentionDuration)
	}
}

func TestParsePipelineDSL_Variables(t *testing.T) {
	yaml := `
version: "1"
name: var-test
vars:
  data_dir: /opt/chronicle
database:
  path: "${data_dir}/data"
inputs:
  - name: test
    type: stdin
`

	p, err := ParsePipelineDSL([]byte(yaml))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	cfg, err := p.ToConfig()
	if err != nil {
		t.Fatalf("ToConfig error: %v", err)
	}

	if cfg.Path != "/opt/chronicle/data" {
		t.Errorf("path = %q, want /opt/chronicle/data", cfg.Path)
	}
}

func TestParsePipelineDSL_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{"missing version", `name: x
database:
  path: /tmp
inputs:
  - name: a
    type: http`},
		{"missing path", `version: "1"
name: x
database:
  path: ""
inputs:
  - name: a
    type: http`},
		{"missing name", `version: "1"
database:
  path: /tmp
inputs:
  - name: a
    type: http`},
		{"invalid input type", `version: "1"
name: x
database:
  path: /tmp
inputs:
  - name: a
    type: invalid`},
		{"duplicate input name", `version: "1"
name: x
database:
  path: /tmp
inputs:
  - name: a
    type: http
  - name: a
    type: file`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePipelineDSL([]byte(tt.yaml))
			if err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"100B", 100},
		{"1KB", 1024},
		{"256MB", 256 * 1024 * 1024},
		{"1GB", 1 << 30},
		{"2TB", 2 * (1 << 40)},
		{"1024", 1024},
	}

	for _, tt := range tests {
		got, err := parseByteSize(tt.input)
		if err != nil {
			t.Errorf("parseByteSize(%q) error: %v", tt.input, err)
			continue
		}
		if got != tt.expected {
			t.Errorf("parseByteSize(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestPipelineDSL_MarshalYAML(t *testing.T) {
	p := &PipelineDSL{
		Version: "1",
		Name:    "roundtrip",
		Database: DatabaseDSL{Path: "/tmp/rt"},
		Inputs: []InputDSL{{Name: "in", Type: "stdin"}},
	}

	data, err := p.MarshalYAML()
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	p2, err := ParsePipelineDSL(data)
	if err != nil {
		t.Fatalf("roundtrip parse error: %v", err)
	}
	if p2.Name != "roundtrip" {
		t.Errorf("name = %q, want roundtrip", p2.Name)
	}
}
