package chronicle

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// PipelineDSL represents a declarative Chronicle pipeline configuration.
type PipelineDSL struct {
	Version  string            `yaml:"version"`
	Name     string            `yaml:"name"`
	Database DatabaseDSL       `yaml:"database"`
	Inputs   []InputDSL        `yaml:"inputs"`
	Transforms []TransformDSL  `yaml:"transforms,omitempty"`
	Outputs  []OutputDSL       `yaml:"outputs,omitempty"`
	Alerts   []AlertDSL        `yaml:"alerts,omitempty"`
	Vars     map[string]string `yaml:"vars,omitempty"`
}

// DatabaseDSL configures the Chronicle database.
type DatabaseDSL struct {
	Path           string         `yaml:"path"`
	Storage        StorageDSL     `yaml:"storage,omitempty"`
	WAL            WALDSL         `yaml:"wal,omitempty"`
	Retention      RetentionDSL   `yaml:"retention,omitempty"`
	Compression    CompressionDSL `yaml:"compression,omitempty"`
}

// StorageDSL maps to StorageConfig.
type StorageDSL struct {
	MaxMemory      string `yaml:"max_memory,omitempty"`
	MaxSeries      int    `yaml:"max_series,omitempty"`
	SegmentSize    string `yaml:"segment_size,omitempty"`
	FlushInterval  string `yaml:"flush_interval,omitempty"`
}

// WALDSL maps to WALConfig.
type WALDSL struct {
	Enabled       bool   `yaml:"enabled"`
	SyncOnWrite   bool   `yaml:"sync_on_write,omitempty"`
	MaxSegmentSize string `yaml:"max_segment_size,omitempty"`
}

// RetentionDSL maps to RetentionConfig.
type RetentionDSL struct {
	MaxAge     string `yaml:"max_age,omitempty"`
	MaxSize    string `yaml:"max_size,omitempty"`
	MaxPoints  int64  `yaml:"max_points,omitempty"`
	CheckEvery string `yaml:"check_every,omitempty"`
}

// CompressionDSL configures compression.
type CompressionDSL struct {
	Algorithm string `yaml:"algorithm,omitempty"`
	Level     int    `yaml:"level,omitempty"`
}

// InputDSL defines a data input source.
type InputDSL struct {
	Name     string            `yaml:"name"`
	Type     string            `yaml:"type"`
	Metric   string            `yaml:"metric,omitempty"`
	Interval string            `yaml:"interval,omitempty"`
	Bind     string            `yaml:"bind,omitempty"`
	Path     string            `yaml:"path,omitempty"`
	Tags     map[string]string `yaml:"tags,omitempty"`
	Options  map[string]string `yaml:"options,omitempty"`
}

// TransformDSL defines a data transformation step.
type TransformDSL struct {
	Name      string            `yaml:"name"`
	Type      string            `yaml:"type"`
	Input     string            `yaml:"input,omitempty"`
	Field     string            `yaml:"field,omitempty"`
	Window    string            `yaml:"window,omitempty"`
	Function  string            `yaml:"function,omitempty"`
	Condition string            `yaml:"condition,omitempty"`
	Tags      map[string]string `yaml:"tags,omitempty"`
	Options   map[string]string `yaml:"options,omitempty"`
}

// OutputDSL defines a data output destination.
type OutputDSL struct {
	Name     string            `yaml:"name"`
	Type     string            `yaml:"type"`
	Path     string            `yaml:"path,omitempty"`
	Bind     string            `yaml:"bind,omitempty"`
	Metric   string            `yaml:"metric,omitempty"`
	Format   string            `yaml:"format,omitempty"`
	Options  map[string]string `yaml:"options,omitempty"`
}

// AlertDSL defines an alerting rule.
type AlertDSL struct {
	Name      string `yaml:"name"`
	Metric    string `yaml:"metric"`
	Condition string `yaml:"condition"`
	Window    string `yaml:"window,omitempty"`
	Cooldown  string `yaml:"cooldown,omitempty"`
	Message   string `yaml:"message,omitempty"`
}

// ParsePipelineDSL parses a YAML pipeline definition from bytes.
func ParsePipelineDSL(data []byte) (*PipelineDSL, error) {
	var p PipelineDSL
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("pipeline: invalid YAML: %w", err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

// ParsePipelineDSLFile parses a YAML pipeline from a file path.
func ParsePipelineDSLFile(path string) (*PipelineDSL, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pipeline: cannot read %s: %w", path, err)
	}
	return ParsePipelineDSL(data)
}

// Validate checks the pipeline for structural correctness.
func (p *PipelineDSL) Validate() error {
	if p.Version == "" {
		return fmt.Errorf("pipeline: version is required")
	}
	if p.Database.Path == "" {
		return fmt.Errorf("pipeline: database.path is required")
	}
	if p.Name == "" {
		return fmt.Errorf("pipeline: name is required")
	}

	inputNames := make(map[string]bool)
	for i, inp := range p.Inputs {
		if inp.Name == "" {
			return fmt.Errorf("pipeline: inputs[%d].name is required", i)
		}
		if inp.Type == "" {
			return fmt.Errorf("pipeline: inputs[%d].type is required", i)
		}
		validTypes := map[string]bool{
			"http": true, "file": true, "stdin": true,
			"prometheus": true, "statsd": true, "otel": true,
		}
		if !validTypes[inp.Type] {
			return fmt.Errorf("pipeline: inputs[%d].type %q is not supported (valid: http, file, stdin, prometheus, statsd, otel)", i, inp.Type)
		}
		if inputNames[inp.Name] {
			return fmt.Errorf("pipeline: duplicate input name %q", inp.Name)
		}
		inputNames[inp.Name] = true
	}

	for i, tr := range p.Transforms {
		if tr.Name == "" {
			return fmt.Errorf("pipeline: transforms[%d].name is required", i)
		}
		validTypes := map[string]bool{
			"filter": true, "downsample": true, "aggregate": true,
			"rename": true, "enrich": true, "rate": true,
		}
		if tr.Type != "" && !validTypes[tr.Type] {
			return fmt.Errorf("pipeline: transforms[%d].type %q is not supported", i, tr.Type)
		}
	}

	for i, out := range p.Outputs {
		if out.Name == "" {
			return fmt.Errorf("pipeline: outputs[%d].name is required", i)
		}
		if out.Type == "" {
			return fmt.Errorf("pipeline: outputs[%d].type is required", i)
		}
	}

	return nil
}

// ToConfig converts the DSL database section into a Chronicle Config.
func (p *PipelineDSL) ToConfig() (Config, error) {
	cfg := DefaultConfig(p.expandVars(p.Database.Path))

	if p.Database.Storage.MaxMemory != "" {
		bytes, err := parseByteSize(p.expandVars(p.Database.Storage.MaxMemory))
		if err != nil {
			return cfg, fmt.Errorf("pipeline: storage.max_memory: %w", err)
		}
		cfg.Storage.MaxMemory = bytes
	}
	if p.Database.Storage.FlushInterval != "" {
		d, err := time.ParseDuration(p.expandVars(p.Database.Storage.FlushInterval))
		if err != nil {
			return cfg, fmt.Errorf("pipeline: storage.flush_interval: %w", err)
		}
		cfg.Storage.PartitionDuration = d
	}
	if p.Database.Storage.MaxSeries > 0 {
		cfg.Storage.BufferSize = p.Database.Storage.MaxSeries
	}

	if p.Database.WAL.SyncOnWrite {
		cfg.WAL.SyncInterval = 0
	}
	if p.Database.WAL.MaxSegmentSize != "" {
		bytes, err := parseByteSize(p.expandVars(p.Database.WAL.MaxSegmentSize))
		if err != nil {
			return cfg, fmt.Errorf("pipeline: wal.max_segment_size: %w", err)
		}
		cfg.WAL.WALMaxSize = bytes
	}

	if p.Database.Retention.MaxAge != "" {
		d, err := time.ParseDuration(p.expandVars(p.Database.Retention.MaxAge))
		if err != nil {
			return cfg, fmt.Errorf("pipeline: retention.max_age: %w", err)
		}
		cfg.Retention.RetentionDuration = d
	}

	return cfg, nil
}

// expandVars replaces ${VAR} references with values from p.Vars and env.
func (p *PipelineDSL) expandVars(s string) string {
	for k, v := range p.Vars {
		s = strings.ReplaceAll(s, "${"+k+"}", v)
	}
	return os.Expand(s, os.Getenv)
}

// parseByteSize parses human-readable sizes like "256MB", "1GB".
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	multiplier := int64(1)

	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1},
	}

	for _, sf := range suffixes {
		if strings.HasSuffix(s, sf.suffix) {
			s = strings.TrimSuffix(s, sf.suffix)
			multiplier = sf.mult
			break
		}
	}

	var val float64
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%f", &val); err != nil {
		return 0, fmt.Errorf("invalid byte size %q", s)
	}
	return int64(val * float64(multiplier)), nil
}

// MarshalYAML serializes a PipelineDSL back to YAML bytes.
func (p *PipelineDSL) MarshalYAML() ([]byte, error) {
	return yaml.Marshal(p)
}
