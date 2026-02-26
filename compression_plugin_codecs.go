// compression_plugin_codecs.go contains extended compression plugin functionality.
package chronicle

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
)

func (p *GzipPlugin) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (p *GzipPlugin) Metadata() PluginMetadata {
	return PluginMetadata{
		Name:        "gzip",
		Version:     "1.0.0",
		Author:      "Chronicle",
		Description: "Standard gzip compression",
		DataTypes:   []string{"general", "logs", "json"},
		Properties: map[string]string{
			"algorithm": "DEFLATE",
			"level":     "default",
		},
		CompressionLevel: 6,
		IsLossy:          false,
	}
}

// PassthroughPlugin provides no compression (passthrough).
type PassthroughPlugin struct{}

func (p *PassthroughPlugin) Name() string        { return "none" }
func (p *PassthroughPlugin) Version() string     { return "1.0.0" }
func (p *PassthroughPlugin) Description() string { return "No compression (passthrough)" }

func (p *PassthroughPlugin) Compress(data []byte) ([]byte, error) {
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

func (p *PassthroughPlugin) Decompress(data []byte) ([]byte, error) {
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

func (p *PassthroughPlugin) Metadata() PluginMetadata {
	return PluginMetadata{
		Name:        "none",
		Version:     "1.0.0",
		Author:      "Chronicle",
		Description: "No compression (passthrough)",
		DataTypes:   []string{"all"},
		Properties:  map[string]string{},
		IsLossy:     false,
	}
}

// RLEPlugin provides run-length encoding compression.
type RLEPlugin struct{}

func (p *RLEPlugin) Name() string        { return "rle" }
func (p *RLEPlugin) Version() string     { return "1.0.0" }
func (p *RLEPlugin) Description() string { return "Run-length encoding for repeated values" }

func (p *RLEPlugin) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	var result bytes.Buffer
	current := data[0]
	count := 1

	for i := 1; i < len(data); i++ {
		if data[i] == current && count < 255 {
			count++
		} else {
			result.WriteByte(byte(count))
			result.WriteByte(current)
			current = data[i]
			count = 1
		}
	}

	result.WriteByte(byte(count))
	result.WriteByte(current)

	return result.Bytes(), nil
}

func (p *RLEPlugin) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	if len(data)%2 != 0 {
		return nil, errors.New("invalid RLE data")
	}

	var result bytes.Buffer
	for i := 0; i < len(data); i += 2 {
		count := int(data[i])
		value := data[i+1]
		for j := 0; j < count; j++ {
			result.WriteByte(value)
		}
	}

	return result.Bytes(), nil
}

func (p *RLEPlugin) Metadata() PluginMetadata {
	return PluginMetadata{
		Name:        "rle",
		Version:     "1.0.0",
		Author:      "Chronicle",
		Description: "Run-length encoding for repeated values",
		DataTypes:   []string{"metrics", "counters", "status"},
		Properties: map[string]string{
			"best_for": "repeated_values",
		},
		IsLossy: false,
	}
}

// DeltaPlugin provides delta encoding for time-series data.
type DeltaPlugin struct{}

func (p *DeltaPlugin) Name() string        { return "delta" }
func (p *DeltaPlugin) Version() string     { return "1.0.0" }
func (p *DeltaPlugin) Description() string { return "Delta encoding for sequential numeric data" }

func (p *DeltaPlugin) Compress(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	// Interpret as int64 values
	numValues := len(data) / 8
	if numValues < 2 {
		return data, nil
	}

	result := make([]byte, len(data))
	copy(result[:8], data[:8]) // First value unchanged

	for i := 1; i < numValues; i++ {
		prev := binary.BigEndian.Uint64(data[(i-1)*8 : i*8])
		curr := binary.BigEndian.Uint64(data[i*8 : (i+1)*8])
		delta := curr - prev
		binary.BigEndian.PutUint64(result[i*8:(i+1)*8], delta)
	}

	return result, nil
}

func (p *DeltaPlugin) Decompress(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	numValues := len(data) / 8
	if numValues < 2 {
		return data, nil
	}

	result := make([]byte, len(data))
	copy(result[:8], data[:8]) // First value unchanged

	for i := 1; i < numValues; i++ {
		prev := binary.BigEndian.Uint64(result[(i-1)*8 : i*8])
		delta := binary.BigEndian.Uint64(data[i*8 : (i+1)*8])
		curr := prev + delta
		binary.BigEndian.PutUint64(result[i*8:(i+1)*8], curr)
	}

	return result, nil
}

func (p *DeltaPlugin) Metadata() PluginMetadata {
	return PluginMetadata{
		Name:        "delta",
		Version:     "1.0.0",
		Author:      "Chronicle",
		Description: "Delta encoding for sequential numeric data",
		DataTypes:   []string{"timestamps", "counters", "sequences"},
		Properties: map[string]string{
			"best_for": "monotonic_data",
		},
		IsLossy: false,
	}
}

// MetricsPlugin is optimized for time-series metrics.
type MetricsPlugin struct{}

func (p *MetricsPlugin) Name() string        { return "metrics" }
func (p *MetricsPlugin) Version() string     { return "1.0.0" }
func (p *MetricsPlugin) Description() string { return "Optimized compression for time-series metrics" }

func (p *MetricsPlugin) Compress(data []byte) ([]byte, error) {
	// Use delta encoding followed by gzip for metrics
	delta := &DeltaPlugin{}
	deltaCompressed, err := delta.Compress(data)
	if err != nil {
		return nil, err
	}

	gzipPlugin := &GzipPlugin{}
	return gzipPlugin.Compress(deltaCompressed)
}

func (p *MetricsPlugin) Decompress(data []byte) ([]byte, error) {
	gzipPlugin := &GzipPlugin{}
	deltaCompressed, err := gzipPlugin.Decompress(data)
	if err != nil {
		return nil, err
	}

	delta := &DeltaPlugin{}
	return delta.Decompress(deltaCompressed)
}

func (p *MetricsPlugin) Metadata() PluginMetadata {
	return PluginMetadata{
		Name:        "metrics",
		Version:     "1.0.0",
		Author:      "Chronicle",
		Description: "Optimized compression for time-series metrics",
		DataTypes:   []string{"metrics", "timeseries", "telemetry"},
		Properties: map[string]string{
			"algorithm": "delta+gzip",
			"optimized": "numeric_sequences",
		},
		IsLossy: false,
	}
}

// LogsPlugin is optimized for log data.
type LogsPlugin struct{}

func (p *LogsPlugin) Name() string        { return "logs" }
func (p *LogsPlugin) Version() string     { return "1.0.0" }
func (p *LogsPlugin) Description() string { return "Optimized compression for log data" }

func (p *LogsPlugin) Compress(data []byte) ([]byte, error) {
	// For logs, use dictionary-enhanced gzip
	// Simplified: just use gzip with best compression
	var buf bytes.Buffer
	w, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *LogsPlugin) Decompress(data []byte) ([]byte, error) {
	gzipPlugin := &GzipPlugin{}
	return gzipPlugin.Decompress(data)
}

func (p *LogsPlugin) Metadata() PluginMetadata {
	return PluginMetadata{
		Name:        "logs",
		Version:     "1.0.0",
		Author:      "Chronicle",
		Description: "Optimized compression for log data",
		DataTypes:   []string{"logs", "traces", "events"},
		Properties: map[string]string{
			"algorithm": "gzip-best",
			"optimized": "text_data",
		},
		CompressionLevel: 9,
		IsLossy:          false,
	}
}

// --- Custom Plugin Factory ---

// CustomPluginBuilder helps build custom compression plugins.
type CustomPluginBuilder struct {
	name           string
	version        string
	author         string
	description    string
	dataTypes      []string
	properties     map[string]string
	compressFunc   func([]byte) ([]byte, error)
	decompressFunc func([]byte) ([]byte, error)
}

// NewCustomPlugin creates a new custom plugin builder.
func NewCustomPlugin(name string) *CustomPluginBuilder {
	return &CustomPluginBuilder{
		name:       name,
		version:    "1.0.0",
		properties: make(map[string]string),
		dataTypes:  []string{},
	}
}

// Version sets the version.
func (b *CustomPluginBuilder) WithVersion(v string) *CustomPluginBuilder {
	b.version = v
	return b
}

// Author sets the author.
func (b *CustomPluginBuilder) WithAuthor(a string) *CustomPluginBuilder {
	b.author = a
	return b
}

// Description sets the description.
func (b *CustomPluginBuilder) WithDescription(d string) *CustomPluginBuilder {
	b.description = d
	return b
}

// DataTypes sets supported data types.
func (b *CustomPluginBuilder) WithDataTypes(types ...string) *CustomPluginBuilder {
	b.dataTypes = types
	return b
}

// Property adds a property.
func (b *CustomPluginBuilder) WithProperty(key, value string) *CustomPluginBuilder {
	b.properties[key] = value
	return b
}

// CompressFunc sets the compression function.
func (b *CustomPluginBuilder) WithCompressFunc(f func([]byte) ([]byte, error)) *CustomPluginBuilder {
	b.compressFunc = f
	return b
}

// DecompressFunc sets the decompression function.
func (b *CustomPluginBuilder) WithDecompressFunc(f func([]byte) ([]byte, error)) *CustomPluginBuilder {
	b.decompressFunc = f
	return b
}

// Build creates the custom plugin.
func (b *CustomPluginBuilder) Build() (CompressionPlugin, error) {
	if b.name == "" {
		return nil, errors.New("name required")
	}
	if b.compressFunc == nil {
		return nil, errors.New("compress function required")
	}
	if b.decompressFunc == nil {
		return nil, errors.New("decompress function required")
	}

	return &customPlugin{
		name:           b.name,
		version:        b.version,
		author:         b.author,
		description:    b.description,
		dataTypes:      b.dataTypes,
		properties:     b.properties,
		compressFunc:   b.compressFunc,
		decompressFunc: b.decompressFunc,
	}, nil
}

type customPlugin struct {
	name           string
	version        string
	author         string
	description    string
	dataTypes      []string
	properties     map[string]string
	compressFunc   func([]byte) ([]byte, error)
	decompressFunc func([]byte) ([]byte, error)
}

func (p *customPlugin) Name() string        { return p.name }
func (p *customPlugin) Version() string     { return p.version }
func (p *customPlugin) Description() string { return p.description }

func (p *customPlugin) Compress(data []byte) ([]byte, error) {
	return p.compressFunc(data)
}

func (p *customPlugin) Decompress(data []byte) ([]byte, error) {
	return p.decompressFunc(data)
}

func (p *customPlugin) Metadata() PluginMetadata {
	return PluginMetadata{
		Name:        p.name,
		Version:     p.version,
		Author:      p.author,
		Description: p.description,
		DataTypes:   p.dataTypes,
		Properties:  p.properties,
		IsLossy:     false,
	}
}
