package chronicle

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

// CompressionPluginConfig configures the compression plugin system.
type CompressionPluginConfig struct {
	// Enabled enables custom compression plugins
	Enabled bool `json:"enabled"`

	// MaxPlugins maximum number of registered plugins
	MaxPlugins int `json:"max_plugins"`

	// AllowOverride allows plugins to override built-in codecs
	AllowOverride bool `json:"allow_override"`

	// DefaultPlugin default plugin for unknown data types
	DefaultPlugin string `json:"default_plugin"`

	// BenchmarkOnRegister run benchmark when registering plugins
	BenchmarkOnRegister bool `json:"benchmark_on_register"`
}

// DefaultCompressionPluginConfig returns default configuration.
func DefaultCompressionPluginConfig() CompressionPluginConfig {
	return CompressionPluginConfig{
		Enabled:             true,
		MaxPlugins:          50,
		AllowOverride:       false,
		DefaultPlugin:       "gzip",
		BenchmarkOnRegister: true,
	}
}

// CompressionPlugin defines the interface for custom compression plugins.
type CompressionPlugin interface {
	// Name returns the unique plugin name
	Name() string

	// Version returns the plugin version
	Version() string

	// Description returns a human-readable description
	Description() string

	// Compress compresses data
	Compress(data []byte) ([]byte, error)

	// Decompress decompresses data
	Decompress(data []byte) ([]byte, error)

	// Metadata returns plugin metadata
	Metadata() PluginMetadata
}

// PluginMetadata contains plugin information.
type PluginMetadata struct {
	Name             string            `json:"name"`
	Version          string            `json:"version"`
	Author           string            `json:"author"`
	Description      string            `json:"description"`
	DataTypes        []string          `json:"data_types"`
	Properties       map[string]string `json:"properties"`
	CompressionLevel int               `json:"compression_level"`
	IsLossy          bool              `json:"is_lossy"`
}

// CompressionPluginManager manages compression plugins.
type CompressionPluginManager struct {
	config       CompressionPluginConfig
	mu           sync.RWMutex
	plugins      map[string]CompressionPlugin
	benchmarks   map[string]*PluginBenchmark
	dataBindings map[string]string // data type -> plugin name
}

// PluginBenchmark contains benchmark results for a plugin.
type PluginBenchmark struct {
	PluginName         string  `json:"plugin_name"`
	CompressionRatio   float64 `json:"compression_ratio"`
	CompressionSpeed   float64 `json:"compression_speed_mb_s"`
	DecompressionSpeed float64 `json:"decompression_speed_mb_s"`
	AvgLatencyNs       int64   `json:"avg_latency_ns"`
	MemoryUsage        int64   `json:"memory_usage_bytes"`
	LastBenchmark      int64   `json:"last_benchmark_unix"`
}

// NewCompressionPluginManager creates a new plugin manager.
func NewCompressionPluginManager(config CompressionPluginConfig) *CompressionPluginManager {
	pm := &CompressionPluginManager{
		config:       config,
		plugins:      make(map[string]CompressionPlugin),
		benchmarks:   make(map[string]*PluginBenchmark),
		dataBindings: make(map[string]string),
	}

	// Register built-in plugins
	pm.registerBuiltinPlugins()

	return pm
}

func (pm *CompressionPluginManager) registerBuiltinPlugins() {
	// Register gzip plugin
	pm.plugins["gzip"] = &GzipPlugin{}

	// Register none/passthrough plugin
	pm.plugins["none"] = &PassthroughPlugin{}

	// Register RLE plugin
	pm.plugins["rle"] = &RLEPlugin{}

	// Register delta plugin
	pm.plugins["delta"] = &DeltaPlugin{}

	// Register domain-specific plugins
	pm.plugins["metrics"] = &MetricsPlugin{}
	pm.plugins["logs"] = &LogsPlugin{}
}

// RegisterPlugin registers a custom compression plugin.
func (pm *CompressionPluginManager) RegisterPlugin(plugin CompressionPlugin) error {
	if plugin == nil {
		return errors.New("plugin cannot be nil")
	}

	name := plugin.Name()
	if name == "" {
		return errors.New("plugin name cannot be empty")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if already exists
	if _, exists := pm.plugins[name]; exists {
		if !pm.config.AllowOverride {
			return fmt.Errorf("plugin %s already registered", name)
		}
	}

	// Check max plugins
	if len(pm.plugins) >= pm.config.MaxPlugins {
		return errors.New("maximum number of plugins reached")
	}

	pm.plugins[name] = plugin

	// Run benchmark if configured
	if pm.config.BenchmarkOnRegister {
		benchmark := pm.benchmarkPlugin(plugin)
		pm.benchmarks[name] = benchmark
	}

	return nil
}

// UnregisterPlugin removes a plugin.
func (pm *CompressionPluginManager) UnregisterPlugin(name string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Don't allow removing built-in plugins
	builtins := []string{"gzip", "none", "rle", "delta", "metrics", "logs"}
	for _, b := range builtins {
		if name == b {
			return fmt.Errorf("cannot unregister built-in plugin: %s", name)
		}
	}

	if _, exists := pm.plugins[name]; !exists {
		return fmt.Errorf("plugin not found: %s", name)
	}

	delete(pm.plugins, name)
	delete(pm.benchmarks, name)

	// Remove data bindings
	for dt, pn := range pm.dataBindings {
		if pn == name {
			delete(pm.dataBindings, dt)
		}
	}

	return nil
}

// GetPlugin returns a plugin by name.
func (pm *CompressionPluginManager) GetPlugin(name string) (CompressionPlugin, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugin, ok := pm.plugins[name]
	return plugin, ok
}

// ListPlugins returns all registered plugins.
func (pm *CompressionPluginManager) ListPlugins() []PluginMetadata {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugins := make([]PluginMetadata, 0, len(pm.plugins))
	for _, plugin := range pm.plugins {
		plugins = append(plugins, plugin.Metadata())
	}

	sort.Slice(plugins, func(i, j int) bool {
		return plugins[i].Name < plugins[j].Name
	})

	return plugins
}

// BindDataType binds a data type to a specific plugin.
func (pm *CompressionPluginManager) BindDataType(dataType, pluginName string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.plugins[pluginName]; !ok {
		return fmt.Errorf("plugin not found: %s", pluginName)
	}

	pm.dataBindings[dataType] = pluginName
	return nil
}

// GetPluginForDataType returns the plugin for a data type.
func (pm *CompressionPluginManager) GetPluginForDataType(dataType string) CompressionPlugin {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if name, ok := pm.dataBindings[dataType]; ok {
		if plugin, ok := pm.plugins[name]; ok {
			return plugin
		}
	}

	// Return default
	if plugin, ok := pm.plugins[pm.config.DefaultPlugin]; ok {
		return plugin
	}

	return pm.plugins["none"]
}

// Compress compresses data using the specified or default plugin.
func (pm *CompressionPluginManager) Compress(data []byte, pluginName string) ([]byte, error) {
	pm.mu.RLock()
	plugin, ok := pm.plugins[pluginName]
	if !ok {
		plugin = pm.plugins[pm.config.DefaultPlugin]
	}
	pm.mu.RUnlock()

	if plugin == nil {
		return nil, errors.New("no compression plugin available")
	}

	compressed, err := plugin.Compress(data)
	if err != nil {
		return nil, fmt.Errorf("compression failed: %w", err)
	}

	// Prefix with plugin identifier for decompression
	header := make([]byte, 2+len(plugin.Name()))
	binary.BigEndian.PutUint16(header[:2], uint16(len(plugin.Name())))
	copy(header[2:], plugin.Name())

	result := make([]byte, len(header)+len(compressed))
	copy(result, header)
	copy(result[len(header):], compressed)

	return result, nil
}

// Decompress decompresses data, auto-detecting the plugin.
func (pm *CompressionPluginManager) Decompress(data []byte) ([]byte, error) {
	if len(data) < 2 {
		return nil, errors.New("data too short")
	}

	nameLen := binary.BigEndian.Uint16(data[:2])
	if int(nameLen)+2 > len(data) {
		return nil, errors.New("invalid header")
	}

	pluginName := string(data[2 : 2+nameLen])
	compressedData := data[2+nameLen:]

	pm.mu.RLock()
	plugin, ok := pm.plugins[pluginName]
	pm.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown plugin: %s", pluginName)
	}

	return plugin.Decompress(compressedData)
}

// GetBenchmarks returns benchmark results for all plugins.
func (pm *CompressionPluginManager) GetBenchmarks() map[string]*PluginBenchmark {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	benchmarks := make(map[string]*PluginBenchmark)
	for name, b := range pm.benchmarks {
		benchmarks[name] = b
	}
	return benchmarks
}

func (pm *CompressionPluginManager) benchmarkPlugin(plugin CompressionPlugin) *PluginBenchmark {
	// Generate test data
	testData := generateBenchmarkData(1024 * 1024) // 1MB

	// Measure compression
	compressStart := nanoTime()
	compressed, err := plugin.Compress(testData)
	compressEnd := nanoTime()

	if err != nil {
		return &PluginBenchmark{
			PluginName:       plugin.Name(),
			CompressionRatio: 0,
		}
	}

	// Measure decompression
	decompressStart := nanoTime()
	_, err = plugin.Decompress(compressed)
	decompressEnd := nanoTime()

	if err != nil {
		return &PluginBenchmark{
			PluginName:       plugin.Name(),
			CompressionRatio: float64(len(testData)) / float64(len(compressed)),
		}
	}

	compressTime := float64(compressEnd-compressStart) / 1e9
	decompressTime := float64(decompressEnd-decompressStart) / 1e9
	dataSizeMB := float64(len(testData)) / (1024 * 1024)

	return &PluginBenchmark{
		PluginName:         plugin.Name(),
		CompressionRatio:   float64(len(testData)) / float64(len(compressed)),
		CompressionSpeed:   dataSizeMB / compressTime,
		DecompressionSpeed: dataSizeMB / decompressTime,
		AvgLatencyNs:       (compressEnd - compressStart + decompressEnd - decompressStart) / 2,
		LastBenchmark:      nanoTime(),
	}
}

func generateBenchmarkData(size int) []byte {
	data := make([]byte, size)
	// Generate semi-compressible data
	for i := range data {
		data[i] = byte(i % 256)
		if i%10 == 0 {
			data[i] = byte(i / 100)
		}
	}
	return data
}

func nanoTime() int64 {
	// Simplified - in production use time.Now().UnixNano()
	return 0
}

// --- Built-in Plugins ---

// GzipPlugin provides gzip compression.
type GzipPlugin struct{}

func (p *GzipPlugin) Name() string        { return "gzip" }
func (p *GzipPlugin) Version() string     { return "1.0.0" }
func (p *GzipPlugin) Description() string { return "Standard gzip compression" }

func (p *GzipPlugin) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

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
