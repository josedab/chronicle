package chronicle

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
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
