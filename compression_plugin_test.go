package chronicle

import (
	"bytes"
	"testing"
)

func TestCompressionPluginManager_RegisterPlugin(t *testing.T) {
	pm := NewCompressionPluginManager(DefaultCompressionPluginConfig())

	// Create custom plugin
	plugin, err := NewCustomPlugin("test-plugin").
		WithVersion("1.0.0").
		WithAuthor("Test").
		WithDescription("Test plugin").
		WithCompressFunc(func(data []byte) ([]byte, error) {
			return data, nil
		}).
		WithDecompressFunc(func(data []byte) ([]byte, error) {
			return data, nil
		}).
		Build()

	if err != nil {
		t.Fatalf("Build error: %v", err)
	}

	err = pm.RegisterPlugin(plugin)
	if err != nil {
		t.Fatalf("RegisterPlugin error: %v", err)
	}

	// Verify plugin is registered
	registered, ok := pm.GetPlugin("test-plugin")
	if !ok {
		t.Error("expected plugin to be registered")
	}
	if registered.Name() != "test-plugin" {
		t.Errorf("expected name test-plugin, got %s", registered.Name())
	}
}

func TestCompressionPluginManager_RegisterNilPlugin(t *testing.T) {
	pm := NewCompressionPluginManager(DefaultCompressionPluginConfig())

	err := pm.RegisterPlugin(nil)
	if err == nil {
		t.Error("expected error for nil plugin")
	}
}

func TestCompressionPluginManager_DuplicatePlugin(t *testing.T) {
	config := DefaultCompressionPluginConfig()
	config.AllowOverride = false
	pm := NewCompressionPluginManager(config)

	plugin, _ := NewCustomPlugin("duplicate").
		WithCompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		WithDecompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		Build()

	pm.RegisterPlugin(plugin)

	// Try to register again
	err := pm.RegisterPlugin(plugin)
	if err == nil {
		t.Error("expected error for duplicate plugin")
	}
}

func TestCompressionPluginManager_ListPlugins(t *testing.T) {
	pm := NewCompressionPluginManager(DefaultCompressionPluginConfig())

	plugins := pm.ListPlugins()
	if len(plugins) == 0 {
		t.Error("expected built-in plugins")
	}

	// Check for built-in plugins
	names := make(map[string]bool)
	for _, p := range plugins {
		names[p.Name] = true
	}

	builtins := []string{"gzip", "none", "rle", "delta", "metrics", "logs"}
	for _, b := range builtins {
		if !names[b] {
			t.Errorf("expected built-in plugin: %s", b)
		}
	}
}

func TestCompressionPluginManager_UnregisterPlugin(t *testing.T) {
	pm := NewCompressionPluginManager(DefaultCompressionPluginConfig())

	plugin, _ := NewCustomPlugin("removable").
		WithCompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		WithDecompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		Build()

	pm.RegisterPlugin(plugin)

	err := pm.UnregisterPlugin("removable")
	if err != nil {
		t.Errorf("UnregisterPlugin error: %v", err)
	}

	_, ok := pm.GetPlugin("removable")
	if ok {
		t.Error("expected plugin to be removed")
	}
}

func TestCompressionPluginManager_CannotUnregisterBuiltin(t *testing.T) {
	pm := NewCompressionPluginManager(DefaultCompressionPluginConfig())

	err := pm.UnregisterPlugin("gzip")
	if err == nil {
		t.Error("expected error when unregistering built-in plugin")
	}
}

func TestCompressionPluginManager_BindDataType(t *testing.T) {
	pm := NewCompressionPluginManager(DefaultCompressionPluginConfig())

	err := pm.BindDataType("timeseries", "metrics")
	if err != nil {
		t.Fatalf("BindDataType error: %v", err)
	}

	plugin := pm.GetPluginForDataType("timeseries")
	if plugin.Name() != "metrics" {
		t.Errorf("expected metrics plugin, got %s", plugin.Name())
	}
}

func TestCompressionPluginManager_CompressDecompress(t *testing.T) {
	pm := NewCompressionPluginManager(DefaultCompressionPluginConfig())

	testData := []byte("Hello, this is test data for compression!")

	compressed, err := pm.Compress(testData, "gzip")
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := pm.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Errorf("data mismatch after roundtrip")
	}
}

func TestGzipPlugin(t *testing.T) {
	plugin := &GzipPlugin{}

	testData := []byte("Test data for gzip compression. " +
		"Test data for gzip compression. " +
		"Test data for gzip compression.")

	compressed, err := plugin.Compress(testData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	// Should be smaller
	if len(compressed) >= len(testData) {
		t.Logf("Warning: compressed size %d >= original %d", len(compressed), len(testData))
	}

	decompressed, err := plugin.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("data mismatch")
	}
}

func TestPassthroughPlugin(t *testing.T) {
	plugin := &PassthroughPlugin{}

	testData := []byte("Test data that should not change")

	compressed, err := plugin.Compress(testData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	if !bytes.Equal(testData, compressed) {
		t.Error("passthrough should not modify data")
	}

	decompressed, err := plugin.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("data mismatch")
	}
}

func TestRLEPlugin(t *testing.T) {
	plugin := &RLEPlugin{}

	// Data with repeated values
	testData := []byte{1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3}

	compressed, err := plugin.Compress(testData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	// Should be smaller for repeated data
	if len(compressed) >= len(testData) {
		t.Logf("Warning: RLE compressed size %d >= original %d", len(compressed), len(testData))
	}

	decompressed, err := plugin.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("data mismatch")
	}
}

func TestRLEPlugin_EmptyData(t *testing.T) {
	plugin := &RLEPlugin{}

	compressed, err := plugin.Compress([]byte{})
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := plugin.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if len(decompressed) != 0 {
		t.Error("expected empty result")
	}
}

func TestDeltaPlugin(t *testing.T) {
	plugin := &DeltaPlugin{}

	// Create sequential int64 values
	testData := make([]byte, 40) // 5 int64 values
	for i := 0; i < 5; i++ {
		val := uint64(100 + i*10) // 100, 110, 120, 130, 140
		putUint64BE(testData[i*8:], val)
	}

	compressed, err := plugin.Compress(testData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := plugin.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("data mismatch")
	}
}

func putUint64BE(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func TestMetricsPlugin(t *testing.T) {
	plugin := &MetricsPlugin{}

	// Create test metrics data
	testData := make([]byte, 80)
	for i := 0; i < 10; i++ {
		putUint64BE(testData[i*8:], uint64(1000+i*5))
	}

	compressed, err := plugin.Compress(testData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := plugin.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("data mismatch")
	}
}

func TestLogsPlugin(t *testing.T) {
	plugin := &LogsPlugin{}

	testData := []byte(`{"timestamp":"2024-01-01T00:00:00Z","level":"INFO","message":"Application started"}
{"timestamp":"2024-01-01T00:00:01Z","level":"INFO","message":"Application started"}
{"timestamp":"2024-01-01T00:00:02Z","level":"ERROR","message":"Connection failed"}`)

	compressed, err := plugin.Compress(testData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := plugin.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("data mismatch")
	}
}

func TestCustomPluginBuilder(t *testing.T) {
	// Test missing name
	_, err := NewCustomPlugin("").
		WithCompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		WithDecompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		Build()
	if err == nil {
		t.Error("expected error for missing name")
	}

	// Test missing compress func
	_, err = NewCustomPlugin("test").
		WithDecompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		Build()
	if err == nil {
		t.Error("expected error for missing compress func")
	}

	// Test missing decompress func
	_, err = NewCustomPlugin("test").
		WithCompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		Build()
	if err == nil {
		t.Error("expected error for missing decompress func")
	}

	// Test successful build
	plugin, err := NewCustomPlugin("custom").
		WithVersion("2.0.0").
		WithAuthor("Test Author").
		WithDescription("Test Description").
		WithDataTypes("type1", "type2").
		WithProperty("key", "value").
		WithCompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		WithDecompressFunc(func(data []byte) ([]byte, error) { return data, nil }).
		Build()

	if err != nil {
		t.Fatalf("Build error: %v", err)
	}

	meta := plugin.Metadata()
	if meta.Version != "2.0.0" {
		t.Errorf("expected version 2.0.0, got %s", meta.Version)
	}
	if meta.Author != "Test Author" {
		t.Errorf("expected author Test Author, got %s", meta.Author)
	}
	if len(meta.DataTypes) != 2 {
		t.Errorf("expected 2 data types, got %d", len(meta.DataTypes))
	}
	if meta.Properties["key"] != "value" {
		t.Error("expected property key=value")
	}
}

func TestPluginMetadata(t *testing.T) {
	plugins := []CompressionPlugin{
		&GzipPlugin{},
		&PassthroughPlugin{},
		&RLEPlugin{},
		&DeltaPlugin{},
		&MetricsPlugin{},
		&LogsPlugin{},
	}

	for _, p := range plugins {
		meta := p.Metadata()
		if meta.Name == "" {
			t.Errorf("plugin %s has empty name in metadata", p.Name())
		}
		if meta.Name != p.Name() {
			t.Errorf("metadata name %s != plugin name %s", meta.Name, p.Name())
		}
	}
}

func TestDefaultCompressionPluginConfig(t *testing.T) {
	config := DefaultCompressionPluginConfig()

	if !config.Enabled {
		t.Error("expected Enabled to be true")
	}
	if config.MaxPlugins != 50 {
		t.Errorf("expected MaxPlugins 50, got %d", config.MaxPlugins)
	}
	if config.DefaultPlugin != "gzip" {
		t.Errorf("expected DefaultPlugin gzip, got %s", config.DefaultPlugin)
	}
}
