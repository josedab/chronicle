package chronicle

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPluginRegistry(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")
	config.AutoUpdate = false

	registry, err := NewPluginRegistry(config)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	defer registry.Close()

	// Verify initial state
	stats := registry.Stats()
	if stats.InstalledCount != 0 {
		t.Errorf("expected 0 installed, got %d", stats.InstalledCount)
	}
}

func TestPluginCategories(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	categories := registry.GetCategories()
	if len(categories) == 0 {
		t.Error("expected categories")
	}

	// Verify expected categories exist
	typeMap := make(map[PluginType]bool)
	for _, cat := range categories {
		typeMap[cat.Type] = true
	}

	expectedTypes := []PluginType{
		PluginTypeCompression,
		PluginTypeStorage,
		PluginTypeQueryFunc,
		PluginTypeIntegration,
	}

	for _, pt := range expectedTypes {
		if !typeMap[pt] {
			t.Errorf("missing category: %s", pt)
		}
	}
}

func TestRegisterFactory(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	// Register a factory
	factoryCalled := false
	registry.RegisterFactory("test-plugin", func() (Plugin, error) {
		factoryCalled = true
		return &testPlugin{}, nil
	})

	// Load the plugin
	plugin, err := registry.LoadPlugin(context.Background(), "test-plugin", nil)
	if err != nil {
		t.Fatalf("failed to load plugin: %v", err)
	}
	defer plugin.Close()

	if !factoryCalled {
		t.Error("factory was not called")
	}

	// Verify it's in running list
	running := registry.ListRunning()
	found := false
	for _, r := range running {
		if r.ID == "test-plugin" {
			found = true
			break
		}
	}
	if !found {
		t.Error("plugin not in running list")
	}
}

type testPlugin struct {
	initialized bool
	closed      bool
	config      map[string]interface{}
}

func (p *testPlugin) Init(config map[string]interface{}) error {
	p.initialized = true
	p.config = config
	return nil
}

func (p *testPlugin) Capabilities() []PluginCapability {
	return []PluginCapability{CapabilityCompress, CapabilityDecompress}
}

func (p *testPlugin) Close() error {
	p.closed = true
	return nil
}

func TestUnloadPlugin(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	var testPlug *testPlugin
	registry.RegisterFactory("test-plugin", func() (Plugin, error) {
		testPlug = &testPlugin{}
		return testPlug, nil
	})

	// Load then unload
	registry.LoadPlugin(context.Background(), "test-plugin", nil)
	
	if err := registry.UnloadPlugin("test-plugin"); err != nil {
		t.Fatalf("failed to unload: %v", err)
	}

	if !testPlug.closed {
		t.Error("plugin was not closed")
	}

	// Verify removed from running list
	running := registry.ListRunning()
	for _, r := range running {
		if r.ID == "test-plugin" {
			t.Error("plugin still in running list")
		}
	}
}

func TestPluginConfig(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	var testPlug *testPlugin
	registry.RegisterFactory("test-plugin", func() (Plugin, error) {
		testPlug = &testPlugin{}
		return testPlug, nil
	})

	// Load with config
	pluginConfig := map[string]interface{}{
		"endpoint": "http://example.com",
		"timeout":  30,
		"enabled":  true,
	}

	_, err := registry.LoadPlugin(context.Background(), "test-plugin", pluginConfig)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}

	if !testPlug.initialized {
		t.Error("plugin not initialized")
	}

	if testPlug.config["endpoint"] != "http://example.com" {
		t.Error("config not passed to plugin")
	}
}

func TestSearchOptions(t *testing.T) {
	opts := SearchOptions{
		Query:        "compression",
		Type:         PluginTypeCompression,
		VerifiedOnly: true,
		SortBy:       "downloads",
		SortDesc:     true,
		Limit:        10,
		Offset:       0,
	}

	if opts.Query != "compression" {
		t.Error("query not set")
	}
	if opts.Type != PluginTypeCompression {
		t.Error("type not set")
	}
	if !opts.VerifiedOnly {
		t.Error("verified_only not set")
	}
}

func TestMarketplacePluginInfo(t *testing.T) {
	meta := MarketplacePluginInfo{
		ID:                  "test-compression",
		Name:                "Test Compression",
		Description:         "A test compression plugin",
		Version:             "1.0.0",
		Type:                PluginTypeCompression,
		Capabilities:        []PluginCapability{CapabilityCompress, CapabilityDecompress},
		Author:              "test-author",
		License:             "MIT",
		MinChronicleVersion: "1.0.0",
		Verified:            true,
		Downloads:           1000,
		Rating:              4.5,
		RatingCount:         50,
		Platforms:           []string{"linux/amd64", "darwin/arm64"},
		CreatedAt:           time.Now().Add(-30 * 24 * time.Hour),
		UpdatedAt:           time.Now().Add(-7 * 24 * time.Hour),
	}

	if meta.ID != "test-compression" {
		t.Error("ID not set")
	}
	if len(meta.Capabilities) != 2 {
		t.Error("capabilities not set")
	}
	if !meta.Verified {
		t.Error("verified not set")
	}
}

func TestPluginDependency(t *testing.T) {
	dep := PluginDependency{
		ID:         "base-plugin",
		MinVersion: "1.0.0",
		MaxVersion: "2.0.0",
	}

	if dep.ID != "base-plugin" {
		t.Error("ID not set")
	}
	if dep.MinVersion != "1.0.0" {
		t.Error("min version not set")
	}
}

func TestInstalledPlugin(t *testing.T) {
	installed := InstalledPlugin{
		Metadata: MarketplacePluginInfo{
			ID:      "test-plugin",
			Name:    "Test Plugin",
			Version: "1.0.0",
		},
		State:       PluginStateInstalled,
		InstalledAt: time.Now(),
		InstallPath: "/path/to/plugin",
	}

	if installed.State != PluginStateInstalled {
		t.Error("state not set")
	}
	if installed.InstallPath == "" {
		t.Error("install path not set")
	}
}

func TestPluginStates(t *testing.T) {
	states := []PluginState{
		PluginStateAvailable,
		PluginStateInstalled,
		PluginStateEnabled,
		PluginStateDisabled,
		PluginStateOutdated,
		PluginStateInstalling,
		PluginStateUninstalling,
	}

	for _, state := range states {
		if state == "" {
			t.Error("empty state")
		}
	}
}

func TestPluginTypes(t *testing.T) {
	types := []PluginType{
		PluginTypeCompression,
		PluginTypeStorage,
		PluginTypeQueryFunc,
		PluginTypeIntegration,
		PluginTypeEncoder,
		PluginTypeAuthProvider,
		PluginTypeExporter,
	}

	for _, pt := range types {
		if pt == "" {
			t.Error("empty type")
		}
	}
}

func TestPluginCapabilities(t *testing.T) {
	caps := []PluginCapability{
		CapabilityCompress,
		CapabilityDecompress,
		CapabilityStore,
		CapabilityRetrieve,
		CapabilityQuery,
		CapabilityAggregate,
		CapabilityTransform,
		CapabilityExport,
		CapabilityImport,
		CapabilityAuthenticate,
	}

	for _, cap := range caps {
		if cap == "" {
			t.Error("empty capability")
		}
	}
}

func TestPluginSDK(t *testing.T) {
	meta := MarketplacePluginInfo{
		ID:      "test-sdk",
		Name:    "Test SDK Plugin",
		Version: "1.0.0",
	}

	sdk := NewPluginSDK(meta)

	// Test config methods
	sdk.config = map[string]interface{}{
		"string_val": "hello",
		"int_val":    float64(42), // JSON numbers come as float64
		"bool_val":   true,
	}

	if sdk.GetConfigString("string_val", "") != "hello" {
		t.Error("GetConfigString failed")
	}
	if sdk.GetConfigString("missing", "default") != "default" {
		t.Error("GetConfigString default failed")
	}

	if sdk.GetConfigInt("int_val", 0) != 42 {
		t.Error("GetConfigInt failed")
	}
	if sdk.GetConfigInt("missing", 10) != 10 {
		t.Error("GetConfigInt default failed")
	}

	if !sdk.GetConfigBool("bool_val", false) {
		t.Error("GetConfigBool failed")
	}
	if sdk.GetConfigBool("missing", true) != true {
		t.Error("GetConfigBool default failed")
	}
}

func TestPluginSDKLogging(t *testing.T) {
	sdk := NewPluginSDK(MarketplacePluginInfo{ID: "test"})

	var loggedLevel, loggedMsg string
	sdk.SetLogger(func(level, msg string, args ...interface{}) {
		loggedLevel = level
		loggedMsg = msg
	})

	sdk.Info("test info")
	if loggedLevel != "info" || loggedMsg != "test info" {
		t.Error("Info logging failed")
	}

	sdk.Error("test error")
	if loggedLevel != "error" || loggedMsg != "test error" {
		t.Error("Error logging failed")
	}

	sdk.Debug("test debug")
	if loggedLevel != "debug" || loggedMsg != "test debug" {
		t.Error("Debug logging failed")
	}
}

func TestPluginManifest(t *testing.T) {
	manifest := PluginManifest{
		APIVersion: "1.0",
		Metadata: MarketplacePluginInfo{
			ID:      "test-plugin",
			Name:    "Test Plugin",
			Version: "1.0.0",
			Type:    PluginTypeCompression,
		},
		EntryPoint: "main.so",
		Config: []PluginConfigOption{
			{
				Name:        "threshold",
				Type:        "int",
				Required:    true,
				Default:     100,
				Description: "Compression threshold",
			},
		},
		Permissions: []string{"network", "filesystem"},
	}

	if err := ValidateManifest(&manifest); err != nil {
		t.Errorf("validation failed: %v", err)
	}
}

func TestValidateManifest(t *testing.T) {
	tests := []struct {
		name     string
		manifest PluginManifest
		wantErr  bool
	}{
		{
			name: "valid manifest",
			manifest: PluginManifest{
				APIVersion: "1.0",
				Metadata: MarketplacePluginInfo{
					ID:      "test",
					Name:    "Test",
					Version: "1.0.0",
					Type:    PluginTypeCompression,
				},
				EntryPoint: "main.so",
			},
			wantErr: false,
		},
		{
			name: "missing api version",
			manifest: PluginManifest{
				Metadata: MarketplacePluginInfo{
					ID:      "test",
					Name:    "Test",
					Version: "1.0.0",
					Type:    PluginTypeCompression,
				},
				EntryPoint: "main.so",
			},
			wantErr: true,
		},
		{
			name: "missing id",
			manifest: PluginManifest{
				APIVersion: "1.0",
				Metadata: MarketplacePluginInfo{
					Name:    "Test",
					Version: "1.0.0",
					Type:    PluginTypeCompression,
				},
				EntryPoint: "main.so",
			},
			wantErr: true,
		},
		{
			name: "missing entry point",
			manifest: PluginManifest{
				APIVersion: "1.0",
				Metadata: MarketplacePluginInfo{
					ID:      "test",
					Name:    "Test",
					Version: "1.0.0",
					Type:    PluginTypeCompression,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateManifest(&tt.manifest)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateManifest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPluginCLI(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	var output bytes.Buffer
	cli := NewPluginCLI(registry, &output)

	// Test list with no plugins
	err := cli.List()
	if err != nil {
		t.Errorf("list failed: %v", err)
	}

	if !bytes.Contains(output.Bytes(), []byte("No plugins installed")) {
		t.Error("expected 'No plugins installed' message")
	}
}

func TestPluginCLISearch(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	var output bytes.Buffer
	cli := NewPluginCLI(registry, &output)

	// Search will fail network but should handle gracefully
	// (returns cached results or empty)
	_ = cli.Search("compression", "")
}

func TestPluginUpdate(t *testing.T) {
	update := PluginUpdate{
		PluginID:       "test-plugin",
		CurrentVersion: "1.0.0",
		LatestVersion:  "1.1.0",
		ReleaseNotes:   "Bug fixes",
	}

	if update.PluginID != "test-plugin" {
		t.Error("plugin id not set")
	}
	if update.CurrentVersion == update.LatestVersion {
		t.Error("versions should differ for update")
	}
}

func TestPluginMarketplaceStats(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	// Register and load a plugin
	registry.RegisterFactory("stat-test", func() (Plugin, error) {
		return &testPlugin{}, nil
	})
	registry.LoadPlugin(context.Background(), "stat-test", nil)

	stats := registry.Stats()
	if stats.RunningCount != 1 {
		t.Errorf("expected 1 running, got %d", stats.RunningCount)
	}
}

func TestPluginDefaultConfig(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()

	if config.RegistryURL == "" {
		t.Error("registry URL not set")
	}
	if config.PluginsDir == "" {
		t.Error("plugins dir not set")
	}
	if config.CacheDir == "" {
		t.Error("cache dir not set")
	}
	if config.MaxConcurrentDownloads <= 0 {
		t.Error("max concurrent downloads should be positive")
	}
	if config.NetworkTimeout <= 0 {
		t.Error("network timeout should be positive")
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		v1, v2 string
		want   int
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.1.0", "1.0.0", 1},
		{"1.0.0", "1.1.0", -1},
		{"2.0.0", "1.9.9", 1},
	}

	for _, tt := range tests {
		t.Run(tt.v1+"_vs_"+tt.v2, func(t *testing.T) {
			got := compareVersions(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("compareVersions(%s, %s) = %d, want %d", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

func TestSortPlugins(t *testing.T) {
	plugins := []MarketplacePluginInfo{
		{Name: "B Plugin", Downloads: 100, Rating: 4.0, UpdatedAt: time.Now().Add(-1 * time.Hour)},
		{Name: "A Plugin", Downloads: 200, Rating: 3.0, UpdatedAt: time.Now().Add(-2 * time.Hour)},
		{Name: "C Plugin", Downloads: 50, Rating: 5.0, UpdatedAt: time.Now()},
	}

	// Sort by name
	sortPlugins(plugins, "name", false)
	if plugins[0].Name != "A Plugin" {
		t.Error("sort by name failed")
	}

	// Sort by downloads desc
	sortPlugins(plugins, "downloads", true)
	if plugins[0].Downloads != 200 {
		t.Error("sort by downloads desc failed")
	}

	// Sort by rating
	sortPlugins(plugins, "rating", true)
	if plugins[0].Rating != 5.0 {
		t.Error("sort by rating failed")
	}
}

func TestPluginInstance(t *testing.T) {
	instance := PluginInstance{
		ID:      "test",
		Started: time.Now(),
		stats: PluginInstanceStats{
			Invocations: 10,
			Errors:      1,
			TotalTime:   time.Second,
		},
	}

	if instance.ID != "test" {
		t.Error("ID not set")
	}
	if instance.stats.Invocations != 10 {
		t.Error("invocations not set")
	}
}

func TestPluginConfigOption(t *testing.T) {
	opt := PluginConfigOption{
		Name:        "batch_size",
		Type:        "int",
		Required:    true,
		Default:     100,
		Description: "Number of items per batch",
		Validate:    "^[0-9]+$",
	}

	if opt.Name != "batch_size" {
		t.Error("name not set")
	}
	if opt.Type != "int" {
		t.Error("type not set")
	}
	if !opt.Required {
		t.Error("required not set")
	}
}

func TestPluginCategory(t *testing.T) {
	cat := PluginCategory{
		Type:        PluginTypeCompression,
		Name:        "Compression",
		Description: "Data compression algorithms",
	}

	if cat.Type != PluginTypeCompression {
		t.Error("type not set")
	}
	if cat.Name == "" {
		t.Error("name not set")
	}
}

func TestRegistryClose(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")
	config.AutoUpdate = true // Start background worker

	registry, _ := NewPluginRegistry(config)

	// Load a plugin
	registry.RegisterFactory("close-test", func() (Plugin, error) {
		return &testPlugin{}, nil
	})
	registry.LoadPlugin(context.Background(), "close-test", nil)

	// Close should stop all plugins
	if err := registry.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}

	// Verify no running plugins
	if len(registry.ListRunning()) != 0 {
		t.Error("plugins still running after close")
	}
}

func TestEnableDisable(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	// Add a mock installed plugin
	registry.mu.Lock()
	registry.installed["test-plugin"] = &InstalledPlugin{
		Metadata: MarketplacePluginInfo{ID: "test-plugin"},
		State:    PluginStateInstalled,
	}
	registry.mu.Unlock()

	// Enable
	if err := registry.Enable(context.Background(), "test-plugin"); err != nil {
		t.Errorf("enable failed: %v", err)
	}

	registry.mu.RLock()
	if registry.installed["test-plugin"].State != PluginStateEnabled {
		t.Error("plugin not enabled")
	}
	registry.mu.RUnlock()

	// Disable
	if err := registry.Disable(context.Background(), "test-plugin"); err != nil {
		t.Errorf("disable failed: %v", err)
	}

	registry.mu.RLock()
	if registry.installed["test-plugin"].State != PluginStateDisabled {
		t.Error("plugin not disabled")
	}
	registry.mu.RUnlock()
}

func TestSaveLoadInstalled(t *testing.T) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(t.TempDir(), "plugins")
	config.CacheDir = filepath.Join(t.TempDir(), "cache")

	registry1, _ := NewPluginRegistry(config)

	// Add installed plugin
	registry1.mu.Lock()
	registry1.installed["persist-test"] = &InstalledPlugin{
		Metadata: MarketplacePluginInfo{
			ID:      "persist-test",
			Name:    "Persist Test",
			Version: "1.0.0",
		},
		State:       PluginStateInstalled,
		InstalledAt: time.Now(),
	}
	registry1.mu.Unlock()

	registry1.saveInstalled()
	registry1.Close()

	// Create new registry and verify it loads
	registry2, _ := NewPluginRegistry(config)
	defer registry2.Close()

	installed := registry2.ListInstalled()
	found := false
	for _, p := range installed {
		if p.Metadata.ID == "persist-test" {
			found = true
			break
		}
	}
	if !found {
		t.Error("installed plugin not persisted")
	}
}

func TestDirectoryCreation(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(tempDir, "new", "plugins")
	config.CacheDir = filepath.Join(tempDir, "new", "cache")

	registry, err := NewPluginRegistry(config)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}
	defer registry.Close()

	// Verify directories exist
	if _, err := os.Stat(config.PluginsDir); os.IsNotExist(err) {
		t.Error("plugins directory not created")
	}
	if _, err := os.Stat(config.CacheDir); os.IsNotExist(err) {
		t.Error("cache directory not created")
	}
}

func BenchmarkPluginLoad(b *testing.B) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(b.TempDir(), "plugins")
	config.CacheDir = filepath.Join(b.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	registry.RegisterFactory("bench-plugin", func() (Plugin, error) {
		return &testPlugin{}, nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.LoadPlugin(context.Background(), "bench-plugin", nil)
		registry.UnloadPlugin("bench-plugin")
	}
}

func BenchmarkPluginSearch(b *testing.B) {
	config := DefaultPluginMarketplaceConfig()
	config.PluginsDir = filepath.Join(b.TempDir(), "plugins")
	config.CacheDir = filepath.Join(b.TempDir(), "cache")

	registry, _ := NewPluginRegistry(config)
	defer registry.Close()

	// Add some catalog entries
	registry.mu.Lock()
	for i := 0; i < 100; i++ {
		registry.catalog[string(rune('a'+i))] = &MarketplacePluginInfo{
			ID:        string(rune('a' + i)),
			Name:      "Plugin " + string(rune('a'+i)),
			Downloads: int64(i * 100),
		}
	}
	registry.catalogUpdated = time.Now()
	registry.mu.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Search(context.Background(), SearchOptions{
			Query:  "plugin",
			SortBy: "downloads",
			Limit:  10,
		})
	}
}
