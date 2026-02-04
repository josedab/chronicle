package chronicle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"plugin"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PluginType represents the type of plugin
type PluginType string

const (
	PluginTypeCompression  PluginType = "compression"
	PluginTypeStorage      PluginType = "storage"
	PluginTypeQueryFunc    PluginType = "query_function"
	PluginTypeIntegration  PluginType = "integration"
	PluginTypeEncoder      PluginType = "encoder"
	PluginTypeAuthProvider PluginType = "auth_provider"
	PluginTypeExporter     PluginType = "exporter"
)

// PluginState represents the installation state of a plugin
type PluginState string

const (
	PluginStateAvailable   PluginState = "available"
	PluginStateInstalled   PluginState = "installed"
	PluginStateEnabled     PluginState = "enabled"
	PluginStateDisabled    PluginState = "disabled"
	PluginStateOutdated    PluginState = "outdated"
	PluginStateInstalling  PluginState = "installing"
	PluginStateUninstalling PluginState = "uninstalling"
)

// PluginCapability represents what a plugin can do
type PluginCapability string

const (
	CapabilityCompress    PluginCapability = "compress"
	CapabilityDecompress  PluginCapability = "decompress"
	CapabilityStore       PluginCapability = "store"
	CapabilityRetrieve    PluginCapability = "retrieve"
	CapabilityQuery       PluginCapability = "query"
	CapabilityAggregate   PluginCapability = "aggregate"
	CapabilityTransform   PluginCapability = "transform"
	CapabilityExport      PluginCapability = "export"
	CapabilityImport      PluginCapability = "import"
	CapabilityAuthenticate PluginCapability = "authenticate"
)

// PluginMarketplaceConfig configures the plugin marketplace
type PluginMarketplaceConfig struct {
	// RegistryURL is the URL of the plugin registry
	RegistryURL string `json:"registry_url"`

	// PluginsDir is the directory where plugins are installed
	PluginsDir string `json:"plugins_dir"`

	// CacheDir is the directory for caching plugin metadata
	CacheDir string `json:"cache_dir"`

	// AutoUpdate enables automatic plugin updates
	AutoUpdate bool `json:"auto_update"`

	// UpdateCheckInterval is how often to check for updates
	UpdateCheckInterval time.Duration `json:"update_check_interval"`

	// AllowUnverified allows installation of unverified plugins
	AllowUnverified bool `json:"allow_unverified"`

	// TrustedPublishers is a list of trusted plugin publishers
	TrustedPublishers []string `json:"trusted_publishers"`

	// MaxConcurrentDownloads limits concurrent plugin downloads
	MaxConcurrentDownloads int `json:"max_concurrent_downloads"`

	// NetworkTimeout is the timeout for network operations
	NetworkTimeout time.Duration `json:"network_timeout"`

	// EnableSandbox enables plugin sandboxing
	EnableSandbox bool `json:"enable_sandbox"`

	// EnableMetrics enables plugin usage metrics collection
	EnableMetrics bool `json:"enable_metrics"`
}

// DefaultPluginMarketplaceConfig returns default configuration
func DefaultPluginMarketplaceConfig() *PluginMarketplaceConfig {
	homeDir, _ := os.UserHomeDir()
	return &PluginMarketplaceConfig{
		RegistryURL:            "https://registry.chronicle.dev/v1",
		PluginsDir:             filepath.Join(homeDir, ".chronicle", "plugins"),
		CacheDir:               filepath.Join(homeDir, ".chronicle", "cache"),
		AutoUpdate:             false,
		UpdateCheckInterval:    24 * time.Hour,
		AllowUnverified:        false,
		TrustedPublishers:      []string{"chronicle-dev", "verified"},
		MaxConcurrentDownloads: 3,
		NetworkTimeout:         30 * time.Second,
		EnableSandbox:          true,
		EnableMetrics:          true,
	}
}

// MarketplacePluginInfo contains information about a plugin
type MarketplacePluginInfo struct {
	// ID is the unique identifier for the plugin
	ID string `json:"id"`

	// Name is the human-readable name
	Name string `json:"name"`

	// Description describes what the plugin does
	Description string `json:"description"`

	// Version is the semantic version
	Version string `json:"version"`

	// Type is the plugin type
	Type PluginType `json:"type"`

	// Capabilities lists what this plugin can do
	Capabilities []PluginCapability `json:"capabilities"`

	// Author is the plugin author/publisher
	Author string `json:"author"`

	// Homepage is the plugin's homepage URL
	Homepage string `json:"homepage,omitempty"`

	// Repository is the source code repository URL
	Repository string `json:"repository,omitempty"`

	// License is the SPDX license identifier
	License string `json:"license"`

	// MinChronicleVersion is the minimum compatible Chronicle version
	MinChronicleVersion string `json:"min_chronicle_version"`

	// MaxChronicleVersion is the maximum compatible Chronicle version (optional)
	MaxChronicleVersion string `json:"max_chronicle_version,omitempty"`

	// Dependencies lists other plugins this depends on
	Dependencies []PluginDependency `json:"dependencies,omitempty"`

	// Keywords for search
	Keywords []string `json:"keywords,omitempty"`

	// Downloads is the total download count
	Downloads int64 `json:"downloads"`

	// Rating is the average user rating (1-5)
	Rating float64 `json:"rating"`

	// RatingCount is the number of ratings
	RatingCount int `json:"rating_count"`

	// Verified indicates if the plugin is verified
	Verified bool `json:"verified"`

	// Featured indicates if the plugin is featured
	Featured bool `json:"featured"`

	// CreatedAt is when the plugin was first published
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the plugin was last updated
	UpdatedAt time.Time `json:"updated_at"`

	// Checksum is the SHA256 checksum of the plugin binary
	Checksum string `json:"checksum"`

	// Size is the plugin size in bytes
	Size int64 `json:"size"`

	// Platforms lists supported platforms (e.g., "linux/amd64", "darwin/arm64")
	Platforms []string `json:"platforms"`
}

// PluginDependency represents a dependency on another plugin
type PluginDependency struct {
	ID         string `json:"id"`
	MinVersion string `json:"min_version,omitempty"`
	MaxVersion string `json:"max_version,omitempty"`
}

// InstalledPlugin represents a locally installed plugin
type InstalledPlugin struct {
	Metadata      MarketplacePluginInfo `json:"metadata"`
	State         PluginState    `json:"state"`
	InstalledAt   time.Time      `json:"installed_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	InstallPath   string         `json:"install_path"`
	ConfigPath    string         `json:"config_path,omitempty"`
	LastUsed      time.Time      `json:"last_used,omitempty"`
	UsageCount    int64          `json:"usage_count"`
	ErrorCount    int64          `json:"error_count"`
	LastError     string         `json:"last_error,omitempty"`
	LastErrorTime time.Time      `json:"last_error_time,omitempty"`
}

// PluginInstance represents a running plugin instance
type PluginInstance struct {
	ID       string
	Metadata MarketplacePluginInfo
	Plugin   Plugin
	Started  time.Time
	mu       sync.RWMutex
	stats    PluginInstanceStats
}

// PluginInstanceStats tracks plugin usage statistics
type PluginInstanceStats struct {
	Invocations int64
	Errors      int64
	TotalTime   time.Duration
	LastInvoke  time.Time
}

// Plugin is the interface all plugins must implement
type Plugin interface {
	// Init initializes the plugin with configuration
	Init(config map[string]interface{}) error

	// Capabilities returns the plugin's capabilities
	Capabilities() []PluginCapability

	// Close releases plugin resources
	Close() error
}

// MarketplaceCompressionPlugin is the interface for compression plugins
type MarketplaceCompressionPlugin interface {
	Plugin

	// Compress compresses data
	Compress(data []byte) ([]byte, error)

	// Decompress decompresses data
	Decompress(data []byte) ([]byte, error)

	// CompressionRatio returns the expected compression ratio
	CompressionRatio() float64
}

// StoragePlugin is the interface for storage backend plugins
type StoragePlugin interface {
	Plugin

	// Store stores data at the given key
	Store(ctx context.Context, key string, data []byte) error

	// Retrieve retrieves data for the given key
	Retrieve(ctx context.Context, key string) ([]byte, error)

	// Delete removes data for the given key
	Delete(ctx context.Context, key string) error

	// List lists keys with the given prefix
	List(ctx context.Context, prefix string) ([]string, error)

	// Exists checks if a key exists
	Exists(ctx context.Context, key string) (bool, error)
}

// QueryFunctionPlugin is the interface for custom query functions
type QueryFunctionPlugin interface {
	Plugin

	// Name returns the function name
	Name() string

	// ArgTypes returns the expected argument types
	ArgTypes() []string

	// ReturnType returns the return type
	ReturnType() string

	// Execute executes the function
	Execute(args ...interface{}) (interface{}, error)
}

// IntegrationPlugin is the interface for external integrations
type IntegrationPlugin interface {
	Plugin

	// Connect establishes connection to external system
	Connect(ctx context.Context) error

	// Disconnect closes connection
	Disconnect(ctx context.Context) error

	// IsConnected checks if connection is active
	IsConnected() bool

	// Export exports data to external system
	Export(ctx context.Context, data interface{}) error

	// Import imports data from external system
	Import(ctx context.Context, query interface{}) (interface{}, error)
}

// PluginFactory creates plugin instances
type PluginFactory func() (Plugin, error)

// PluginRegistry manages plugin discovery and installation
type PluginRegistry struct {
	mu             sync.RWMutex
	config         *PluginMarketplaceConfig
	installed      map[string]*InstalledPlugin
	instances      map[string]*PluginInstance
	factories      map[string]PluginFactory
	catalog        map[string]*MarketplacePluginInfo
	catalogUpdated time.Time
	httpClient     *http.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	searchCount      int64
	installCount     int64
	uninstallCount   int64
	updateCount      int64
	downloadBytes    int64
}

// NewPluginRegistry creates a new plugin registry
func NewPluginRegistry(config *PluginMarketplaceConfig) (*PluginRegistry, error) {
	if config == nil {
		config = DefaultPluginMarketplaceConfig()
	}

	// Create directories
	if err := os.MkdirAll(config.PluginsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create plugins directory: %w", err)
	}
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	registry := &PluginRegistry{
		config:     config,
		installed:  make(map[string]*InstalledPlugin),
		instances:  make(map[string]*PluginInstance),
		factories:  make(map[string]PluginFactory),
		catalog:    make(map[string]*MarketplacePluginInfo),
		httpClient: &http.Client{Timeout: config.NetworkTimeout},
		ctx:        ctx,
		cancel:     cancel,
	}

	// Load installed plugins
	if err := registry.loadInstalled(); err != nil {
		cancel()
		return nil, err
	}

	// Start background workers
	if config.AutoUpdate {
		registry.wg.Add(1)
		go registry.updateChecker()
	}

	return registry, nil
}

// RegisterFactory registers a built-in plugin factory
func (r *PluginRegistry) RegisterFactory(pluginID string, factory PluginFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[pluginID] = factory
}

// Search searches for plugins in the registry
func (r *PluginRegistry) Search(ctx context.Context, opts SearchOptions) ([]MarketplacePluginInfo, error) {
	atomic.AddInt64(&r.searchCount, 1)

	// Refresh catalog if needed
	if err := r.refreshCatalog(ctx, false); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []MarketplacePluginInfo
	query := strings.ToLower(opts.Query)

	for _, meta := range r.catalog {
		// Filter by type
		if opts.Type != "" && meta.Type != opts.Type {
			continue
		}

		// Filter by capability
		if opts.Capability != "" {
			hasCapability := false
			for _, cap := range meta.Capabilities {
				if cap == opts.Capability {
					hasCapability = true
					break
				}
			}
			if !hasCapability {
				continue
			}
		}

		// Filter by verified status
		if opts.VerifiedOnly && !meta.Verified {
			continue
		}

		// Filter by query
		if query != "" {
			matches := strings.Contains(strings.ToLower(meta.Name), query) ||
				strings.Contains(strings.ToLower(meta.Description), query) ||
				strings.Contains(strings.ToLower(meta.ID), query)

			for _, keyword := range meta.Keywords {
				if strings.Contains(strings.ToLower(keyword), query) {
					matches = true
					break
				}
			}

			if !matches {
				continue
			}
		}

		results = append(results, *meta)
	}

	// Sort results
	sortPlugins(results, opts.SortBy, opts.SortDesc)

	// Apply pagination
	if opts.Offset > 0 && opts.Offset < len(results) {
		results = results[opts.Offset:]
	}
	if opts.Limit > 0 && opts.Limit < len(results) {
		results = results[:opts.Limit]
	}

	return results, nil
}

// SearchOptions configures plugin search
type SearchOptions struct {
	Query        string           `json:"query"`
	Type         PluginType       `json:"type,omitempty"`
	Capability   PluginCapability `json:"capability,omitempty"`
	VerifiedOnly bool             `json:"verified_only"`
	SortBy       string           `json:"sort_by"` // name, downloads, rating, updated
	SortDesc     bool             `json:"sort_desc"`
	Limit        int              `json:"limit"`
	Offset       int              `json:"offset"`
}

// GetPlugin retrieves metadata for a specific plugin
func (r *PluginRegistry) GetPlugin(ctx context.Context, pluginID string) (*MarketplacePluginInfo, error) {
	if err := r.refreshCatalog(ctx, false); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if meta, ok := r.catalog[pluginID]; ok {
		return meta, nil
	}
	return nil, fmt.Errorf("plugin not found: %s", pluginID)
}

// Install installs a plugin from the registry
func (r *PluginRegistry) Install(ctx context.Context, pluginID string, opts InstallOptions) error {
	// Get plugin metadata
	meta, err := r.GetPlugin(ctx, pluginID)
	if err != nil {
		return err
	}

	// Check if already installed
	r.mu.RLock()
	if installed, ok := r.installed[pluginID]; ok && !opts.Force {
		r.mu.RUnlock()
		if installed.State == PluginStateInstalled || installed.State == PluginStateEnabled {
			return fmt.Errorf("plugin already installed: %s", pluginID)
		}
	}
	r.mu.RUnlock()

	// Verify plugin
	if !r.config.AllowUnverified && !meta.Verified {
		if !r.isTrustedPublisher(meta.Author) {
			return fmt.Errorf("plugin not verified and author not trusted: %s", meta.Author)
		}
	}

	// Check platform compatibility
	platform := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	compatible := false
	for _, p := range meta.Platforms {
		if p == platform || p == "any" {
			compatible = true
			break
		}
	}
	if !compatible {
		return fmt.Errorf("plugin not compatible with platform: %s", platform)
	}

	// Install dependencies first
	for _, dep := range meta.Dependencies {
		r.mu.RLock()
		depInstalled := r.installed[dep.ID]
		r.mu.RUnlock()

		if depInstalled == nil {
			if err := r.Install(ctx, dep.ID, opts); err != nil {
				return fmt.Errorf("failed to install dependency %s: %w", dep.ID, err)
			}
		}
	}

	// Mark as installing
	r.mu.Lock()
	r.installed[pluginID] = &InstalledPlugin{
		Metadata: *meta,
		State:    PluginStateInstalling,
	}
	r.mu.Unlock()

	// Download plugin
	data, err := r.downloadPlugin(ctx, meta)
	if err != nil {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("failed to download plugin: %w", err)
	}

	// Verify checksum
	checksum := sha256.Sum256(data)
	if hex.EncodeToString(checksum[:]) != meta.Checksum {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("checksum mismatch for plugin: %s", pluginID)
	}

	// Install to disk
	installPath := filepath.Join(r.config.PluginsDir, pluginID)
	if err := os.MkdirAll(installPath, 0755); err != nil {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("failed to create plugin directory: %w", err)
	}

	pluginFile := filepath.Join(installPath, "plugin.so")
	if err := os.WriteFile(pluginFile, data, 0755); err != nil {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("failed to write plugin file: %w", err)
	}

	// Save metadata
	metaFile := filepath.Join(installPath, "metadata.json")
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(metaFile, metaJSON, 0644); err != nil {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Update installed state
	now := time.Now()
	r.mu.Lock()
	r.installed[pluginID] = &InstalledPlugin{
		Metadata:    *meta,
		State:       PluginStateInstalled,
		InstalledAt: now,
		UpdatedAt:   now,
		InstallPath: installPath,
	}
	r.mu.Unlock()

	// Save installed plugins list
	if err := r.saveInstalled(); err != nil {
		return fmt.Errorf("failed to save installed list: %w", err)
	}

	atomic.AddInt64(&r.installCount, 1)
	return nil
}

// InstallOptions configures plugin installation
type InstallOptions struct {
	Version string `json:"version,omitempty"` // Specific version to install
	Force   bool   `json:"force"`             // Force reinstall
}

// Uninstall removes an installed plugin
func (r *PluginRegistry) Uninstall(ctx context.Context, pluginID string) error {
	r.mu.Lock()
	installed, ok := r.installed[pluginID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("plugin not installed: %s", pluginID)
	}

	// Check if other plugins depend on this
	for id, inst := range r.installed {
		if id == pluginID {
			continue
		}
		for _, dep := range inst.Metadata.Dependencies {
			if dep.ID == pluginID {
				r.mu.Unlock()
				return fmt.Errorf("plugin %s depends on %s", id, pluginID)
			}
		}
	}

	installed.State = PluginStateUninstalling
	r.mu.Unlock()

	// Stop instance if running
	if err := r.stopPlugin(pluginID); err != nil {
		return fmt.Errorf("failed to stop plugin: %w", err)
	}

	// Remove files
	if installed.InstallPath != "" {
		if err := os.RemoveAll(installed.InstallPath); err != nil {
			return fmt.Errorf("failed to remove plugin files: %w", err)
		}
	}

	// Update state
	r.mu.Lock()
	delete(r.installed, pluginID)
	r.mu.Unlock()

	// Save installed plugins list
	if err := r.saveInstalled(); err != nil {
		return fmt.Errorf("failed to save installed list: %w", err)
	}

	atomic.AddInt64(&r.uninstallCount, 1)
	return nil
}

// Enable enables an installed plugin
func (r *PluginRegistry) Enable(ctx context.Context, pluginID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	installed, ok := r.installed[pluginID]
	if !ok {
		return fmt.Errorf("plugin not installed: %s", pluginID)
	}

	if installed.State == PluginStateEnabled {
		return nil
	}

	installed.State = PluginStateEnabled
	return r.saveInstalled()
}

// Disable disables a plugin
func (r *PluginRegistry) Disable(ctx context.Context, pluginID string) error {
	// Stop the plugin first
	if err := r.stopPlugin(pluginID); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	installed, ok := r.installed[pluginID]
	if !ok {
		return fmt.Errorf("plugin not installed: %s", pluginID)
	}

	installed.State = PluginStateDisabled
	return r.saveInstalled()
}

// Update updates a plugin to the latest version
func (r *PluginRegistry) Update(ctx context.Context, pluginID string) error {
	// Get latest version from registry
	meta, err := r.GetPlugin(ctx, pluginID)
	if err != nil {
		return err
	}

	r.mu.RLock()
	installed, ok := r.installed[pluginID]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("plugin not installed: %s", pluginID)
	}

	// Check if update needed
	if installed.Metadata.Version == meta.Version {
		return nil // Already up to date
	}

	// Reinstall with force
	if err := r.Install(ctx, pluginID, InstallOptions{Force: true}); err != nil {
		return err
	}

	atomic.AddInt64(&r.updateCount, 1)
	return nil
}

// CheckUpdates checks for available updates
func (r *PluginRegistry) CheckUpdates(ctx context.Context) ([]PluginUpdate, error) {
	if err := r.refreshCatalog(ctx, true); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var updates []PluginUpdate
	for id, installed := range r.installed {
		if meta, ok := r.catalog[id]; ok {
			if compareVersions(meta.Version, installed.Metadata.Version) > 0 {
				updates = append(updates, PluginUpdate{
					PluginID:       id,
					CurrentVersion: installed.Metadata.Version,
					LatestVersion:  meta.Version,
					ReleaseNotes:   "", // Would come from detailed endpoint
				})
			}
		}
	}

	return updates, nil
}

// PluginUpdate represents an available update
type PluginUpdate struct {
	PluginID       string `json:"plugin_id"`
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version"`
	ReleaseNotes   string `json:"release_notes,omitempty"`
}

// LoadPlugin loads and initializes a plugin
func (r *PluginRegistry) LoadPlugin(ctx context.Context, pluginID string, config map[string]interface{}) (Plugin, error) {
	// Check if already loaded
	r.mu.RLock()
	if instance, ok := r.instances[pluginID]; ok {
		r.mu.RUnlock()
		return instance.Plugin, nil
	}
	r.mu.RUnlock()

	// Check if we have a factory for this plugin
	r.mu.RLock()
	factory, hasFactory := r.factories[pluginID]
	installed, isInstalled := r.installed[pluginID]
	r.mu.RUnlock()

	var plugin Plugin
	var err error

	if hasFactory {
		plugin, err = factory()
	} else if isInstalled {
		plugin, err = r.loadFromDisk(installed)
	} else {
		return nil, fmt.Errorf("plugin not found: %s", pluginID)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load plugin: %w", err)
	}

	// Initialize plugin
	if err := plugin.Init(config); err != nil {
		plugin.Close()
		return nil, fmt.Errorf("failed to initialize plugin: %w", err)
	}

	// Store instance
	r.mu.Lock()
	r.instances[pluginID] = &PluginInstance{
		ID:       pluginID,
		Plugin:   plugin,
		Started:  time.Now(),
	}
	if installed != nil {
		r.instances[pluginID].Metadata = installed.Metadata
	}
	r.mu.Unlock()

	return plugin, nil
}

// UnloadPlugin stops and unloads a plugin
func (r *PluginRegistry) UnloadPlugin(pluginID string) error {
	return r.stopPlugin(pluginID)
}

// ListInstalled returns all installed plugins
func (r *PluginRegistry) ListInstalled() []InstalledPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]InstalledPlugin, 0, len(r.installed))
	for _, p := range r.installed {
		result = append(result, *p)
	}
	return result
}

// ListRunning returns all currently loaded plugins
func (r *PluginRegistry) ListRunning() []PluginInstance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]PluginInstance, 0, len(r.instances))
	for _, p := range r.instances {
		result = append(result, *p)
	}
	return result
}

// GetFeatured returns featured plugins
func (r *PluginRegistry) GetFeatured(ctx context.Context) ([]MarketplacePluginInfo, error) {
	return r.Search(ctx, SearchOptions{VerifiedOnly: true, SortBy: "downloads", SortDesc: true, Limit: 10})
}

// GetCategories returns available plugin categories
func (r *PluginRegistry) GetCategories() []PluginCategory {
	return []PluginCategory{
		{Type: PluginTypeCompression, Name: "Compression", Description: "Data compression algorithms"},
		{Type: PluginTypeStorage, Name: "Storage", Description: "Storage backends"},
		{Type: PluginTypeQueryFunc, Name: "Query Functions", Description: "Custom query functions"},
		{Type: PluginTypeIntegration, Name: "Integrations", Description: "External system integrations"},
		{Type: PluginTypeEncoder, Name: "Encoders", Description: "Data encoding formats"},
		{Type: PluginTypeAuthProvider, Name: "Auth Providers", Description: "Authentication providers"},
		{Type: PluginTypeExporter, Name: "Exporters", Description: "Data exporters"},
	}
}

// PluginCategory represents a plugin category
type PluginCategory struct {
	Type        PluginType `json:"type"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
}

// Stats returns marketplace statistics
func (r *PluginRegistry) Stats() PluginMarketplaceStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var enabled, disabled int
	for _, p := range r.installed {
		if p.State == PluginStateEnabled {
			enabled++
		} else if p.State == PluginStateDisabled {
			disabled++
		}
	}

	return PluginMarketplaceStats{
		InstalledCount:   len(r.installed),
		EnabledCount:     enabled,
		DisabledCount:    disabled,
		RunningCount:     len(r.instances),
		CatalogSize:      len(r.catalog),
		SearchCount:      atomic.LoadInt64(&r.searchCount),
		InstallCount:     atomic.LoadInt64(&r.installCount),
		UninstallCount:   atomic.LoadInt64(&r.uninstallCount),
		UpdateCount:      atomic.LoadInt64(&r.updateCount),
		DownloadBytes:    atomic.LoadInt64(&r.downloadBytes),
		CatalogUpdatedAt: r.catalogUpdated,
	}
}

// PluginMarketplaceStats contains marketplace statistics
type PluginMarketplaceStats struct {
	InstalledCount   int       `json:"installed_count"`
	EnabledCount     int       `json:"enabled_count"`
	DisabledCount    int       `json:"disabled_count"`
	RunningCount     int       `json:"running_count"`
	CatalogSize      int       `json:"catalog_size"`
	SearchCount      int64     `json:"search_count"`
	InstallCount     int64     `json:"install_count"`
	UninstallCount   int64     `json:"uninstall_count"`
	UpdateCount      int64     `json:"update_count"`
	DownloadBytes    int64     `json:"download_bytes"`
	CatalogUpdatedAt time.Time `json:"catalog_updated_at"`
}

// Close shuts down the registry
func (r *PluginRegistry) Close() error {
	r.cancel()

	// Stop all running plugins
	r.mu.Lock()
	pluginIDs := make([]string, 0, len(r.instances))
	for id := range r.instances {
		pluginIDs = append(pluginIDs, id)
	}
	r.mu.Unlock()

	for _, id := range pluginIDs {
		r.stopPlugin(id)
	}

	r.wg.Wait()
	return nil
}

// Internal methods

func (r *PluginRegistry) loadInstalled() error {
	installedFile := filepath.Join(r.config.PluginsDir, "installed.json")
	data, err := os.ReadFile(installedFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var installed map[string]*InstalledPlugin
	if err := json.Unmarshal(data, &installed); err != nil {
		return err
	}

	r.installed = installed
	return nil
}

func (r *PluginRegistry) saveInstalled() error {
	data, err := json.MarshalIndent(r.installed, "", "  ")
	if err != nil {
		return err
	}

	installedFile := filepath.Join(r.config.PluginsDir, "installed.json")
	return os.WriteFile(installedFile, data, 0644)
}

func (r *PluginRegistry) refreshCatalog(ctx context.Context, force bool) error {
	r.mu.RLock()
	needsRefresh := force || r.catalogUpdated.IsZero() || time.Since(r.catalogUpdated) > time.Hour
	r.mu.RUnlock()

	if !needsRefresh {
		return nil
	}

	// Try to fetch from registry
	url := fmt.Sprintf("%s/plugins", r.config.RegistryURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		// Try cache
		return r.loadCatalogCache()
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return r.loadCatalogCache()
	}

	var plugins []MarketplacePluginInfo
	if err := json.NewDecoder(resp.Body).Decode(&plugins); err != nil {
		return err
	}

	r.mu.Lock()
	r.catalog = make(map[string]*MarketplacePluginInfo)
	for i := range plugins {
		r.catalog[plugins[i].ID] = &plugins[i]
	}
	r.catalogUpdated = time.Now()
	r.mu.Unlock()

	// Save to cache
	return r.saveCatalogCache(plugins)
}

func (r *PluginRegistry) loadCatalogCache() error {
	cacheFile := filepath.Join(r.config.CacheDir, "catalog.json")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return err
	}

	var plugins []MarketplacePluginInfo
	if err := json.Unmarshal(data, &plugins); err != nil {
		return err
	}

	r.mu.Lock()
	r.catalog = make(map[string]*MarketplacePluginInfo)
	for i := range plugins {
		r.catalog[plugins[i].ID] = &plugins[i]
	}
	r.mu.Unlock()

	return nil
}

func (r *PluginRegistry) saveCatalogCache(plugins []MarketplacePluginInfo) error {
	data, err := json.MarshalIndent(plugins, "", "  ")
	if err != nil {
		return err
	}

	cacheFile := filepath.Join(r.config.CacheDir, "catalog.json")
	return os.WriteFile(cacheFile, data, 0644)
}

func (r *PluginRegistry) downloadPlugin(ctx context.Context, meta *MarketplacePluginInfo) ([]byte, error) {
	platform := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	url := fmt.Sprintf("%s/plugins/%s/download?version=%s&platform=%s",
		r.config.RegistryURL, meta.ID, meta.Version, platform)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download failed: %s", resp.Status)
	}

	var buf bytes.Buffer
	n, err := io.Copy(&buf, resp.Body)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&r.downloadBytes, n)
	return buf.Bytes(), nil
}

func (r *PluginRegistry) loadFromDisk(installed *InstalledPlugin) (Plugin, error) {
	pluginID := installed.Metadata.ID
	pluginPath := filepath.Join(r.config.PluginsDir, pluginID, pluginID+".so")

	// Check if plugin file exists
	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		// Fall back to placeholder for plugins that don't have .so files
		return &placeholderPlugin{
			capabilities: installed.Metadata.Capabilities,
		}, nil
	}

	// Load the plugin using Go's plugin package
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin %s: %w", pluginPath, err)
	}

	// Look up the NewPlugin symbol
	sym, err := p.Lookup("NewPlugin")
	if err != nil {
		return nil, fmt.Errorf("plugin %s missing NewPlugin symbol: %w", pluginID, err)
	}

	// Assert the symbol is a function that returns Plugin
	newPluginFunc, ok := sym.(func() Plugin)
	if !ok {
		// Try alternative signature with error
		newPluginFuncWithErr, ok := sym.(func() (Plugin, error))
		if !ok {
			return nil, fmt.Errorf("plugin %s has invalid NewPlugin signature", pluginID)
		}
		return newPluginFuncWithErr()
	}

	return newPluginFunc(), nil
}

func (r *PluginRegistry) stopPlugin(pluginID string) error {
	r.mu.Lock()
	instance, ok := r.instances[pluginID]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	delete(r.instances, pluginID)
	r.mu.Unlock()

	return instance.Plugin.Close()
}

func (r *PluginRegistry) isTrustedPublisher(author string) bool {
	for _, trusted := range r.config.TrustedPublishers {
		if trusted == author {
			return true
		}
	}
	return false
}

func (r *PluginRegistry) updateChecker() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.UpdateCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.CheckUpdates(r.ctx)
		}
	}
}

// Helper types and functions

type placeholderPlugin struct {
	capabilities []PluginCapability
	config       map[string]interface{}
}

func (p *placeholderPlugin) Init(config map[string]interface{}) error {
	p.config = config
	return nil
}

func (p *placeholderPlugin) Capabilities() []PluginCapability {
	return p.capabilities
}

func (p *placeholderPlugin) Close() error {
	return nil
}

func sortPlugins(plugins []MarketplacePluginInfo, sortBy string, desc bool) {
	sort.Slice(plugins, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "name":
			less = plugins[i].Name < plugins[j].Name
		case "downloads":
			less = plugins[i].Downloads < plugins[j].Downloads
		case "rating":
			less = plugins[i].Rating < plugins[j].Rating
		case "updated":
			less = plugins[i].UpdatedAt.Before(plugins[j].UpdatedAt)
		default:
			less = plugins[i].Downloads < plugins[j].Downloads
		}
		if desc {
			return !less
		}
		return less
	})
}

func compareVersions(v1, v2 string) int {
	// Simple version comparison - in production, use semver library
	if v1 == v2 {
		return 0
	}
	if v1 > v2 {
		return 1
	}
	return -1
}

// PluginSDK provides helpers for building plugins

// PluginSDK helps create Chronicle plugins
type PluginSDK struct {
	metadata MarketplacePluginInfo
	config   map[string]interface{}
	logger   func(level, msg string, args ...interface{})
}

// NewPluginSDK creates a new plugin SDK instance
func NewPluginSDK(metadata MarketplacePluginInfo) *PluginSDK {
	return &PluginSDK{
		metadata: metadata,
		config:   make(map[string]interface{}),
		logger:   func(level, msg string, args ...interface{}) {},
	}
}

// SetLogger sets the logging function
func (sdk *PluginSDK) SetLogger(logger func(level, msg string, args ...interface{})) {
	sdk.logger = logger
}

// GetConfig returns configuration value
func (sdk *PluginSDK) GetConfig(key string) interface{} {
	return sdk.config[key]
}

// GetConfigString returns configuration value as string
func (sdk *PluginSDK) GetConfigString(key string, defaultValue string) string {
	if v, ok := sdk.config[key].(string); ok {
		return v
	}
	return defaultValue
}

// GetConfigInt returns configuration value as int
func (sdk *PluginSDK) GetConfigInt(key string, defaultValue int) int {
	if v, ok := sdk.config[key].(float64); ok {
		return int(v)
	}
	if v, ok := sdk.config[key].(int); ok {
		return v
	}
	return defaultValue
}

// GetConfigBool returns configuration value as bool
func (sdk *PluginSDK) GetConfigBool(key string, defaultValue bool) bool {
	if v, ok := sdk.config[key].(bool); ok {
		return v
	}
	return defaultValue
}

// Log logs a message
func (sdk *PluginSDK) Log(level, msg string, args ...interface{}) {
	sdk.logger(level, msg, args...)
}

// Info logs an info message
func (sdk *PluginSDK) Info(msg string, args ...interface{}) {
	sdk.Log("info", msg, args...)
}

// Error logs an error message
func (sdk *PluginSDK) Error(msg string, args ...interface{}) {
	sdk.Log("error", msg, args...)
}

// Debug logs a debug message
func (sdk *PluginSDK) Debug(msg string, args ...interface{}) {
	sdk.Log("debug", msg, args...)
}

// PluginManifest is the manifest file format for plugins
type PluginManifest struct {
	APIVersion  string                 `json:"api_version"`
	Metadata    MarketplacePluginInfo         `json:"metadata"`
	EntryPoint  string                 `json:"entry_point"`
	Config      []PluginConfigOption   `json:"config,omitempty"`
	Permissions []string               `json:"permissions,omitempty"`
}

// PluginConfigOption defines a configuration option
type PluginConfigOption struct {
	Name        string      `json:"name"`
	Type        string      `json:"type"` // string, int, bool, float, []string
	Required    bool        `json:"required"`
	Default     interface{} `json:"default,omitempty"`
	Description string      `json:"description"`
	Validate    string      `json:"validate,omitempty"` // Validation regex or rule
}

// ValidateManifest validates a plugin manifest
func ValidateManifest(manifest *PluginManifest) error {
	if manifest.APIVersion == "" {
		return fmt.Errorf("api_version is required")
	}
	if manifest.Metadata.ID == "" {
		return fmt.Errorf("metadata.id is required")
	}
	if manifest.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	if manifest.Metadata.Version == "" {
		return fmt.Errorf("metadata.version is required")
	}
	if manifest.Metadata.Type == "" {
		return fmt.Errorf("metadata.type is required")
	}
	if manifest.EntryPoint == "" {
		return fmt.Errorf("entry_point is required")
	}
	return nil
}

// CLI commands for plugin management

// PluginCLI provides CLI commands for plugin management
type PluginCLI struct {
	registry *PluginRegistry
	output   io.Writer
}

// NewPluginCLI creates a new plugin CLI
func NewPluginCLI(registry *PluginRegistry, output io.Writer) *PluginCLI {
	return &PluginCLI{
		registry: registry,
		output:   output,
	}
}

// Search searches for plugins
func (cli *PluginCLI) Search(query string, pluginType string) error {
	opts := SearchOptions{
		Query:    query,
		SortBy:   "downloads",
		SortDesc: true,
		Limit:    20,
	}
	if pluginType != "" {
		opts.Type = PluginType(pluginType)
	}

	results, err := cli.registry.Search(context.Background(), opts)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		fmt.Fprintln(cli.output, "No plugins found")
		return nil
	}

	fmt.Fprintf(cli.output, "Found %d plugins:\n\n", len(results))
	for _, p := range results {
		verified := ""
		if p.Verified {
			verified = " ✓"
		}
		fmt.Fprintf(cli.output, "  %s%s (%s)\n", p.Name, verified, p.Version)
		fmt.Fprintf(cli.output, "    %s\n", p.Description)
		fmt.Fprintf(cli.output, "    Downloads: %d | Rating: %.1f\n\n", p.Downloads, p.Rating)
	}

	return nil
}

// Install installs a plugin
func (cli *PluginCLI) Install(pluginID string, force bool) error {
	fmt.Fprintf(cli.output, "Installing %s...\n", pluginID)
	
	opts := InstallOptions{Force: force}
	if err := cli.registry.Install(context.Background(), pluginID, opts); err != nil {
		return fmt.Errorf("installation failed: %w", err)
	}

	fmt.Fprintf(cli.output, "✓ Successfully installed %s\n", pluginID)
	return nil
}

// Uninstall removes a plugin
func (cli *PluginCLI) Uninstall(pluginID string) error {
	fmt.Fprintf(cli.output, "Uninstalling %s...\n", pluginID)
	
	if err := cli.registry.Uninstall(context.Background(), pluginID); err != nil {
		return fmt.Errorf("uninstall failed: %w", err)
	}

	fmt.Fprintf(cli.output, "✓ Successfully uninstalled %s\n", pluginID)
	return nil
}

// List lists installed plugins
func (cli *PluginCLI) List() error {
	installed := cli.registry.ListInstalled()
	
	if len(installed) == 0 {
		fmt.Fprintln(cli.output, "No plugins installed")
		return nil
	}

	fmt.Fprintf(cli.output, "Installed plugins (%d):\n\n", len(installed))
	for _, p := range installed {
		state := string(p.State)
		fmt.Fprintf(cli.output, "  %s (%s) [%s]\n", p.Metadata.Name, p.Metadata.Version, state)
		fmt.Fprintf(cli.output, "    Type: %s | Installed: %s\n\n", 
			p.Metadata.Type, p.InstalledAt.Format("2006-01-02"))
	}

	return nil
}

// Update updates plugins
func (cli *PluginCLI) Update(pluginID string) error {
	if pluginID == "" {
		// Update all
		updates, err := cli.registry.CheckUpdates(context.Background())
		if err != nil {
			return err
		}

		if len(updates) == 0 {
			fmt.Fprintln(cli.output, "All plugins are up to date")
			return nil
		}

		for _, u := range updates {
			fmt.Fprintf(cli.output, "Updating %s (%s -> %s)...\n", 
				u.PluginID, u.CurrentVersion, u.LatestVersion)
			if err := cli.registry.Update(context.Background(), u.PluginID); err != nil {
				fmt.Fprintf(cli.output, "  ✗ Failed: %v\n", err)
			} else {
				fmt.Fprintf(cli.output, "  ✓ Updated\n")
			}
		}
	} else {
		fmt.Fprintf(cli.output, "Updating %s...\n", pluginID)
		if err := cli.registry.Update(context.Background(), pluginID); err != nil {
			return fmt.Errorf("update failed: %w", err)
		}
		fmt.Fprintf(cli.output, "✓ Successfully updated %s\n", pluginID)
	}

	return nil
}

// Info shows plugin details
func (cli *PluginCLI) Info(pluginID string) error {
	meta, err := cli.registry.GetPlugin(context.Background(), pluginID)
	if err != nil {
		return err
	}

	fmt.Fprintf(cli.output, "Plugin: %s\n", meta.Name)
	fmt.Fprintf(cli.output, "ID: %s\n", meta.ID)
	fmt.Fprintf(cli.output, "Version: %s\n", meta.Version)
	fmt.Fprintf(cli.output, "Type: %s\n", meta.Type)
	fmt.Fprintf(cli.output, "Author: %s\n", meta.Author)
	fmt.Fprintf(cli.output, "License: %s\n", meta.License)
	fmt.Fprintf(cli.output, "Description: %s\n", meta.Description)
	fmt.Fprintf(cli.output, "\n")
	
	if meta.Verified {
		fmt.Fprintf(cli.output, "✓ Verified\n")
	}
	
	fmt.Fprintf(cli.output, "Downloads: %d\n", meta.Downloads)
	fmt.Fprintf(cli.output, "Rating: %.1f (%d ratings)\n", meta.Rating, meta.RatingCount)
	
	if len(meta.Capabilities) > 0 {
		fmt.Fprintf(cli.output, "\nCapabilities:\n")
		for _, cap := range meta.Capabilities {
			fmt.Fprintf(cli.output, "  - %s\n", cap)
		}
	}
	
	if len(meta.Dependencies) > 0 {
		fmt.Fprintf(cli.output, "\nDependencies:\n")
		for _, dep := range meta.Dependencies {
			fmt.Fprintf(cli.output, "  - %s", dep.ID)
			if dep.MinVersion != "" {
				fmt.Fprintf(cli.output, " (>= %s)", dep.MinVersion)
			}
			fmt.Fprintln(cli.output)
		}
	}
	
	if len(meta.Platforms) > 0 {
		fmt.Fprintf(cli.output, "\nPlatforms: %s\n", strings.Join(meta.Platforms, ", "))
	}

	return nil
}
