package pluginmkt

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
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
	PluginStateAvailable    PluginState = "available"
	PluginStateInstalled    PluginState = "installed"
	PluginStateEnabled      PluginState = "enabled"
	PluginStateDisabled     PluginState = "disabled"
	PluginStateOutdated     PluginState = "outdated"
	PluginStateInstalling   PluginState = "installing"
	PluginStateUninstalling PluginState = "uninstalling"
)

// PluginCapability represents what a plugin can do
type PluginCapability string

const (
	CapabilityCompress     PluginCapability = "compress"
	CapabilityDecompress   PluginCapability = "decompress"
	CapabilityStore        PluginCapability = "store"
	CapabilityRetrieve     PluginCapability = "retrieve"
	CapabilityQuery        PluginCapability = "query"
	CapabilityAggregate    PluginCapability = "aggregate"
	CapabilityTransform    PluginCapability = "transform"
	CapabilityExport       PluginCapability = "export"
	CapabilityImport       PluginCapability = "import"
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
	State         PluginState           `json:"state"`
	InstalledAt   time.Time             `json:"installed_at"`
	UpdatedAt     time.Time             `json:"updated_at"`
	InstallPath   string                `json:"install_path"`
	ConfigPath    string                `json:"config_path,omitempty"`
	LastUsed      time.Time             `json:"last_used,omitempty"`
	UsageCount    int64                 `json:"usage_count"`
	ErrorCount    int64                 `json:"error_count"`
	LastError     string                `json:"last_error,omitempty"`
	LastErrorTime time.Time             `json:"last_error_time,omitempty"`
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
	searchCount    int64
	installCount   int64
	uninstallCount int64
	updateCount    int64
	downloadBytes  int64
}

// NewPluginRegistry creates a new plugin registry
func NewPluginRegistry(config *PluginMarketplaceConfig) (*PluginRegistry, error) {
	if config == nil {
		config = DefaultPluginMarketplaceConfig()
	}

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

	if err := registry.loadInstalled(); err != nil {
		cancel()
		return nil, err
	}

	if config.AutoUpdate {
		registry.wg.Add(1)
		go registry.updateChecker()
	}

	return registry, nil
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

// InstallOptions configures plugin installation
type InstallOptions struct {
	Version string `json:"version,omitempty"` // Specific version to install
	Force   bool   `json:"force"`             // Force reinstall
}

// PluginUpdate represents an available update
type PluginUpdate struct {
	PluginID       string `json:"plugin_id"`
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version"`
	ReleaseNotes   string `json:"release_notes,omitempty"`
}

// PluginCategory represents a plugin category
type PluginCategory struct {
	Type        PluginType `json:"type"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
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

type placeholderPlugin struct {
	capabilities []PluginCapability
	config       map[string]interface{}
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

	if v1 == v2 {
		return 0
	}
	if v1 > v2 {
		return 1
	}
	return -1
}

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

// PluginManifest is the manifest file format for plugins
type PluginManifest struct {
	APIVersion  string                `json:"api_version"`
	Metadata    MarketplacePluginInfo `json:"metadata"`
	EntryPoint  string                `json:"entry_point"`
	Config      []PluginConfigOption  `json:"config,omitempty"`
	Permissions []string              `json:"permissions,omitempty"`
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
