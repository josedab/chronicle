package chronicle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// PluginType identifies the category of a plugin.
type PluginType string

const (
	PluginTypeAggregator     PluginType = "aggregator"
	PluginTypeStorageBackend PluginType = "storage_backend"
	PluginTypeIngestor       PluginType = "ingestor"
	PluginTypeAlertHandler   PluginType = "alert_handler"
	PluginTypeTransformer    PluginType = "transformer"
	PluginTypeExporter       PluginType = "exporter"
)

// PluginState is the lifecycle state of a plugin.
type PluginState string

const (
	PluginStateRegistered PluginState = "registered"
	PluginStateLoaded     PluginState = "loaded"
	PluginStateRunning    PluginState = "running"
	PluginStateStopped    PluginState = "stopped"
	PluginStateFailed     PluginState = "failed"
)

// PluginSDKConfig configures the plugin SDK and marketplace.
type PluginSDKConfig struct {
	// Enabled enables plugin support.
	Enabled bool `json:"enabled"`

	// PluginDir is the directory for installed plugins.
	PluginDir string `json:"plugin_dir"`

	// MaxPlugins limits the number of loaded plugins.
	MaxPlugins int `json:"max_plugins"`

	// MaxMemoryPerPlugin limits memory per plugin (bytes).
	MaxMemoryPerPlugin int64 `json:"max_memory_per_plugin"`

	// ExecutionTimeout limits plugin execution time.
	ExecutionTimeout time.Duration `json:"execution_timeout"`

	// MarketplaceURL is the plugin marketplace endpoint.
	MarketplaceURL string `json:"marketplace_url"`

	// EnableSandbox enables WASM sandboxing for untrusted plugins.
	EnableSandbox bool `json:"enable_sandbox"`

	// AutoUpdate enables automatic plugin updates.
	AutoUpdate bool `json:"auto_update"`
}

// DefaultPluginSDKConfig returns sensible defaults.
func DefaultPluginSDKConfig() PluginSDKConfig {
	return PluginSDKConfig{
		Enabled:            false,
		PluginDir:          "plugins",
		MaxPlugins:         64,
		MaxMemoryPerPlugin: 64 * 1024 * 1024, // 64MB
		ExecutionTimeout:   30 * time.Second,
		MarketplaceURL:     "https://marketplace.chronicle-db.io/api/v1",
		EnableSandbox:      true,
		AutoUpdate:         false,
	}
}

// --- Plugin Interfaces ---

// AggregatorPlugin is the interface for custom aggregation functions.
type AggregatorPlugin interface {
	Name() string
	Aggregate(values []float64) (float64, error)
	Reset()
}

// IngestorPlugin is the interface for custom ingestion formats.
type IngestorPlugin interface {
	Name() string
	Parse(data []byte) ([]Point, error)
	ContentType() string
}

// AlertHandlerPlugin is the interface for custom alert handlers.
type AlertHandlerPlugin interface {
	Name() string
	Handle(alert PluginAlertNotification) error
}

// TransformerPlugin transforms points in a pipeline.
type TransformerPlugin interface {
	Name() string
	Transform(points []Point) ([]Point, error)
}

// PluginAlertNotification is the payload sent to alert handler plugins.
type PluginAlertNotification struct {
	RuleName  string            `json:"rule_name"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Threshold float64           `json:"threshold"`
	Tags      map[string]string `json:"tags,omitempty"`
	FiredAt   time.Time         `json:"fired_at"`
	Message   string            `json:"message"`
}

// --- Plugin Metadata ---

// PluginManifest describes a plugin's metadata.
type PluginManifest struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Version      string     `json:"version"`
	Type         PluginType `json:"type"`
	Description  string     `json:"description"`
	Author       string     `json:"author"`
	License      string     `json:"license"`
	Homepage     string     `json:"homepage,omitempty"`
	MinVersion   string     `json:"min_chronicle_version,omitempty"`
	Dependencies []string   `json:"dependencies,omitempty"`
	Checksum     string     `json:"checksum,omitempty"`
}

// PluginInfo describes a loaded plugin's runtime info.
type PluginInfo struct {
	Manifest    PluginManifest `json:"manifest"`
	State       PluginState    `json:"state"`
	LoadedAt    time.Time      `json:"loaded_at"`
	LastError   string         `json:"last_error,omitempty"`
	InvokeCount int64          `json:"invoke_count"`
	MemoryUsed  int64          `json:"memory_used"`
}

// --- Plugin Registry ---

type pluginEntry struct {
	info        PluginInfo
	aggregator  AggregatorPlugin
	ingestor    IngestorPlugin
	alertHandler AlertHandlerPlugin
	transformer TransformerPlugin
}

// PluginRegistry manages plugin lifecycle and discovery.
type PluginRegistry struct {
	config  PluginSDKConfig
	plugins map[string]*pluginEntry
	mu      sync.RWMutex
}

// NewPluginRegistry creates a new plugin registry.
func NewPluginRegistry(config PluginSDKConfig) *PluginRegistry {
	return &PluginRegistry{
		config:  config,
		plugins: make(map[string]*pluginEntry),
	}
}

// RegisterAggregator registers a custom aggregation plugin.
func (pr *PluginRegistry) RegisterAggregator(manifest PluginManifest, plugin AggregatorPlugin) error {
	return pr.register(manifest, func(entry *pluginEntry) {
		entry.aggregator = plugin
	})
}

// RegisterIngestor registers a custom ingestion format plugin.
func (pr *PluginRegistry) RegisterIngestor(manifest PluginManifest, plugin IngestorPlugin) error {
	return pr.register(manifest, func(entry *pluginEntry) {
		entry.ingestor = plugin
	})
}

// RegisterAlertHandler registers a custom alert handler plugin.
func (pr *PluginRegistry) RegisterAlertHandler(manifest PluginManifest, plugin AlertHandlerPlugin) error {
	return pr.register(manifest, func(entry *pluginEntry) {
		entry.alertHandler = plugin
	})
}

// RegisterTransformer registers a custom transformer plugin.
func (pr *PluginRegistry) RegisterTransformer(manifest PluginManifest, plugin TransformerPlugin) error {
	return pr.register(manifest, func(entry *pluginEntry) {
		entry.transformer = plugin
	})
}

func (pr *PluginRegistry) register(manifest PluginManifest, setter func(*pluginEntry)) error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if manifest.ID == "" {
		return errors.New("plugin_sdk: plugin ID required")
	}
	if _, exists := pr.plugins[manifest.ID]; exists {
		return fmt.Errorf("plugin_sdk: plugin %q already registered", manifest.ID)
	}
	if len(pr.plugins) >= pr.config.MaxPlugins {
		return fmt.Errorf("plugin_sdk: max plugins (%d) reached", pr.config.MaxPlugins)
	}

	entry := &pluginEntry{
		info: PluginInfo{
			Manifest: manifest,
			State:    PluginStateLoaded,
			LoadedAt: time.Now(),
		},
	}
	setter(entry)
	pr.plugins[manifest.ID] = entry
	return nil
}

// Unregister removes a plugin.
func (pr *PluginRegistry) Unregister(id string) error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if _, ok := pr.plugins[id]; !ok {
		return fmt.Errorf("plugin_sdk: plugin %q not found", id)
	}
	delete(pr.plugins, id)
	return nil
}

// Get returns plugin info by ID.
func (pr *PluginRegistry) Get(id string) (*PluginInfo, bool) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	entry, ok := pr.plugins[id]
	if !ok {
		return nil, false
	}
	info := entry.info
	return &info, true
}

// List returns all registered plugins.
func (pr *PluginRegistry) List() []PluginInfo {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	result := make([]PluginInfo, 0, len(pr.plugins))
	for _, e := range pr.plugins {
		result = append(result, e.info)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Manifest.ID < result[j].Manifest.ID
	})
	return result
}

// ListByType returns plugins of a specific type.
func (pr *PluginRegistry) ListByType(pluginType PluginType) []PluginInfo {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	var result []PluginInfo
	for _, e := range pr.plugins {
		if e.info.Manifest.Type == pluginType {
			result = append(result, e.info)
		}
	}
	return result
}

// InvokeAggregator calls a registered aggregator plugin.
func (pr *PluginRegistry) InvokeAggregator(ctx context.Context, id string, values []float64) (float64, error) {
	pr.mu.RLock()
	entry, ok := pr.plugins[id]
	pr.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("plugin_sdk: aggregator %q not found", id)
	}
	if entry.aggregator == nil {
		return 0, fmt.Errorf("plugin_sdk: %q is not an aggregator", id)
	}

	// Execute with timeout
	type result struct {
		val float64
		err error
	}
	ch := make(chan result, 1)

	go func() {
		v, err := entry.aggregator.Aggregate(values)
		ch <- result{v, err}
	}()

	select {
	case r := <-ch:
		pr.mu.Lock()
		entry.info.InvokeCount++
		pr.mu.Unlock()
		return r.val, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// InvokeIngestor calls a registered ingestor plugin to parse data.
func (pr *PluginRegistry) InvokeIngestor(ctx context.Context, id string, data []byte) ([]Point, error) {
	pr.mu.RLock()
	entry, ok := pr.plugins[id]
	pr.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("plugin_sdk: ingestor %q not found", id)
	}
	if entry.ingestor == nil {
		return nil, fmt.Errorf("plugin_sdk: %q is not an ingestor", id)
	}

	type result struct {
		pts []Point
		err error
	}
	ch := make(chan result, 1)

	go func() {
		pts, err := entry.ingestor.Parse(data)
		ch <- result{pts, err}
	}()

	select {
	case r := <-ch:
		pr.mu.Lock()
		entry.info.InvokeCount++
		pr.mu.Unlock()
		return r.pts, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// --- Marketplace ---

// MarketplaceListing represents a plugin in the marketplace.
type MarketplaceListing struct {
	Manifest    PluginManifest `json:"manifest"`
	Downloads   int64          `json:"downloads"`
	Rating      float64        `json:"rating"`
	RatingCount int            `json:"rating_count"`
	Verified    bool           `json:"verified"`
	PublishedAt time.Time      `json:"published_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// MarketplaceSearch defines search parameters.
type MarketplaceSearch struct {
	Query    string     `json:"query,omitempty"`
	Type     PluginType `json:"type,omitempty"`
	SortBy   string     `json:"sort_by,omitempty"` // "downloads", "rating", "updated"
	Page     int        `json:"page,omitempty"`
	PageSize int        `json:"page_size,omitempty"`
}

// PluginMarketplace provides discovery and installation of plugins.
type PluginMarketplace struct {
	baseURL    string
	registry   *PluginRegistry
	listings   map[string]*MarketplaceListing
	mu         sync.RWMutex
}

// NewPluginMarketplace creates a new marketplace client.
func NewPluginMarketplace(baseURL string, registry *PluginRegistry) *PluginMarketplace {
	return &PluginMarketplace{
		baseURL:  baseURL,
		registry: registry,
		listings: make(map[string]*MarketplaceListing),
	}
}

// Search searches the marketplace for plugins matching criteria.
func (pm *PluginMarketplace) Search(search MarketplaceSearch) []MarketplaceListing {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var results []MarketplaceListing
	for _, listing := range pm.listings {
		if search.Type != "" && listing.Manifest.Type != search.Type {
			continue
		}
		results = append(results, *listing)
	}

	switch search.SortBy {
	case "downloads":
		sort.Slice(results, func(i, j int) bool { return results[i].Downloads > results[j].Downloads })
	case "rating":
		sort.Slice(results, func(i, j int) bool { return results[i].Rating > results[j].Rating })
	case "updated":
		sort.Slice(results, func(i, j int) bool { return results[i].UpdatedAt.After(results[j].UpdatedAt) })
	}

	// Paginate
	pageSize := search.PageSize
	if pageSize <= 0 {
		pageSize = 20
	}
	start := search.Page * pageSize
	if start >= len(results) {
		return nil
	}
	end := start + pageSize
	if end > len(results) {
		end = len(results)
	}
	return results[start:end]
}

// Publish adds a plugin listing to the marketplace.
func (pm *PluginMarketplace) Publish(manifest PluginManifest) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if manifest.ID == "" {
		return errors.New("plugin_sdk: plugin ID required")
	}

	pm.listings[manifest.ID] = &MarketplaceListing{
		Manifest:    manifest,
		PublishedAt: time.Now(),
		UpdatedAt:   time.Now(),
	}
	return nil
}

// Rate adds a rating for a plugin.
func (pm *PluginMarketplace) Rate(pluginID string, rating float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	listing, ok := pm.listings[pluginID]
	if !ok {
		return fmt.Errorf("plugin_sdk: plugin %q not found", pluginID)
	}

	if rating < 1 || rating > 5 {
		return errors.New("plugin_sdk: rating must be between 1 and 5")
	}

	// Update weighted average
	total := listing.Rating * float64(listing.RatingCount)
	listing.RatingCount++
	listing.Rating = (total + rating) / float64(listing.RatingCount)
	return nil
}

// ListingCount returns the number of marketplace listings.
func (pm *PluginMarketplace) ListingCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.listings)
}
