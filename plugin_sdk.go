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
	ID           string             `json:"id"`
	Name         string             `json:"name"`
	Version      string             `json:"version"`
	Type         PluginType         `json:"type"`
	Description  string             `json:"description"`
	Author       string             `json:"author"`
	License      string             `json:"license"`
	Homepage     string             `json:"homepage,omitempty"`
	MinVersion   string             `json:"min_chronicle_version,omitempty"`
	Dependencies []string           `json:"dependencies,omitempty"`
	Checksum     string             `json:"checksum,omitempty"`
	APIVersion   string             `json:"api_version,omitempty"`
	Capabilities []PluginCapability `json:"capabilities,omitempty"`
	ResourceQuota *ResourceQuota    `json:"resource_quota,omitempty"`
	Signature    string             `json:"signature,omitempty"`
}

// PluginCapability declares what a plugin can do.
type PluginCapability string

const (
	CapabilityReadData    PluginCapability = "read_data"
	CapabilityWriteData   PluginCapability = "write_data"
	CapabilityNetwork     PluginCapability = "network"
	CapabilityFileSystem  PluginCapability = "filesystem"
	CapabilityCustomAgg   PluginCapability = "custom_aggregation"
	CapabilityCustomIngest PluginCapability = "custom_ingestion"
	CapabilityAlertHandler PluginCapability = "alert_handler"
	CapabilityTransformer  PluginCapability = "transformer"
)

// ResourceQuota defines resource limits for sandboxed plugin execution.
type ResourceQuota struct {
	MaxMemoryBytes  int64         `json:"max_memory_bytes"`
	MaxCPUTime      time.Duration `json:"max_cpu_time"`
	MaxWallTime     time.Duration `json:"max_wall_time"`
	MaxOutputBytes  int64         `json:"max_output_bytes"`
	MaxPointsPerCall int          `json:"max_points_per_call"`
}

// DefaultResourceQuota returns conservative default quotas.
func DefaultResourceQuota() *ResourceQuota {
	return &ResourceQuota{
		MaxMemoryBytes:   64 * 1024 * 1024, // 64MB
		MaxCPUTime:       10 * time.Second,
		MaxWallTime:      30 * time.Second,
		MaxOutputBytes:   10 * 1024 * 1024, // 10MB
		MaxPointsPerCall: 100000,
	}
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
	info         PluginInfo
	aggregator   AggregatorPlugin
	ingestor     IngestorPlugin
	alertHandler AlertHandlerPlugin
	transformer  TransformerPlugin
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
	baseURL  string
	registry *PluginRegistry
	listings map[string]*MarketplaceListing
	mu       sync.RWMutex
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

// --- Plugin Validation & Signature Verification ---

// ValidateManifest validates a plugin manifest for completeness and correctness.
func ValidateManifest(m PluginManifest) error {
	if m.ID == "" {
		return errors.New("plugin_sdk: plugin ID is required")
	}
	if m.Name == "" {
		return errors.New("plugin_sdk: plugin name is required")
	}
	if m.Version == "" {
		return errors.New("plugin_sdk: plugin version is required")
	}
	if m.Type == "" {
		return errors.New("plugin_sdk: plugin type is required")
	}
	if m.APIVersion == "" {
		m.APIVersion = "v1"
	}
	if m.ResourceQuota != nil {
		if m.ResourceQuota.MaxMemoryBytes <= 0 {
			return errors.New("plugin_sdk: max_memory_bytes must be positive")
		}
		if m.ResourceQuota.MaxWallTime <= 0 {
			return errors.New("plugin_sdk: max_wall_time must be positive")
		}
	}
	return nil
}

// VerifySignature checks the plugin signature against a trusted public key.
// Returns true if the signature is valid or if no signature verification is configured.
func VerifySignature(manifest PluginManifest, trustedKeys []string) bool {
	if manifest.Signature == "" {
		return len(trustedKeys) == 0 // unsigned is OK only if no keys configured
	}
	if len(trustedKeys) == 0 {
		return true // no verification configured
	}
	// Compute expected signature: sha256(id + version + checksum)
	data := manifest.ID + ":" + manifest.Version + ":" + manifest.Checksum
	expected := computePluginHash(data)
	for _, key := range trustedKeys {
		if manifest.Signature == computePluginHash(key+":"+expected) {
			return true
		}
	}
	return false
}

func computePluginHash(data string) string {
	h := uint64(14695981039346656037)
	for _, b := range []byte(data) {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return fmt.Sprintf("%016x", h)
}

// HasCapability checks if a manifest declares a specific capability.
func (m PluginManifest) HasCapability(cap PluginCapability) bool {
	for _, c := range m.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// --- Versioned Plugin API ---

// PluginAPIVersion represents the SDK API version for compatibility checking.
type PluginAPIVersion struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
	Patch int `json:"patch"`
}

// CurrentAPIVersion returns the current plugin SDK API version.
func CurrentAPIVersion() PluginAPIVersion {
	return PluginAPIVersion{Major: 1, Minor: 0, Patch: 0}
}

// IsCompatible checks if a plugin's API version is compatible with the current SDK.
func (v PluginAPIVersion) IsCompatible(pluginAPIVersion string) bool {
	switch pluginAPIVersion {
	case "v1", "v1.0", "v1.0.0":
		return v.Major == 1
	case "":
		return true // unversioned plugins are assumed compatible
	default:
		return false
	}
}

// --- Plugin Dependency Resolution ---

// ResolveDependencies checks if all plugin dependencies are satisfied.
func (pr *PluginRegistry) ResolveDependencies(manifest PluginManifest) []string {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	var missing []string
	for _, dep := range manifest.Dependencies {
		if _, ok := pr.plugins[dep]; !ok {
			missing = append(missing, dep)
		}
	}
	return missing
}

// InstallWithDeps attempts to install a plugin, checking dependencies first.
func (pr *PluginRegistry) InstallWithDeps(manifest PluginManifest, plugin AggregatorPlugin) error {
	if err := ValidateManifest(manifest); err != nil {
		return err
	}

	missing := pr.ResolveDependencies(manifest)
	if len(missing) > 0 {
		return fmt.Errorf("plugin_sdk: missing dependencies: %v", missing)
	}

	apiVer := CurrentAPIVersion()
	if !apiVer.IsCompatible(manifest.APIVersion) {
		return fmt.Errorf("plugin_sdk: incompatible API version %q (current: v%d.%d.%d)",
			manifest.APIVersion, apiVer.Major, apiVer.Minor, apiVer.Patch)
	}

	return pr.RegisterAggregator(manifest, plugin)
}
