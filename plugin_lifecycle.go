package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// PluginLifecycleConfig configures the plugin lifecycle manager.
type PluginLifecycleConfig struct {
	PluginDir           string        `json:"plugin_dir"`
	CacheDir            string        `json:"cache_dir"`
	AutoUpdate          bool          `json:"auto_update"`
	SandboxEnabled      bool          `json:"sandbox_enabled"`
	MaxPlugins          int           `json:"max_plugins"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	InstallTimeout      time.Duration `json:"install_timeout"`
}

// DefaultPluginLifecycleConfig returns sensible defaults for plugin lifecycle management.
func DefaultPluginLifecycleConfig() PluginLifecycleConfig {
	return PluginLifecycleConfig{
		PluginDir:           "plugins",
		CacheDir:            "plugin-cache",
		AutoUpdate:          false,
		SandboxEnabled:      true,
		MaxPlugins:          50,
		HealthCheckInterval: 30 * time.Second,
		InstallTimeout:      5 * time.Minute,
	}
}

// InstalledPluginInfo describes a locally installed plugin and its runtime state.
type InstalledPluginInfo struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	Version         string    `json:"version"`
	Type            string    `json:"type"`
	State           string    `json:"state"` // installed, enabled, disabled, error
	InstalledAt     time.Time `json:"installed_at"`
	LastHealthCheck time.Time `json:"last_health_check"`
	Healthy         bool      `json:"healthy"`
	ErrorCount      int       `json:"error_count"`
	InvokeCount     int64     `json:"invoke_count"`
}

// PluginEvent records a lifecycle event for a plugin.
type PluginEvent struct {
	ID        string    `json:"id"`
	PluginID  string    `json:"plugin_id"`
	Type      string    `json:"type"` // installed, uninstalled, enabled, disabled, updated, error
	Timestamp time.Time `json:"timestamp"`
	Details   string    `json:"details"`
}

// PluginUpdate describes an available update for an installed plugin.
type PluginUpdate struct {
	PluginID       string `json:"plugin_id"`
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version"`
	ReleaseNotes   string `json:"release_notes"`
}

// PluginLifecycleStats aggregates statistics across all managed plugins.
type PluginLifecycleStats struct {
	InstalledCount   int       `json:"installed_count"`
	EnabledCount     int       `json:"enabled_count"`
	DisabledCount    int       `json:"disabled_count"`
	ErrorCount       int       `json:"error_count"`
	TotalInvocations int64     `json:"total_invocations"`
	LastHealthCheck  time.Time `json:"last_health_check"`
}

// PluginLifecycleManager provides complete lifecycle management for plugins
// including installation, health checking, updates, and HTTP API registration.
type PluginLifecycleManager struct {
	config      PluginLifecycleConfig
	registry    *PluginRegistry
	marketplace *PluginMarketplace
	wasmRuntime *WASMRuntime
	installed   map[string]*InstalledPluginInfo
	running     bool
	cancel      context.CancelFunc
	mu          sync.RWMutex
	events      []PluginEvent
}

// NewPluginLifecycleManager creates a new plugin lifecycle manager.
func NewPluginLifecycleManager(config PluginLifecycleConfig, registry *PluginRegistry, marketplace *PluginMarketplace) *PluginLifecycleManager {
	if config.MaxPlugins <= 0 {
		config.MaxPlugins = 50
	}
	if config.HealthCheckInterval <= 0 {
		config.HealthCheckInterval = 30 * time.Second
	}
	if config.InstallTimeout <= 0 {
		config.InstallTimeout = 5 * time.Minute
	}
	return &PluginLifecycleManager{
		config:      config,
		registry:    registry,
		marketplace: marketplace,
		installed:   make(map[string]*InstalledPluginInfo),
		events:      make([]PluginEvent, 0),
	}
}

// SetWASMRuntime attaches a WASM runtime for sandbox execution.
func (plm *PluginLifecycleManager) SetWASMRuntime(rt *WASMRuntime) {
	plm.mu.Lock()
	defer plm.mu.Unlock()
	plm.wasmRuntime = rt
}

// Start begins the background health check loop.
func (plm *PluginLifecycleManager) Start(ctx context.Context) error {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	if plm.running {
		return fmt.Errorf("plugin_lifecycle: already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	plm.cancel = cancel
	plm.running = true

	go plm.healthCheckLoop(ctx)
	return nil
}

// Stop terminates the background health check loop.
func (plm *PluginLifecycleManager) Stop() {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	if plm.cancel != nil {
		plm.cancel()
		plm.cancel = nil
	}
	plm.running = false
}

// InstallPlugin downloads a plugin from the marketplace, validates it, and registers it.
func (plm *PluginLifecycleManager) InstallPlugin(id, version string) error {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	if id == "" {
		return fmt.Errorf("plugin_lifecycle: plugin ID required")
	}
	if _, exists := plm.installed[id]; exists {
		return fmt.Errorf("plugin_lifecycle: plugin %q already installed", id)
	}
	if len(plm.installed) >= plm.config.MaxPlugins {
		return fmt.Errorf("plugin_lifecycle: max plugins (%d) reached", plm.config.MaxPlugins)
	}

	// Look up listing in marketplace
	listings := plm.marketplace.Search(MarketplaceSearch{Query: id, PageSize: 10})
	var found *MarketplaceListing
	for i := range listings {
		if listings[i].Manifest.ID == id {
			found = &listings[i]
			break
		}
	}
	if found == nil {
		return fmt.Errorf("plugin_lifecycle: plugin %q not found in marketplace", id)
	}

	if version == "" {
		version = found.Manifest.Version
	}

	info := &InstalledPluginInfo{
		ID:          id,
		Name:        found.Manifest.Name,
		Version:     version,
		Type:        string(found.Manifest.Type),
		State:       "installed",
		InstalledAt: time.Now(),
		Healthy:     true,
	}
	plm.installed[id] = info

	plm.recordEvent(id, "installed", fmt.Sprintf("installed version %s", version))
	return nil
}

// UninstallPlugin deregisters and removes a plugin.
func (plm *PluginLifecycleManager) UninstallPlugin(id string) error {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	if _, exists := plm.installed[id]; !exists {
		return fmt.Errorf("plugin_lifecycle: plugin %q not installed", id)
	}

	// Attempt to unregister from the plugin registry
	_ = plm.registry.Unregister(id)

	delete(plm.installed, id)
	plm.recordEvent(id, "uninstalled", "plugin removed")
	return nil
}

// EnablePlugin transitions an installed or disabled plugin to the enabled state.
func (plm *PluginLifecycleManager) EnablePlugin(id string) error {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	info, exists := plm.installed[id]
	if !exists {
		return fmt.Errorf("plugin_lifecycle: plugin %q not installed", id)
	}
	if info.State == "enabled" {
		return nil
	}

	info.State = "enabled"
	plm.recordEvent(id, "enabled", "plugin enabled")
	return nil
}

// DisablePlugin transitions an enabled plugin to the disabled state.
func (plm *PluginLifecycleManager) DisablePlugin(id string) error {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	info, exists := plm.installed[id]
	if !exists {
		return fmt.Errorf("plugin_lifecycle: plugin %q not installed", id)
	}
	if info.State == "disabled" {
		return nil
	}

	info.State = "disabled"
	plm.recordEvent(id, "disabled", "plugin disabled")
	return nil
}

// UpdatePlugin checks for a newer version in the marketplace and applies the update.
func (plm *PluginLifecycleManager) UpdatePlugin(id string) error {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	info, exists := plm.installed[id]
	if !exists {
		return fmt.Errorf("plugin_lifecycle: plugin %q not installed", id)
	}

	listings := plm.marketplace.Search(MarketplaceSearch{Query: id, PageSize: 10})
	var found *MarketplaceListing
	for i := range listings {
		if listings[i].Manifest.ID == id {
			found = &listings[i]
			break
		}
	}
	if found == nil {
		return fmt.Errorf("plugin_lifecycle: plugin %q not found in marketplace", id)
	}

	if found.Manifest.Version == info.Version {
		return fmt.Errorf("plugin_lifecycle: plugin %q already at latest version %s", id, info.Version)
	}

	oldVersion := info.Version
	info.Version = found.Manifest.Version
	plm.recordEvent(id, "updated", fmt.Sprintf("updated from %s to %s", oldVersion, info.Version))
	return nil
}

// ListInstalled returns a snapshot of all installed plugins sorted by ID.
func (plm *PluginLifecycleManager) ListInstalled() []InstalledPluginInfo {
	plm.mu.RLock()
	defer plm.mu.RUnlock()

	result := make([]InstalledPluginInfo, 0, len(plm.installed))
	for _, info := range plm.installed {
		result = append(result, *info)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

// GetPlugin returns info for a single installed plugin.
func (plm *PluginLifecycleManager) GetPlugin(id string) (*InstalledPluginInfo, error) {
	plm.mu.RLock()
	defer plm.mu.RUnlock()

	info, exists := plm.installed[id]
	if !exists {
		return nil, fmt.Errorf("plugin_lifecycle: plugin %q not installed", id)
	}
	cp := *info
	return &cp, nil
}

// healthCheck runs a single health check pass over all enabled plugins.
func (plm *PluginLifecycleManager) healthCheck() {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	now := time.Now()
	for id, info := range plm.installed {
		if info.State != "enabled" {
			continue
		}

		// Check the plugin registry for runtime state
		regInfo, ok := plm.registry.Get(id)
		if ok && regInfo.State == PluginStateFailed {
			info.Healthy = false
			info.ErrorCount++
			info.State = "error"
			plm.recordEvent(id, "error", "health check failed: plugin in failed state")
		} else {
			info.Healthy = true
		}
		info.LastHealthCheck = now

		if ok {
			info.InvokeCount = regInfo.InvokeCount
		}
	}
}

func (plm *PluginLifecycleManager) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(plm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			plm.healthCheck()
		}
	}
}

// CheckUpdates queries the marketplace for available updates to installed plugins.
func (plm *PluginLifecycleManager) CheckUpdates() []PluginUpdate {
	plm.mu.RLock()
	installed := make(map[string]*InstalledPluginInfo, len(plm.installed))
	for k, v := range plm.installed {
		installed[k] = v
	}
	plm.mu.RUnlock()

	var updates []PluginUpdate
	for id, info := range installed {
		listings := plm.marketplace.Search(MarketplaceSearch{Query: id, PageSize: 10})
		for _, listing := range listings {
			if listing.Manifest.ID == id && listing.Manifest.Version != info.Version {
				updates = append(updates, PluginUpdate{
					PluginID:       id,
					CurrentVersion: info.Version,
					LatestVersion:  listing.Manifest.Version,
				})
				break
			}
		}
	}
	return updates
}

// Events returns a copy of all recorded plugin lifecycle events.
func (plm *PluginLifecycleManager) Events() []PluginEvent {
	plm.mu.RLock()
	defer plm.mu.RUnlock()

	out := make([]PluginEvent, len(plm.events))
	copy(out, plm.events)
	return out
}

// Stats returns aggregate statistics for all managed plugins.
func (plm *PluginLifecycleManager) Stats() PluginLifecycleStats {
	plm.mu.RLock()
	defer plm.mu.RUnlock()

	var stats PluginLifecycleStats
	stats.InstalledCount = len(plm.installed)
	for _, info := range plm.installed {
		switch info.State {
		case "enabled":
			stats.EnabledCount++
		case "disabled":
			stats.DisabledCount++
		case "error":
			stats.ErrorCount++
		}
		stats.TotalInvocations += info.InvokeCount
		if info.LastHealthCheck.After(stats.LastHealthCheck) {
			stats.LastHealthCheck = info.LastHealthCheck
		}
	}
	return stats
}

// recordEvent appends a lifecycle event (caller must hold plm.mu).
func (plm *PluginLifecycleManager) recordEvent(pluginID, eventType, details string) {
	plm.events = append(plm.events, PluginEvent{
		ID:        fmt.Sprintf("evt-%d", len(plm.events)+1),
		PluginID:  pluginID,
		Type:      eventType,
		Timestamp: time.Now(),
		Details:   details,
	})
}

// RegisterHTTPHandlers registers plugin management API endpoints on the given mux.
func (plm *PluginLifecycleManager) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/plugins", plm.handlePlugins)
	mux.HandleFunc("/api/v1/plugins/install", plm.handleInstall)
	mux.HandleFunc("/api/v1/plugins/updates", plm.handleUpdates)
	mux.HandleFunc("/api/v1/plugins/events", plm.handleEvents)
	mux.HandleFunc("/api/v1/plugins/stats", plm.handleStats)
	mux.HandleFunc("/api/v1/plugins/", plm.handlePluginAction)
}

func (plm *PluginLifecycleManager) handlePlugins(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, plm.ListInstalled())
}

func (plm *PluginLifecycleManager) handleInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID      string `json:"id"`
		Version string `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := plm.InstallPlugin(req.ID, req.Version); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSONStatus(w, http.StatusCreated, map[string]string{
		"status": "installed",
		"id":     req.ID,
	})
}

func (plm *PluginLifecycleManager) handleUpdates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, plm.CheckUpdates())
}

func (plm *PluginLifecycleManager) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, plm.Events())
}

func (plm *PluginLifecycleManager) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, plm.Stats())
}

// handlePluginAction routes /api/v1/plugins/{id}[/action] requests.
func (plm *PluginLifecycleManager) handlePluginAction(w http.ResponseWriter, r *http.Request) {
	// Parse path: /api/v1/plugins/{id} or /api/v1/plugins/{id}/{action}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/plugins/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, "plugin ID required", http.StatusBadRequest)
		return
	}

	id := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	switch {
	case action == "" && r.Method == http.MethodDelete:
		// DELETE /api/v1/plugins/{id}
		if err := plm.UninstallPlugin(id); err != nil {
			writeError(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	case action == "enable" && r.Method == http.MethodPost:
		// POST /api/v1/plugins/{id}/enable
		if err := plm.EnablePlugin(id); err != nil {
			writeError(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]string{"status": "enabled", "id": id})

	case action == "disable" && r.Method == http.MethodPost:
		// POST /api/v1/plugins/{id}/disable
		if err := plm.DisablePlugin(id); err != nil {
			writeError(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]string{"status": "disabled", "id": id})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
