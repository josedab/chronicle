package pluginmkt

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
	"strings"
	"sync/atomic"
	"time"
)

// RegisterFactory registers a built-in plugin factory
func (r *PluginRegistry) RegisterFactory(pluginID string, factory PluginFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[pluginID] = factory
}

// Search searches for plugins in the registry
func (r *PluginRegistry) Search(ctx context.Context, opts SearchOptions) ([]MarketplacePluginInfo, error) {
	atomic.AddInt64(&r.searchCount, 1)

	if err := r.refreshCatalog(ctx, false); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []MarketplacePluginInfo
	query := strings.ToLower(opts.Query)

	for _, meta := range r.catalog {

		if opts.Type != "" && meta.Type != opts.Type {
			continue
		}

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

		if opts.VerifiedOnly && !meta.Verified {
			continue
		}

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

	sortPlugins(results, opts.SortBy, opts.SortDesc)

	if opts.Offset > 0 && opts.Offset < len(results) {
		results = results[opts.Offset:]
	}
	if opts.Limit > 0 && opts.Limit < len(results) {
		results = results[:opts.Limit]
	}

	return results, nil
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

	meta, err := r.GetPlugin(ctx, pluginID)
	if err != nil {
		return err
	}

	r.mu.RLock()
	if installed, ok := r.installed[pluginID]; ok && !opts.Force {
		r.mu.RUnlock()
		if installed.State == PluginStateInstalled || installed.State == PluginStateEnabled {
			return fmt.Errorf("plugin already installed: %s", pluginID)
		}
	}
	r.mu.RUnlock()

	if !r.config.AllowUnverified && !meta.Verified {
		if !r.isTrustedPublisher(meta.Author) {
			return fmt.Errorf("plugin not verified and author not trusted: %s", meta.Author)
		}
	}

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

	r.mu.Lock()
	r.installed[pluginID] = &InstalledPlugin{
		Metadata: *meta,
		State:    PluginStateInstalling,
	}
	r.mu.Unlock()

	data, err := r.downloadPlugin(ctx, meta)
	if err != nil {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("failed to download plugin: %w", err)
	}

	checksum := sha256.Sum256(data)
	if hex.EncodeToString(checksum[:]) != meta.Checksum {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("checksum mismatch for plugin: %s", pluginID)
	}

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

	metaFile := filepath.Join(installPath, "metadata.json")
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(metaFile, metaJSON, 0644); err != nil {
		r.mu.Lock()
		delete(r.installed, pluginID)
		r.mu.Unlock()
		return fmt.Errorf("failed to write metadata: %w", err)
	}

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

	if err := r.saveInstalled(); err != nil {
		return fmt.Errorf("failed to save installed list: %w", err)
	}

	atomic.AddInt64(&r.installCount, 1)
	return nil
}

// Uninstall removes an installed plugin
func (r *PluginRegistry) Uninstall(ctx context.Context, pluginID string) error {
	r.mu.Lock()
	installed, ok := r.installed[pluginID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("plugin not installed: %s", pluginID)
	}

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

	if err := r.stopPlugin(pluginID); err != nil {
		return fmt.Errorf("failed to stop plugin: %w", err)
	}

	if installed.InstallPath != "" {
		if err := os.RemoveAll(installed.InstallPath); err != nil {
			return fmt.Errorf("failed to remove plugin files: %w", err)
		}
	}

	r.mu.Lock()
	delete(r.installed, pluginID)
	r.mu.Unlock()

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

	if installed.Metadata.Version == meta.Version {
		return nil
	}

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
					ReleaseNotes:   "",
				})
			}
		}
	}

	return updates, nil
}

// LoadPlugin loads and initializes a plugin
func (r *PluginRegistry) LoadPlugin(ctx context.Context, pluginID string, config map[string]interface{}) (Plugin, error) {

	r.mu.RLock()
	if instance, ok := r.instances[pluginID]; ok {
		r.mu.RUnlock()
		return instance.Plugin, nil
	}
	r.mu.RUnlock()

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

	if err := plugin.Init(config); err != nil {
		plugin.Close()
		return nil, fmt.Errorf("failed to initialize plugin: %w", err)
	}

	r.mu.Lock()
	r.instances[pluginID] = &PluginInstance{
		ID:      pluginID,
		Plugin:  plugin,
		Started: time.Now(),
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
func (r *PluginRegistry) ListRunning() []*PluginInstance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*PluginInstance, 0, len(r.instances))
	for _, p := range r.instances {
		result = append(result, p)
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

// Close shuts down the registry
func (r *PluginRegistry) Close() error {
	r.cancel()

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

	url := fmt.Sprintf("%s/plugins", r.config.RegistryURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {

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

	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {

		return &placeholderPlugin{
			capabilities: installed.Metadata.Capabilities,
		}, nil
	}

	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin %s: %w", pluginPath, err)
	}

	sym, err := p.Lookup("NewPlugin")
	if err != nil {
		return nil, fmt.Errorf("plugin %s missing NewPlugin symbol: %w", pluginID, err)
	}

	newPluginFunc, ok := sym.(func() Plugin)
	if !ok {

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
