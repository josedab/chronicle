package chronicle

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// WASMPluginType defines the type of operation a WASM plugin provides.
type WASMPluginType string

const (
	WASMPluginTransform  WASMPluginType = "transform"
	WASMPluginAggregate  WASMPluginType = "aggregate"
	WASMPluginAlert      WASMPluginType = "alert"
	WASMPluginFilter     WASMPluginType = "filter"
)

// WASMPluginManifest describes a WASM plugin's capabilities and requirements.
type WASMPluginManifest struct {
	Name            string            `json:"name"`
	Version         string            `json:"version"`
	Description     string            `json:"description"`
	Author          string            `json:"author"`
	PluginType      WASMPluginType    `json:"plugin_type"`
	ExportedFuncs   []string          `json:"exported_functions"`
	RequiredHostFuncs []string        `json:"required_host_functions"`
	MaxMemoryMB     int               `json:"max_memory_mb"`
	MaxExecTimeMs   int64             `json:"max_exec_time_ms"`
	Dependencies    []PluginDependency `json:"dependencies,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// PluginDependency describes a plugin dependency.
type PluginDependency struct {
	Name       string `json:"name"`
	MinVersion string `json:"min_version"`
	MaxVersion string `json:"max_version,omitempty"`
	Optional   bool   `json:"optional"`
}

// PluginSignature contains ed25519 signature verification data.
type PluginSignature struct {
	PublicKeyHex string `json:"public_key"`
	SignatureHex string `json:"signature"`
	SignedAt     time.Time `json:"signed_at"`
}

// WASMMarketplaceEntry extends marketplace listing with WASM-specific metadata.
type WASMMarketplaceEntry struct {
	Manifest     WASMPluginManifest `json:"manifest"`
	WASMBytes    []byte             `json:"-"`
	WASMSize     int64              `json:"wasm_size"`
	Signature    *PluginSignature   `json:"signature,omitempty"`
	Published    bool               `json:"published"`
	Verified     bool               `json:"verified"`
	Downloads    int64              `json:"downloads"`
	PublishedAt  time.Time          `json:"published_at"`
}

// WASMPluginInstallation tracks an installed WASM plugin.
type WASMPluginInstallation struct {
	Name         string         `json:"name"`
	Version      string         `json:"version"`
	PluginType   WASMPluginType `json:"plugin_type"`
	Enabled      bool           `json:"enabled"`
	InstalledAt  time.Time      `json:"installed_at"`
	LastRunAt    time.Time      `json:"last_run_at,omitempty"`
	RunCount     int64          `json:"run_count"`
	ErrorCount   int64          `json:"error_count"`
	Pinned       bool           `json:"pinned"` // version pinned
}

// WASMMarketplaceConfig configures the WASM marketplace.
type WASMMarketplaceConfig struct {
	MaxPlugins         int    `json:"max_plugins"`
	MaxPluginSizeBytes int64  `json:"max_plugin_size_bytes"`
	VerifySignatures   bool   `json:"verify_signatures"`
	EnableHotReload    bool   `json:"enable_hot_reload"`
	SandboxEnabled     bool   `json:"sandbox_enabled"`
	CacheDir           string `json:"cache_dir"`
	MaxMemoryPerPlugin int64  `json:"max_memory_per_plugin"` // bytes
	MaxExecTimeMs      int64  `json:"max_exec_time_ms"`
}

// DefaultWASMMarketplaceConfig returns sensible defaults.
func DefaultWASMMarketplaceConfig() WASMMarketplaceConfig {
	return WASMMarketplaceConfig{
		MaxPlugins:         100,
		MaxPluginSizeBytes: 50 * 1024 * 1024, // 50MB
		VerifySignatures:   true,
		EnableHotReload:    true,
		SandboxEnabled:     true,
		MaxMemoryPerPlugin: 64 * 1024 * 1024, // 64MB
		MaxExecTimeMs:      5000,              // 5s
	}
}

// WASMMarketplace manages WASM plugin discovery, installation, and lifecycle.
type WASMMarketplace struct {
	config        WASMMarketplaceConfig
	runtime       *WASMRuntime
	registry      map[string]*WASMMarketplaceEntry
	installations map[string]*WASMPluginInstallation
	trustedKeys   map[string]ed25519.PublicKey
	cache         map[string]*pluginCacheEntry
	mu            sync.RWMutex
}

// pluginCacheEntry holds cached plugin data with integrity info.
type pluginCacheEntry struct {
	WASMBytes   []byte    `json:"-"`
	SHA256Hash  string    `json:"sha256_hash"`
	CachedAt    time.Time `json:"cached_at"`
	SizeBytes   int64     `json:"size_bytes"`
	Verified    bool      `json:"verified"`
}

// NewWASMMarketplace creates a new WASM marketplace.
func NewWASMMarketplace(runtime *WASMRuntime, config WASMMarketplaceConfig) *WASMMarketplace {
	return &WASMMarketplace{
		config:        config,
		runtime:       runtime,
		registry:      make(map[string]*WASMMarketplaceEntry),
		installations: make(map[string]*WASMPluginInstallation),
		trustedKeys:   make(map[string]ed25519.PublicKey),
		cache:         make(map[string]*pluginCacheEntry),
	}
}

// AddTrustedKey adds a trusted ed25519 public key for signature verification.
func (m *WASMMarketplace) AddTrustedKey(name string, pubKeyHex string) error {
	keyBytes, err := hex.DecodeString(pubKeyHex)
	if err != nil {
		return fmt.Errorf("wasm_marketplace: invalid public key hex: %w", err)
	}
	if len(keyBytes) != ed25519.PublicKeySize {
		return fmt.Errorf("wasm_marketplace: invalid public key size: expected %d, got %d", ed25519.PublicKeySize, len(keyBytes))
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.trustedKeys[name] = ed25519.PublicKey(keyBytes)
	return nil
}

// PublishPlugin publishes a plugin to the registry.
func (m *WASMMarketplace) PublishPlugin(manifest WASMPluginManifest, wasmBytes []byte, sig *PluginSignature) error {
	if manifest.Name == "" {
		return fmt.Errorf("wasm_marketplace: plugin name required")
	}
	if len(wasmBytes) == 0 {
		return fmt.Errorf("wasm_marketplace: WASM bytes required")
	}
	if int64(len(wasmBytes)) > m.config.MaxPluginSizeBytes {
		return fmt.Errorf("wasm_marketplace: plugin too large (%d > %d)", len(wasmBytes), m.config.MaxPluginSizeBytes)
	}

	// Verify signature if required
	verified := false
	if m.config.VerifySignatures && sig != nil {
		v, err := m.verifySignature(wasmBytes, sig)
		if err != nil {
			return fmt.Errorf("wasm_marketplace: signature verification failed: %w", err)
		}
		verified = v
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.registry[manifest.Name] = &WASMMarketplaceEntry{
		Manifest:    manifest,
		WASMBytes:   wasmBytes,
		WASMSize:    int64(len(wasmBytes)),
		Signature:   sig,
		Published:   true,
		Verified:    verified,
		PublishedAt: time.Now(),
	}

	return nil
}

// verifySignature verifies an ed25519 signature.
func (m *WASMMarketplace) verifySignature(data []byte, sig *PluginSignature) (bool, error) {
	if sig == nil || sig.PublicKeyHex == "" || sig.SignatureHex == "" {
		return false, fmt.Errorf("missing signature data")
	}

	pubKeyBytes, err := hex.DecodeString(sig.PublicKeyHex)
	if err != nil {
		return false, fmt.Errorf("invalid public key hex: %w", err)
	}
	if len(pubKeyBytes) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid public key size")
	}

	sigBytes, err := hex.DecodeString(sig.SignatureHex)
	if err != nil {
		return false, fmt.Errorf("invalid signature hex: %w", err)
	}

	pubKey := ed25519.PublicKey(pubKeyBytes)
	return ed25519.Verify(pubKey, data, sigBytes), nil
}

// InstallPlugin installs a plugin from the registry.
func (m *WASMMarketplace) InstallPlugin(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.registry[name]
	if !exists {
		return fmt.Errorf("wasm_marketplace: plugin %q not found", name)
	}

	if len(m.installations) >= m.config.MaxPlugins {
		return fmt.Errorf("wasm_marketplace: max plugins (%d) reached", m.config.MaxPlugins)
	}

	// Check dependencies
	for _, dep := range entry.Manifest.Dependencies {
		if !dep.Optional {
			if _, installed := m.installations[dep.Name]; !installed {
				return fmt.Errorf("wasm_marketplace: required dependency %q not installed", dep.Name)
			}
		}
	}

	m.installations[name] = &WASMPluginInstallation{
		Name:        name,
		Version:     entry.Manifest.Version,
		PluginType:  entry.Manifest.PluginType,
		Enabled:     true,
		InstalledAt: time.Now(),
	}

	entry.Downloads++

	// Load into runtime if available
	if m.runtime != nil {
		cfg := WASMPluginConfig{
			Name:        name,
			WASMBytes:   entry.WASMBytes,
			MaxMemoryMB: entry.Manifest.MaxMemoryMB,
			MaxExecTimeMs: entry.Manifest.MaxExecTimeMs,
			Permissions: DefaultWASMPermissions(),
		}
		m.runtime.LoadPlugin(context.Background(), cfg)
	}

	return nil
}

// UninstallPlugin removes an installed plugin.
func (m *WASMMarketplace) UninstallPlugin(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.installations[name]; !exists {
		return fmt.Errorf("wasm_marketplace: plugin %q not installed", name)
	}

	delete(m.installations, name)

	if m.runtime != nil {
		m.runtime.UnloadPlugin(context.Background(), name)
	}

	return nil
}

// EnablePlugin enables a disabled plugin.
func (m *WASMMarketplace) EnablePlugin(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.installations[name]
	if !exists {
		return fmt.Errorf("wasm_marketplace: plugin %q not installed", name)
	}
	inst.Enabled = true
	return nil
}

// DisablePlugin disables a plugin without uninstalling.
func (m *WASMMarketplace) DisablePlugin(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.installations[name]
	if !exists {
		return fmt.Errorf("wasm_marketplace: plugin %q not installed", name)
	}
	inst.Enabled = false
	return nil
}

// PinVersion pins a plugin to its current version.
func (m *WASMMarketplace) PinVersion(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.installations[name]
	if !exists {
		return fmt.Errorf("wasm_marketplace: plugin %q not installed", name)
	}
	inst.Pinned = true
	return nil
}

// ListInstalled returns all installed plugins.
func (m *WASMMarketplace) ListInstalled() []WASMPluginInstallation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]WASMPluginInstallation, 0, len(m.installations))
	for _, inst := range m.installations {
		result = append(result, *inst)
	}
	return result
}

// ListRegistry returns all published plugins.
func (m *WASMMarketplace) ListRegistry() []WASMMarketplaceEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]WASMMarketplaceEntry, 0, len(m.registry))
	for _, entry := range m.registry {
		// Don't include WASM bytes in listing
		e := *entry
		e.WASMBytes = nil
		result = append(result, e)
	}
	return result
}

// SearchPlugins searches the registry by name or type.
func (m *WASMMarketplace) SearchPlugins(query string, pluginType WASMPluginType) []WASMMarketplaceEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []WASMMarketplaceEntry
	for _, entry := range m.registry {
		if !entry.Published {
			continue
		}
		if pluginType != "" && entry.Manifest.PluginType != pluginType {
			continue
		}
		if query != "" {
			nameMatch := strings.Contains(entry.Manifest.Name, query)
			descMatch := strings.Contains(entry.Manifest.Description, query)
			if !nameMatch && !descMatch {
				continue
			}
		}
		e := *entry
		e.WASMBytes = nil
		results = append(results, e)
	}
	return results
}

// HotReload replaces a running plugin with a new version without downtime.
func (m *WASMMarketplace) HotReload(name string, newWASMBytes []byte) error {
	if !m.config.EnableHotReload {
		return fmt.Errorf("wasm_marketplace: hot reload disabled")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.installations[name]
	if !exists {
		return fmt.Errorf("wasm_marketplace: plugin %q not installed", name)
	}

	if inst.Pinned {
		return fmt.Errorf("wasm_marketplace: plugin %q is version-pinned", name)
	}

	// Update registry entry
	if entry, ok := m.registry[name]; ok {
		entry.WASMBytes = newWASMBytes
		entry.WASMSize = int64(len(newWASMBytes))
	}

	// Reload in runtime
	if m.runtime != nil {
		m.runtime.UnloadPlugin(context.Background(), name)
		cfg := WASMPluginConfig{
			Name:      name,
			WASMBytes: newWASMBytes,
		}
		m.runtime.LoadPlugin(context.Background(), cfg)
	}

	return nil
}

// GetPluginInfo returns detailed information about an installed plugin.
func (m *WASMMarketplace) GetPluginInfo(name string) (*WASMPluginInstallation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inst, exists := m.installations[name]
	if !exists {
		return nil, fmt.Errorf("wasm_marketplace: plugin %q not installed", name)
	}
	return inst, nil
}

// RegisterHTTPHandlers registers HTTP endpoints for the WASM marketplace.
func (m *WASMMarketplace) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/plugins/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.ListInstalled())
	})
	mux.HandleFunc("/api/v1/plugins/registry", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.ListRegistry())
	})
	mux.HandleFunc("/api/v1/plugins/install", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxQueryBodySize)
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := m.InstallPlugin(req.Name); err != nil {
			log.Printf("plugin install error: %v", err)
			http.Error(w, "failed to install plugin", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "installed", "name": req.Name})
	})
	mux.HandleFunc("/api/v1/plugins/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxQueryBodySize)
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := m.UninstallPlugin(req.Name); err != nil {
			log.Printf("plugin uninstall error: %v", err)
			http.Error(w, "failed to uninstall plugin", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/v1/plugins/search", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		pluginType := WASMPluginType(r.URL.Query().Get("type"))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.SearchPlugins(query, pluginType))
	})

	mux.HandleFunc("/api/v1/plugins/update", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxQueryBodySize)
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := m.UpdatePlugin(req.Name); err != nil {
			log.Printf("plugin update error: %v", err)
			http.Error(w, "update failed", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "updated", "name": req.Name})
	})

	mux.HandleFunc("/api/v1/plugins/cache/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.CacheStats())
	})
}

// PluginArchive represents a .chronicle-plugin archive format.
type PluginArchive struct {
	FormatVersion int                `json:"format_version"`
	Manifest      WASMPluginManifest `json:"manifest"`
	WASMBytes     []byte             `json:"-"`
	Metadata      map[string]string  `json:"metadata,omitempty"`
	Signature     *PluginSignature   `json:"signature,omitempty"`
	SHA256        string             `json:"sha256"`
}

// NewPluginArchive creates a plugin archive from manifest and WASM binary.
func NewPluginArchive(manifest WASMPluginManifest, wasmBytes []byte) *PluginArchive {
	hash := computeSHA256(wasmBytes)
	return &PluginArchive{
		FormatVersion: 1,
		Manifest:      manifest,
		WASMBytes:     wasmBytes,
		Metadata:      make(map[string]string),
		SHA256:        hash,
	}
}

// Validate checks the archive integrity.
func (a *PluginArchive) Validate() error {
	if a.Manifest.Name == "" {
		return fmt.Errorf("archive: missing plugin name")
	}
	if a.Manifest.Version == "" {
		return fmt.Errorf("archive: missing plugin version")
	}
	if len(a.WASMBytes) == 0 {
		return fmt.Errorf("archive: empty WASM binary")
	}
	// Verify SHA256 integrity
	computed := computeSHA256(a.WASMBytes)
	if a.SHA256 != "" && computed != a.SHA256 {
		return fmt.Errorf("archive: SHA256 mismatch (expected %s, got %s)", a.SHA256, computed)
	}
	return nil
}

func computeSHA256(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// CachePlugin caches a plugin's WASM binary with integrity verification.
func (m *WASMMarketplace) CachePlugin(name string, wasmBytes []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	hash := computeSHA256(wasmBytes)
	m.cache[name] = &pluginCacheEntry{
		WASMBytes:  wasmBytes,
		SHA256Hash: hash,
		CachedAt:   time.Now(),
		SizeBytes:  int64(len(wasmBytes)),
		Verified:   true,
	}
	return nil
}

// GetCachedPlugin returns a cached plugin if available and integrity-verified.
func (m *WASMMarketplace) GetCachedPlugin(name string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.cache[name]
	if !ok {
		return nil, false
	}

	// Verify integrity on cache read
	computed := computeSHA256(entry.WASMBytes)
	if computed != entry.SHA256Hash {
		return nil, false // cache corrupted
	}

	return entry.WASMBytes, true
}

// ClearCache removes all cached plugin binaries.
func (m *WASMMarketplace) ClearCache() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = make(map[string]*pluginCacheEntry)
}

// CacheStats returns cache statistics.
func (m *WASMMarketplace) CacheStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var totalSize int64
	for _, entry := range m.cache {
		totalSize += entry.SizeBytes
	}

	return map[string]interface{}{
		"cached_plugins":  len(m.cache),
		"total_size_bytes": totalSize,
	}
}

// UpdatePlugin updates an installed plugin to the latest registry version.
func (m *WASMMarketplace) UpdatePlugin(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.installations[name]
	if !ok {
		return fmt.Errorf("wasm_marketplace: plugin %q not installed", name)
	}
	if inst.Pinned {
		return fmt.Errorf("wasm_marketplace: plugin %q is version-pinned", name)
	}

	entry, ok := m.registry[name]
	if !ok {
		return fmt.Errorf("wasm_marketplace: plugin %q not in registry", name)
	}

	if entry.Manifest.Version == inst.Version {
		return nil // already up to date
	}

	// Update installation
	inst.Version = entry.Manifest.Version
	inst.InstalledAt = time.Now()

	// Update cache
	m.cache[name] = &pluginCacheEntry{
		WASMBytes:  entry.WASMBytes,
		SHA256Hash: computeSHA256(entry.WASMBytes),
		CachedAt:   time.Now(),
		SizeBytes:  int64(len(entry.WASMBytes)),
		Verified:   true,
	}

	return nil
}

// PluginResourceLimits describes the enforced resource limits for a plugin.
type PluginResourceLimits struct {
	MaxMemoryBytes int64         `json:"max_memory_bytes"`
	MaxExecTime    time.Duration `json:"max_exec_time"`
	MaxCPUPercent  int           `json:"max_cpu_percent"`
}

// GetResourceLimits returns the resource limits for a plugin based on its manifest
// and marketplace-wide configuration.
func (m *WASMMarketplace) GetResourceLimits(name string) (*PluginResourceLimits, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.registry[name]
	if !ok {
		return nil, fmt.Errorf("wasm_marketplace: plugin %q not found", name)
	}

	maxMem := m.config.MaxMemoryPerPlugin
	manifestMax := int64(entry.Manifest.MaxMemoryMB) * 1024 * 1024
	if manifestMax > 0 && manifestMax < maxMem {
		maxMem = manifestMax
	}

	maxExec := time.Duration(m.config.MaxExecTimeMs) * time.Millisecond
	manifestExec := time.Duration(entry.Manifest.MaxExecTimeMs) * time.Millisecond
	if manifestExec > 0 && manifestExec < maxExec {
		maxExec = manifestExec
	}

	return &PluginResourceLimits{
		MaxMemoryBytes: maxMem,
		MaxExecTime:    maxExec,
		MaxCPUPercent:  80,
	}, nil
}

// PublishFromArchive publishes a plugin from a PluginArchive.
func (m *WASMMarketplace) PublishFromArchive(archive *PluginArchive) error {
	if err := archive.Validate(); err != nil {
		return fmt.Errorf("wasm_marketplace: invalid archive: %w", err)
	}

	return m.PublishPlugin(archive.Manifest, archive.WASMBytes, archive.Signature)
}
