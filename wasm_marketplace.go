package chronicle

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	MaxPlugins         int   `json:"max_plugins"`
	MaxPluginSizeBytes int64 `json:"max_plugin_size_bytes"`
	VerifySignatures   bool  `json:"verify_signatures"`
	EnableHotReload    bool  `json:"enable_hot_reload"`
	SandboxEnabled     bool  `json:"sandbox_enabled"`
}

// DefaultWASMMarketplaceConfig returns sensible defaults.
func DefaultWASMMarketplaceConfig() WASMMarketplaceConfig {
	return WASMMarketplaceConfig{
		MaxPlugins:         100,
		MaxPluginSizeBytes: 50 * 1024 * 1024, // 50MB
		VerifySignatures:   true,
		EnableHotReload:    true,
		SandboxEnabled:     true,
	}
}

// WASMMarketplace manages WASM plugin discovery, installation, and lifecycle.
type WASMMarketplace struct {
	config        WASMMarketplaceConfig
	runtime       *WASMRuntime
	registry      map[string]*WASMMarketplaceEntry
	installations map[string]*WASMPluginInstallation
	trustedKeys   map[string]ed25519.PublicKey
	mu            sync.RWMutex
}

// NewWASMMarketplace creates a new WASM marketplace.
func NewWASMMarketplace(runtime *WASMRuntime, config WASMMarketplaceConfig) *WASMMarketplace {
	return &WASMMarketplace{
		config:        config,
		runtime:       runtime,
		registry:      make(map[string]*WASMMarketplaceEntry),
		installations: make(map[string]*WASMPluginInstallation),
		trustedKeys:   make(map[string]ed25519.PublicKey),
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
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := m.InstallPlugin(req.Name); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
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
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := m.UninstallPlugin(req.Name); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
