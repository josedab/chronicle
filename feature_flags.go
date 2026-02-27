// Package chronicle feature flags.
//
// Chronicle supports build-time feature flags via Go build tags:
//
//	go build -tags=core          # Only stable API, no experimental features
//	go build -tags=core,beta     # Stable + beta features
//	go build                     # All features (default)
//
// Runtime feature flags can also be used to disable specific features:
//
//	cfg := chronicle.DefaultConfig(path)
//	db, _ := chronicle.Open(path, cfg)
//	flags := db.FeatureFlags()
//	flags.Disable("experimental.wasm_udf")
//	flags.Disable("experimental.wire_protocol")

package chronicle

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
)

// FeatureFlagConfig configures the feature flag system.
type FeatureFlagConfig struct {
	Enabled           bool
	DefaultPolicy     string // "allow_all", "stable_only", "stable_beta"
	DisabledFeatures  []string
}

// DefaultFeatureFlagConfig returns sensible defaults.
func DefaultFeatureFlagConfig() FeatureFlagConfig {
	return FeatureFlagConfig{
		Enabled:       true,
		DefaultPolicy: "allow_all",
	}
}

// FeatureFlag represents a single feature and its state.
type FeatureFlag struct {
	Name      string `json:"name"`
	Tier      string `json:"tier"` // stable, beta, experimental
	Enabled   bool   `json:"enabled"`
	Reason    string `json:"reason,omitempty"`
}

// FeatureFlagStats holds flag system statistics.
type FeatureFlagStats struct {
	TotalFlags    int `json:"total_flags"`
	EnabledFlags  int `json:"enabled_flags"`
	DisabledFlags int `json:"disabled_flags"`
	StableCount   int `json:"stable_count"`
	BetaCount     int `json:"beta_count"`
	ExperimentalCount int `json:"experimental_count"`
}

// FeatureFlagEngine manages runtime feature flags.
type FeatureFlagEngine struct {
	db     *DB
	config FeatureFlagConfig
	mu     sync.RWMutex
	flags  map[string]*FeatureFlag
	running bool
	stopCh  chan struct{}
}

// NewFeatureFlagEngine creates a new feature flag engine.
func NewFeatureFlagEngine(db *DB, cfg FeatureFlagConfig) *FeatureFlagEngine {
	e := &FeatureFlagEngine{
		db:     db,
		config: cfg,
		flags:  make(map[string]*FeatureFlag),
		stopCh: make(chan struct{}),
	}
	e.initFlags()
	return e
}

func (e *FeatureFlagEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *FeatureFlagEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; close(e.stopCh)
}

func (e *FeatureFlagEngine) initFlags() {
	for _, sym := range StableAPI() {
		e.flags[sym.Name] = &FeatureFlag{Name: sym.Name, Tier: "stable", Enabled: true}
	}
	for _, sym := range BetaAPI() {
		enabled := e.config.DefaultPolicy != "stable_only"
		e.flags[sym.Name] = &FeatureFlag{Name: sym.Name, Tier: "beta", Enabled: enabled}
	}
	for _, sym := range ExperimentalAPI() {
		enabled := e.config.DefaultPolicy == "allow_all"
		e.flags[sym.Name] = &FeatureFlag{Name: sym.Name, Tier: "experimental", Enabled: enabled}
	}
	for _, name := range e.config.DisabledFeatures {
		if f, ok := e.flags[name]; ok {
			f.Enabled = false
			f.Reason = "disabled by config"
		}
	}
}

// IsEnabled checks if a feature is enabled.
func (e *FeatureFlagEngine) IsEnabled(name string) bool {
	e.mu.RLock(); defer e.mu.RUnlock()
	if f, ok := e.flags[name]; ok { return f.Enabled }
	return true // unknown features default to enabled
}

// Enable enables a feature.
func (e *FeatureFlagEngine) Enable(name string) {
	e.mu.Lock(); defer e.mu.Unlock()
	if f, ok := e.flags[name]; ok { f.Enabled = true; f.Reason = "manually enabled" }
}

// Disable disables a feature.
func (e *FeatureFlagEngine) Disable(name string) {
	e.mu.Lock(); defer e.mu.Unlock()
	if f, ok := e.flags[name]; ok { f.Enabled = false; f.Reason = "manually disabled" }
}

// List returns all feature flags.
func (e *FeatureFlagEngine) List() []FeatureFlag {
	e.mu.RLock(); defer e.mu.RUnlock()
	result := make([]FeatureFlag, 0, len(e.flags))
	for _, f := range e.flags { result = append(result, *f) }
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// GetStats returns flag statistics.
func (e *FeatureFlagEngine) GetStats() FeatureFlagStats {
	e.mu.RLock(); defer e.mu.RUnlock()
	stats := FeatureFlagStats{TotalFlags: len(e.flags)}
	for _, f := range e.flags {
		if f.Enabled { stats.EnabledFlags++ } else { stats.DisabledFlags++ }
		switch f.Tier {
		case "stable": stats.StableCount++
		case "beta": stats.BetaCount++
		case "experimental": stats.ExperimentalCount++
		}
	}
	return stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *FeatureFlagEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/flags", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.List())
	})
	mux.HandleFunc("/api/v1/flags/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/flags/toggle", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var req struct { Name string `json:"name"`; Enabled bool `json:"enabled"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
		if req.Enabled { e.Enable(req.Name) } else { e.Disable(req.Name) }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
}
