package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ConfigReloadConfig configures the hot reload engine.
type ConfigReloadConfig struct {
	Enabled       bool          `json:"enabled"`
	WatchInterval time.Duration `json:"watch_interval"`
}

// DefaultConfigReloadConfig returns sensible defaults.
func DefaultConfigReloadConfig() ConfigReloadConfig {
	return ConfigReloadConfig{
		Enabled:       true,
		WatchInterval: 30 * time.Second,
	}
}

// ConfigChange describes a single configuration change.
type ConfigChange struct {
	Field    string `json:"field"`
	OldValue string `json:"old_value"`
	NewValue string `json:"new_value"`
	Applied  bool   `json:"applied"`
	Reason   string `json:"reason"`
}

// ConfigReloadStats holds reload statistics.
type ConfigReloadStats struct {
	TotalReloads int64     `json:"total_reloads"`
	TotalChanges int64     `json:"total_changes"`
	LastReloadAt time.Time `json:"last_reload_at"`
}

// ConfigReloadEngine manages hot reloading of configuration.
type ConfigReloadEngine struct {
	db         *DB
	config     ConfigReloadConfig
	mu         sync.RWMutex
	currentCfg Config
	lastReload time.Time
	stats      ConfigReloadStats
	history    []ConfigChange
	running    bool
	stopCh     chan struct{}
}

// NewConfigReloadEngine creates a new engine.
func NewConfigReloadEngine(db *DB, cfg ConfigReloadConfig) *ConfigReloadEngine {
	return &ConfigReloadEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// Start starts the engine.
func (e *ConfigReloadEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop stops the engine.
func (e *ConfigReloadEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

var configReloadSafeFields = map[string]bool{
	"HTTPPort":          true,
	"RetentionDuration": true,
	"BufferSize":        true,
	"RateLimitPerSecond": true,
}

// Diff compares two configs and returns the changes.
func (e *ConfigReloadEngine) Diff(a, b Config) []ConfigChange {
	var changes []ConfigChange

	if a.HTTPPort != b.HTTPPort {
		changes = append(changes, ConfigChange{
			Field:    "HTTPPort",
			OldValue: fmt.Sprintf("%d", a.HTTPPort),
			NewValue: fmt.Sprintf("%d", b.HTTPPort),
			Applied:  true,
			Reason:   "safe to change at runtime",
		})
	}

	if a.RetentionDuration != b.RetentionDuration {
		changes = append(changes, ConfigChange{
			Field:    "RetentionDuration",
			OldValue: a.RetentionDuration.String(),
			NewValue: b.RetentionDuration.String(),
			Applied:  true,
			Reason:   "safe to change at runtime",
		})
	}

	if a.BufferSize != b.BufferSize {
		changes = append(changes, ConfigChange{
			Field:    "BufferSize",
			OldValue: fmt.Sprintf("%d", a.BufferSize),
			NewValue: fmt.Sprintf("%d", b.BufferSize),
			Applied:  true,
			Reason:   "safe to change at runtime",
		})
	}

	if a.RateLimitPerSecond != b.RateLimitPerSecond {
		changes = append(changes, ConfigChange{
			Field:    "RateLimitPerSecond",
			OldValue: fmt.Sprintf("%d", a.RateLimitPerSecond),
			NewValue: fmt.Sprintf("%d", b.RateLimitPerSecond),
			Applied:  true,
			Reason:   "safe to change at runtime",
		})
	}

	if a.Path != b.Path {
		changes = append(changes, ConfigChange{
			Field:    "Path",
			OldValue: a.Path,
			NewValue: b.Path,
			Applied:  false,
			Reason:   "unsafe: requires restart",
		})
	}

	if (a.Encryption == nil) != (b.Encryption == nil) {
		changes = append(changes, ConfigChange{
			Field:    "Encryption",
			OldValue: fmt.Sprintf("%v", a.Encryption != nil),
			NewValue: fmt.Sprintf("%v", b.Encryption != nil),
			Applied:  false,
			Reason:   "unsafe: requires restart",
		})
	}

	return changes
}

// Apply applies a new configuration, returning changes made.
func (e *ConfigReloadEngine) Apply(newCfg Config) []ConfigChange {
	e.mu.Lock()
	defer e.mu.Unlock()

	changes := e.Diff(e.currentCfg, newCfg)

	now := time.Now()
	e.lastReload = now
	e.stats.TotalReloads++
	e.stats.LastReloadAt = now
	for i := range changes {
		if changes[i].Applied {
			e.stats.TotalChanges++
		}
	}
	e.history = append(e.history, changes...)
	e.currentCfg = newCfg

	return changes
}

// LastReload returns the time of the last reload.
func (e *ConfigReloadEngine) LastReload() time.Time {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastReload
}

// GetStats returns reload statistics.
func (e *ConfigReloadEngine) GetStats() ConfigReloadStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *ConfigReloadEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/config/reload/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/config/reload/history", func(w http.ResponseWriter, r *http.Request) {
		e.mu.RLock()
		h := make([]ConfigChange, len(e.history))
		copy(h, e.history)
		e.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(h)
	})
}
