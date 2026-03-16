package chronicle

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// ConfigReloadConfig configures the hot reload engine.
type ConfigReloadConfig struct {
	Enabled       bool          `json:"enabled"`
	WatchInterval time.Duration `json:"watch_interval"`
	ConfigPath    string        `json:"config_path"`
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
	TotalReloads    int64     `json:"total_reloads"`
	TotalChanges    int64     `json:"total_changes"`
	LastReloadAt    time.Time `json:"last_reload_at"`
	WatchErrors     int64     `json:"watch_errors"`
	LastWatchError  string    `json:"last_watch_error,omitempty"`
}

// ConfigReloadEngine manages hot reloading of configuration.
type ConfigReloadEngine struct {
	db         *DB
	config     ConfigReloadConfig
	mu         sync.RWMutex
	currentCfg Config
	lastReload time.Time
	lastModTime time.Time
	stats      ConfigReloadStats
	history    []ConfigChange
	running    bool
	stopCh     chan struct{}
	onReload   func([]ConfigChange) // optional callback for testing/extensibility
}

// NewConfigReloadEngine creates a new engine.
func NewConfigReloadEngine(db *DB, cfg ConfigReloadConfig) *ConfigReloadEngine {
	return &ConfigReloadEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// Start begins watching the config file for changes. If no ConfigPath is set,
// the engine runs in manual-reload mode (Apply only, no file watching).
func (e *ConfigReloadEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	if e.config.ConfigPath != "" && e.config.Enabled {
		go e.watchLoop()
	}
}

// Stop stops the file watcher and the engine.
func (e *ConfigReloadEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
}

// watchLoop polls the config file for modifications and applies changes.
func (e *ConfigReloadEngine) watchLoop() {
	interval := e.config.WatchInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			if err := e.checkAndReload(); err != nil {
				e.mu.Lock()
				e.stats.WatchErrors++
				e.stats.LastWatchError = err.Error()
				e.mu.Unlock()
				slog.Warn("config hot reload: watch error", "error", err)
			}
		}
	}
}

// checkAndReload stats the config file and reloads if it has been modified.
func (e *ConfigReloadEngine) checkAndReload() error {
	info, err := os.Stat(e.config.ConfigPath)
	if err != nil {
		return fmt.Errorf("stat config file: %w", err)
	}

	e.mu.RLock()
	lastMod := e.lastModTime
	e.mu.RUnlock()

	if !info.ModTime().After(lastMod) {
		return nil
	}

	data, err := os.ReadFile(e.config.ConfigPath)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	var newCfg Config
	if err := json.Unmarshal(data, &newCfg); err != nil {
		return fmt.Errorf("parse config file: %w", err)
	}

	changes, err := e.Apply(newCfg)
	if err != nil {
		return fmt.Errorf("apply config: %w", err)
	}

	e.mu.Lock()
	e.lastModTime = info.ModTime()
	e.mu.Unlock()

	if len(changes) > 0 {
		applied := 0
		for _, c := range changes {
			if c.Applied {
				applied++
			}
		}
		slog.Info("config hot reload: applied changes",
			"total", len(changes), "applied", applied)
	}

	return nil
}

var configReloadSafeFields = map[string]bool{
	"HTTPPort":          true,
	"RetentionDuration": true,
	"BufferSize":        true,
	"RateLimitPerSecond": true,
	"LogLevel":          true,
	"QueryTimeout":      true,
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

	if a.LogLevel != b.LogLevel {
		changes = append(changes, ConfigChange{
			Field:    "LogLevel",
			OldValue: a.LogLevel,
			NewValue: b.LogLevel,
			Applied:  true,
			Reason:   "safe to change at runtime",
		})
	}

	if a.Query.QueryTimeout != b.Query.QueryTimeout {
		changes = append(changes, ConfigChange{
			Field:    "QueryTimeout",
			OldValue: a.Query.QueryTimeout.String(),
			NewValue: b.Query.QueryTimeout.String(),
			Applied:  true,
			Reason:   "safe to change at runtime",
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

// Apply applies a new configuration after validation, returning changes made.
// If the new configuration is invalid, it returns an error and no changes are applied.
// Safe fields are applied to the running DB atomically; unsafe fields are logged but rejected.
func (e *ConfigReloadEngine) Apply(newCfg Config) ([]ConfigChange, error) {
	if err := newCfg.Validate(); err != nil {
		return nil, fmt.Errorf("config reload rejected: %w", err)
	}

	// Additional runtime safety checks for hot-reloaded fields that could
	// crash a running system even if they pass static validation.
	if newCfg.Storage.BufferSize <= 0 {
		return nil, fmt.Errorf("config reload rejected: BufferSize must be positive")
	}
	if newCfg.Query.QueryTimeout < 0 {
		return nil, fmt.Errorf("config reload rejected: QueryTimeout must be non-negative")
	}
	if newCfg.RateLimitPerSecond < 0 {
		return nil, fmt.Errorf("config reload rejected: RateLimitPerSecond must be non-negative")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	changes := e.Diff(e.currentCfg, newCfg)

	// Apply all safe changes under a single db.mu lock to prevent torn reads
	if e.db != nil {
		var hasApplied bool
		for _, c := range changes {
			if c.Applied {
				hasApplied = true
				break
			}
		}
		if hasApplied {
			e.db.mu.Lock()
			for i := range changes {
				if !changes[i].Applied {
					continue
				}
				switch changes[i].Field {
				case "RetentionDuration":
					e.db.config.Retention.RetentionDuration = newCfg.Retention.RetentionDuration
					e.db.config.RetentionDuration = newCfg.Retention.RetentionDuration
				case "BufferSize":
					e.db.config.Storage.BufferSize = newCfg.Storage.BufferSize
					e.db.config.BufferSize = newCfg.Storage.BufferSize
				case "RateLimitPerSecond":
					e.db.config.RateLimitPerSecond = newCfg.RateLimitPerSecond
				case "LogLevel":
					e.db.config.LogLevel = newCfg.LogLevel
				case "QueryTimeout":
					e.db.config.Query.QueryTimeout = newCfg.Query.QueryTimeout
					e.db.config.QueryTimeout = newCfg.Query.QueryTimeout
				}
			}
			e.db.mu.Unlock()

			// Side effects outside db.mu (these don't read db.config)
			for _, c := range changes {
				if !c.Applied {
					continue
				}
				if c.Field == "LogLevel" {
					var level slog.Level
					switch newCfg.LogLevel {
					case "debug":
						level = slog.LevelDebug
					case "warn":
						level = slog.LevelWarn
					case "error":
						level = slog.LevelError
					default:
						level = slog.LevelInfo
					}
					slog.SetLogLoggerLevel(level)
				}
			}
		}
	}

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

	if e.onReload != nil {
		e.onReload(changes)
	}

	return changes, nil
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
