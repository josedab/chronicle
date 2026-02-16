package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// DeprecationConfig configures the deprecation system.
type DeprecationConfig struct {
	Enabled           bool
	WarnOnUse         bool
	MinWarningVersions int // minimum minor versions before removal
}

// DefaultDeprecationConfig returns sensible defaults.
func DefaultDeprecationConfig() DeprecationConfig {
	return DeprecationConfig{
		Enabled:            true,
		WarnOnUse:          true,
		MinWarningVersions: 2,
	}
}

// DeprecatedSymbol describes a deprecated API symbol.
type DeprecatedSymbol struct {
	Name            string `json:"name"`
	DeprecatedSince string `json:"deprecated_since"`
	RemovalVersion  string `json:"removal_version"`
	Replacement     string `json:"replacement"`
	Reason          string `json:"reason"`
	Migration       string `json:"migration_guide"`
}

// APIDeprecationReport summarizes all deprecated symbols.
type APIDeprecationReport struct {
	GeneratedAt    time.Time          `json:"generated_at"`
	APIVersion     string             `json:"api_version"`
	TotalSymbols   int                `json:"total_symbols"`
	Deprecated     int                `json:"deprecated_count"`
	Symbols        []DeprecatedSymbol `json:"symbols"`
}

// DeprecationEngine manages API deprecation lifecycle.
type DeprecationEngine struct {
	db     *DB
	config DeprecationConfig
	mu     sync.RWMutex
	symbols []DeprecatedSymbol
	running bool
	stopCh  chan struct{}
}

// NewDeprecationEngine creates a new deprecation engine.
func NewDeprecationEngine(db *DB, cfg DeprecationConfig) *DeprecationEngine {
	e := &DeprecationEngine{
		db:      db,
		config:  cfg,
		symbols: defaultDeprecatedSymbols(),
		stopCh:  make(chan struct{}),
	}
	return e
}

func (e *DeprecationEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *DeprecationEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; close(e.stopCh)
}

// AddDeprecation registers a new deprecated symbol.
func (e *DeprecationEngine) AddDeprecation(sym DeprecatedSymbol) {
	e.mu.Lock(); defer e.mu.Unlock()
	e.symbols = append(e.symbols, sym)
}

// IsDeprecated checks if a symbol is deprecated.
func (e *DeprecationEngine) IsDeprecated(name string) (bool, *DeprecatedSymbol) {
	e.mu.RLock(); defer e.mu.RUnlock()
	for i := range e.symbols {
		if e.symbols[i].Name == name {
			s := e.symbols[i]
			return true, &s
		}
	}
	return false, nil
}

// GenerateReport creates a full deprecation report.
func (e *DeprecationEngine) GenerateReport() APIDeprecationReport {
	e.mu.RLock(); defer e.mu.RUnlock()
	syms := make([]DeprecatedSymbol, len(e.symbols))
	copy(syms, e.symbols)

	total := len(StableAPI()) + len(BetaAPI()) + len(ExperimentalAPI())
	return APIDeprecationReport{
		GeneratedAt:  time.Now(),
		APIVersion:   APIVersion,
		TotalSymbols: total,
		Deprecated:   len(syms),
		Symbols:      syms,
	}
}

// List returns all deprecated symbols.
func (e *DeprecationEngine) List() []DeprecatedSymbol {
	e.mu.RLock(); defer e.mu.RUnlock()
	r := make([]DeprecatedSymbol, len(e.symbols))
	copy(r, e.symbols)
	return r
}

func defaultDeprecatedSymbols() []DeprecatedSymbol {
	return []DeprecatedSymbol{
		{
			Name:            "Config.MaxMemory",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Storage.MaxMemory",
			Reason:          "Moved to nested StorageConfig for consistency",
			Migration:       "Replace cfg.MaxMemory with cfg.Storage.MaxMemory",
		},
		{
			Name:            "Config.MaxStorageBytes",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Storage.MaxStorageBytes",
			Reason:          "Moved to nested StorageConfig for consistency",
			Migration:       "Replace cfg.MaxStorageBytes with cfg.Storage.MaxStorageBytes",
		},
		{
			Name:            "Config.SyncInterval",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.WAL.SyncInterval",
			Reason:          "Moved to nested WALConfig for consistency",
			Migration:       "Replace cfg.SyncInterval with cfg.WAL.SyncInterval",
		},
		{
			Name:            "Config.PartitionDuration",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Storage.PartitionDuration",
			Reason:          "Moved to nested StorageConfig for consistency",
			Migration:       "Replace cfg.PartitionDuration with cfg.Storage.PartitionDuration",
		},
		{
			Name:            "Config.RetentionDuration",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Retention.RetentionDuration",
			Reason:          "Moved to nested RetentionConfig",
			Migration:       "Replace cfg.RetentionDuration with cfg.Retention.RetentionDuration",
		},
		{
			Name:            "Config.BufferSize",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Storage.BufferSize",
			Reason:          "Moved to nested StorageConfig",
			Migration:       "Replace cfg.BufferSize with cfg.Storage.BufferSize",
		},
		{
			Name:            "Config.HTTPEnabled",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.HTTP.HTTPEnabled",
			Reason:          "Moved to nested HTTPConfig",
			Migration:       "Replace cfg.HTTPEnabled with cfg.HTTP.HTTPEnabled",
		},
		{
			Name:            "Config.WALMaxSize",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.WAL.WALMaxSize",
			Reason:          "Moved to nested WALConfig",
			Migration:       "Replace cfg.WALMaxSize with cfg.WAL.WALMaxSize",
		},
		{
			Name:            "Config.WALRetain",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.WAL.WALRetain",
			Reason:          "Moved to nested WALConfig",
			Migration:       "Replace cfg.WALRetain with cfg.WAL.WALRetain",
		},
		{
			Name:            "Config.DownsampleRules",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Retention.DownsampleRules",
			Reason:          "Moved to nested RetentionConfig",
			Migration:       "Replace cfg.DownsampleRules with cfg.Retention.DownsampleRules",
		},
		{
			Name:            "Config.CompactionWorkers",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Retention.CompactionWorkers",
			Reason:          "Moved to nested RetentionConfig",
			Migration:       "Replace cfg.CompactionWorkers with cfg.Retention.CompactionWorkers",
		},
		{
			Name:            "Config.CompactionInterval",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Retention.CompactionInterval",
			Reason:          "Moved to nested RetentionConfig",
			Migration:       "Replace cfg.CompactionInterval with cfg.Retention.CompactionInterval",
		},
		{
			Name:            "Config.QueryTimeout",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.Query.QueryTimeout",
			Reason:          "Moved to nested QueryConfig",
			Migration:       "Replace cfg.QueryTimeout with cfg.Query.QueryTimeout",
		},
		{
			Name:            "Config.HTTPPort",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.HTTP.HTTPPort",
			Reason:          "Moved to nested HTTPConfig",
			Migration:       "Replace cfg.HTTPPort with cfg.HTTP.HTTPPort",
		},
		{
			Name:            "Config.PrometheusRemoteWriteEnabled",
			DeprecatedSince: "0.1.0",
			RemovalVersion:  "1.0.0",
			Replacement:     "Config.HTTP.PrometheusRemoteWriteEnabled",
			Reason:          "Moved to nested HTTPConfig",
			Migration:       "Replace cfg.PrometheusRemoteWriteEnabled with cfg.HTTP.PrometheusRemoteWriteEnabled",
		},
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *DeprecationEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/deprecations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.List())
	})
	mux.HandleFunc("/api/v1/deprecations/report", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GenerateReport())
	})
}
