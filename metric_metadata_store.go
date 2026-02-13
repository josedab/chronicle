package chronicle

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

// MetricMetadataStoreConfig configures the metric metadata store.
type MetricMetadataStoreConfig struct {
	Enabled    bool `json:"enabled"`
	MaxEntries int  `json:"max_entries"`
}

// DefaultMetricMetadataStoreConfig returns sensible defaults.
func DefaultMetricMetadataStoreConfig() MetricMetadataStoreConfig {
	return MetricMetadataStoreConfig{
		Enabled:    true,
		MaxEntries: 10000,
	}
}

// MetricMetadataEntry holds metadata for a single metric.
type MetricMetadataEntry struct {
	Metric      string            `json:"metric"`
	Description string            `json:"description"`
	Unit        string            `json:"unit"`
	Type        string            `json:"type"`
	Labels      map[string]string `json:"labels"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// MetricMetadataStoreStats holds store statistics.
type MetricMetadataStoreStats struct {
	TotalEntries int            `json:"total_entries"`
	Types        map[string]int `json:"types"`
}

// MetricMetadataStoreEngine manages metric metadata.
type MetricMetadataStoreEngine struct {
	db      *DB
	config  MetricMetadataStoreConfig
	mu      sync.RWMutex
	entries map[string]*MetricMetadataEntry
	running bool
	stopCh  chan struct{}
}

// NewMetricMetadataStoreEngine creates a new engine.
func NewMetricMetadataStoreEngine(db *DB, cfg MetricMetadataStoreConfig) *MetricMetadataStoreEngine {
	return &MetricMetadataStoreEngine{
		db:      db,
		config:  cfg,
		entries: make(map[string]*MetricMetadataEntry),
		stopCh:  make(chan struct{}),
	}
}

// Start starts the engine.
func (e *MetricMetadataStoreEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop stops the engine.
func (e *MetricMetadataStoreEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Set adds or updates a metric metadata entry.
func (e *MetricMetadataStoreEngine) Set(entry MetricMetadataEntry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	if existing, ok := e.entries[entry.Metric]; ok {
		entry.CreatedAt = existing.CreatedAt
		entry.UpdatedAt = now
	} else {
		if e.config.MaxEntries > 0 && len(e.entries) >= e.config.MaxEntries {
			return
		}
		entry.CreatedAt = now
		entry.UpdatedAt = now
	}
	cp := entry
	e.entries[entry.Metric] = &cp
}

// Get returns a metric metadata entry or nil.
func (e *MetricMetadataStoreEngine) Get(metric string) *MetricMetadataEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	entry, ok := e.entries[metric]
	if !ok {
		return nil
	}
	cp := *entry
	return &cp
}

// Delete removes a metric metadata entry.
func (e *MetricMetadataStoreEngine) Delete(metric string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.entries, metric)
}

// List returns all metadata entries.
func (e *MetricMetadataStoreEngine) List() []MetricMetadataEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]MetricMetadataEntry, 0, len(e.entries))
	for _, entry := range e.entries {
		result = append(result, *entry)
	}
	return result
}

// Search returns entries matching a substring in metric name or description.
func (e *MetricMetadataStoreEngine) Search(query string) []MetricMetadataEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	q := strings.ToLower(query)
	var result []MetricMetadataEntry
	for _, entry := range e.entries {
		if strings.Contains(strings.ToLower(entry.Metric), q) || strings.Contains(strings.ToLower(entry.Description), q) {
			result = append(result, *entry)
		}
	}
	return result
}

// Stats returns store statistics.
func (e *MetricMetadataStoreEngine) Stats() MetricMetadataStoreStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	types := make(map[string]int)
	for _, entry := range e.entries {
		types[entry.Type]++
	}
	return MetricMetadataStoreStats{
		TotalEntries: len(e.entries),
		Types:        types,
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *MetricMetadataStoreEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/metadata/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.List())
	})
	mux.HandleFunc("/api/v1/metadata/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
	mux.HandleFunc("/api/v1/metadata/search", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Search(query))
	})
}
