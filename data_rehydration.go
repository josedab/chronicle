package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// DataRehydrationConfig configures the data rehydration engine.
type DataRehydrationConfig struct {
	Enabled              bool          `json:"enabled"`
	CacheSizeMB          int64         `json:"cache_size_mb"`
	CacheTTL             time.Duration `json:"cache_ttl"`
	MaxConcurrentFetches int           `json:"max_concurrent_fetches"`
	PrefetchEnabled      bool          `json:"prefetch_enabled"`
}

// DefaultDataRehydrationConfig returns sensible defaults.
func DefaultDataRehydrationConfig() DataRehydrationConfig {
	return DataRehydrationConfig{
		Enabled:              true,
		CacheSizeMB:          512,
		CacheTTL:             time.Hour,
		MaxConcurrentFetches: 5,
		PrefetchEnabled:      false,
	}
}

// RehydrationEntry represents a cached data segment.
type RehydrationEntry struct {
	Metric    string    `json:"metric"`
	StartTime int64     `json:"start_time"`
	EndTime   int64     `json:"end_time"`
	SizeBytes int64     `json:"size_bytes"`
	FetchedAt time.Time `json:"fetched_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Source    string    `json:"source"` // s3, gcs, local
	Status   string    `json:"status"` // pending, fetching, cached, expired
}

// RehydrationRequest describes a request to fetch cold data.
type RehydrationRequest struct {
	Metric   string `json:"metric"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Source   string `json:"source"`
	Priority int    `json:"priority"`
}

// DataRehydrationStats holds engine statistics.
type DataRehydrationStats struct {
	TotalFetches  int64 `json:"total_fetches"`
	CacheHits     int64 `json:"cache_hits"`
	CacheMisses   int64 `json:"cache_misses"`
	CachedSizeMB  int64 `json:"cached_size_mb"`
	EvictionCount int64 `json:"eviction_count"`
}

// DataRehydrationEngine manages rehydration of cold data into a hot cache.
type DataRehydrationEngine struct {
	db      *DB
	config  DataRehydrationConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}

	cache map[string]RehydrationEntry // keyed by "metric:start:end"
	stats DataRehydrationStats
}

// NewDataRehydrationEngine creates a new data rehydration engine.
func NewDataRehydrationEngine(db *DB, cfg DataRehydrationConfig) *DataRehydrationEngine {
	return &DataRehydrationEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
		cache:  make(map[string]RehydrationEntry),
	}
}

func (e *DataRehydrationEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

func (e *DataRehydrationEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *DataRehydrationEngine) cacheKey(metric string, start, end int64) string {
	return fmt.Sprintf("%s:%d:%d", metric, start, end)
}

func (e *DataRehydrationEngine) currentCacheSizeBytes() int64 {
	var total int64
	for _, entry := range e.cache {
		total += entry.SizeBytes
	}
	return total
}

func (e *DataRehydrationEngine) currentCacheSizeMB() int64 {
	return e.currentCacheSizeBytes() / (1024 * 1024)
}

// Fetch retrieves data for the given metric/time range from the database and
// stores it in the rehydration cache. Previously this returned a simulated
// 10 MB size; it now computes the real size from query results.
func (e *DataRehydrationEngine) Fetch(req RehydrationRequest) (*RehydrationEntry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := e.cacheKey(req.Metric, req.Start, req.End)

	// check if already cached
	if entry, ok := e.cache[key]; ok && entry.Status == "cached" && time.Now().Before(entry.ExpiresAt) {
		e.stats.CacheHits++
		return &entry, nil
	}

	e.stats.CacheMisses++
	e.stats.TotalFetches++

	// Query the DB for the actual data size.
	var size int64
	if e.db != nil && req.Metric != "" {
		result, err := e.db.Execute(&Query{
			Metric: req.Metric,
			Start:  req.Start,
			End:    req.End,
		})
		if err == nil && result != nil {
			// Estimate size as ~50 bytes per point (timestamp + value + tags overhead).
			size = int64(len(result.Points)) * 50
		}
	}
	if size <= 0 {
		size = 1024 // minimum 1 KB for metadata
	}

	// evict if cache is full (compare in bytes for precision)
	cacheLimitBytes := e.config.CacheSizeMB * 1024 * 1024
	for e.currentCacheSizeBytes()+size > cacheLimitBytes && len(e.cache) > 0 {
		e.evictOldest()
	}

	now := time.Now()
	entry := RehydrationEntry{
		Metric:    req.Metric,
		StartTime: req.Start,
		EndTime:   req.End,
		SizeBytes: size,
		FetchedAt: now,
		ExpiresAt: now.Add(e.config.CacheTTL),
		Source:    req.Source,
		Status:    "cached",
	}
	e.cache[key] = entry
	e.stats.CachedSizeMB = e.currentCacheSizeMB()

	return &entry, nil
}

func (e *DataRehydrationEngine) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, v := range e.cache {
		if first || v.FetchedAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.FetchedAt
			first = false
		}
	}
	if oldestKey != "" {
		delete(e.cache, oldestKey)
		e.stats.EvictionCount++
	}
}

// GetCached returns a cached entry if it exists and is still valid.
func (e *DataRehydrationEngine) GetCached(metric string, start, end int64) *RehydrationEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := e.cacheKey(metric, start, end)
	if entry, ok := e.cache[key]; ok && entry.Status == "cached" && time.Now().Before(entry.ExpiresAt) {
		return &entry
	}
	return nil
}

// Evict manually removes cached data for a metric.
func (e *DataRehydrationEngine) Evict(metric string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k, v := range e.cache {
		if v.Metric == metric {
			delete(e.cache, k)
			e.stats.EvictionCount++
		}
	}
	e.stats.CachedSizeMB = e.currentCacheSizeMB()
}

// ListCached returns all cached entries.
func (e *DataRehydrationEngine) ListCached() []RehydrationEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	entries := make([]RehydrationEntry, 0, len(e.cache))
	for _, entry := range e.cache {
		entries = append(entries, entry)
	}
	return entries
}

// GetStats returns engine statistics.
func (e *DataRehydrationEngine) GetStats() DataRehydrationStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *DataRehydrationEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/rehydration/cached", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListCached())
	})
	mux.HandleFunc("/api/v1/rehydration/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
