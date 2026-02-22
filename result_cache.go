package chronicle

import (
	"container/list"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// ResultCacheConfig configures the query result cache engine.
type ResultCacheConfig struct {
	MaxEntries int           `json:"max_entries"`
	TTL        time.Duration `json:"ttl"`
	Enabled    bool          `json:"enabled"`
}

// DefaultResultCacheConfig returns sensible defaults for ResultCacheConfig.
func DefaultResultCacheConfig() ResultCacheConfig {
	return ResultCacheConfig{
		MaxEntries: 1000,
		TTL:        5 * time.Minute,
		Enabled:    true,
	}
}

// ResultCacheEntry represents a cached query result.
type ResultCacheEntry struct {
	Hash     string    `json:"hash"`
	Result   *Result   `json:"result"`
	CachedAt time.Time `json:"cached_at"`
	TTL      time.Duration `json:"ttl"`
	Hits     int64     `json:"hits"`
	metrics  []string  // metrics referenced by this cache entry
}

// ResultCacheStats holds cache statistics.
type ResultCacheStats struct {
	TotalHits      int64   `json:"total_hits"`
	TotalMisses    int64   `json:"total_misses"`
	TotalEvictions int64   `json:"total_evictions"`
	HitRate        float64 `json:"hit_rate"`
	EntryCount     int     `json:"entry_count"`
}

// ResultCacheEngine provides LRU-based query result caching.
type ResultCacheEngine struct {
	db      *DB
	config  ResultCacheConfig
	mu      sync.RWMutex
	items   map[string]*list.Element
	order   *list.List
	stats   ResultCacheStats
	stopCh  chan struct{}
	running bool
}

// NewResultCacheEngine creates a new ResultCacheEngine.
func NewResultCacheEngine(db *DB, cfg ResultCacheConfig) *ResultCacheEngine {
	return &ResultCacheEngine{
		db:     db,
		config: cfg,
		items:  make(map[string]*list.Element),
		order:  list.New(),
		stopCh: make(chan struct{}),
	}
}

// Start begins the result cache engine.
func (e *ResultCacheEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop halts the result cache engine.
func (e *ResultCacheEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Get retrieves a cached result by query hash. Returns nil if not found or expired.
func (e *ResultCacheEngine) Get(hash string) *Result {
	e.mu.Lock()
	defer e.mu.Unlock()
	elem, ok := e.items[hash]
	if !ok {
		e.stats.TotalMisses++
		e.updateHitRate()
		return nil
	}
	entry := elem.Value.(*ResultCacheEntry)
	if time.Since(entry.CachedAt) > entry.TTL {
		e.removeLocked(hash, elem)
		e.stats.TotalMisses++
		e.updateHitRate()
		return nil
	}
	entry.Hits++
	e.order.MoveToFront(elem)
	e.stats.TotalHits++
	e.updateHitRate()
	return entry.Result
}

// Put stores a result in the cache with the configured TTL.
func (e *ResultCacheEngine) Put(hash string, result *Result, metrics ...string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if elem, ok := e.items[hash]; ok {
		e.order.MoveToFront(elem)
		entry := elem.Value.(*ResultCacheEntry)
		entry.Result = result
		entry.CachedAt = time.Now()
		entry.TTL = e.config.TTL
		entry.metrics = metrics
		return
	}
	// Evict if at capacity
	for e.order.Len() >= e.config.MaxEntries {
		e.evictOldest()
	}
	entry := &ResultCacheEntry{
		Hash:     hash,
		Result:   result,
		CachedAt: time.Now(),
		TTL:      e.config.TTL,
		metrics:  metrics,
	}
	elem := e.order.PushFront(entry)
	e.items[hash] = elem
	e.stats.EntryCount = e.order.Len()
}

// Invalidate removes all cache entries associated with the given metric.
func (e *ResultCacheEngine) Invalidate(metric string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	removed := 0
	for hash, elem := range e.items {
		entry := elem.Value.(*ResultCacheEntry)
		for _, m := range entry.metrics {
			if m == metric {
				e.order.Remove(elem)
				delete(e.items, hash)
				removed++
				break
			}
		}
	}
	e.stats.EntryCount = e.order.Len()
	return removed
}

// Clear removes all entries from the cache.
func (e *ResultCacheEngine) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.items = make(map[string]*list.Element)
	e.order.Init()
	e.stats.EntryCount = 0
}

// GetStats returns cache statistics.
func (e *ResultCacheEngine) GetStats() ResultCacheStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *ResultCacheEngine) removeLocked(hash string, elem *list.Element) {
	e.order.Remove(elem)
	delete(e.items, hash)
	e.stats.EntryCount = e.order.Len()
}

func (e *ResultCacheEngine) evictOldest() {
	back := e.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*ResultCacheEntry)
	e.order.Remove(back)
	delete(e.items, entry.Hash)
	e.stats.TotalEvictions++
	e.stats.EntryCount = e.order.Len()
}

func (e *ResultCacheEngine) updateHitRate() {
	total := e.stats.TotalHits + e.stats.TotalMisses
	if total == 0 {
		e.stats.HitRate = 0
		return
	}
	e.stats.HitRate = float64(e.stats.TotalHits) / float64(total)
}

// RegisterHTTPHandlers registers result cache HTTP endpoints.
func (e *ResultCacheEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/resultcache/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/resultcache/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		e.Clear()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
	})
	mux.HandleFunc("/api/v1/resultcache/invalidate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
			return
		}
		removed := e.Invalidate(metric)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"removed": removed})
	})
}
