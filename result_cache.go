package chronicle

import (
	"container/list"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// QueryCacheKey produces a canonical cache key for a query by normalising tag
// filter order and aggregation fields. Two semantically equivalent queries
// will always produce the same key.
func QueryCacheKey(q *Query) string {
	if q == nil {
		return ""
	}

	var b strings.Builder
	fmt.Fprintf(&b, "m=%s;s=%d;e=%d;l=%d", q.Metric, q.Start, q.End, q.Limit)

	if len(q.Tags) > 0 {
		keys := make([]string, 0, len(q.Tags))
		for k := range q.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString(";t=")
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s:%s", k, q.Tags[k])
		}
	}

	if len(q.TagFilters) > 0 {
		sorted := make([]TagFilter, len(q.TagFilters))
		copy(sorted, q.TagFilters)
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].Key != sorted[j].Key {
				return sorted[i].Key < sorted[j].Key
			}
			return strings.Join(sorted[i].Values, "|") < strings.Join(sorted[j].Values, "|")
		})
		b.WriteString(";tf=")
		for i, tf := range sorted {
			if i > 0 {
				b.WriteByte(',')
			}
			vals := make([]string, len(tf.Values))
			copy(vals, tf.Values)
			sort.Strings(vals)
			fmt.Fprintf(&b, "%s:%d:%s", tf.Key, tf.Op, strings.Join(vals, "|"))
		}
	}

	if q.Aggregation != nil && q.Aggregation.Function != 0 {
		fmt.Fprintf(&b, ";agg=%d:%d:%.6f", q.Aggregation.Function, q.Aggregation.Window, q.Aggregation.Percentile)
	}

	h := sha256.Sum256([]byte(b.String()))
	return fmt.Sprintf("%x", h[:16])
}

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

// Start begins the result cache engine and its background TTL reaper.
func (e *ResultCacheEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
	go e.reapLoop()
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

// reapLoop periodically removes expired cache entries.
func (e *ResultCacheEngine) reapLoop() {
	interval := e.config.TTL / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.reapExpired()
		}
	}
}

// reapExpired removes all entries whose TTL has elapsed.
func (e *ResultCacheEngine) reapExpired() {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	for hash, elem := range e.items {
		entry := elem.Value.(*ResultCacheEntry)
		if now.Sub(entry.CachedAt) > entry.TTL {
			e.order.Remove(elem)
			delete(e.items, hash)
			e.stats.TotalEvictions++
		}
	}
	e.stats.EntryCount = e.order.Len()
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
