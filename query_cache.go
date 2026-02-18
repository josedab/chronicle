package chronicle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// QueryCacheConfig configures the query result caching layer.
type QueryCacheConfig struct {
	Enabled            bool                `json:"enabled"`
	MaxEntries         int                 `json:"max_entries"`
	MaxMemoryBytes     int64               `json:"max_memory_bytes"`
	DefaultTTL         time.Duration       `json:"default_ttl"`
	EvictionPolicy     CacheEvictionPolicy `json:"eviction_policy"`
	CompressionEnabled bool                `json:"compression_enabled"`
	PartialReuse       bool                `json:"partial_reuse"`
	InvalidateOnWrite  bool                `json:"invalidate_on_write"`
}

// CacheEvictionPolicy controls how entries are evicted.
type CacheEvictionPolicy string

const (
	CacheEvictLRU CacheEvictionPolicy = "lru"
	CacheEvictLFU CacheEvictionPolicy = "lfu"
	CacheEvictTTL CacheEvictionPolicy = "ttl"
)

// DefaultQueryCacheConfig returns sensible defaults.
func DefaultQueryCacheConfig() QueryCacheConfig {
	return QueryCacheConfig{
		Enabled:            true,
		MaxEntries:         10000,
		MaxMemoryBytes:     256 * 1024 * 1024, // 256 MB
		DefaultTTL:         5 * time.Minute,
		EvictionPolicy:     CacheEvictLRU,
		CompressionEnabled: true,
		PartialReuse:       true,
		InvalidateOnWrite:  true,
	}
}

// CacheEntry is a single cached query result.
type CacheEntry struct {
	Key         string    `json:"key"`
	Metric      string    `json:"metric"`
	Query       string    `json:"query"`
	Points      []Point   `json:"points"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	AccessCount int64     `json:"access_count"`
	LastAccess  time.Time `json:"last_access"`
	SizeBytes   int64     `json:"size_bytes"`
}

// QueryCacheStats contains cache statistics.
type QueryCacheStats struct {
	Entries           int           `json:"entries"`
	MemoryBytes       int64         `json:"memory_bytes"`
	HitCount          int64         `json:"hit_count"`
	MissCount         int64         `json:"miss_count"`
	HitRate           float64       `json:"hit_rate"`
	EvictionCount     int64         `json:"eviction_count"`
	InvalidationCount int64         `json:"invalidation_count"`
	AvgAccessTime     time.Duration `json:"avg_access_time"`
}

// QueryCache provides a content-addressed query result cache with automatic invalidation.
type QueryCache struct {
	db     *DB
	config QueryCacheConfig

	entries     map[string]*CacheEntry
	accessOrder []string // for LRU eviction
	memoryUsed  int64

	hitCount      atomic.Int64
	missCount     atomic.Int64
	evictionCount atomic.Int64
	invalidCount  atomic.Int64
	totalAccessNs atomic.Int64
	accessCount   atomic.Int64

	// metric -> set of cache keys (for invalidation on write)
	metricIndex map[string]map[string]bool

	mu sync.RWMutex
}

// NewQueryCache creates a new query result cache.
func NewQueryCache(db *DB, cfg QueryCacheConfig) *QueryCache {
	return &QueryCache{
		db:          db,
		config:      cfg,
		entries:     make(map[string]*CacheEntry),
		accessOrder: make([]string, 0),
		metricIndex: make(map[string]map[string]bool),
	}
}

// cacheKey generates a content-addressed key for a query.
func cacheKey(metric string, start, end time.Time, tags map[string]string) string {
	raw := fmt.Sprintf("%s|%d|%d", metric, start.UnixNano(), end.UnixNano())
	if len(tags) > 0 {
		keys := make([]string, 0, len(tags))
		for k := range tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			raw += fmt.Sprintf("|%s=%s", k, tags[k])
		}
	}
	hash := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", hash[:16])
}

// Get retrieves a cached query result.
func (qc *QueryCache) Get(metric string, start, end time.Time, tags map[string]string) ([]Point, bool) {
	if !qc.config.Enabled {
		return nil, false
	}

	accessStart := time.Now()
	defer func() {
		qc.totalAccessNs.Add(int64(time.Since(accessStart)))
		qc.accessCount.Add(1)
	}()

	key := cacheKey(metric, start, end, tags)

	qc.mu.Lock()
	defer qc.mu.Unlock()

	entry, ok := qc.entries[key]
	if !ok {
		qc.missCount.Add(1)
		return nil, false
	}

	if time.Now().After(entry.ExpiresAt) {
		qc.removeEntryLocked(key)
		qc.missCount.Add(1)
		return nil, false
	}

	entry.AccessCount++
	entry.LastAccess = time.Now()
	qc.promoteAccessOrder(key)
	qc.hitCount.Add(1)

	points := make([]Point, len(entry.Points))
	copy(points, entry.Points)
	return points, true
}

// Put stores a query result in the cache.
func (qc *QueryCache) Put(metric string, start, end time.Time, tags map[string]string, points []Point) {
	if !qc.config.Enabled {
		return
	}

	key := cacheKey(metric, start, end, tags)
	sizeBytes := int64(len(points) * 64) // approximate size

	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Evict if needed
	for len(qc.entries) >= qc.config.MaxEntries || (qc.config.MaxMemoryBytes > 0 && qc.memoryUsed+sizeBytes > qc.config.MaxMemoryBytes) {
		if !qc.evictOneLocked() {
			break
		}
	}

	pointsCopy := make([]Point, len(points))
	copy(pointsCopy, points)

	entry := &CacheEntry{
		Key:       key,
		Metric:    metric,
		Points:    pointsCopy,
		Start:     start,
		End:       end,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(qc.config.DefaultTTL),
		SizeBytes: sizeBytes,
	}

	qc.entries[key] = entry
	qc.accessOrder = append(qc.accessOrder, key)
	qc.memoryUsed += sizeBytes

	// Update metric index for invalidation
	if _, ok := qc.metricIndex[metric]; !ok {
		qc.metricIndex[metric] = make(map[string]bool)
	}
	qc.metricIndex[metric][key] = true
}

// InvalidateMetric removes all cached entries for a metric.
func (qc *QueryCache) InvalidateMetric(metric string) int {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	keys, ok := qc.metricIndex[metric]
	if !ok {
		return 0
	}

	count := 0
	for key := range keys {
		qc.removeEntryLocked(key)
		count++
	}
	delete(qc.metricIndex, metric)
	qc.invalidCount.Add(int64(count))
	return count
}

// InvalidateAll clears the entire cache.
func (qc *QueryCache) InvalidateAll() {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	count := int64(len(qc.entries))
	qc.entries = make(map[string]*CacheEntry)
	qc.accessOrder = nil
	qc.metricIndex = make(map[string]map[string]bool)
	qc.memoryUsed = 0
	qc.invalidCount.Add(count)
}

// Entries returns the number of cached entries.
func (qc *QueryCache) Entries() int {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return len(qc.entries)
}

// Stats returns cache statistics.
func (qc *QueryCache) Stats() QueryCacheStats {
	qc.mu.RLock()
	entries := len(qc.entries)
	memBytes := qc.memoryUsed
	qc.mu.RUnlock()

	hits := qc.hitCount.Load()
	misses := qc.missCount.Load()
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	var avgAccess time.Duration
	if ac := qc.accessCount.Load(); ac > 0 {
		avgAccess = time.Duration(qc.totalAccessNs.Load() / ac)
	}

	return QueryCacheStats{
		Entries:           entries,
		MemoryBytes:       memBytes,
		HitCount:          hits,
		MissCount:         misses,
		HitRate:           hitRate,
		EvictionCount:     qc.evictionCount.Load(),
		InvalidationCount: qc.invalidCount.Load(),
		AvgAccessTime:     avgAccess,
	}
}

func (qc *QueryCache) evictOneLocked() bool {
	if len(qc.accessOrder) == 0 {
		return false
	}
	// LRU: evict oldest access
	key := qc.accessOrder[0]
	qc.removeEntryLocked(key)
	qc.evictionCount.Add(1)
	return true
}

func (qc *QueryCache) removeEntryLocked(key string) {
	entry, ok := qc.entries[key]
	if !ok {
		return
	}
	qc.memoryUsed -= entry.SizeBytes
	delete(qc.entries, key)

	// Remove from access order
	for i, k := range qc.accessOrder {
		if k == key {
			qc.accessOrder = append(qc.accessOrder[:i], qc.accessOrder[i+1:]...)
			break
		}
	}

	// Remove from metric index
	if keys, ok := qc.metricIndex[entry.Metric]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(qc.metricIndex, entry.Metric)
		}
	}
}

func (qc *QueryCache) promoteAccessOrder(key string) {
	for i, k := range qc.accessOrder {
		if k == key {
			qc.accessOrder = append(qc.accessOrder[:i], qc.accessOrder[i+1:]...)
			qc.accessOrder = append(qc.accessOrder, key)
			return
		}
	}
}

// RegisterHTTPHandlers registers query cache HTTP endpoints.
func (qc *QueryCache) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/cache/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(qc.Stats())
	})
	mux.HandleFunc("/api/v1/cache/invalidate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			qc.InvalidateAll()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "all cache cleared"})
		} else {
			count := qc.InvalidateMetric(metric)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"metric": metric, "invalidated": count})
		}
	})
}
