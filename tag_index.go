package chronicle

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// TagIndexConfig configures the tag inverted index.
type TagIndexConfig struct {
	Enabled        bool
	MaxCardinality int
	MaxTagKeys     int
}

// DefaultTagIndexConfig returns sensible defaults.
func DefaultTagIndexConfig() TagIndexConfig {
	return TagIndexConfig{Enabled: true, MaxCardinality: 1000000, MaxTagKeys: 1000}
}

type tagPostingList struct {
	Key    string
	Value  string
	Series []uint64
}

// TagIndexStats holds inverted index statistics.
type TagIndexStats struct {
	TotalKeys      int   `json:"total_keys"`
	TotalPairs     int   `json:"total_pairs"`
	TotalPostings  int64 `json:"total_postings"`
	IndexSizeBytes int64 `json:"index_size_bytes"`
}

// TagInvertedIndex provides fast tag-based lookups via an inverted index.
type TagInvertedIndex struct {
	db     *DB
	config TagIndexConfig
	mu     sync.RWMutex
	index    map[string]*tagPostingList // "key=value" -> posting list
	keyIndex map[string]map[string]bool // tag key -> values
	seriesMap map[uint64]string         // series ID -> metric
	nextID   uint64
	running  bool
	stopCh   chan struct{}
	stats    TagIndexStats
}

// NewTagInvertedIndex creates a new tag inverted index.
func NewTagInvertedIndex(db *DB, cfg TagIndexConfig) *TagInvertedIndex {
	return &TagInvertedIndex{
		db:        db,
		config:    cfg,
		index:     make(map[string]*tagPostingList),
		keyIndex:  make(map[string]map[string]bool),
		seriesMap: make(map[uint64]string),
		stopCh:    make(chan struct{}),
	}
}

func (idx *TagInvertedIndex) Start() {
	idx.mu.Lock(); if idx.running { idx.mu.Unlock(); return }; idx.running = true; idx.mu.Unlock()
}

func (idx *TagInvertedIndex) Stop() {
	idx.mu.Lock(); defer idx.mu.Unlock(); if !idx.running { return }; idx.running = false; close(idx.stopCh)
}

// Index adds a point's tags to the inverted index and returns a series ID.
func (idx *TagInvertedIndex) Index(metric string, tags map[string]string) uint64 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.nextID++
	sid := idx.nextID
	idx.seriesMap[sid] = metric

	for k, v := range tags {
		pairKey := k + "=" + v
		pl, exists := idx.index[pairKey]
		if !exists {
			pl = &tagPostingList{Key: k, Value: v, Series: make([]uint64, 0, 16)}
			idx.index[pairKey] = pl
			idx.stats.TotalPairs++
		}
		pl.Series = append(pl.Series, sid)
		idx.stats.TotalPostings++

		if _, ok := idx.keyIndex[k]; !ok {
			idx.keyIndex[k] = make(map[string]bool)
			idx.stats.TotalKeys++
		}
		idx.keyIndex[k][v] = true
	}
	return sid
}

// Lookup finds all series IDs matching a tag key=value pair.
func (idx *TagInvertedIndex) Lookup(key, value string) []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	pl, ok := idx.index[key+"="+value]
	if !ok { return nil }
	result := make([]uint64, len(pl.Series))
	copy(result, pl.Series)
	return result
}

// LookupAnd finds series IDs matching ALL tag filters (intersection).
func (idx *TagInvertedIndex) LookupAnd(filters map[string]string) []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(filters) == 0 { return nil }

	var sets [][]uint64
	for k, v := range filters {
		pl, ok := idx.index[k+"="+v]
		if !ok { return nil }
		s := make([]uint64, len(pl.Series))
		copy(s, pl.Series)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		sets = append(sets, s)
	}
	sort.Slice(sets, func(i, j int) bool { return len(sets[i]) < len(sets[j]) })

	result := sets[0]
	for i := 1; i < len(sets) && len(result) > 0; i++ {
		result = intersectSortedU64(result, sets[i])
	}
	return result
}

func intersectSortedU64(a, b []uint64) []uint64 {
	var result []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] { result = append(result, a[i]); i++; j++
		} else if a[i] < b[j] { i++
		} else { j++ }
	}
	return result
}

// TagKeys returns all known tag keys.
func (idx *TagInvertedIndex) TagKeys() []string {
	idx.mu.RLock(); defer idx.mu.RUnlock()
	keys := make([]string, 0, len(idx.keyIndex))
	for k := range idx.keyIndex { keys = append(keys, k) }
	sort.Strings(keys)
	return keys
}

// TagValues returns all values for a given tag key.
func (idx *TagInvertedIndex) TagValues(key string) []string {
	idx.mu.RLock(); defer idx.mu.RUnlock()
	vals, ok := idx.keyIndex[key]
	if !ok { return nil }
	result := make([]string, 0, len(vals))
	for v := range vals { result = append(result, v) }
	sort.Strings(result)
	return result
}

// TagPrefixSearch returns tag values matching a prefix.
func (idx *TagInvertedIndex) TagPrefixSearch(key, prefix string) []string {
	vals := idx.TagValues(key)
	var matches []string
	for _, v := range vals {
		if strings.HasPrefix(v, prefix) { matches = append(matches, v) }
	}
	return matches
}

// GetStats returns index statistics.
func (idx *TagInvertedIndex) GetStats() TagIndexStats {
	idx.mu.RLock(); defer idx.mu.RUnlock()
	stats := idx.stats
	stats.IndexSizeBytes = int64(len(idx.index)*64) + idx.stats.TotalPostings*8
	return stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (idx *TagInvertedIndex) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/tags/keys", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json"); json.NewEncoder(w).Encode(idx.TagKeys())
	})
	mux.HandleFunc("/api/v1/tags/values", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json"); json.NewEncoder(w).Encode(idx.TagValues(r.URL.Query().Get("key")))
	})
	mux.HandleFunc("/api/v1/tags/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json"); json.NewEncoder(w).Encode(idx.GetStats())
	})
}
