package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// SeriesDedupConfig configures the series deduplication engine.
type SeriesDedupConfig struct {
	Enabled           bool          `json:"enabled"`
	DeduplicateWindow time.Duration `json:"deduplicate_window"`
	MaxTracked        int           `json:"max_tracked"`
	MergeStrategy     string        `json:"merge_strategy"` // keep_latest, keep_first, keep_avg
}

// DefaultSeriesDedupConfig returns sensible defaults.
func DefaultSeriesDedupConfig() SeriesDedupConfig {
	return SeriesDedupConfig{
		Enabled:           true,
		DeduplicateWindow: 5 * time.Second,
		MaxTracked:        100000,
		MergeStrategy:     "keep_latest",
	}
}

// DedupResult holds the result of a deduplication operation.
type DedupResult struct {
	Metric        string `json:"metric"`
	OriginalCount int    `json:"original_count"`
	DedupCount    int    `json:"dedup_count"`
	MergedCount   int    `json:"merged_count"`
}

// SeriesDedupStats holds statistics for the dedup engine.
type SeriesDedupStats struct {
	TotalChecked    int64 `json:"total_checked"`
	TotalDuplicates int64 `json:"total_duplicates"`
	TotalMerged     int64 `json:"total_merged"`
}

type dedupEntry struct {
	Timestamp int64
	Value     float64
}

// SeriesDedupEngine detects and removes duplicate series data points.
type SeriesDedupEngine struct {
	db      *DB
	config  SeriesDedupConfig
	running bool

	recent          map[string][]dedupEntry
	totalChecked    int64
	totalDuplicates int64
	totalMerged     int64

	mu sync.RWMutex
}

// NewSeriesDedupEngine creates a new series deduplication engine.
func NewSeriesDedupEngine(db *DB, cfg SeriesDedupConfig) *SeriesDedupEngine {
	return &SeriesDedupEngine{
		db:     db,
		config: cfg,
		recent: make(map[string][]dedupEntry),
	}
}

// Start starts the dedup engine.
func (e *SeriesDedupEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop stops the dedup engine.
func (e *SeriesDedupEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// CheckDuplicate returns true if the point is a duplicate of a recently seen point.
// Deduplication is scoped to the full series key (metric + tags) to avoid
// incorrectly deduplicating different series with the same metric name and value.
func (e *SeriesDedupEngine) CheckDuplicate(p Point) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.totalChecked++
	window := e.config.DeduplicateWindow.Nanoseconds()

	key := seriesKey(p.Metric, p.Tags)
	entries := e.recent[key]
	for _, ent := range entries {
		diff := p.Timestamp - ent.Timestamp
		if diff < 0 {
			diff = -diff
		}
		if diff <= window && ent.Value == p.Value {
			e.totalDuplicates++
			return true
		}
	}

	// Track this point
	e.recent[key] = append(entries, dedupEntry{
		Timestamp: p.Timestamp,
		Value:     p.Value,
	})

	// Enforce max tracked per series
	if len(e.recent[key]) > e.config.MaxTracked {
		e.recent[key] = e.recent[key][1:]
	}

	return false
}

// Deduplicate removes duplicate points from a batch.
func (e *SeriesDedupEngine) Deduplicate(points []Point) []Point {
	result := make([]Point, 0, len(points))
	for _, p := range points {
		if !e.CheckDuplicate(p) {
			result = append(result, p)
		}
	}
	return result
}

// GetStats returns deduplication statistics.
func (e *SeriesDedupEngine) GetStats() SeriesDedupStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return SeriesDedupStats{
		TotalChecked:    e.totalChecked,
		TotalDuplicates: e.totalDuplicates,
		TotalMerged:     e.totalMerged,
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the dedup engine.
func (e *SeriesDedupEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/series-dedup/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
