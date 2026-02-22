package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// StorageStatsConfig configures the storage statistics engine.
type StorageStatsConfig struct {
	Enabled            bool          `json:"enabled"`
	CollectionInterval time.Duration `json:"collection_interval"`
}

// DefaultStorageStatsConfig returns sensible defaults.
func DefaultStorageStatsConfig() StorageStatsConfig {
	return StorageStatsConfig{
		Enabled:            true,
		CollectionInterval: 1 * time.Minute,
	}
}

// StorageSnapshot represents a point-in-time snapshot of storage metrics.
type StorageSnapshot struct {
	Timestamp        time.Time `json:"timestamp"`
	TotalSizeBytes   int64     `json:"total_size_bytes"`
	PartitionCount   int       `json:"partition_count"`
	WALSizeBytes     int64     `json:"wal_size_bytes"`
	IndexSizeBytes   int64     `json:"index_size_bytes"`
	CompressionRatio float64   `json:"compression_ratio"`
	MetricCount      int       `json:"metric_count"`
	PointCount       int64     `json:"point_count"`
}

// StorageStatsOverview holds high-level statistics.
type StorageStatsOverview struct {
	TotalSnapshots int     `json:"total_snapshots"`
	AvgGrowthRate  float64 `json:"avg_growth_rate"`
}

// StorageStatsEngine collects and tracks storage statistics over time.
type StorageStatsEngine struct {
	db      *DB
	config  StorageStatsConfig
	running bool

	snapshots []StorageSnapshot

	mu sync.RWMutex
}

// NewStorageStatsEngine creates a new storage statistics engine.
func NewStorageStatsEngine(db *DB, cfg StorageStatsConfig) *StorageStatsEngine {
	return &StorageStatsEngine{
		db:        db,
		config:    cfg,
		snapshots: make([]StorageSnapshot, 0),
	}
}

// Start starts the storage stats engine.
func (e *StorageStatsEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop stops the storage stats engine.
func (e *StorageStatsEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// Collect takes a snapshot of current storage metrics and stores it.
func (e *StorageStatsEngine) Collect() StorageSnapshot {
	snap := StorageSnapshot{
		Timestamp: time.Now(),
	}

	// Gather real metrics from DB if available
	if e.db != nil {
		snap.MetricCount = e.db.SeriesCount()
	}

	e.mu.Lock()
	e.snapshots = append(e.snapshots, snap)
	e.mu.Unlock()

	return snap
}

// History returns the most recent snapshots up to the given limit.
func (e *StorageStatsEngine) History(limit int) []StorageSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	total := len(e.snapshots)
	if limit <= 0 || limit > total {
		limit = total
	}

	start := total - limit
	out := make([]StorageSnapshot, limit)
	copy(out, e.snapshots[start:])
	return out
}

// GetGrowthRate calculates the bytes-per-second growth rate from snapshots.
func (e *StorageStatsEngine) GetGrowthRate() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.snapshots) < 2 {
		return 0
	}

	first := e.snapshots[0]
	last := e.snapshots[len(e.snapshots)-1]

	elapsed := last.Timestamp.Sub(first.Timestamp).Seconds()
	if elapsed <= 0 {
		return 0
	}

	return float64(last.TotalSizeBytes-first.TotalSizeBytes) / elapsed
}

// GetStats returns high-level storage statistics.
func (e *StorageStatsEngine) GetStats() StorageStatsOverview {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rate := 0.0
	if len(e.snapshots) >= 2 {
		first := e.snapshots[0]
		last := e.snapshots[len(e.snapshots)-1]
		elapsed := last.Timestamp.Sub(first.Timestamp).Seconds()
		if elapsed > 0 {
			rate = float64(last.TotalSizeBytes-first.TotalSizeBytes) / elapsed
		}
	}

	return StorageStatsOverview{
		TotalSnapshots: len(e.snapshots),
		AvgGrowthRate:  rate,
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the storage stats engine.
func (e *StorageStatsEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/storage-stats/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/storage-stats/snapshot", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Collect())
	})
}
