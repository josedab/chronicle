package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
)

// PartitionPrunerConfig configures the partition pruner engine.
type PartitionPrunerConfig struct {
	Enabled       bool `json:"enabled"`
	MinPartitions int  `json:"min_partitions"`
}

// DefaultPartitionPrunerConfig returns sensible defaults.
func DefaultPartitionPrunerConfig() PartitionPrunerConfig {
	return PartitionPrunerConfig{
		Enabled:       true,
		MinPartitions: 2,
	}
}

// PartitionMeta describes a single partition.
type PartitionMeta struct {
	ID         string `json:"id"`
	StartTime  int64  `json:"start_time"`
	EndTime    int64  `json:"end_time"`
	PointCount int64  `json:"point_count"`
	SizeBytes  int64  `json:"size_bytes"`
	Metric     string `json:"metric"`
}

// PruneResult holds the outcome of a pruning operation.
type PruneResult struct {
	TotalPartitions   int    `json:"total_partitions"`
	PrunedPartitions  int    `json:"pruned_partitions"`
	ScannedPartitions int    `json:"scanned_partitions"`
	Reason            string `json:"reason"`
}

// PartitionPrunerStats holds statistics for the pruner engine.
type PartitionPrunerStats struct {
	TotalPruneOps      int64 `json:"total_prune_ops"`
	TotalPruned        int64 `json:"total_pruned"`
	TotalScanned       int64 `json:"total_scanned"`
	RegisteredCount    int   `json:"registered_count"`
}

// PartitionPrunerEngine determines which partitions can be skipped for a query.
type PartitionPrunerEngine struct {
	db      *DB
	config  PartitionPrunerConfig
	running bool

	partitions     []PartitionMeta
	totalPruneOps  int64
	totalPruned    int64
	totalScanned   int64

	mu sync.RWMutex
}

// NewPartitionPrunerEngine creates a new partition pruner engine.
func NewPartitionPrunerEngine(db *DB, cfg PartitionPrunerConfig) *PartitionPrunerEngine {
	return &PartitionPrunerEngine{
		db:         db,
		config:     cfg,
		partitions: make([]PartitionMeta, 0),
	}
}

// Start starts the pruner engine.
func (e *PartitionPrunerEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop stops the pruner engine.
func (e *PartitionPrunerEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// RegisterPartition adds a partition to the pruner's registry.
func (e *PartitionPrunerEngine) RegisterPartition(meta PartitionMeta) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.partitions = append(e.partitions, meta)
}

// Prune determines which partitions can be skipped based on query time range.
func (e *PartitionPrunerEngine) Prune(q *Query) PruneResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.totalPruneOps++
	total := len(e.partitions)

	if total < e.config.MinPartitions {
		return PruneResult{
			TotalPartitions:   total,
			PrunedPartitions:  0,
			ScannedPartitions: total,
			Reason:            "below_min_partitions",
		}
	}

	// Unbounded query: no pruning possible
	if q.Start == 0 && q.End == 0 {
		e.totalScanned += int64(total)
		return PruneResult{
			TotalPartitions:   total,
			PrunedPartitions:  0,
			ScannedPartitions: total,
			Reason:            "unbounded_query",
		}
	}

	pruned := 0
	scanned := 0

	for _, p := range e.partitions {
		// Partition is entirely outside the query range
		if (q.End != 0 && p.StartTime >= q.End) || (q.Start != 0 && p.EndTime <= q.Start) {
			pruned++
		} else {
			scanned++
		}
	}

	e.totalPruned += int64(pruned)
	e.totalScanned += int64(scanned)

	return PruneResult{
		TotalPartitions:   total,
		PrunedPartitions:  pruned,
		ScannedPartitions: scanned,
		Reason:            "time_range",
	}
}

// ListPartitions returns all registered partitions.
func (e *PartitionPrunerEngine) ListPartitions() []PartitionMeta {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]PartitionMeta, len(e.partitions))
	copy(out, e.partitions)
	return out
}

// GetStats returns pruner statistics.
func (e *PartitionPrunerEngine) GetStats() PartitionPrunerStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return PartitionPrunerStats{
		TotalPruneOps:   e.totalPruneOps,
		TotalPruned:     e.totalPruned,
		TotalScanned:    e.totalScanned,
		RegisteredCount: len(e.partitions),
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the pruner engine.
func (e *PartitionPrunerEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/partition-pruner/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/partition-pruner/partitions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListPartitions())
	})
}

// PruneWithBloomFilters extends time-range pruning with bloom-filter-based tag predicate pruning.
func (e *PartitionPrunerEngine) PruneWithBloomFilters(q *Query, blooms map[string]*PartitionBloomFilter) PruneResult {
	// First: time-range pruning
	timeResult := e.Prune(q)

	if len(blooms) == 0 || q == nil || len(q.Tags) == 0 {
		return timeResult
	}

	// Second: tag predicate pruning via bloom filters
	e.mu.Lock()
	defer e.mu.Unlock()

	bloomPruned := 0
	for _, p := range e.partitions {
		// Skip already time-pruned partitions
		if (q.End != 0 && p.StartTime >= q.End) || (q.Start != 0 && p.EndTime <= q.Start) {
			continue
		}

		bf, hasBF := blooms[p.ID]
		if !hasBF {
			continue
		}

		for key, value := range q.Tags {
			if !bf.MayContain(key, value) {
				bloomPruned++
				break
			}
		}
	}

	e.totalPruned += int64(bloomPruned)
	if e.totalScanned > int64(bloomPruned) {
		e.totalScanned -= int64(bloomPruned)
	}

	return PruneResult{
		TotalPartitions:   timeResult.TotalPartitions,
		PrunedPartitions:  timeResult.PrunedPartitions + bloomPruned,
		ScannedPartitions: timeResult.ScannedPartitions - bloomPruned,
		Reason:            "time_range+bloom_filter",
	}
}
