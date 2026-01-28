package chronicle

import (
	"sort"
	"sync"
	"time"
)

// Exemplar represents a sample trace reference linked to a metric value.
// Exemplars enable correlation between metrics and distributed traces.
type Exemplar struct {
	// Labels are identifying labels (typically trace_id, span_id).
	Labels map[string]string

	// Value is the observed metric value.
	Value float64

	// Timestamp is when the exemplar was recorded.
	Timestamp int64
}

// ExemplarPoint is a metric point with an associated exemplar.
type ExemplarPoint struct {
	Metric    string
	Tags      map[string]string
	Value     float64
	Timestamp int64
	Exemplar  *Exemplar
}

// ExemplarStore manages exemplar storage with automatic pruning.
type ExemplarStore struct {
	db         *DB
	config     ExemplarConfig
	data       map[string]*exemplarSeries
	mu         sync.RWMutex
	totalCount int64
}

// ExemplarConfig configures exemplar storage.
type ExemplarConfig struct {
	// Enabled enables exemplar storage.
	Enabled bool

	// MaxExemplarsPerSeries limits exemplars per series.
	// Oldest exemplars are pruned when exceeded.
	MaxExemplarsPerSeries int

	// MaxTotalExemplars limits total exemplars globally.
	MaxTotalExemplars int64

	// RetentionDuration is how long exemplars are kept.
	RetentionDuration time.Duration
}

// DefaultExemplarConfig returns default exemplar configuration.
func DefaultExemplarConfig() ExemplarConfig {
	return ExemplarConfig{
		Enabled:               true,
		MaxExemplarsPerSeries: 100,
		MaxTotalExemplars:     100_000,
		RetentionDuration:     24 * time.Hour,
	}
}

type exemplarSeries struct {
	metric    string
	tags      map[string]string
	exemplars []timedExemplar
}

type timedExemplar struct {
	exemplar  *Exemplar
	timestamp int64
}

// NewExemplarStore creates an exemplar store.
func NewExemplarStore(db *DB, config ExemplarConfig) *ExemplarStore {
	return &ExemplarStore{
		db:     db,
		config: config,
		data:   make(map[string]*exemplarSeries),
	}
}

// Write stores a point with its exemplar.
func (es *ExemplarStore) Write(p ExemplarPoint) error {
	if !es.config.Enabled || p.Exemplar == nil {
		return nil
	}

	key := makeSeriesKey(p.Metric, p.Tags)

	es.mu.Lock()
	defer es.mu.Unlock()

	// Check global limit
	if es.config.MaxTotalExemplars > 0 && es.totalCount >= es.config.MaxTotalExemplars {
		es.pruneOldest()
	}

	series, ok := es.data[key]
	if !ok {
		series = &exemplarSeries{
			metric: p.Metric,
			tags:   cloneTags(p.Tags),
		}
		es.data[key] = series
	}

	// Check per-series limit
	if es.config.MaxExemplarsPerSeries > 0 && len(series.exemplars) >= es.config.MaxExemplarsPerSeries {
		// Remove oldest
		series.exemplars = series.exemplars[1:]
		es.totalCount--
	}

	series.exemplars = append(series.exemplars, timedExemplar{
		exemplar:  cloneExemplar(p.Exemplar),
		timestamp: p.Timestamp,
	})
	es.totalCount++

	return nil
}

// Query retrieves exemplars for a metric.
func (es *ExemplarStore) Query(metric string, tags map[string]string, start, end int64) ([]ExemplarPoint, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	var results []ExemplarPoint

	for _, series := range es.data {
		if series.metric != metric {
			continue
		}
		if !tagsMatch(series.tags, tags) {
			continue
		}

		for _, te := range series.exemplars {
			if te.timestamp >= start && te.timestamp <= end {
				results = append(results, ExemplarPoint{
					Metric:    series.metric,
					Tags:      cloneTags(series.tags),
					Timestamp: te.timestamp,
					Value:     te.exemplar.Value,
					Exemplar:  cloneExemplar(te.exemplar),
				})
			}
		}
	}

	// Sort by timestamp
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	return results, nil
}

// QueryByTraceID retrieves exemplars by trace ID.
func (es *ExemplarStore) QueryByTraceID(traceID string) ([]ExemplarPoint, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	var results []ExemplarPoint

	for _, series := range es.data {
		for _, te := range series.exemplars {
			if te.exemplar.Labels["trace_id"] == traceID {
				results = append(results, ExemplarPoint{
					Metric:    series.metric,
					Tags:      cloneTags(series.tags),
					Timestamp: te.timestamp,
					Value:     te.exemplar.Value,
					Exemplar:  cloneExemplar(te.exemplar),
				})
			}
		}
	}

	return results, nil
}

// Prune removes expired exemplars.
func (es *ExemplarStore) Prune() int {
	if es.config.RetentionDuration <= 0 {
		return 0
	}

	es.mu.Lock()
	defer es.mu.Unlock()

	cutoff := time.Now().Add(-es.config.RetentionDuration).UnixNano()
	pruned := 0

	for key, series := range es.data {
		originalLen := len(series.exemplars)
		filtered := series.exemplars[:0]

		for _, te := range series.exemplars {
			if te.timestamp >= cutoff {
				filtered = append(filtered, te)
			}
		}

		series.exemplars = filtered
		removed := originalLen - len(filtered)
		pruned += removed
		es.totalCount -= int64(removed)

		// Remove empty series
		if len(series.exemplars) == 0 {
			delete(es.data, key)
		}
	}

	return pruned
}

func (es *ExemplarStore) pruneOldest() {
	// Find and remove oldest exemplar
	var oldestKey string
	var oldestIdx int
	var oldestTime int64 = 1<<63 - 1

	for key, series := range es.data {
		for i, te := range series.exemplars {
			if te.timestamp < oldestTime {
				oldestTime = te.timestamp
				oldestKey = key
				oldestIdx = i
			}
		}
	}

	if oldestKey != "" {
		series := es.data[oldestKey]
		series.exemplars = append(series.exemplars[:oldestIdx], series.exemplars[oldestIdx+1:]...)
		es.totalCount--

		if len(series.exemplars) == 0 {
			delete(es.data, oldestKey)
		}
	}
}

// Stats returns exemplar storage statistics.
func (es *ExemplarStore) Stats() ExemplarStats {
	es.mu.RLock()
	defer es.mu.RUnlock()

	stats := ExemplarStats{
		TotalExemplars: es.totalCount,
		SeriesCount:    len(es.data),
	}

	for _, series := range es.data {
		if len(series.exemplars) > stats.MaxPerSeries {
			stats.MaxPerSeries = len(series.exemplars)
		}
	}

	return stats
}

// ExemplarStats contains exemplar storage statistics.
type ExemplarStats struct {
	TotalExemplars int64
	SeriesCount    int
	MaxPerSeries   int
}

func cloneExemplar(e *Exemplar) *Exemplar {
	if e == nil {
		return nil
	}
	clone := &Exemplar{
		Value:     e.Value,
		Timestamp: e.Timestamp,
	}
	if e.Labels != nil {
		clone.Labels = make(map[string]string, len(e.Labels))
		for k, v := range e.Labels {
			clone.Labels[k] = v
		}
	}
	return clone
}

// WriteWithExemplar writes a point with an exemplar to the database.
func (db *DB) WriteWithExemplar(p Point, exemplar *Exemplar) error {
	// Write the regular point
	if err := db.Write(p); err != nil {
		return err
	}

	// If we have an exemplar store, record the exemplar
	if db.exemplarStore != nil && exemplar != nil {
		return db.exemplarStore.Write(ExemplarPoint{
			Metric:    p.Metric,
			Tags:      p.Tags,
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Exemplar:  exemplar,
		})
	}

	return nil
}

// QueryExemplars retrieves exemplars for a metric.
func (db *DB) QueryExemplars(metric string, tags map[string]string, start, end int64) ([]ExemplarPoint, error) {
	if db.exemplarStore == nil {
		return nil, nil
	}
	return db.exemplarStore.Query(metric, tags, start, end)
}
