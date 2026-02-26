package chronicle

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TimeTravelConfig configures time-travel query capabilities.
type TimeTravelConfig struct {
	// Enabled enables time-travel queries.
	Enabled bool

	// MaxRetention is the maximum time to retain historical versions.
	MaxRetention time.Duration

	// SnapshotInterval is how often to create automatic snapshots.
	SnapshotInterval time.Duration

	// ChangeDataCaptureEnabled enables CDC for audit trails.
	ChangeDataCaptureEnabled bool

	// MaxSnapshots is the maximum number of snapshots to retain.
	MaxSnapshots int

	// CompactAfter is when to compact old versions.
	CompactAfter time.Duration
}

// DefaultTimeTravelConfig returns default time-travel configuration.
func DefaultTimeTravelConfig() TimeTravelConfig {
	return TimeTravelConfig{
		Enabled:                  true,
		MaxRetention:             7 * 24 * time.Hour,
		SnapshotInterval:         time.Hour,
		ChangeDataCaptureEnabled: true,
		MaxSnapshots:             168, // 1 week of hourly snapshots
		CompactAfter:             24 * time.Hour,
	}
}

// TimeTravelEngine provides time-travel query capabilities.
type TimeTravelEngine struct {
	db     *DB
	config TimeTravelConfig

	// Snapshots
	snapshots   map[string]*Snapshot
	snapshotsMu sync.RWMutex

	// Version tracking
	versions   map[string][]Version
	versionsMu sync.RWMutex

	// Change data capture log
	cdcLog   []ChangeEvent
	cdcLogMu sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Snapshot represents a point-in-time snapshot.
type Snapshot struct {
	ID          string            `json:"id"`
	Timestamp   int64             `json:"timestamp"`
	Description string            `json:"description,omitempty"`
	Metrics     []string          `json:"metrics"`
	PointCount  int64             `json:"point_count"`
	Size        int64             `json:"size"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Data        []Point           `json:"-"` // Actual snapshot data
	checksum    string
}

// Version represents a versioned data point.
type Version struct {
	Point     Point  `json:"point"`
	Version   int64  `json:"version"`
	Timestamp int64  `json:"timestamp"`
	Operation string `json:"operation"` // INSERT, UPDATE, DELETE
	Previous  *Point `json:"previous,omitempty"`
}

// ChangeEvent represents a change data capture event.
type ChangeEvent struct {
	ID        string            `json:"id"`
	Timestamp int64             `json:"timestamp"`
	Operation CDCOp             `json:"operation"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Before    *Point            `json:"before,omitempty"`
	After     *Point            `json:"after,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// CDCOp represents a CDC operation type.
type CDCOp string

const (
	CDCOpInsert CDCOp = "INSERT"
	CDCOpUpdate CDCOp = "UPDATE"
	CDCOpDelete CDCOp = "DELETE"
)

// NewTimeTravelEngine creates a new time-travel engine.
func NewTimeTravelEngine(db *DB, config TimeTravelConfig) *TimeTravelEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &TimeTravelEngine{
		db:        db,
		config:    config,
		snapshots: make(map[string]*Snapshot),
		versions:  make(map[string][]Version),
		cdcLog:    make([]ChangeEvent, 0),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start starts the time-travel engine.
func (t *TimeTravelEngine) Start() error {
	if !t.config.Enabled {
		return nil
	}

	// Start automatic snapshot creation
	if t.config.SnapshotInterval > 0 {
		t.wg.Add(1)
		go t.snapshotLoop()
	}

	// Start cleanup routine
	t.wg.Add(1)
	go t.cleanupLoop()

	return nil
}

// Stop stops the time-travel engine.
func (t *TimeTravelEngine) Stop() error {
	t.cancel()
	t.wg.Wait()
	return nil
}

func (t *TimeTravelEngine) snapshotLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(t.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			_, _ = t.CreateSnapshot("auto", "Automatic snapshot") //nolint:errcheck // best-effort auto-snapshot
		}
	}
}

func (t *TimeTravelEngine) cleanupLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.cleanup()
		}
	}
}

func (t *TimeTravelEngine) cleanup() {
	cutoff := time.Now().Add(-t.config.MaxRetention).UnixNano()

	// Clean up old snapshots
	t.snapshotsMu.Lock()
	for id, snap := range t.snapshots {
		if snap.Timestamp < cutoff {
			delete(t.snapshots, id)
		}
	}

	// Limit snapshot count
	if len(t.snapshots) > t.config.MaxSnapshots {
		// Sort by timestamp and remove oldest
		var snaps []*Snapshot
		for _, s := range t.snapshots {
			snaps = append(snaps, s)
		}
		sort.Slice(snaps, func(i, j int) bool {
			return snaps[i].Timestamp < snaps[j].Timestamp
		})

		for i := 0; i < len(snaps)-t.config.MaxSnapshots; i++ {
			delete(t.snapshots, snaps[i].ID)
		}
	}
	t.snapshotsMu.Unlock()

	// Clean up old versions
	t.versionsMu.Lock()
	for key, versions := range t.versions {
		var kept []Version
		for _, v := range versions {
			if v.Timestamp >= cutoff {
				kept = append(kept, v)
			}
		}
		if len(kept) > 0 {
			t.versions[key] = kept
		} else {
			delete(t.versions, key)
		}
	}
	t.versionsMu.Unlock()

	// Clean up old CDC events
	t.cdcLogMu.Lock()
	var keptEvents []ChangeEvent
	for _, event := range t.cdcLog {
		if event.Timestamp >= cutoff {
			keptEvents = append(keptEvents, event)
		}
	}
	t.cdcLog = keptEvents
	t.cdcLogMu.Unlock()
}

// CreateSnapshot creates a point-in-time snapshot.
func (t *TimeTravelEngine) CreateSnapshot(id, description string) (*Snapshot, error) {
	if id == "" {
		id = fmt.Sprintf("snap-%d", time.Now().UnixNano())
	}

	// Get all current data
	metrics := t.db.Metrics()
	var allPoints []Point
	var totalSize int64

	now := time.Now().UnixNano()
	for _, metric := range metrics {
		result, err := t.db.Execute(&Query{
			Metric: metric,
			Start:  0,
			End:    now,
		})
		if err != nil {
			continue
		}
		allPoints = append(allPoints, result.Points...)
		totalSize += int64(len(result.Points) * 64) // Rough estimate
	}

	snapshot := &Snapshot{
		ID:          id,
		Timestamp:   now,
		Description: description,
		Metrics:     metrics,
		PointCount:  int64(len(allPoints)),
		Size:        totalSize,
		Metadata:    make(map[string]string),
		Data:        allPoints,
	}

	t.snapshotsMu.Lock()
	t.snapshots[id] = snapshot
	t.snapshotsMu.Unlock()

	return snapshot, nil
}

// GetSnapshot retrieves a snapshot by ID.
func (t *TimeTravelEngine) GetSnapshot(id string) (*Snapshot, error) {
	t.snapshotsMu.RLock()
	snapshot, ok := t.snapshots[id]
	t.snapshotsMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("snapshot not found: %s", id)
	}

	return snapshot, nil
}

// ListSnapshots lists all available snapshots.
func (t *TimeTravelEngine) ListSnapshots() []*Snapshot {
	t.snapshotsMu.RLock()
	defer t.snapshotsMu.RUnlock()

	snapshots := make([]*Snapshot, 0, len(t.snapshots))
	for _, s := range t.snapshots {
		// Don't include data in listing
		snapshots = append(snapshots, &Snapshot{
			ID:          s.ID,
			Timestamp:   s.Timestamp,
			Description: s.Description,
			Metrics:     s.Metrics,
			PointCount:  s.PointCount,
			Size:        s.Size,
			Metadata:    s.Metadata,
		})
	}

	// Sort by timestamp descending
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Timestamp > snapshots[j].Timestamp
	})

	return snapshots
}

// DeleteSnapshot deletes a snapshot.
func (t *TimeTravelEngine) DeleteSnapshot(id string) error {
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()

	if _, ok := t.snapshots[id]; !ok {
		return fmt.Errorf("snapshot not found: %s", id)
	}

	delete(t.snapshots, id)
	return nil
}

// RestoreSnapshot restores data from a snapshot.
func (t *TimeTravelEngine) RestoreSnapshot(id string) error {
	snapshot, err := t.GetSnapshot(id)
	if err != nil {
		return err
	}

	// Write all points from snapshot
	return t.db.WriteBatch(snapshot.Data)
}

// QueryAsOf executes a query as of a specific timestamp.
func (t *TimeTravelEngine) QueryAsOf(query *Query, asOf int64) (*Result, error) {
	// First, try to find a snapshot close to the requested time
	var bestSnapshot *Snapshot
	var bestDiff int64 = int64(^uint64(0) >> 1) // Max int64

	t.snapshotsMu.RLock()
	for _, snap := range t.snapshots {
		if snap.Timestamp <= asOf {
			diff := asOf - snap.Timestamp
			if diff < bestDiff {
				bestDiff = diff
				bestSnapshot = snap
			}
		}
	}
	t.snapshotsMu.RUnlock()

	// If we have a snapshot, query from it
	if bestSnapshot != nil {
		return t.queryFromSnapshot(bestSnapshot, query, asOf)
	}

	// Otherwise, reconstruct from versions
	return t.reconstructAsOf(query, asOf)
}

func (t *TimeTravelEngine) queryFromSnapshot(snap *Snapshot, query *Query, asOf int64) (*Result, error) {
	var points []Point

	for _, p := range snap.Data {
		// Check if point matches query
		if query.Metric != "" && query.Metric != p.Metric {
			continue
		}

		if query.Start > 0 && p.Timestamp < query.Start {
			continue
		}

		if query.End > 0 && p.Timestamp > query.End {
			continue
		}

		// Check time constraint
		if p.Timestamp > asOf {
			continue
		}

		// Check tags
		if !matchesTags(p.Tags, query.Tags) {
			continue
		}

		points = append(points, p)
	}

	// Apply any changes between snapshot and asOf time
	points = t.applyVersions(points, query.Metric, snap.Timestamp, asOf)

	return &Result{Points: points}, nil
}

func (t *TimeTravelEngine) applyVersions(points []Point, metric string, fromTime, toTime int64) []Point {
	t.versionsMu.RLock()
	defer t.versionsMu.RUnlock()

	// Build a map of current points
	pointMap := make(map[string]Point)
	for _, p := range points {
		key := pointKey(p)
		pointMap[key] = p
	}

	// Apply versions in order
	for key, versions := range t.versions {
		for _, v := range versions {
			if v.Timestamp > fromTime && v.Timestamp <= toTime {
				if metric != "" && v.Point.Metric != metric {
					continue
				}

				switch v.Operation {
				case "INSERT":
					pointMap[key] = v.Point
				case "UPDATE":
					pointMap[key] = v.Point
				case "DELETE":
					delete(pointMap, key)
				}
			}
		}
	}

	// Convert back to slice
	result := make([]Point, 0, len(pointMap))
	for _, p := range pointMap {
		result = append(result, p)
	}

	// Sort by timestamp
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}
