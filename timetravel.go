package chronicle

import (
	"context"
	"encoding/json"
	"errors"
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
			_, _ = t.CreateSnapshot("auto", "Automatic snapshot")
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

func (t *TimeTravelEngine) reconstructAsOf(query *Query, asOf int64) (*Result, error) {
	// Get current data up to asOf time
	originalEnd := query.End
	query.End = asOf

	result, err := t.db.Execute(query)
	if err != nil {
		return nil, err
	}

	query.End = originalEnd

	// Filter out points after asOf
	var filtered []Point
	for _, p := range result.Points {
		if p.Timestamp <= asOf {
			filtered = append(filtered, p)
		}
	}

	return &Result{Points: filtered}, nil
}

func pointKey(p Point) string {
	return fmt.Sprintf("%s:%d:%v", p.Metric, p.Timestamp, p.Tags)
}

// TrackVersion records a version for a data point.
func (t *TimeTravelEngine) TrackVersion(p Point, operation string, previous *Point) {
	if !t.config.Enabled {
		return
	}

	key := pointKey(p)
	version := Version{
		Point:     p,
		Version:   time.Now().UnixNano(),
		Timestamp: time.Now().UnixNano(),
		Operation: operation,
		Previous:  previous,
	}

	t.versionsMu.Lock()
	t.versions[key] = append(t.versions[key], version)
	t.versionsMu.Unlock()

	// Record CDC event
	if t.config.ChangeDataCaptureEnabled {
		t.recordCDCEvent(p, CDCOp(operation), previous)
	}
}

func (t *TimeTravelEngine) recordCDCEvent(p Point, op CDCOp, previous *Point) {
	event := ChangeEvent{
		ID:        fmt.Sprintf("cdc-%d", time.Now().UnixNano()),
		Timestamp: time.Now().UnixNano(),
		Operation: op,
		Metric:    p.Metric,
		Tags:      p.Tags,
		After:     &p,
		Before:    previous,
	}

	t.cdcLogMu.Lock()
	t.cdcLog = append(t.cdcLog, event)
	t.cdcLogMu.Unlock()
}

// GetVersions retrieves version history for a specific point.
func (t *TimeTravelEngine) GetVersions(metric string, tags map[string]string, timestamp int64) []Version {
	key := pointKey(Point{Metric: metric, Tags: tags, Timestamp: timestamp})

	t.versionsMu.RLock()
	defer t.versionsMu.RUnlock()

	versions, ok := t.versions[key]
	if !ok {
		return nil
	}

	// Return a copy
	result := make([]Version, len(versions))
	copy(result, versions)
	return result
}

// GetChanges retrieves CDC events within a time range.
func (t *TimeTravelEngine) GetChanges(start, end int64, metric string) []ChangeEvent {
	t.cdcLogMu.RLock()
	defer t.cdcLogMu.RUnlock()

	var events []ChangeEvent
	for _, event := range t.cdcLog {
		if event.Timestamp >= start && event.Timestamp <= end {
			if metric == "" || event.Metric == metric {
				events = append(events, event)
			}
		}
	}

	return events
}

// StreamChanges returns a channel for CDC events.
func (t *TimeTravelEngine) StreamChanges(ctx context.Context, metric string) <-chan ChangeEvent {
	ch := make(chan ChangeEvent, 100)

	go func() {
		defer close(ch)

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		lastSeen := int64(0)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				events := t.GetChanges(lastSeen+1, time.Now().UnixNano(), metric)
				for _, event := range events {
					select {
					case ch <- event:
						if event.Timestamp > lastSeen {
							lastSeen = event.Timestamp
						}
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch
}

// Diff compares data between two timestamps.
func (t *TimeTravelEngine) Diff(metric string, time1, time2 int64) (*DiffResult, error) {
	// Query at both times
	query := &Query{Metric: metric}

	result1, err := t.QueryAsOf(query, time1)
	if err != nil {
		return nil, fmt.Errorf("failed to query at time1: %w", err)
	}

	result2, err := t.QueryAsOf(query, time2)
	if err != nil {
		return nil, fmt.Errorf("failed to query at time2: %w", err)
	}

	// Build maps for comparison
	map1 := make(map[string]Point)
	for _, p := range result1.Points {
		map1[pointKey(p)] = p
	}

	map2 := make(map[string]Point)
	for _, p := range result2.Points {
		map2[pointKey(p)] = p
	}

	diff := &DiffResult{
		Time1:    time1,
		Time2:    time2,
		Added:    make([]Point, 0),
		Removed:  make([]Point, 0),
		Modified: make([]PointDiff, 0),
	}

	// Find added and modified
	for key, p2 := range map2 {
		if p1, ok := map1[key]; ok {
			if p1.Value != p2.Value {
				diff.Modified = append(diff.Modified, PointDiff{
					Before: p1,
					After:  p2,
				})
			}
		} else {
			diff.Added = append(diff.Added, p2)
		}
	}

	// Find removed
	for key, p1 := range map1 {
		if _, ok := map2[key]; !ok {
			diff.Removed = append(diff.Removed, p1)
		}
	}

	return diff, nil
}

// DiffResult contains the result of a diff operation.
type DiffResult struct {
	Time1    int64       `json:"time1"`
	Time2    int64       `json:"time2"`
	Added    []Point     `json:"added"`
	Removed  []Point     `json:"removed"`
	Modified []PointDiff `json:"modified"`
}

// PointDiff represents a modification to a point.
type PointDiff struct {
	Before Point `json:"before"`
	After  Point `json:"after"`
}

// CountChanges returns statistics about changes.
func (d *DiffResult) CountChanges() (added, removed, modified int) {
	return len(d.Added), len(d.Removed), len(d.Modified)
}

// Stats returns time-travel engine statistics.
func (t *TimeTravelEngine) Stats() TimeTravelStats {
	t.snapshotsMu.RLock()
	snapshotCount := len(t.snapshots)
	t.snapshotsMu.RUnlock()

	t.versionsMu.RLock()
	versionCount := 0
	for _, versions := range t.versions {
		versionCount += len(versions)
	}
	t.versionsMu.RUnlock()

	t.cdcLogMu.RLock()
	cdcEventCount := len(t.cdcLog)
	t.cdcLogMu.RUnlock()

	return TimeTravelStats{
		Enabled:       t.config.Enabled,
		SnapshotCount: snapshotCount,
		VersionCount:  versionCount,
		CDCEventCount: cdcEventCount,
		MaxRetention:  t.config.MaxRetention.String(),
	}
}

// TimeTravelStats contains time-travel statistics.
type TimeTravelStats struct {
	Enabled       bool   `json:"enabled"`
	SnapshotCount int    `json:"snapshot_count"`
	VersionCount  int    `json:"version_count"`
	CDCEventCount int    `json:"cdc_event_count"`
	MaxRetention  string `json:"max_retention"`
}

// TimeTravelDB wraps a DB with time-travel capabilities.
type TimeTravelDB struct {
	*DB
	timeTravel *TimeTravelEngine
}

// NewTimeTravelDB creates a time-travel enabled database wrapper.
func NewTimeTravelDB(db *DB, config TimeTravelConfig) (*TimeTravelDB, error) {
	engine := NewTimeTravelEngine(db, config)

	return &TimeTravelDB{
		DB:         db,
		timeTravel: engine,
	}, nil
}

// Start starts time-travel tracking.
func (t *TimeTravelDB) Start() error {
	return t.timeTravel.Start()
}

// Stop stops time-travel tracking.
func (t *TimeTravelDB) Stop() error {
	return t.timeTravel.Stop()
}

// Write writes a point with version tracking.
func (t *TimeTravelDB) Write(p Point) error {
	// Check if this is an update
	existing, _ := t.DB.Execute(&Query{
		Metric: p.Metric,
		Tags:   p.Tags,
		Start:  p.Timestamp,
		End:    p.Timestamp,
	})

	var previous *Point
	operation := "INSERT"
	if existing != nil && len(existing.Points) > 0 {
		previous = &existing.Points[0]
		operation = "UPDATE"
	}

	if err := t.DB.Write(p); err != nil {
		return err
	}

	t.timeTravel.TrackVersion(p, operation, previous)
	return nil
}

// QueryAsOf executes a query as of a specific timestamp.
func (t *TimeTravelDB) QueryAsOf(query *Query, asOf int64) (*Result, error) {
	return t.timeTravel.QueryAsOf(query, asOf)
}

// QueryAsOfTime executes a query as of a specific time.
func (t *TimeTravelDB) QueryAsOfTime(query *Query, asOf time.Time) (*Result, error) {
	return t.timeTravel.QueryAsOf(query, asOf.UnixNano())
}

// CreateSnapshot creates a snapshot.
func (t *TimeTravelDB) CreateSnapshot(id, description string) (*Snapshot, error) {
	return t.timeTravel.CreateSnapshot(id, description)
}

// RestoreSnapshot restores from a snapshot.
func (t *TimeTravelDB) RestoreSnapshot(id string) error {
	return t.timeTravel.RestoreSnapshot(id)
}

// GetChanges retrieves CDC events.
func (t *TimeTravelDB) GetChanges(start, end int64, metric string) []ChangeEvent {
	return t.timeTravel.GetChanges(start, end, metric)
}

// StreamChanges streams CDC events.
func (t *TimeTravelDB) StreamChanges(ctx context.Context, metric string) <-chan ChangeEvent {
	return t.timeTravel.StreamChanges(ctx, metric)
}

// Diff compares data between two timestamps.
func (t *TimeTravelDB) Diff(metric string, time1, time2 int64) (*DiffResult, error) {
	return t.timeTravel.Diff(metric, time1, time2)
}

// TimeTravel returns the underlying time-travel engine.
func (t *TimeTravelDB) TimeTravel() *TimeTravelEngine {
	return t.timeTravel
}

// QueryBuilder for time-travel queries.
type TimeTravelQueryBuilder struct {
	query *Query
	asOf  *int64
	db    *TimeTravelDB
}

// NewTimeTravelQuery creates a new time-travel query builder.
func NewTimeTravelQuery(db *TimeTravelDB) *TimeTravelQueryBuilder {
	return &TimeTravelQueryBuilder{
		query: &Query{},
		db:    db,
	}
}

// Select sets the metric to query.
func (b *TimeTravelQueryBuilder) Select(metric string) *TimeTravelQueryBuilder {
	b.query.Metric = metric
	return b
}

// Where adds tag filters.
func (b *TimeTravelQueryBuilder) Where(tags map[string]string) *TimeTravelQueryBuilder {
	b.query.Tags = tags
	return b
}

// Between sets the time range.
func (b *TimeTravelQueryBuilder) Between(start, end int64) *TimeTravelQueryBuilder {
	b.query.Start = start
	b.query.End = end
	return b
}

// AsOf sets the AS OF timestamp.
func (b *TimeTravelQueryBuilder) AsOf(timestamp int64) *TimeTravelQueryBuilder {
	b.asOf = &timestamp
	return b
}

// AsOfTime sets the AS OF time.
func (b *TimeTravelQueryBuilder) AsOfTime(t time.Time) *TimeTravelQueryBuilder {
	ts := t.UnixNano()
	b.asOf = &ts
	return b
}

// Execute runs the query.
func (b *TimeTravelQueryBuilder) Execute() (*Result, error) {
	if b.asOf != nil {
		return b.db.QueryAsOf(b.query, *b.asOf)
	}
	return b.db.Execute(b.query)
}

// ExportCDCLog exports the CDC log as JSON.
func (t *TimeTravelEngine) ExportCDCLog(start, end int64) ([]byte, error) {
	events := t.GetChanges(start, end, "")
	return json.Marshal(events)
}

// ImportCDCLog imports CDC events from JSON.
func (t *TimeTravelEngine) ImportCDCLog(data []byte) error {
	var events []ChangeEvent
	if err := json.Unmarshal(data, &events); err != nil {
		return err
	}

	t.cdcLogMu.Lock()
	t.cdcLog = append(t.cdcLog, events...)
	t.cdcLogMu.Unlock()

	return nil
}

// PointInTimeRecovery recovers data to a specific point in time.
func (t *TimeTravelEngine) PointInTimeRecovery(targetTime int64) error {
	// Find the best snapshot before target time
	var bestSnapshot *Snapshot
	t.snapshotsMu.RLock()
	for _, snap := range t.snapshots {
		if snap.Timestamp <= targetTime {
			if bestSnapshot == nil || snap.Timestamp > bestSnapshot.Timestamp {
				bestSnapshot = snap
			}
		}
	}
	t.snapshotsMu.RUnlock()

	if bestSnapshot == nil {
		return errors.New("no suitable snapshot found for recovery")
	}

	// Restore from snapshot
	if err := t.RestoreSnapshot(bestSnapshot.ID); err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	// Apply changes between snapshot and target time
	changes := t.GetChanges(bestSnapshot.Timestamp, targetTime, "")
	for _, change := range changes {
		if change.After != nil {
			if err := t.db.Write(*change.After); err != nil {
				return fmt.Errorf("failed to apply change: %w", err)
			}
		}
	}

	return nil
}
