// timetravel_reconstruct.go contains extended timetravel functionality.
package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

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

	go func(ctx context.Context) {
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
	}(ctx)

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
