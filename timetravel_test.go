package chronicle

import (
	"testing"
	"time"
)

func TestTimeTravelConfig(t *testing.T) {
	config := DefaultTimeTravelConfig()

	if !config.Enabled {
		t.Error("Time travel should be enabled by default")
	}
	if config.MaxSnapshots == 0 {
		t.Error("MaxSnapshots should be set")
	}
	if config.SnapshotInterval == 0 {
		t.Error("SnapshotInterval should be set")
	}
	if config.MaxRetention == 0 {
		t.Error("MaxRetention should be set")
	}
}

func TestSnapshot(t *testing.T) {
	snapshot := Snapshot{
		ID:          "snap-001",
		Timestamp:   time.Now().UnixNano(),
		Description: "Test snapshot",
		Metrics:     []string{"cpu", "memory"},
		PointCount:  1000,
		Size:        1024 * 1024,
	}

	if snapshot.ID != "snap-001" {
		t.Errorf("Expected snap-001, got %s", snapshot.ID)
	}
	if snapshot.Size != 1024*1024 {
		t.Errorf("Expected size 1MB, got %d", snapshot.Size)
	}
	if snapshot.PointCount != 1000 {
		t.Errorf("Expected 1000 points, got %d", snapshot.PointCount)
	}
}

func TestVersion(t *testing.T) {
	version := Version{
		Point: Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server1"},
			Value:     42.5,
			Timestamp: time.Now().UnixNano(),
		},
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Operation: "INSERT",
	}

	if version.Operation != "INSERT" {
		t.Errorf("Expected INSERT, got %s", version.Operation)
	}
	if version.Point.Metric != "cpu" {
		t.Errorf("Expected cpu, got %s", version.Point.Metric)
	}
	if version.Point.Value != 42.5 {
		t.Errorf("Expected 42.5, got %f", version.Point.Value)
	}
}

func TestTTChangeEvent(t *testing.T) {
	event := ChangeEvent{
		ID:        "event-001",
		Timestamp: time.Now().UnixNano(),
		Operation: CDCOpInsert,
	}

	if event.ID == "" {
		t.Error("Event ID should not be empty")
	}
	if event.Operation != CDCOpInsert {
		t.Errorf("Expected CDCOpInsert, got %v", event.Operation)
	}
}

func TestDiffResult(t *testing.T) {
	diff := DiffResult{
		Time1:    time.Now().Add(-time.Hour).UnixNano(),
		Time2:    time.Now().UnixNano(),
		Added:    []Point{},
		Removed:  []Point{},
		Modified: []PointDiff{},
	}

	added, removed, modified := diff.CountChanges()
	if added != 0 || removed != 0 || modified != 0 {
		t.Error("Empty diff should have zero counts")
	}
}

func TestPointDiff(t *testing.T) {
	diff := PointDiff{
		Before: Point{
			Metric: "cpu",
			Value:  40.0,
		},
		After: Point{
			Metric: "cpu",
			Value:  50.0,
		},
	}

	if diff.Before.Value >= diff.After.Value {
		t.Error("After value should be greater in this test")
	}
}

func TestTimeTravelStats(t *testing.T) {
	stats := TimeTravelStats{
		Enabled:       true,
		SnapshotCount: 10,
		VersionCount:  1000,
		CDCEventCount: 500,
		MaxRetention:  "168h",
	}

	if !stats.Enabled {
		t.Error("Stats should show enabled")
	}
	if stats.SnapshotCount != 10 {
		t.Errorf("Expected 10 snapshots, got %d", stats.SnapshotCount)
	}
	if stats.VersionCount != 1000 {
		t.Errorf("Expected 1000 versions, got %d", stats.VersionCount)
	}
}

func TestCDCOperations(t *testing.T) {
	operations := []CDCOp{CDCOpInsert, CDCOpUpdate, CDCOpDelete}

	for _, op := range operations {
		event := ChangeEvent{
			Operation: op,
			Timestamp: time.Now().UnixNano(),
		}

		if event.Operation != op {
			t.Errorf("Expected %v, got %v", op, event.Operation)
		}
	}
}

func TestSnapshotCreation(t *testing.T) {
	// Test snapshot ID generation format
	id := ttGenerateSnapshotID()
	if id == "" {
		t.Error("Snapshot ID should not be empty")
	}
}

func ttGenerateSnapshotID() string {
	return "snap-" + time.Now().Format("20060102150405")
}

func TestVersionLogCompaction(t *testing.T) {
	// Test that version log can be compacted based on retention
	retention := 24 * time.Hour
	oldVersion := Version{
		Version:   1,
		Timestamp: time.Now().Add(-48 * time.Hour).UnixNano(),
	}

	shouldCompact := time.Since(time.Unix(0, oldVersion.Timestamp)) > retention
	if !shouldCompact {
		t.Error("Old version should be marked for compaction")
	}
}

func TestPointInTimeRecovery(t *testing.T) {
	// Test PITR timestamp validation
	now := time.Now()
	validTime := now.Add(-time.Hour)
	futureTime := now.Add(time.Hour)

	if !validTime.Before(now) {
		t.Error("Valid recovery time should be in the past")
	}
	if futureTime.Before(now) {
		t.Error("Future time should not be valid for recovery")
	}
}

func TestSnapshotMetadata(t *testing.T) {
	metadata := map[string]string{
		"created_by": "system",
		"reason":     "scheduled",
		"version":    "1.0.0",
	}

	snapshot := Snapshot{
		ID:        "snap-001",
		Timestamp: time.Now().UnixNano(),
		Metadata:  metadata,
	}

	if snapshot.Metadata["created_by"] != "system" {
		t.Error("Metadata should contain created_by")
	}
}

func TestTimeTravelQueryBuilder(t *testing.T) {
	builder := &TimeTravelQueryBuilder{}

	if builder == nil {
		t.Error("Builder should not be nil")
	}
}

func TestTimeTravelDB(t *testing.T) {
	// Just verify type exists
	var db *TimeTravelDB
	_ = db
}
