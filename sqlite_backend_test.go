package chronicle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewSQLiteBackend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	config := DefaultSQLiteBackendConfig()
	config.Path = path

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	// Verify database file was created
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("database file was not created")
	}
}

func TestSQLiteBackend_ReadWriteDelete(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write
	key := "partitions/2024/01/01/data.bin"
	data := []byte("test data content")
	if err := backend.Write(ctx, key, data); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read
	readData, err := backend.Read(ctx, key)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if string(readData) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", readData, data)
	}

	// Exists
	exists, err := backend.Exists(ctx, key)
	if err != nil {
		t.Fatalf("failed to check exists: %v", err)
	}
	if !exists {
		t.Error("key should exist")
	}

	// Delete
	if err := backend.Delete(ctx, key); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify deleted
	exists, err = backend.Exists(ctx, key)
	if err != nil {
		t.Fatalf("failed to check exists after delete: %v", err)
	}
	if exists {
		t.Error("key should not exist after delete")
	}
}

func TestSQLiteBackend_List(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write multiple partitions
	keys := []string{
		"partitions/2024/01/01/data.bin",
		"partitions/2024/01/02/data.bin",
		"partitions/2024/02/01/data.bin",
		"other/file.bin",
	}
	for _, key := range keys {
		if err := backend.Write(ctx, key, []byte("data")); err != nil {
			t.Fatalf("failed to write %s: %v", key, err)
		}
	}

	// List with prefix
	list, err := backend.List(ctx, "partitions/2024/01")
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("expected 2 results, got %d", len(list))
	}
}

func TestSQLiteBackend_WritePoint(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	point := Point{
		Metric:    "cpu.usage",
		Value:     75.5,
		Timestamp: time.Now().UnixNano(),
		Tags: map[string]string{
			"host": "server1",
			"dc":   "us-west",
		},
	}

	if err := backend.WritePoint(ctx, point); err != nil {
		t.Fatalf("failed to write point: %v", err)
	}

	// Query back
	points, err := backend.QueryRange(ctx, "cpu.usage", 0, time.Now().Add(time.Hour).UnixNano())
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}
	if points[0].Value != 75.5 {
		t.Errorf("value mismatch: got %f, want 75.5", points[0].Value)
	}
	if points[0].Tags["host"] != "server1" {
		t.Error("tags mismatch")
	}
}

func TestSQLiteBackend_WritePoints(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "mem.usage", Value: 50.0, Timestamp: now},
		{Metric: "mem.usage", Value: 55.0, Timestamp: now + 1000000},
		{Metric: "mem.usage", Value: 60.0, Timestamp: now + 2000000},
	}

	if err := backend.WritePoints(ctx, points); err != nil {
		t.Fatalf("failed to write points: %v", err)
	}

	// Query back
	results, err := backend.QueryRange(ctx, "mem.usage", 0, now+10000000)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 points, got %d", len(results))
	}
}

func TestSQLiteBackend_ExecuteSQL(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write some data
	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "test.metric", Value: 10.0, Timestamp: now},
		{Metric: "test.metric", Value: 20.0, Timestamp: now + 1000000},
		{Metric: "test.metric", Value: 30.0, Timestamp: now + 2000000},
	}
	_ = backend.WritePoints(ctx, points)

	// Execute raw SQL
	result, err := backend.ExecuteSQL(ctx, "SELECT COUNT(*), AVG(value) FROM time_series WHERE metric = ?", "test.metric")
	if err != nil {
		t.Fatalf("failed to execute SQL: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// Count should be 3
	count := result.Rows[0][0].(int64)
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestSQLiteBackend_Aggregate(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write data with known timestamps
	baseTs := int64(1000000000)
	points := []Point{
		{Metric: "agg.test", Value: 10.0, Timestamp: baseTs},
		{Metric: "agg.test", Value: 20.0, Timestamp: baseTs + 100},
		{Metric: "agg.test", Value: 30.0, Timestamp: baseTs + 1000000},
	}
	_ = backend.WritePoints(ctx, points)

	// Aggregate with interval
	results, err := backend.Aggregate(ctx, "agg.test", 0, baseTs+2000000, "AVG", 1000000)
	if err != nil {
		t.Fatalf("failed to aggregate: %v", err)
	}

	if len(results) < 1 {
		t.Error("expected at least 1 aggregate result")
	}
}

func TestSQLiteBackend_GetMetrics(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write data for different metrics
	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "metric.a", Value: 1.0, Timestamp: now},
		{Metric: "metric.b", Value: 2.0, Timestamp: now},
		{Metric: "metric.a", Value: 3.0, Timestamp: now + 1000},
	}
	_ = backend.WritePoints(ctx, points)

	// Get metrics
	metrics, err := backend.GetMetrics(ctx)
	if err != nil {
		t.Fatalf("failed to get metrics: %v", err)
	}

	if len(metrics) != 2 {
		t.Errorf("expected 2 metrics, got %d", len(metrics))
	}
}

func TestSQLiteBackend_Stats(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write some data
	_ = backend.Write(ctx, "partition1", []byte("data1"))
	_ = backend.WritePoint(ctx, Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()})

	stats, err := backend.Stats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.PartitionCount != 1 {
		t.Errorf("expected 1 partition, got %d", stats.PartitionCount)
	}
	if stats.PointCount != 1 {
		t.Errorf("expected 1 point, got %d", stats.PointCount)
	}
	if stats.MetricCount != 1 {
		t.Errorf("expected 1 metric, got %d", stats.MetricCount)
	}
}

func TestSQLiteBackend_Vacuum(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write and delete some data
	for i := 0; i < 100; i++ {
		_ = backend.Write(ctx, "key"+string(rune(i)), []byte("data"))
	}
	for i := 0; i < 100; i++ {
		_ = backend.Delete(ctx, "key"+string(rune(i)))
	}

	// Vacuum should work
	if err := backend.Vacuum(ctx); err != nil {
		t.Errorf("vacuum failed: %v", err)
	}
}

func TestSQLiteBackend_ExportToCSV(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write data
	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "export.test", Value: 100.0, Timestamp: now, Tags: map[string]string{"env": "prod"}},
		{Metric: "export.test", Value: 200.0, Timestamp: now + 1000},
	}
	_ = backend.WritePoints(ctx, points)

	// Export
	csv, err := backend.ExportToCSV(ctx, "export.test", 0, now+10000)
	if err != nil {
		t.Fatalf("failed to export: %v", err)
	}

	if csv == "" {
		t.Error("expected non-empty CSV")
	}
	if !strContains(csv, "timestamp,metric,value,tags") {
		t.Error("CSV should have header")
	}
	if !strContains(csv, "export.test") {
		t.Error("CSV should contain metric name")
	}
}

func TestSQLiteBackend_ClosedOperations(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	backend.Close()
	ctx := context.Background()

	// Operations should fail after close
	_, err = backend.Read(ctx, "key")
	if err == nil {
		t.Error("expected error for Read after close")
	}

	err = backend.Write(ctx, "key", []byte("data"))
	if err == nil {
		t.Error("expected error for Write after close")
	}

	_, err = backend.List(ctx, "")
	if err == nil {
		t.Error("expected error for List after close")
	}
}

func TestSQLiteBackend_ReadNotFound(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSQLiteBackendConfig()
	config.Path = filepath.Join(dir, "test.db")

	backend, err := NewSQLiteBackend(config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	_, err = backend.Read(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
}

func TestDefaultSQLiteBackendConfig(t *testing.T) {
	config := DefaultSQLiteBackendConfig()

	if config.Path == "" {
		t.Error("expected non-empty path")
	}
	if config.JournalMode != "WAL" {
		t.Error("expected WAL journal mode")
	}
	if config.CacheSize <= 0 {
		t.Error("expected positive cache size")
	}
	if config.BusyTimeout <= 0 {
		t.Error("expected positive busy timeout")
	}
}

// strContains checks if a string contains a substring.
func strContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
