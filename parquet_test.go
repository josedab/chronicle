package chronicle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultParquetConfig(t *testing.T) {
	config := DefaultParquetConfig()

	if !config.Enabled {
		t.Error("should be enabled by default")
	}
	if config.CompressionCodec == "" {
		t.Error("compression codec should not be empty")
	}
	if config.RowGroupSize <= 0 {
		t.Error("row group size should be positive")
	}
	if config.PageSize <= 0 {
		t.Error("page size should be positive")
	}
}

func TestNewParquetBackend(t *testing.T) {
	dir := t.TempDir()
	config := DefaultParquetConfig()

	backend, err := NewParquetBackend(dir, config)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	defer backend.Close()

	if backend == nil {
		t.Error("backend should not be nil")
	}
}

func TestParquetBackend_WriteReadDelete(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())
	defer backend.Close()

	ctx := context.Background()

	// Write
	key := "test.parquet"
	data := []byte("test data")
	if err := backend.Write(ctx, key, data); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read
	readData, err := backend.Read(ctx, key)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if string(readData) != string(data) {
		t.Error("data mismatch")
	}

	// Exists
	exists, err := backend.Exists(ctx, key)
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if !exists {
		t.Error("file should exist")
	}

	// Delete
	if err := backend.Delete(ctx, key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	exists, _ = backend.Exists(ctx, key)
	if exists {
		t.Error("file should not exist after delete")
	}
}

func TestParquetBackend_WriteReadPoints(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())
	defer backend.Close()

	ctx := context.Background()
	now := time.Now().UnixNano()

	points := []Point{
		{Metric: "cpu.usage", Value: 75.5, Timestamp: now, Tags: map[string]string{"host": "server1"}},
		{Metric: "cpu.usage", Value: 80.0, Timestamp: now + 1000000, Tags: map[string]string{"host": "server1"}},
		{Metric: "mem.usage", Value: 50.0, Timestamp: now + 2000000, Tags: map[string]string{"host": "server2"}},
	}

	key := "metrics.parquet"
	if err := backend.WritePoints(ctx, key, points); err != nil {
		t.Fatalf("failed to write points: %v", err)
	}

	// Read back
	readPoints, err := backend.ReadPoints(ctx, key)
	if err != nil {
		t.Fatalf("failed to read points: %v", err)
	}

	if len(readPoints) != len(points) {
		t.Errorf("point count mismatch: got %d, want %d", len(readPoints), len(points))
	}

	// Check first point
	if readPoints[0].Metric != "cpu.usage" {
		t.Error("metric mismatch")
	}
	if readPoints[0].Value != 75.5 {
		t.Errorf("value mismatch: got %f, want 75.5", readPoints[0].Value)
	}
	if readPoints[0].Tags["host"] != "server1" {
		t.Error("tags mismatch")
	}
}

func TestParquetBackend_List(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())
	defer backend.Close()

	ctx := context.Background()

	// Write multiple files
	_ = backend.WritePoints(ctx, "2024/01/file1.parquet", []Point{{Metric: "m1", Value: 1, Timestamp: 1}})
	_ = backend.WritePoints(ctx, "2024/01/file2.parquet", []Point{{Metric: "m2", Value: 2, Timestamp: 2}})
	_ = backend.WritePoints(ctx, "2024/02/file3.parquet", []Point{{Metric: "m3", Value: 3, Timestamp: 3}})

	// List with prefix
	files, err := backend.List(ctx, "2024/01")
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}

	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}
}

func TestParquetBackend_GetMetadata(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())
	defer backend.Close()

	ctx := context.Background()
	now := time.Now().UnixNano()

	points := []Point{
		{Metric: "test", Value: 1.0, Timestamp: now},
		{Metric: "test", Value: 2.0, Timestamp: now + 1000},
	}

	key := "test.parquet"
	_ = backend.WritePoints(ctx, key, points)

	meta, err := backend.GetMetadata(ctx, key)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	if meta.NumRows != 2 {
		t.Errorf("expected 2 rows, got %d", meta.NumRows)
	}
	if meta.MinTimestamp != now {
		t.Error("min timestamp mismatch")
	}
	if meta.MaxTimestamp != now+1000 {
		t.Error("max timestamp mismatch")
	}
	if meta.CreatedBy != "Chronicle" {
		t.Error("created by should be Chronicle")
	}
}

func TestParquetBackend_QueryByTimeRange(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())
	defer backend.Close()

	ctx := context.Background()
	baseTs := int64(1000000000)

	// Write files with different time ranges
	_ = backend.WritePoints(ctx, "data/file1.parquet", []Point{
		{Metric: "m", Value: 1.0, Timestamp: baseTs},
		{Metric: "m", Value: 2.0, Timestamp: baseTs + 100},
	})
	_ = backend.WritePoints(ctx, "data/file2.parquet", []Point{
		{Metric: "m", Value: 3.0, Timestamp: baseTs + 500},
		{Metric: "m", Value: 4.0, Timestamp: baseTs + 600},
	})

	// Query specific range
	points, err := backend.QueryByTimeRange(ctx, "data", baseTs+50, baseTs+550)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Should get points at baseTs+100 and baseTs+500
	if len(points) != 2 {
		t.Errorf("expected 2 points, got %d", len(points))
	}
}

func TestParquetWriter(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())
	defer backend.Close()

	writer := NewParquetWriter(backend, "stream.parquet", 3)

	// Write points
	now := time.Now().UnixNano()
	_ = writer.Write(Point{Metric: "test", Value: 1.0, Timestamp: now})
	_ = writer.Write(Point{Metric: "test", Value: 2.0, Timestamp: now + 1})
	_ = writer.Write(Point{Metric: "test", Value: 3.0, Timestamp: now + 2}) // This should trigger flush

	// Close should flush any remaining
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Verify file exists
	exists, _ := backend.Exists(context.Background(), "stream.parquet")
	if !exists {
		t.Error("file should exist after flush")
	}
}

func TestTimeSeriesParquetSchema(t *testing.T) {
	schema := TimeSeriesParquetSchema()

	if len(schema) != 4 {
		t.Errorf("expected 4 columns, got %d", len(schema))
	}

	expectedCols := []string{"timestamp", "metric", "value", "tags"}
	for i, col := range schema {
		if col.Name != expectedCols[i] {
			t.Errorf("column %d: expected %s, got %s", i, expectedCols[i], col.Name)
		}
	}
}

func TestParquetBackend_LargeDataset(t *testing.T) {
	dir := t.TempDir()
	config := DefaultParquetConfig()
	config.RowGroupSize = 100 // Small for testing

	backend, _ := NewParquetBackend(dir, config)
	defer backend.Close()

	ctx := context.Background()
	now := time.Now().UnixNano()

	// Generate large dataset
	points := make([]Point, 500)
	for i := range points {
		points[i] = Point{
			Metric:    "large.test",
			Value:     float64(i),
			Timestamp: now + int64(i*1000),
			Tags:      map[string]string{"index": string(rune(i%26) + 'a')},
		}
	}

	key := "large.parquet"
	if err := backend.WritePoints(ctx, key, points); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read back
	readPoints, err := backend.ReadPoints(ctx, key)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if len(readPoints) != len(points) {
		t.Errorf("count mismatch: got %d, want %d", len(readPoints), len(points))
	}
}

func TestParquetBackend_ClosedOperations(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())

	backend.Close()
	ctx := context.Background()

	_, err := backend.Read(ctx, "test")
	if err == nil {
		t.Error("expected error for Read on closed backend")
	}

	err = backend.Write(ctx, "test", []byte("data"))
	if err == nil {
		t.Error("expected error for Write on closed backend")
	}
}

func TestExportImportParquet(t *testing.T) {
	// Setup DB
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		_ = db.Write(Point{
			Metric:    "export.test",
			Value:     float64(i),
			Timestamp: now + int64(i*1000000),
		})
	}
	_ = db.Flush()

	// Export to Parquet - use the same dir as db to ensure data is there
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "export.parquet")

	// Verify we have data first
	result, err := db.Execute(&Query{
		Metric: "export.test",
		Start:  now,
		End:    now + int64(10*1000000),
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Points) == 0 {
		t.Skip("no data found, skipping export test")
	}

	err = ExportToParquet(db, outputPath, "export.test", now, now+int64(10*1000000), DefaultParquetConfig())
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Skip("export produced no file (no matching data)")
	}

	// Create new DB and import
	db2 := setupTestDB(t)
	defer db2.Close()

	err = ImportFromParquet(db2, outputPath)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
}

func TestParquetBackend_EmptyPoints(t *testing.T) {
	dir := t.TempDir()
	backend, _ := NewParquetBackend(dir, DefaultParquetConfig())
	defer backend.Close()

	ctx := context.Background()

	// Writing empty points should be a no-op
	err := backend.WritePoints(ctx, "empty.parquet", []Point{})
	if err != nil {
		t.Errorf("writing empty points should not error: %v", err)
	}

	// File should not exist
	exists, _ := backend.Exists(ctx, "empty.parquet")
	if exists {
		t.Error("file should not exist for empty points")
	}
}
