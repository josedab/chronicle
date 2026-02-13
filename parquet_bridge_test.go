package chronicle

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParquetBridge_New(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultParquetBridgeConfig(dir)

	pb, err := NewParquetBridge(nil, cfg)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if pb == nil {
		t.Fatal("expected non-nil bridge")
	}
}

func TestParquetBridge_RegisterTable(t *testing.T) {
	dir := t.TempDir()
	pb, _ := NewParquetBridge(nil, DefaultParquetBridgeConfig(dir))

	err := pb.RegisterTable(ExternalTable{
		Name: "metrics_archive",
		Path: "/data/metrics.parquet",
		Schema: []ParquetColumn{
			{Name: "metric", Type: "string"},
			{Name: "value", Type: "double"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	tables := pb.ListTables()
	if len(tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(tables))
	}
	if tables[0].Name != "metrics_archive" {
		t.Errorf("name = %q, want metrics_archive", tables[0].Name)
	}
}

func TestParquetBridge_UnregisterTable(t *testing.T) {
	dir := t.TempDir()
	pb, _ := NewParquetBridge(nil, DefaultParquetBridgeConfig(dir))
	pb.RegisterTable(ExternalTable{Name: "t1", Path: "/a"})

	pb.UnregisterTable("t1")
	tables := pb.ListTables()
	if len(tables) != 0 {
		t.Error("expected 0 tables after unregister")
	}
}

func TestParquetBridge_GetTable(t *testing.T) {
	dir := t.TempDir()
	pb, _ := NewParquetBridge(nil, DefaultParquetBridgeConfig(dir))
	pb.RegisterTable(ExternalTable{Name: "t1", Path: "/a"})

	tbl, err := pb.GetTable("t1")
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Name != "t1" {
		t.Error("wrong table name")
	}

	_, err = pb.GetTable("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestParquetBridge_RegisterValidation(t *testing.T) {
	dir := t.TempDir()
	pb, _ := NewParquetBridge(nil, DefaultParquetBridgeConfig(dir))

	err := pb.RegisterTable(ExternalTable{})
	if err == nil {
		t.Error("expected error for empty name")
	}
	err = pb.RegisterTable(ExternalTable{Name: "x"})
	if err == nil {
		t.Error("expected error for empty path")
	}
}

func TestParquetBridge_NewMissingDir(t *testing.T) {
	_, err := NewParquetBridge(nil, ParquetBridgeConfig{})
	if err == nil {
		t.Error("expected error for empty data dir")
	}
}

func TestWriteSimpleParquet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.parquet")

	points := []Point{
		{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"host": "a"}},
		{Metric: "cpu", Value: 84.0, Timestamp: time.Now().UnixNano()},
	}

	err := writeSimpleParquet(path, points)
	if err != nil {
		t.Fatal(err)
	}

	// Verify file starts with PAR1
	f, _ := os.Open(path)
	defer f.Close()
	magic := make([]byte, 4)
	f.Read(magic)
	if string(magic) != "PAR1" {
		t.Error("missing PAR1 magic")
	}

	// Read row count
	rc, err := ReadParquetRowCount(path)
	if err != nil {
		t.Fatalf("ReadParquetRowCount error: %v", err)
	}
	if rc != 2 {
		t.Errorf("row count = %d, want 2", rc)
	}
}

func TestInspectParquetFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.parquet")

	writeSimpleParquet(path, []Point{
		{Metric: "m", Value: 1.0},
	})

	info, err := InspectParquetFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsValid {
		t.Error("expected valid")
	}
	if info.RowCount != 1 {
		t.Errorf("row count = %d, want 1", info.RowCount)
	}
}

func TestParquetPartition_String(t *testing.T) {
	tests := []struct {
		p    ParquetPartition
		want string
	}{
		{ParquetPartitionHourly, "hourly"},
		{ParquetPartitionDaily, "daily"},
		{ParquetPartitionWeekly, "weekly"},
		{ParquetPartitionMonthly, "monthly"},
	}
	for _, tt := range tests {
		if got := tt.p.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.p, got, tt.want)
		}
	}
}

func TestDefaultParquetBridgeConfig(t *testing.T) {
	cfg := DefaultParquetBridgeConfig("/data")
	if cfg.RowGroupSize != 65536 {
		t.Errorf("row group size = %d", cfg.RowGroupSize)
	}
	if cfg.PartitionBy != ParquetPartitionDaily {
		t.Error("expected daily partition")
	}
}
