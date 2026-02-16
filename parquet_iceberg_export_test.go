package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestDefaultIcebergExportConfig(t *testing.T) {
	cfg := DefaultIcebergExportConfig()

	if cfg.Enabled {
		t.Error("expected Enabled=false by default")
	}
	if cfg.ExportDir != "iceberg-export" {
		t.Errorf("ExportDir = %q, want %q", cfg.ExportDir, "iceberg-export")
	}
	if cfg.CatalogType != "rest" {
		t.Errorf("CatalogType = %q, want %q", cfg.CatalogType, "rest")
	}
	if cfg.CatalogURI != "http://localhost:8181" {
		t.Errorf("CatalogURI = %q, want %q", cfg.CatalogURI, "http://localhost:8181")
	}
	if cfg.Warehouse != "chronicle-warehouse" {
		t.Errorf("Warehouse = %q, want %q", cfg.Warehouse, "chronicle-warehouse")
	}
	if cfg.Namespace != "chronicle" {
		t.Errorf("Namespace = %q, want %q", cfg.Namespace, "chronicle")
	}
	if cfg.TablePrefix != "ts_" {
		t.Errorf("TablePrefix = %q, want %q", cfg.TablePrefix, "ts_")
	}
	if cfg.ExportInterval != 1*time.Hour {
		t.Errorf("ExportInterval = %v, want %v", cfg.ExportInterval, 1*time.Hour)
	}
	if cfg.ColdPartitionAge != 24*time.Hour {
		t.Errorf("ColdPartitionAge = %v, want %v", cfg.ColdPartitionAge, 24*time.Hour)
	}
	if cfg.ParquetCompression != "snappy" {
		t.Errorf("ParquetCompression = %q, want %q", cfg.ParquetCompression, "snappy")
	}
	if cfg.RowGroupSize != 65536 {
		t.Errorf("RowGroupSize = %d, want %d", cfg.RowGroupSize, 65536)
	}
	if cfg.TargetFileSizeMB != 128 {
		t.Errorf("TargetFileSizeMB = %d, want %d", cfg.TargetFileSizeMB, 128)
	}
	if cfg.EnableScheduledExport {
		t.Error("expected EnableScheduledExport=false by default")
	}
	if cfg.ScheduleCron != "0 * * * *" {
		t.Errorf("ScheduleCron = %q, want %q", cfg.ScheduleCron, "0 * * * *")
	}
	if cfg.RetainExportedDays != 90 {
		t.Errorf("RetainExportedDays = %d, want %d", cfg.RetainExportedDays, 90)
	}
}

func TestDefaultIcebergSchema(t *testing.T) {
	schema := DefaultTimeSeriesIcebergSchema()

	if schema.SchemaID != 0 {
		t.Errorf("SchemaID = %d, want 0", schema.SchemaID)
	}

	if len(schema.Fields) != 4 {
		t.Fatalf("Fields count = %d, want 4", len(schema.Fields))
	}

	expectedFields := []struct {
		name     string
		typ      IcebergFieldType
		required bool
	}{
		{"timestamp", IcebergTimestampTZ, true},
		{"metric", IcebergString, true},
		{"value", IcebergDouble, true},
		{"tags", IcebergString, false},
	}
	for i, ef := range expectedFields {
		f := schema.Fields[i]
		if f.Name != ef.name {
			t.Errorf("Fields[%d].Name = %q, want %q", i, f.Name, ef.name)
		}
		if f.Type != ef.typ {
			t.Errorf("Fields[%d].Type = %q, want %q", i, f.Type, ef.typ)
		}
		if f.Required != ef.required {
			t.Errorf("Fields[%d].Required = %v, want %v", i, f.Required, ef.required)
		}
		if f.ID != i+1 {
			t.Errorf("Fields[%d].ID = %d, want %d", i, f.ID, i+1)
		}
	}

	if len(schema.PartitionSpec) != 2 {
		t.Fatalf("PartitionSpec count = %d, want 2", len(schema.PartitionSpec))
	}
	if schema.PartitionSpec[0].Transform != "day" {
		t.Errorf("PartitionSpec[0].Transform = %q, want %q", schema.PartitionSpec[0].Transform, "day")
	}
	if schema.PartitionSpec[1].Transform != "hour" {
		t.Errorf("PartitionSpec[1].Transform = %q, want %q", schema.PartitionSpec[1].Transform, "hour")
	}

	if len(schema.SortOrder) != 1 {
		t.Fatalf("SortOrder count = %d, want 1", len(schema.SortOrder))
	}
	if schema.SortOrder[0].Direction != "asc" {
		t.Errorf("SortOrder[0].Direction = %q, want %q", schema.SortOrder[0].Direction, "asc")
	}
}

func TestIcebergCatalog_CreateNamespace(t *testing.T) {
	cfg := DefaultIcebergExportConfig()
	catalog := NewIcebergCatalog(cfg)

	if err := catalog.CreateNamespace("test_ns"); err != nil {
		t.Fatalf("CreateNamespace failed: %v", err)
	}

	// Duplicate should error.
	if err := catalog.CreateNamespace("test_ns"); err == nil {
		t.Error("expected error creating duplicate namespace")
	}

	// Different namespace should succeed.
	if err := catalog.CreateNamespace("other_ns"); err != nil {
		t.Fatalf("CreateNamespace for other_ns failed: %v", err)
	}
}

func TestIcebergCatalog_CreateTable(t *testing.T) {
	cfg := DefaultIcebergExportConfig()
	cfg.ExportDir = t.TempDir()
	catalog := NewIcebergCatalog(cfg)
	schema := DefaultTimeSeriesIcebergSchema()

	meta, err := catalog.CreateTable("ns1", "metrics", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if meta.FormatVersion != 2 {
		t.Errorf("FormatVersion = %d, want 2", meta.FormatVersion)
	}
	if meta.TableUUID == "" {
		t.Error("expected non-empty TableUUID")
	}
	if meta.CurrentSnapshot != -1 {
		t.Errorf("CurrentSnapshot = %d, want -1", meta.CurrentSnapshot)
	}
	if len(meta.Schema.Fields) != 4 {
		t.Errorf("Schema.Fields count = %d, want 4", len(meta.Schema.Fields))
	}

	// LoadTable should return same metadata.
	loaded, err := catalog.LoadTable("ns1", "metrics")
	if err != nil {
		t.Fatalf("LoadTable failed: %v", err)
	}
	if loaded.TableUUID != meta.TableUUID {
		t.Error("LoadTable returned different UUID")
	}

	// Duplicate table should error.
	if _, err := catalog.CreateTable("ns1", "metrics", schema); err == nil {
		t.Error("expected error creating duplicate table")
	}

	// Loading non-existent table should error.
	if _, err := catalog.LoadTable("ns1", "nonexistent"); err == nil {
		t.Error("expected error loading non-existent table")
	}
}

func TestIcebergCatalog_ListTables(t *testing.T) {
	cfg := DefaultIcebergExportConfig()
	cfg.ExportDir = t.TempDir()
	catalog := NewIcebergCatalog(cfg)
	schema := DefaultTimeSeriesIcebergSchema()

	// Empty namespace returns nil/empty.
	tables := catalog.ListTables("ns1")
	if len(tables) != 0 {
		t.Errorf("expected 0 tables, got %d", len(tables))
	}

	catalog.CreateTable("ns1", "alpha", schema)
	catalog.CreateTable("ns1", "beta", schema)
	catalog.CreateTable("ns2", "gamma", schema)

	tables = catalog.ListTables("ns1")
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables in ns1, got %d", len(tables))
	}
	// ListTables sorts results.
	if tables[0] != "alpha" || tables[1] != "beta" {
		t.Errorf("tables = %v, want [alpha, beta]", tables)
	}

	tables = catalog.ListTables("ns2")
	if len(tables) != 1 || tables[0] != "gamma" {
		t.Errorf("ns2 tables = %v, want [gamma]", tables)
	}
}

func TestIcebergCatalog_UpdateTable(t *testing.T) {
	cfg := DefaultIcebergExportConfig()
	cfg.ExportDir = t.TempDir()
	catalog := NewIcebergCatalog(cfg)
	schema := DefaultTimeSeriesIcebergSchema()

	catalog.CreateTable("ns", "tbl", schema)

	snap := IcebergSnapshot{
		SnapshotID:   100,
		Timestamp:    time.Now(),
		ManifestList: "/data/manifest-1.avro",
		Summary:      map[string]string{"added-data-files": "1"},
	}
	if err := catalog.UpdateTable("ns", "tbl", snap); err != nil {
		t.Fatalf("UpdateTable failed: %v", err)
	}

	meta, _ := catalog.LoadTable("ns", "tbl")
	if meta.CurrentSnapshot != 100 {
		t.Errorf("CurrentSnapshot = %d, want 100", meta.CurrentSnapshot)
	}
	if len(meta.Snapshots) != 1 {
		t.Fatalf("Snapshots count = %d, want 1", len(meta.Snapshots))
	}

	// Add second snapshot.
	snap2 := IcebergSnapshot{
		SnapshotID:       200,
		ParentSnapshotID: 100,
		Timestamp:        time.Now(),
		ManifestList:     "/data/manifest-2.avro",
		Summary:          map[string]string{"added-data-files": "1"},
	}
	if err := catalog.UpdateTable("ns", "tbl", snap2); err != nil {
		t.Fatalf("UpdateTable (second) failed: %v", err)
	}

	meta, _ = catalog.LoadTable("ns", "tbl")
	if meta.CurrentSnapshot != 200 {
		t.Errorf("CurrentSnapshot = %d, want 200", meta.CurrentSnapshot)
	}
	if len(meta.Snapshots) != 2 {
		t.Errorf("Snapshots count = %d, want 2", len(meta.Snapshots))
	}

	// Update non-existent table should error.
	if err := catalog.UpdateTable("ns", "missing", snap); err == nil {
		t.Error("expected error updating non-existent table")
	}
}

func TestPartitionExporter_ExportPartition(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// Write test data.
	for i := 0; i < 10; i++ {
		ts := startOfDay.Add(time.Duration(i) * time.Minute)
		if err := db.Write(Point{
			Metric:    "cpu.usage",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(i) * 10.5,
			Timestamp: ts.UnixNano(),
		}); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	exportCfg := DefaultIcebergExportConfig()
	exportCfg.ExportDir = dir + "/export"
	exporter := NewPartitionExporter(db, exportCfg)

	endOfDay := startOfDay.Add(24 * time.Hour)
	info, err := exporter.ExportPartition(context.Background(), "cpu.usage", startOfDay, endOfDay)
	if err != nil {
		t.Fatalf("ExportPartition failed: %v", err)
	}

	if info.Metric != "cpu.usage" {
		t.Errorf("Metric = %q, want %q", info.Metric, "cpu.usage")
	}
	if info.RecordCount != 10 {
		t.Errorf("RecordCount = %d, want 10", info.RecordCount)
	}
	if info.SizeBytes <= 0 {
		t.Error("expected SizeBytes > 0")
	}
	if info.FilePath == "" {
		t.Error("expected non-empty FilePath")
	}
	if info.Partition != startOfDay.Format("2006-01-02") {
		t.Errorf("Partition = %q, want %q", info.Partition, startOfDay.Format("2006-01-02"))
	}

	// ListExportedFiles should contain the exported file.
	files := exporter.ListExportedFiles("cpu.usage")
	if len(files) != 1 {
		t.Fatalf("ListExportedFiles count = %d, want 1", len(files))
	}

	// Export with no data should error.
	_, err = exporter.ExportPartition(context.Background(), "nonexistent", startOfDay, endOfDay)
	if err == nil {
		t.Error("expected error exporting non-existent metric")
	}
}

func TestPartitionExporter_DetectColdPartitions(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// DetectColdPartitions queries from (now - 2*ColdPartitionAge) to (now - ColdPartitionAge).
	// With ColdPartitionAge=24h, that's -48h to -24h. Write data in that window.
	baseTime := time.Now().UTC().Add(-36 * time.Hour)
	for i := 0; i < 10; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Minute)
		if err := db.Write(Point{
			Metric:    "mem.free",
			Tags:      map[string]string{"host": "node1"},
			Value:     float64(i) * 100,
			Timestamp: ts.UnixNano(),
		}); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify data is queryable.
	metrics := db.Metrics()
	if len(metrics) == 0 {
		t.Fatal("expected at least one metric after write+flush")
	}

	exportCfg := DefaultIcebergExportConfig()
	exportCfg.ColdPartitionAge = 24 * time.Hour
	exporter := NewPartitionExporter(db, exportCfg)

	cold := exporter.DetectColdPartitions()
	if len(cold) == 0 {
		t.Fatal("expected at least one cold partition")
	}

	found := false
	for _, cp := range cold {
		if cp.Metric == "mem.free" {
			found = true
			if cp.Age == "" {
				t.Error("expected non-empty Age")
			}
		}
	}
	if !found {
		t.Error("expected cold partition for mem.free")
	}
}

func TestNewIcebergExportManager(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	exportCfg := DefaultIcebergExportConfig()
	exportCfg.ExportDir = dir + "/export"

	mgr := NewIcebergExportManager(db, exportCfg)
	if mgr == nil {
		t.Fatal("NewIcebergExportManager returned nil")
	}

	status := mgr.GetExportStatus()
	if status.Running {
		t.Error("expected Running=false before Start")
	}
	if status.TotalExports != 0 {
		t.Errorf("TotalExports = %d, want 0", status.TotalExports)
	}
}

func TestIcebergExportManager_StartStop(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	exportCfg := DefaultIcebergExportConfig()
	exportCfg.ExportDir = dir + "/export"
	exportCfg.Enabled = true

	mgr := NewIcebergExportManager(db, exportCfg)

	// Start should succeed.
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	status := mgr.GetExportStatus()
	if !status.Running {
		t.Error("expected Running=true after Start")
	}

	// Double start should error.
	if err := mgr.Start(); err == nil {
		t.Error("expected error on double Start")
	}

	// Stop should succeed.
	if err := mgr.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	status = mgr.GetExportStatus()
	if status.Running {
		t.Error("expected Running=false after Stop")
	}

	// Stop again should be a no-op (no error).
	if err := mgr.Stop(); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}

	// Start with disabled config should error.
	disabledCfg := DefaultIcebergExportConfig()
	disabledCfg.ExportDir = dir + "/export2"
	disabledMgr := NewIcebergExportManager(db, disabledCfg)
	if err := disabledMgr.Start(); err == nil {
		t.Error("expected error starting disabled manager")
	}
}

func TestIcebergExportManager_ExportPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	exportCfg := DefaultIcebergExportConfig()
	exportCfg.ExportDir = dir + "/export"
	mgr := NewIcebergExportManager(db, exportCfg)

	// Initially no policies.
	policies := mgr.ListExportPolicies()
	if len(policies) != 0 {
		t.Errorf("expected 0 policies, got %d", len(policies))
	}

	// Create a policy.
	p := ExportPolicy{
		ID:           "policy-1",
		CronSchedule: "0 */6 * * *",
		MetricFilter: "cpu\\..*",
		Enabled:      true,
	}
	if err := mgr.CreateExportPolicy(p); err != nil {
		t.Fatalf("CreateExportPolicy failed: %v", err)
	}

	policies = mgr.ListExportPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].ID != "policy-1" {
		t.Errorf("Policy ID = %q, want %q", policies[0].ID, "policy-1")
	}
	if policies[0].MaxParallelExports != 2 {
		t.Errorf("MaxParallelExports = %d, want 2 (default)", policies[0].MaxParallelExports)
	}

	// Duplicate policy should error.
	if err := mgr.CreateExportPolicy(p); err == nil {
		t.Error("expected error creating duplicate policy")
	}

	// Policy with empty ID gets auto-generated.
	p2 := ExportPolicy{
		CronSchedule: "0 0 * * *",
		Enabled:      false,
	}
	if err := mgr.CreateExportPolicy(p2); err != nil {
		t.Fatalf("CreateExportPolicy (auto-id) failed: %v", err)
	}
	policies = mgr.ListExportPolicies()
	if len(policies) != 2 {
		t.Errorf("expected 2 policies, got %d", len(policies))
	}
}

func TestIcebergExportManager_GetExportStatus(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	exportCfg := DefaultIcebergExportConfig()
	exportCfg.ExportDir = dir + "/export"
	exportCfg.Enabled = true
	mgr := NewIcebergExportManager(db, exportCfg)

	// Add an enabled and disabled policy.
	mgr.CreateExportPolicy(ExportPolicy{
		ID:      "active-policy",
		Enabled: true,
	})
	mgr.CreateExportPolicy(ExportPolicy{
		ID:      "inactive-policy",
		Enabled: false,
	})

	status := mgr.GetExportStatus()
	if status.Running {
		t.Error("expected Running=false before Start")
	}
	if status.ActivePolicies != 1 {
		t.Errorf("ActivePolicies = %d, want 1", status.ActivePolicies)
	}
	if status.TotalExports != 0 {
		t.Errorf("TotalExports = %d, want 0", status.TotalExports)
	}
	if status.FailedExports != 0 {
		t.Errorf("FailedExports = %d, want 0", status.FailedExports)
	}
	if status.CatalogTables != 0 {
		t.Errorf("CatalogTables = %d, want 0", status.CatalogTables)
	}

	// Start manager and verify status.
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer mgr.Stop()

	status = mgr.GetExportStatus()
	if !status.Running {
		t.Error("expected Running=true after Start")
	}
}
