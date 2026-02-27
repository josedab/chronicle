package chronicle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// IcebergExportConfig configures the Parquet/Iceberg export engine.
type IcebergExportConfig struct {
	Enabled               bool          `json:"enabled"`
	ExportDir             string        `json:"export_dir"`
	CatalogType           string        `json:"catalog_type"` // rest, hive, glue
	CatalogURI            string        `json:"catalog_uri"`
	Warehouse             string        `json:"warehouse"`
	Namespace             string        `json:"namespace"`
	TablePrefix           string        `json:"table_prefix"`
	ExportInterval        time.Duration `json:"export_interval"`
	ColdPartitionAge      time.Duration `json:"cold_partition_age"`
	ParquetCompression    string        `json:"parquet_compression"`
	RowGroupSize          int           `json:"row_group_size"`
	TargetFileSizeMB      int           `json:"target_file_size_mb"`
	EnableScheduledExport bool          `json:"enable_scheduled_export"`
	ScheduleCron          string        `json:"schedule_cron"`
	RetainExportedDays    int           `json:"retain_exported_days"`
}

// DefaultIcebergExportConfig returns sensible defaults for Iceberg export.
func DefaultIcebergExportConfig() IcebergExportConfig {
	return IcebergExportConfig{
		Enabled:               false,
		ExportDir:             "iceberg-export",
		CatalogType:           "rest",
		CatalogURI:            "http://localhost:8181",
		Warehouse:             "chronicle-warehouse",
		Namespace:             "chronicle",
		TablePrefix:           "ts_",
		ExportInterval:        1 * time.Hour,
		ColdPartitionAge:      24 * time.Hour,
		ParquetCompression:    "snappy",
		RowGroupSize:          65536,
		TargetFileSizeMB:      128,
		EnableScheduledExport: false,
		ScheduleCron:          "0 * * * *",
		RetainExportedDays:    90,
	}
}

// ---------------------------------------------------------------------------
// Iceberg Schema
// ---------------------------------------------------------------------------

// IcebergFieldType represents an Iceberg primitive type.
type IcebergFieldType string

const (
	IcebergLong        IcebergFieldType = "long"
	IcebergDouble      IcebergFieldType = "double"
	IcebergString      IcebergFieldType = "string"
	IcebergTimestampTZ IcebergFieldType = "timestamptz"
)

// IcebergField defines a single field in an Iceberg schema.
type IcebergField struct {
	ID       int              `json:"id"`
	Name     string           `json:"name"`
	Type     IcebergFieldType `json:"type"`
	Required bool             `json:"required"`
	Doc      string           `json:"doc,omitempty"`
}

// IcebergPartitionField defines a partition transform.
type IcebergPartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"` // day, hour, identity, etc.
}

// IcebergSortField defines sort ordering.
type IcebergSortField struct {
	SourceID  int    `json:"source-id"`
	Transform string `json:"transform"`
	Direction string `json:"direction"` // asc, desc
	NullOrder string `json:"null-order"`
}

// IcebergSchema represents an Iceberg table schema with partition spec and sort order.
type IcebergSchema struct {
	SchemaID      int                     `json:"schema-id"`
	Fields        []IcebergField          `json:"fields"`
	PartitionSpec []IcebergPartitionField `json:"partition-spec"`
	SortOrder     []IcebergSortField      `json:"sort-order"`
}

// DefaultTimeSeriesIcebergSchema returns the standard Iceberg schema for Chronicle time-series data.
func DefaultTimeSeriesIcebergSchema() IcebergSchema {
	return IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergField{
			{ID: 1, Name: "timestamp", Type: IcebergTimestampTZ, Required: true, Doc: "Event timestamp"},
			{ID: 2, Name: "metric", Type: IcebergString, Required: true, Doc: "Metric name"},
			{ID: 3, Name: "value", Type: IcebergDouble, Required: true, Doc: "Metric value"},
			{ID: 4, Name: "tags", Type: IcebergString, Required: false, Doc: "JSON-encoded tags"},
		},
		PartitionSpec: []IcebergPartitionField{
			{SourceID: 1, FieldID: 1000, Name: "ts_day", Transform: "day"},
			{SourceID: 1, FieldID: 1001, Name: "ts_hour", Transform: "hour"},
		},
		SortOrder: []IcebergSortField{
			{SourceID: 1, Transform: "identity", Direction: "asc", NullOrder: "nulls-last"},
		},
	}
}

// ---------------------------------------------------------------------------
// Iceberg Snapshot
// ---------------------------------------------------------------------------

// IcebergSnapshot represents a point-in-time snapshot of an Iceberg table.
type IcebergSnapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID int64             `json:"parent-snapshot-id,omitempty"`
	Timestamp        time.Time         `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary"`
}

// ---------------------------------------------------------------------------
// Iceberg Manifest
// ---------------------------------------------------------------------------

// IcebergDataFile describes a single data file within a manifest.
type IcebergDataFile struct {
	FilePath      string            `json:"file-path"`
	Format        string            `json:"file-format"`
	RecordCount   int64             `json:"record-count"`
	FileSizeBytes int64             `json:"file-size-in-bytes"`
	Partition     map[string]string `json:"partition,omitempty"`
	ColumnStats   map[string]string `json:"column-stats,omitempty"`
}

// IcebergManifest tracks data files belonging to a snapshot.
type IcebergManifest struct {
	ManifestPath  string            `json:"manifest-path"`
	DataFiles     []IcebergDataFile `json:"data-files"`
	PartitionInfo map[string]string `json:"partition-info,omitempty"`
}

// ---------------------------------------------------------------------------
// Iceberg Catalog
// ---------------------------------------------------------------------------

// IcebergTableMetadata holds the metadata for a single Iceberg table.
type IcebergTableMetadata struct {
	FormatVersion   int               `json:"format-version"`
	TableUUID       string            `json:"table-uuid"`
	Location        string            `json:"location"`
	Schema          IcebergSchema     `json:"schema"`
	Snapshots       []IcebergSnapshot `json:"snapshots"`
	CurrentSnapshot int64             `json:"current-snapshot-id"`
	CreatedAt       time.Time         `json:"created-at"`
}

// IcebergCatalog provides catalog management for Iceberg tables.
type IcebergCatalog struct {
	config     IcebergExportConfig
	mu         sync.RWMutex
	namespaces map[string]bool
	tables     map[string]*IcebergTableMetadata // key: namespace.table
}

// NewIcebergCatalog creates a new catalog manager.
func NewIcebergCatalog(config IcebergExportConfig) *IcebergCatalog {
	return &IcebergCatalog{
		config:     config,
		namespaces: make(map[string]bool),
		tables:     make(map[string]*IcebergTableMetadata),
	}
}

// CreateNamespace registers a new namespace.
func (c *IcebergCatalog) CreateNamespace(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.namespaces[name] {
		return fmt.Errorf("namespace %q already exists", name)
	}
	c.namespaces[name] = true
	return nil
}

// CreateTable creates a new Iceberg table in the catalog.
func (c *IcebergCatalog) CreateTable(namespace, name string, schema IcebergSchema) (*IcebergTableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate namespace and name to prevent path traversal
	if strings.Contains(namespace, "..") || strings.ContainsAny(namespace, `/\`) {
		return nil, fmt.Errorf("invalid namespace: path traversal not allowed")
	}
	if strings.Contains(name, "..") || strings.ContainsAny(name, `/\`) {
		return nil, fmt.Errorf("invalid table name: path traversal not allowed")
	}

	key := namespace + "." + name
	if _, exists := c.tables[key]; exists {
		return nil, fmt.Errorf("table %q already exists", key)
	}

	if !c.namespaces[namespace] {
		c.namespaces[namespace] = true
	}

	uuid, err := generateUUID()
	if err != nil {
		return nil, err
	}

	location := filepath.Join(c.config.ExportDir, namespace, name)
	// Verify resolved path stays under ExportDir
	resolved := filepath.Clean(location)
	if !strings.HasPrefix(resolved, filepath.Clean(c.config.ExportDir)+string(os.PathSeparator)) {
		return nil, fmt.Errorf("invalid table path: path traversal not allowed")
	}
	meta := &IcebergTableMetadata{
		FormatVersion:   2,
		TableUUID:       uuid,
		Location:        location,
		Schema:          schema,
		Snapshots:       nil,
		CurrentSnapshot: -1,
		CreatedAt:       time.Now(),
	}
	c.tables[key] = meta
	return meta, nil
}

// LoadTable retrieves table metadata from the catalog.
func (c *IcebergCatalog) LoadTable(namespace, name string) (*IcebergTableMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := namespace + "." + name
	meta, ok := c.tables[key]
	if !ok {
		return nil, fmt.Errorf("table %q not found", key)
	}
	return meta, nil
}

// ListTables returns all table names in a namespace.
func (c *IcebergCatalog) ListTables(namespace string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	prefix := namespace + "."
	var names []string
	for key := range c.tables {
		if strings.HasPrefix(key, prefix) {
			names = append(names, strings.TrimPrefix(key, prefix))
		}
	}
	sort.Strings(names)
	return names
}

// UpdateTable appends a snapshot to the table.
func (c *IcebergCatalog) UpdateTable(namespace, name string, snapshot IcebergSnapshot) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := namespace + "." + name
	meta, ok := c.tables[key]
	if !ok {
		return fmt.Errorf("table %q not found", key)
	}
	meta.Snapshots = append(meta.Snapshots, snapshot)
	meta.CurrentSnapshot = snapshot.SnapshotID
	return nil
}

// ---------------------------------------------------------------------------
// Partition Exporter
// ---------------------------------------------------------------------------

// ColdPartitionInfo describes a partition eligible for export.
type ColdPartitionInfo struct {
	Metric    string    `json:"metric"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Age       string    `json:"age"`
}

// ExportedFileInfo describes an exported Parquet file.
type ExportedFileInfo struct {
	FilePath    string    `json:"file_path"`
	Metric      string    `json:"metric"`
	RecordCount int64     `json:"record_count"`
	SizeBytes   int64     `json:"size_bytes"`
	ExportedAt  time.Time `json:"exported_at"`
	Partition   string    `json:"partition"`
}

// PartitionExporter handles exporting Chronicle data to Parquet files in Iceberg layout.
type PartitionExporter struct {
	db     *DB
	config IcebergExportConfig
	mu     sync.RWMutex
	files  []ExportedFileInfo
}

// NewPartitionExporter creates a new partition exporter.
func NewPartitionExporter(db *DB, config IcebergExportConfig) *PartitionExporter {
	return &PartitionExporter{
		db:     db,
		config: config,
	}
}

// DetectColdPartitions identifies partitions older than the configured threshold.
func (pe *PartitionExporter) DetectColdPartitions() []ColdPartitionInfo {
	cutoff := time.Now().Add(-pe.config.ColdPartitionAge)
	metrics := pe.db.Metrics()
	var cold []ColdPartitionInfo

	for _, metric := range metrics {
		// Query one day's worth of data before the cutoff to determine partition boundaries.
		dayStart := cutoff.Add(-24 * time.Hour)
		q := &Query{
			Metric: metric,
			Start:  dayStart.UnixNano(),
			End:    cutoff.UnixNano(),
			Limit:  1,
		}
		result, err := pe.db.Execute(q)
		if err != nil || result == nil || len(result.Points) == 0 {
			continue
		}

		partStart := dayStart.Truncate(24 * time.Hour)
		partEnd := partStart.Add(24 * time.Hour)
		age := time.Since(partEnd)

		cold = append(cold, ColdPartitionInfo{
			Metric:    metric,
			StartTime: partStart,
			EndTime:   partEnd,
			Age:       age.Round(time.Minute).String(),
		})
	}
	return cold
}

// ExportPartition exports a time range for a metric to a Parquet file in Iceberg layout.
func (pe *PartitionExporter) ExportPartition(ctx context.Context, metric string, startTime, endTime time.Time) (*ExportedFileInfo, error) {
	q := &Query{
		Metric: metric,
		Start:  startTime.UnixNano(),
		End:    endTime.UnixNano(),
	}
	result, err := pe.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query failed for metric %q: %w", metric, err)
	}
	if result == nil || len(result.Points) == 0 {
		return nil, fmt.Errorf("no data for metric %q in range %v - %v", metric, startTime, endTime)
	}

	partitionDate := startTime.Format("2006-01-02")
	tableName := pe.config.TablePrefix + icebergTableName(metric)
	dir := filepath.Join(pe.config.ExportDir, pe.config.Namespace, tableName, "data", partitionDate)
	// Verify resolved path stays under ExportDir to prevent path traversal
	resolvedDir := filepath.Clean(dir)
	if !strings.HasPrefix(resolvedDir, filepath.Clean(pe.config.ExportDir)+string(os.PathSeparator)) {
		return nil, fmt.Errorf("invalid export path: path traversal not allowed")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	partUUID, err := generateUUID()
	if err != nil {
		return nil, err
	}
	fileName := fmt.Sprintf("part-%s.parquet", partUUID)
	filePath := filepath.Join(dir, fileName)

	if err := writeSimpleParquet(filePath, result.Points); err != nil {
		return nil, fmt.Errorf("parquet write: %w", err)
	}

	var sizeBytes int64
	if fi, err := os.Stat(filePath); err == nil {
		sizeBytes = fi.Size()
	}

	info := ExportedFileInfo{
		FilePath:    filePath,
		Metric:      metric,
		RecordCount: int64(len(result.Points)),
		SizeBytes:   sizeBytes,
		ExportedAt:  time.Now(),
		Partition:   partitionDate,
	}

	pe.mu.Lock()
	pe.files = append(pe.files, info)
	pe.mu.Unlock()

	return &info, nil
}

// ListExportedFiles returns exported files, optionally filtered by metric.
func (pe *PartitionExporter) ListExportedFiles(metric string) []ExportedFileInfo {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	if metric == "" {
		out := make([]ExportedFileInfo, len(pe.files))
		copy(out, pe.files)
		return out
	}
	var out []ExportedFileInfo
	for _, f := range pe.files {
		if f.Metric == metric {
			out = append(out, f)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Export Policy
// ---------------------------------------------------------------------------

// ExportPolicy defines a scheduled export rule.
type ExportPolicy struct {
	ID                 string        `json:"id"`
	CronSchedule       string        `json:"cron_schedule"`
	MetricFilter       string        `json:"metric_filter"` // regex
	PartitionAge       time.Duration `json:"partition_age"`
	MaxParallelExports int           `json:"max_parallel_exports"`
	Enabled            bool          `json:"enabled"`
	CreatedAt          time.Time     `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Export Status
// ---------------------------------------------------------------------------

// IcebergExportStatus holds aggregate export statistics.
