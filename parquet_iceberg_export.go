package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// IcebergExportConfig configures the Parquet/Iceberg export engine.
type IcebergExportConfig struct {
	Enabled              bool          `json:"enabled"`
	ExportDir            string        `json:"export_dir"`
	CatalogType          string        `json:"catalog_type"` // rest, hive, glue
	CatalogURI           string        `json:"catalog_uri"`
	Warehouse            string        `json:"warehouse"`
	Namespace            string        `json:"namespace"`
	TablePrefix          string        `json:"table_prefix"`
	ExportInterval       time.Duration `json:"export_interval"`
	ColdPartitionAge     time.Duration `json:"cold_partition_age"`
	ParquetCompression   string        `json:"parquet_compression"`
	RowGroupSize         int           `json:"row_group_size"`
	TargetFileSizeMB     int           `json:"target_file_size_mb"`
	EnableScheduledExport bool         `json:"enable_scheduled_export"`
	ScheduleCron         string        `json:"schedule_cron"`
	RetainExportedDays   int           `json:"retain_exported_days"`
}

// DefaultIcebergExportConfig returns sensible defaults for Iceberg export.
func DefaultIcebergExportConfig() IcebergExportConfig {
	return IcebergExportConfig{
		Enabled:              false,
		ExportDir:            "iceberg-export",
		CatalogType:          "rest",
		CatalogURI:           "http://localhost:8181",
		Warehouse:            "chronicle-warehouse",
		Namespace:            "chronicle",
		TablePrefix:          "ts_",
		ExportInterval:       1 * time.Hour,
		ColdPartitionAge:     24 * time.Hour,
		ParquetCompression:   "snappy",
		RowGroupSize:         65536,
		TargetFileSizeMB:     128,
		EnableScheduledExport: false,
		ScheduleCron:         "0 * * * *",
		RetainExportedDays:   90,
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
	SchemaID       int                     `json:"schema-id"`
	Fields         []IcebergField          `json:"fields"`
	PartitionSpec  []IcebergPartitionField `json:"partition-spec"`
	SortOrder      []IcebergSortField      `json:"sort-order"`
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
	ManifestPath string            `json:"manifest-path"`
	DataFiles    []IcebergDataFile `json:"data-files"`
	PartitionInfo map[string]string `json:"partition-info,omitempty"`
}

// ---------------------------------------------------------------------------
// Iceberg Catalog
// ---------------------------------------------------------------------------

// IcebergTableMetadata holds the metadata for a single Iceberg table.
type IcebergTableMetadata struct {
	FormatVersion int               `json:"format-version"`
	TableUUID     string            `json:"table-uuid"`
	Location      string            `json:"location"`
	Schema        IcebergSchema     `json:"schema"`
	Snapshots     []IcebergSnapshot `json:"snapshots"`
	CurrentSnapshot int64           `json:"current-snapshot-id"`
	CreatedAt     time.Time         `json:"created-at"`
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
type IcebergExportStatus struct {
	Running            bool      `json:"running"`
	TotalExports       int64     `json:"total_exports"`
	TotalRecords       int64     `json:"total_records"`
	TotalBytes         int64     `json:"total_bytes"`
	FailedExports      int64     `json:"failed_exports"`
	LastExportTime     time.Time `json:"last_export_time,omitempty"`
	ColdPartitions     int       `json:"cold_partitions"`
	ActivePolicies     int       `json:"active_policies"`
	CatalogTables      int       `json:"catalog_tables"`
}

// ---------------------------------------------------------------------------
// Iceberg Export Manager (main engine)
// ---------------------------------------------------------------------------

// IcebergExportManager orchestrates cold-partition export to Parquet with Iceberg catalog tracking.
type IcebergExportManager struct {
	db       *DB
	config   IcebergExportConfig
	mu       sync.RWMutex
	running  bool
	stopCh   chan struct{}

	catalog  *IcebergCatalog
	exporter *PartitionExporter
	policies map[string]*ExportPolicy

	// stats
	totalExports  atomic.Int64
	totalRecords  atomic.Int64
	totalBytes    atomic.Int64
	failedExports atomic.Int64
	lastExport    time.Time
}

// NewIcebergExportManager creates a new export manager.
func NewIcebergExportManager(db *DB, config IcebergExportConfig) *IcebergExportManager {
	catalog := NewIcebergCatalog(config)
	exporter := NewPartitionExporter(db, config)

	return &IcebergExportManager{
		db:       db,
		config:   config,
		catalog:  catalog,
		exporter: exporter,
		policies: make(map[string]*ExportPolicy),
	}
}

// Start begins the background export scheduler.
func (m *IcebergExportManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("iceberg export manager already running")
	}
	if !m.config.Enabled {
		return fmt.Errorf("iceberg export manager is disabled")
	}

	if err := os.MkdirAll(m.config.ExportDir, 0o755); err != nil {
		return fmt.Errorf("create export dir: %w", err)
	}

	// Ensure default namespace exists (ignore "already exists" errors).
	if err := m.catalog.CreateNamespace(m.config.Namespace); err != nil &&
		!strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("create iceberg namespace: %w", err)
	}

	m.running = true
	m.stopCh = make(chan struct{})

	if m.config.EnableScheduledExport {
		go m.exportLoop()
	}

	return nil
}

// Stop halts the background export scheduler.
func (m *IcebergExportManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	close(m.stopCh)
	m.running = false
	return nil
}

// exportLoop runs periodic cold-partition exports.
func (m *IcebergExportManager) exportLoop() {
	ticker := time.NewTicker(m.config.ExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), m.config.ExportInterval/2)
			_ = m.ExportColdPartitions(ctx) //nolint:errcheck // best-effort periodic export
			cancel()
			m.runPolicies()
		}
	}
}

// ExportColdPartitions detects and exports all cold partitions.
func (m *IcebergExportManager) ExportColdPartitions(ctx context.Context) error {
	cold := m.exporter.DetectColdPartitions()
	if len(cold) == 0 {
		return nil
	}

	var lastErr error
	for _, cp := range cold {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		info, err := m.exporter.ExportPartition(ctx, cp.Metric, cp.StartTime, cp.EndTime)
		if err != nil {
			m.failedExports.Add(1)
			lastErr = err
			continue
		}

		m.totalExports.Add(1)
		m.totalRecords.Add(info.RecordCount)
		m.totalBytes.Add(info.SizeBytes)

		m.mu.Lock()
		m.lastExport = info.ExportedAt
		m.mu.Unlock()

		// Register in catalog.
		tableName := m.config.TablePrefix + icebergTableName(cp.Metric)
		if _, err := m.catalog.LoadTable(m.config.Namespace, tableName); err != nil {
			schema := DefaultTimeSeriesIcebergSchema()
			if _, createErr := m.catalog.CreateTable(m.config.Namespace, tableName, schema); createErr != nil {
				m.failedExports.Add(1)
				lastErr = fmt.Errorf("create iceberg table %s: %w", tableName, createErr)
				continue
			}
		}

		snapshot := IcebergSnapshot{
			SnapshotID:   time.Now().UnixNano(),
			Timestamp:    time.Now(),
			ManifestList: info.FilePath,
			Summary: map[string]string{
				"added-data-files": "1",
				"total-records":    fmt.Sprintf("%d", info.RecordCount),
				"total-file-size":  fmt.Sprintf("%d", info.SizeBytes),
			},
		}
		if err := m.catalog.UpdateTable(m.config.Namespace, tableName, snapshot); err != nil {
			lastErr = fmt.Errorf("update iceberg table %s: %w", tableName, err)
		}
	}
	return lastErr
}

// runPolicies evaluates all enabled export policies.
func (m *IcebergExportManager) runPolicies() {
	m.mu.RLock()
	policies := make([]*ExportPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		if p.Enabled {
			policies = append(policies, p)
		}
	}
	m.mu.RUnlock()

	for _, p := range policies {
		m.runPolicy(p)
	}
}

func (m *IcebergExportManager) runPolicy(p *ExportPolicy) {
	var re *regexp.Regexp
	if p.MetricFilter != "" {
		var err error
		re, err = regexp.Compile(p.MetricFilter)
		if err != nil {
			return
		}
	}

	metrics := m.db.Metrics()
	cutoff := time.Now().Add(-p.PartitionAge)

	sem := make(chan struct{}, max(p.MaxParallelExports, 1))
	var wg sync.WaitGroup

	for _, metric := range metrics {
		if re != nil && !re.MatchString(metric) {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(metric string) {
			defer func() { <-sem; wg.Done() }()

			partStart := cutoff.Add(-24 * time.Hour).Truncate(24 * time.Hour)
			partEnd := partStart.Add(24 * time.Hour)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			info, err := m.exporter.ExportPartition(ctx, metric, partStart, partEnd)
			if err != nil {
				m.failedExports.Add(1)
				return
			}
			m.totalExports.Add(1)
			m.totalRecords.Add(info.RecordCount)
			m.totalBytes.Add(info.SizeBytes)

			m.mu.Lock()
			m.lastExport = info.ExportedAt
			m.mu.Unlock()
		}(metric)
	}
	wg.Wait()
}

// CreateExportPolicy registers a new export policy.
func (m *IcebergExportManager) CreateExportPolicy(policy ExportPolicy) error {
	if policy.ID == "" {
		id, err := generateUUID()
		if err != nil {
			return err
		}
		policy.ID = id
	}
	if policy.PartitionAge <= 0 {
		policy.PartitionAge = m.config.ColdPartitionAge
	}
	if policy.MaxParallelExports <= 0 {
		policy.MaxParallelExports = 2
	}
	policy.CreatedAt = time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.policies[policy.ID]; exists {
		return fmt.Errorf("policy %q already exists", policy.ID)
	}
	m.policies[policy.ID] = &policy
	return nil
}

// ListExportPolicies returns all registered policies.
func (m *IcebergExportManager) ListExportPolicies() []ExportPolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ExportPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		out = append(out, *p)
	}
	return out
}

// GetExportStatus returns aggregate export statistics.
func (m *IcebergExportManager) GetExportStatus() IcebergExportStatus {
	m.mu.RLock()
	running := m.running
	lastExport := m.lastExport
	activePolicies := 0
	for _, p := range m.policies {
		if p.Enabled {
			activePolicies++
		}
	}
	m.mu.RUnlock()

	cold := m.exporter.DetectColdPartitions()
	tables := m.catalog.ListTables(m.config.Namespace)

	return IcebergExportStatus{
		Running:        running,
		TotalExports:   m.totalExports.Load(),
		TotalRecords:   m.totalRecords.Load(),
		TotalBytes:     m.totalBytes.Load(),
		FailedExports:  m.failedExports.Load(),
		LastExportTime: lastExport,
		ColdPartitions: len(cold),
		ActivePolicies: activePolicies,
		CatalogTables:  len(tables),
	}
}

// ListExportedFiles returns exported Parquet files, optionally filtered by metric.
func (m *IcebergExportManager) ListExportedFiles(metric string) []ExportedFileInfo {
	return m.exporter.ListExportedFiles(metric)
}

// ---------------------------------------------------------------------------
// HTTP Route Registration
// ---------------------------------------------------------------------------

// RegisterHTTPHandlers registers Iceberg export API routes.
func (m *IcebergExportManager) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/iceberg/export", m.handleExport)
	mux.HandleFunc("/api/v1/iceberg/status", m.handleStatus)
	mux.HandleFunc("/api/v1/iceberg/policies", m.handlePolicies)
	mux.HandleFunc("/api/v1/iceberg/tables", m.handleTables)
	mux.HandleFunc("/api/v1/iceberg/snapshots/", m.handleSnapshots)
}

func (m *IcebergExportManager) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Metric    string `json:"metric"`
		StartTime string `json:"start_time"`
		EndTime   string `json:"end_time"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// If no specific metric, export all cold partitions.
	if req.Metric == "" {
		err := m.ExportColdPartitions(r.Context())
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		writeJSON(w, map[string]string{"status": "export completed"})
		return
	}

	startTime, err := time.Parse(time.RFC3339, req.StartTime)
	if err != nil {
		http.Error(w, "invalid start_time: use RFC3339", http.StatusBadRequest)
		return
	}
	endTime, err := time.Parse(time.RFC3339, req.EndTime)
	if err != nil {
		http.Error(w, "invalid end_time: use RFC3339", http.StatusBadRequest)
		return
	}

	info, err := m.exporter.ExportPartition(r.Context(), req.Metric, startTime, endTime)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}
	writeJSON(w, info)
}

func (m *IcebergExportManager) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, m.GetExportStatus())
}

func (m *IcebergExportManager) handlePolicies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, m.ListExportPolicies())
	case http.MethodPost:
		var policy ExportPolicy
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&policy); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := m.CreateExportPolicy(policy); err != nil {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, policy)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *IcebergExportManager) handleTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = m.config.Namespace
	}
	writeJSON(w, m.catalog.ListTables(ns))
}

func (m *IcebergExportManager) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract table name from path: /api/v1/iceberg/snapshots/{table}
	table := strings.TrimPrefix(r.URL.Path, "/api/v1/iceberg/snapshots/")
	table = strings.TrimSuffix(table, "/")
	if table == "" {
		http.Error(w, "table name required", http.StatusBadRequest)
		return
	}

	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = m.config.Namespace
	}

	meta, err := m.catalog.LoadTable(ns, table)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, meta.Snapshots)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// icebergTableName converts a metric name to a safe Iceberg table identifier.
func icebergTableName(name string) string {
	r := strings.NewReplacer(".", "_", "-", "_", "/", "_", " ", "_")
	return strings.ToLower(r.Replace(name))
}

// generateUUID produces a random UUID v4 string.
func generateUUID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 2
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16]), nil
}
