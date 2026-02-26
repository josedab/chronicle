// parquet_iceberg_export_iceberg.go contains extended parquet iceberg export functionality.
package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type IcebergExportStatus struct {
	Running        bool      `json:"running"`
	TotalExports   int64     `json:"total_exports"`
	TotalRecords   int64     `json:"total_records"`
	TotalBytes     int64     `json:"total_bytes"`
	FailedExports  int64     `json:"failed_exports"`
	LastExportTime time.Time `json:"last_export_time,omitempty"`
	ColdPartitions int       `json:"cold_partitions"`
	ActivePolicies int       `json:"active_policies"`
	CatalogTables  int       `json:"catalog_tables"`
}

// ---------------------------------------------------------------------------
// Iceberg Export Manager (main engine)
// ---------------------------------------------------------------------------

// IcebergExportManager orchestrates cold-partition export to Parquet with Iceberg catalog tracking.
type IcebergExportManager struct {
	db      *DB
	config  IcebergExportConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}

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
