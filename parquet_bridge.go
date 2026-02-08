// Bridge: parquet_bridge.go
//
// This file bridges the Parquet lakehouse functionality into the public
// chronicle package. It provides ParquetBridge for external Parquet table
// management and archiving operations.
//
// Pattern: Parquet subsystem (implementation) → parquet_bridge.go (public API)

package chronicle

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ParquetBridgeConfig configures the Parquet lakehouse bridge.
type ParquetBridgeConfig struct {
	DataDir             string
	ArchiveDir          string
	PartitionBy         ParquetPartition
	RowGroupSize        int
	CompressionCodec    string
	MaxFileSize         int64
	AutoArchiveInterval time.Duration
}

// ParquetPartition controls how data is partitioned into files.
type ParquetPartition int

const (
	ParquetPartitionHourly ParquetPartition = iota
	ParquetPartitionDaily
	ParquetPartitionWeekly
	ParquetPartitionMonthly
)

func (p ParquetPartition) String() string {
	switch p {
	case ParquetPartitionHourly:
		return "hourly"
	case ParquetPartitionDaily:
		return "daily"
	case ParquetPartitionWeekly:
		return "weekly"
	case ParquetPartitionMonthly:
		return "monthly"
	default:
		return "daily"
	}
}

// DefaultParquetBridgeConfig returns production defaults.
func DefaultParquetBridgeConfig(dataDir string) ParquetBridgeConfig {
	return ParquetBridgeConfig{
		DataDir:             filepath.Join(dataDir, "parquet"),
		ArchiveDir:          filepath.Join(dataDir, "archive"),
		PartitionBy:         ParquetPartitionDaily,
		RowGroupSize:        65536,
		CompressionCodec:    "snappy",
		MaxFileSize:         256 * 1024 * 1024,
		AutoArchiveInterval: 1 * time.Hour,
	}
}

// ExternalTable represents a queryable external Parquet table.
type ExternalTable struct {
	Name       string            `json:"name"`
	Path       string            `json:"path"`
	Schema     []ParquetColumn   `json:"schema"`
	RowCount   int64             `json:"row_count"`
	SizeBytes  int64             `json:"size_bytes"`
	CreatedAt  time.Time         `json:"created_at"`
	Partitions []string          `json:"partitions"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// ParquetColumn describes a column in a Parquet schema.
type ParquetColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// ParquetBridge manages external Parquet tables and archiving.
//
// 🔬 BETA: API may evolve between minor versions with migration guidance.
// See api_stability.go for stability classifications.
type ParquetBridge struct {
	config ParquetBridgeConfig
	db     *DB

	mu     sync.RWMutex
	tables map[string]*ExternalTable

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewParquetBridge creates a new Parquet lakehouse bridge.
func NewParquetBridge(db *DB, config ParquetBridgeConfig) (*ParquetBridge, error) {
	if config.DataDir == "" {
		return nil, fmt.Errorf("parquet: data_dir is required")
	}
	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("parquet: cannot create data dir: %w", err)
	}
	if config.ArchiveDir != "" {
		if err := os.MkdirAll(config.ArchiveDir, 0o755); err != nil {
			return nil, fmt.Errorf("parquet: cannot create archive dir: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &ParquetBridge{
		config: config,
		db:     db,
		tables: make(map[string]*ExternalTable),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start begins background archiving.
func (pb *ParquetBridge) Start() {
	if pb.config.AutoArchiveInterval > 0 {
		pb.wg.Add(1)
		go pb.archiveLoop()
	}
}

// Stop halts background processing.
func (pb *ParquetBridge) Stop() {
	pb.cancel()
	pb.wg.Wait()
}

// RegisterTable registers an external Parquet table for querying.
func (pb *ParquetBridge) RegisterTable(table ExternalTable) error {
	if table.Name == "" {
		return fmt.Errorf("parquet: table name is required")
	}
	if table.Path == "" {
		return fmt.Errorf("parquet: table path is required")
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()
	table.CreatedAt = time.Now()
	pb.tables[table.Name] = &table
	return nil
}

// UnregisterTable removes an external table.
func (pb *ParquetBridge) UnregisterTable(name string) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	delete(pb.tables, name)
}

// GetTable returns info about a registered external table.
func (pb *ParquetBridge) GetTable(name string) (*ExternalTable, error) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	t, ok := pb.tables[name]
	if !ok {
		return nil, fmt.Errorf("parquet: table %q not found", name)
	}
	return t, nil
}

// ListTables returns all registered external tables.
func (pb *ParquetBridge) ListTables() []ExternalTable {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	tables := make([]ExternalTable, 0, len(pb.tables))
	for _, t := range pb.tables {
		tables = append(tables, *t)
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	return tables
}

// ExportToParquet writes points from a metric to a Parquet file.
func (pb *ParquetBridge) ExportToParquet(metric string, start, end time.Time) (string, error) {
	if pb.db == nil {
		return "", fmt.Errorf("parquet: database not available")
	}

	q := &Query{
		Metric: metric,
		Start:  start.UnixNano(),
		End:    end.UnixNano(),
	}
	result, err := pb.db.Execute(q)
	if err != nil {
		return "", fmt.Errorf("parquet: query failed: %w", err)
	}

	partition := pb.partitionKey(start)
	dir := filepath.Join(pb.config.DataDir, metric, partition)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("parquet: cannot create partition dir: %w", err)
	}

	filename := fmt.Sprintf("%s_%s.parquet", metric, start.Format("20060102T150405"))
	path := filepath.Join(dir, filename)

	if err := writeSimpleParquet(path, result.Points); err != nil {
		return "", fmt.Errorf("parquet: write failed: %w", err)
	}

	// Auto-register
	pb.RegisterTable(ExternalTable{
		Name:       fmt.Sprintf("%s_%s", metric, partition),
		Path:       path,
		RowCount:   int64(len(result.Points)),
		Partitions: []string{partition},
		Schema: []ParquetColumn{
			{Name: "metric", Type: "string"},
			{Name: "value", Type: "double"},
			{Name: "timestamp", Type: "int64"},
			{Name: "tags", Type: "map<string,string>"},
		},
	})

	return path, nil
}

// ArchiveOlderThan exports and archives data older than the given duration.
func (pb *ParquetBridge) ArchiveOlderThan(age time.Duration) (int, error) {
	if pb.db == nil {
		return 0, fmt.Errorf("parquet: database not available")
	}

	cutoff := time.Now().Add(-age)
	metrics := pb.db.Metrics()
	archived := 0

	for _, metric := range metrics {
		path, err := pb.ExportToParquet(metric, time.Time{}, cutoff)
		if err != nil {
			continue
		}
		if path != "" {
			archived++
		}
	}

	return archived, nil
}

func (pb *ParquetBridge) partitionKey(t time.Time) string {
	switch pb.config.PartitionBy {
	case ParquetPartitionHourly:
		return t.Format("2006/01/02/15")
	case ParquetPartitionDaily:
		return t.Format("2006/01/02")
	case ParquetPartitionWeekly:
		year, week := t.ISOWeek()
		return fmt.Sprintf("%d/W%02d", year, week)
	case ParquetPartitionMonthly:
		return t.Format("2006/01")
	default:
		return t.Format("2006/01/02")
	}
}

func (pb *ParquetBridge) archiveLoop() {
	defer pb.wg.Done()
	ticker := time.NewTicker(pb.config.AutoArchiveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pb.ctx.Done():
			return
		case <-ticker.C:
			// Archive data older than retention (default 30 days)
			pb.ArchiveOlderThan(30 * 24 * time.Hour)
		}
	}
}

// writeSimpleParquet writes a minimal Parquet file (PAR1 magic + row data + footer).
// This is a simplified implementation; production use should use apache/parquet-go.
func writeSimpleParquet(path string, points []Point) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Parquet magic number
	f.Write([]byte("PAR1"))

	// Write points as binary rows (simplified format)
	buf := make([]byte, 8)
	for _, p := range points {
		// Write metric length + metric
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(p.Metric)))
		f.Write(buf[:4])
		io.WriteString(f, p.Metric)

		// Write value as float64
		binary.LittleEndian.PutUint64(buf, math.Float64bits(p.Value))
		f.Write(buf)

		// Write timestamp
		binary.LittleEndian.PutUint64(buf, uint64(p.Timestamp))
		f.Write(buf)

		// Write tags count + tags
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(p.Tags)))
		f.Write(buf[:4])
		for k, v := range p.Tags {
			binary.LittleEndian.PutUint32(buf[:4], uint32(len(k)))
			f.Write(buf[:4])
			io.WriteString(f, k)
			binary.LittleEndian.PutUint32(buf[:4], uint32(len(v)))
			f.Write(buf[:4])
			io.WriteString(f, v)
		}
	}

	// Row count as footer
	binary.LittleEndian.PutUint64(buf, uint64(len(points)))
	f.Write(buf)

	// Footer length (8 bytes for row count)
	binary.LittleEndian.PutUint32(buf[:4], 8)
	f.Write(buf[:4])

	// Parquet magic (footer)
	f.Write([]byte("PAR1"))

	return nil
}

// ReadParquetRowCount reads the row count from a simplified Parquet file.
func ReadParquetRowCount(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Check magic
	magic := make([]byte, 4)
	if _, err := f.Read(magic); err != nil {
		return 0, err
	}
	if string(magic) != "PAR1" {
		return 0, fmt.Errorf("not a Parquet file")
	}

	// Read footer: last 4 bytes = "PAR1", before that 4 bytes = footer len, before that 8 bytes = row count
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if fi.Size() < 20 {
		return 0, fmt.Errorf("file too small")
	}

	// Seek to row count position (end - 16)
	f.Seek(fi.Size()-16, 0)
	buf := make([]byte, 8)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buf)), nil
}

// ParquetFileInfo returns metadata about a Parquet file.
type ParquetFileInfo struct {
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	RowCount  int64  `json:"row_count"`
	IsValid   bool   `json:"is_valid"`
}

// InspectParquetFile reads basic info from a Parquet file.
func InspectParquetFile(path string) (*ParquetFileInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	info := &ParquetFileInfo{
		Path:      path,
		SizeBytes: fi.Size(),
	}

	rc, err := ReadParquetRowCount(path)
	if err == nil {
		info.RowCount = rc
		info.IsValid = true
	}

	// Check if path looks like a parquet file
	if !strings.HasSuffix(path, ".parquet") {
		info.IsValid = false
	}

	return info, nil
}
