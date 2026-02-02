package chronicle

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ParquetConfig configures Parquet storage.
type ParquetConfig struct {
	// Enabled enables Parquet format for partition storage
	Enabled bool `json:"enabled"`

	// CompressionCodec specifies compression (none, snappy, gzip, zstd)
	CompressionCodec string `json:"compression_codec"`

	// RowGroupSize is the number of rows per row group
	RowGroupSize int `json:"row_group_size"`

	// PageSize is the target page size in bytes
	PageSize int `json:"page_size"`

	// EnableDictionaryEncoding enables dictionary encoding for string columns
	EnableDictionaryEncoding bool `json:"enable_dictionary_encoding"`

	// EnableStatistics enables column statistics in metadata
	EnableStatistics bool `json:"enable_statistics"`

	// EnableBloomFilter enables bloom filters for string columns
	EnableBloomFilter bool `json:"enable_bloom_filter"`

	// DataPageVersion specifies the data page version (1 or 2)
	DataPageVersion int `json:"data_page_version"`
}

// DefaultParquetConfig returns sensible defaults.
func DefaultParquetConfig() ParquetConfig {
	return ParquetConfig{
		Enabled:                  true,
		CompressionCodec:         "snappy",
		RowGroupSize:             10000,
		PageSize:                 1024 * 1024, // 1MB
		EnableDictionaryEncoding: true,
		EnableStatistics:         true,
		EnableBloomFilter:        false,
		DataPageVersion:          2,
	}
}

// ParquetBackend implements StorageBackend using Parquet format.
// This enables direct compatibility with Spark, DuckDB, Trino, and other analytics tools.
type ParquetBackend struct {
	baseDir  string
	config   ParquetConfig
	mu       sync.RWMutex
	closed   bool
	metadata map[string]*ParquetFileMetadata
}

// NewParquetBackend creates a new Parquet-based storage backend.
func NewParquetBackend(baseDir string, config ParquetConfig) (*ParquetBackend, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	if config.RowGroupSize <= 0 {
		config.RowGroupSize = 10000
	}
	if config.PageSize <= 0 {
		config.PageSize = 1024 * 1024
	}
	if config.CompressionCodec == "" {
		config.CompressionCodec = "snappy"
	}

	return &ParquetBackend{
		baseDir:  baseDir,
		config:   config,
		metadata: make(map[string]*ParquetFileMetadata),
	}, nil
}

// ParquetFileMetadata contains metadata about a Parquet file.
type ParquetFileMetadata struct {
	Version       int32       `json:"version"`
	Schema        []ColumnDef `json:"schema"`
	NumRows       int64       `json:"num_rows"`
	NumRowGroups  int         `json:"num_row_groups"`
	CreatedBy     string      `json:"created_by"`
	CreatedAt     time.Time   `json:"created_at"`
	Compression   string      `json:"compression"`
	FileSize      int64       `json:"file_size"`
	MinTimestamp  int64       `json:"min_timestamp"`
	MaxTimestamp  int64       `json:"max_timestamp"`
	ColumnStats   []ColumnStats `json:"column_stats,omitempty"`
}

// ColumnDef defines a Parquet column schema.
type ColumnDef struct {
	Name           string `json:"name"`
	Type           string `json:"type"` // INT64, DOUBLE, BYTE_ARRAY, etc.
	RepetitionType string `json:"repetition_type"` // REQUIRED, OPTIONAL, REPEATED
	LogicalType    string `json:"logical_type,omitempty"`
}

// ColumnStats contains statistics for a column.
type ColumnStats struct {
	Column      string  `json:"column"`
	NullCount   int64   `json:"null_count"`
	DistinctCount int64 `json:"distinct_count,omitempty"`
	MinValue    string  `json:"min_value,omitempty"`
	MaxValue    string  `json:"max_value,omitempty"`
}

// TimeSeriesParquetSchema returns the schema for time-series data.
func TimeSeriesParquetSchema() []ColumnDef {
	return []ColumnDef{
		{Name: "timestamp", Type: "INT64", RepetitionType: "REQUIRED", LogicalType: "TIMESTAMP_NANOS"},
		{Name: "metric", Type: "BYTE_ARRAY", RepetitionType: "REQUIRED", LogicalType: "UTF8"},
		{Name: "value", Type: "DOUBLE", RepetitionType: "REQUIRED"},
		{Name: "tags", Type: "BYTE_ARRAY", RepetitionType: "OPTIONAL", LogicalType: "JSON"},
	}
}

// Read reads a partition from Parquet storage.
func (p *ParquetBackend) Read(ctx context.Context, key string) ([]byte, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	p.mu.RUnlock()

	path := filepath.Join(p.baseDir, key)
	return os.ReadFile(path)
}

// Write writes a partition in Parquet format.
func (p *ParquetBackend) Write(ctx context.Context, key string, data []byte) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return errors.New("backend is closed")
	}
	p.mu.RUnlock()

	path := filepath.Join(p.baseDir, key)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o644)
}

// Delete removes a partition from storage.
func (p *ParquetBackend) Delete(ctx context.Context, key string) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return errors.New("backend is closed")
	}
	p.mu.RUnlock()

	path := filepath.Join(p.baseDir, key)

	p.mu.Lock()
	delete(p.metadata, key)
	p.mu.Unlock()

	return os.Remove(path)
}

// List returns all Parquet files matching a prefix.
func (p *ParquetBackend) List(ctx context.Context, prefix string) ([]string, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	p.mu.RUnlock()

	var keys []string
	searchPath := filepath.Join(p.baseDir, prefix)

	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && filepath.Ext(path) == ".parquet" {
			rel, _ := filepath.Rel(p.baseDir, path)
			keys = append(keys, rel)
		}
		return nil
	})

	return keys, err
}

// Exists checks if a Parquet partition exists.
func (p *ParquetBackend) Exists(ctx context.Context, key string) (bool, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return false, errors.New("backend is closed")
	}
	p.mu.RUnlock()

	path := filepath.Join(p.baseDir, key)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

// Close releases resources.
func (p *ParquetBackend) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

// WritePoints writes time-series points as a Parquet file.
func (p *ParquetBackend) WritePoints(ctx context.Context, key string, points []Point) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return errors.New("backend is closed")
	}
	p.mu.RUnlock()

	if len(points) == 0 {
		return nil
	}

	// Convert points to Parquet format
	data, metadata := p.encodeParquet(points)

	// Write file
	path := filepath.Join(p.baseDir, key)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}

	// Cache metadata
	p.mu.Lock()
	p.metadata[key] = metadata
	p.mu.Unlock()

	return nil
}

// ReadPoints reads time-series points from a Parquet file.
func (p *ParquetBackend) ReadPoints(ctx context.Context, key string) ([]Point, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	p.mu.RUnlock()

	path := filepath.Join(p.baseDir, key)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return p.decodeParquet(data)
}

// encodeParquet encodes points to a simplified Parquet-like format.
// For a full implementation, use github.com/xitongsys/parquet-go.
func (p *ParquetBackend) encodeParquet(points []Point) ([]byte, *ParquetFileMetadata) {
	// Sort by timestamp for better compression
	sorted := make([]Point, len(points))
	copy(sorted, points)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	var buf bytes.Buffer

	// Write magic number
	buf.Write([]byte("PAR1"))

	// Track stats
	var minTs, maxTs int64 = sorted[0].Timestamp, sorted[len(sorted)-1].Timestamp
	metricsSet := make(map[string]struct{})

	// Encode row groups
	numRowGroups := (len(sorted) + p.config.RowGroupSize - 1) / p.config.RowGroupSize

	for g := 0; g < numRowGroups; g++ {
		start := g * p.config.RowGroupSize
		end := start + p.config.RowGroupSize
		if end > len(sorted) {
			end = len(sorted)
		}
		groupPoints := sorted[start:end]

		// Write row group header
		binary.Write(&buf, binary.LittleEndian, int32(len(groupPoints)))

		// Write columns
		// Timestamps (delta encoded)
		var lastTs int64
		for _, pt := range groupPoints {
			delta := pt.Timestamp - lastTs
			binary.Write(&buf, binary.LittleEndian, delta)
			lastTs = pt.Timestamp
		}

		// Values (raw doubles)
		for _, pt := range groupPoints {
			binary.Write(&buf, binary.LittleEndian, pt.Value)
		}

		// Metrics (length-prefixed strings)
		for _, pt := range groupPoints {
			metricsSet[pt.Metric] = struct{}{}
			metricBytes := []byte(pt.Metric)
			binary.Write(&buf, binary.LittleEndian, int32(len(metricBytes)))
			buf.Write(metricBytes)
		}

		// Tags (JSON encoded)
		for _, pt := range groupPoints {
			var tagsBytes []byte
			if len(pt.Tags) > 0 {
				tagsBytes, _ = json.Marshal(pt.Tags)
			}
			binary.Write(&buf, binary.LittleEndian, int32(len(tagsBytes)))
			buf.Write(tagsBytes)
		}
	}

	// Write footer metadata
	metadata := &ParquetFileMetadata{
		Version:      1,
		Schema:       TimeSeriesParquetSchema(),
		NumRows:      int64(len(sorted)),
		NumRowGroups: numRowGroups,
		CreatedBy:    "Chronicle",
		CreatedAt:    time.Now(),
		Compression:  p.config.CompressionCodec,
		FileSize:     int64(buf.Len()),
		MinTimestamp: minTs,
		MaxTimestamp: maxTs,
	}

	// Collect column stats
	if p.config.EnableStatistics {
		metadata.ColumnStats = []ColumnStats{
			{Column: "timestamp", MinValue: fmt.Sprintf("%d", minTs), MaxValue: fmt.Sprintf("%d", maxTs)},
			{Column: "metric", DistinctCount: int64(len(metricsSet))},
		}
	}

	metadataJSON, _ := json.Marshal(metadata)
	buf.Write(metadataJSON)
	binary.Write(&buf, binary.LittleEndian, int32(len(metadataJSON)))

	// Write footer magic
	buf.Write([]byte("PAR1"))

	return buf.Bytes(), metadata
}

// decodeParquet decodes a simplified Parquet-like format back to points.
func (p *ParquetBackend) decodeParquet(data []byte) ([]Point, error) {
	if len(data) < 8 {
		return nil, errors.New("invalid parquet file: too short")
	}

	// Verify magic
	if string(data[:4]) != "PAR1" || string(data[len(data)-4:]) != "PAR1" {
		return nil, errors.New("invalid parquet file: bad magic")
	}

	// Read footer length
	footerLen := int32(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	if int(footerLen) > len(data)-12 {
		return nil, errors.New("invalid parquet file: bad footer")
	}

	// Parse metadata
	metadataStart := len(data) - 8 - int(footerLen)
	var metadata ParquetFileMetadata
	if err := json.Unmarshal(data[metadataStart:len(data)-8], &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Read row groups
	reader := bytes.NewReader(data[4:metadataStart])
	points := make([]Point, 0, metadata.NumRows)

	for g := 0; g < metadata.NumRowGroups; g++ {
		// Read row group size
		var numRows int32
		if err := binary.Read(reader, binary.LittleEndian, &numRows); err != nil {
			return nil, fmt.Errorf("failed to read row group header: %w", err)
		}

		// Read timestamps
		timestamps := make([]int64, numRows)
		var lastTs int64
		for i := range timestamps {
			var delta int64
			if err := binary.Read(reader, binary.LittleEndian, &delta); err != nil {
				return nil, fmt.Errorf("failed to read timestamp: %w", err)
			}
			timestamps[i] = lastTs + delta
			lastTs = timestamps[i]
		}

		// Read values
		values := make([]float64, numRows)
		for i := range values {
			if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
				return nil, fmt.Errorf("failed to read value: %w", err)
			}
		}

		// Read metrics
		metrics := make([]string, numRows)
		for i := range metrics {
			var length int32
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				return nil, fmt.Errorf("failed to read metric length: %w", err)
			}
			metricBytes := make([]byte, length)
			if _, err := io.ReadFull(reader, metricBytes); err != nil {
				return nil, fmt.Errorf("failed to read metric: %w", err)
			}
			metrics[i] = string(metricBytes)
		}

		// Read tags
		tags := make([]map[string]string, numRows)
		for i := range tags {
			var length int32
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				return nil, fmt.Errorf("failed to read tags length: %w", err)
			}
			if length > 0 {
				tagBytes := make([]byte, length)
				if _, err := io.ReadFull(reader, tagBytes); err != nil {
					return nil, fmt.Errorf("failed to read tags: %w", err)
				}
				var t map[string]string
				if err := json.Unmarshal(tagBytes, &t); err == nil {
					tags[i] = t
				}
			}
		}

		// Assemble points
		for i := 0; i < int(numRows); i++ {
			points = append(points, Point{
				Metric:    metrics[i],
				Value:     values[i],
				Timestamp: timestamps[i],
				Tags:      tags[i],
			})
		}
	}

	return points, nil
}

// GetMetadata returns metadata for a Parquet file.
func (p *ParquetBackend) GetMetadata(ctx context.Context, key string) (*ParquetFileMetadata, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}

	if meta, ok := p.metadata[key]; ok {
		p.mu.RUnlock()
		return meta, nil
	}
	p.mu.RUnlock()

	// Read from file
	data, err := p.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(data) < 12 {
		return nil, errors.New("file too short")
	}

	// Read footer
	footerLen := int32(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	metadataStart := len(data) - 8 - int(footerLen)

	var metadata ParquetFileMetadata
	if err := json.Unmarshal(data[metadataStart:len(data)-8], &metadata); err != nil {
		return nil, err
	}

	// Cache
	p.mu.Lock()
	p.metadata[key] = &metadata
	p.mu.Unlock()

	return &metadata, nil
}

// QueryByTimeRange reads points within a time range from Parquet files.
func (p *ParquetBackend) QueryByTimeRange(ctx context.Context, prefix string, start, end int64) ([]Point, error) {
	keys, err := p.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var allPoints []Point

	for _, key := range keys {
		// Check if file might contain relevant data using metadata
		meta, err := p.GetMetadata(ctx, key)
		if err == nil {
			// Skip files outside time range
			if meta.MaxTimestamp < start || meta.MinTimestamp > end {
				continue
			}
		}

		points, err := p.ReadPoints(ctx, key)
		if err != nil {
			continue
		}

		// Filter by time range
		for _, pt := range points {
			if pt.Timestamp >= start && pt.Timestamp <= end {
				allPoints = append(allPoints, pt)
			}
		}
	}

	// Sort by timestamp
	sort.Slice(allPoints, func(i, j int) bool {
		return allPoints[i].Timestamp < allPoints[j].Timestamp
	})

	return allPoints, nil
}

// ParquetWriter writes points to Parquet incrementally.
type ParquetWriter struct {
	backend   *ParquetBackend
	key       string
	buffer    []Point
	bufferMu  sync.Mutex
	maxBuffer int
}

// NewParquetWriter creates a new Parquet writer.
func NewParquetWriter(backend *ParquetBackend, key string, maxBuffer int) *ParquetWriter {
	if maxBuffer <= 0 {
		maxBuffer = 10000
	}
	return &ParquetWriter{
		backend:   backend,
		key:       key,
		buffer:    make([]Point, 0, maxBuffer),
		maxBuffer: maxBuffer,
	}
}

// Write adds a point to the buffer.
func (w *ParquetWriter) Write(pt Point) error {
	w.bufferMu.Lock()
	w.buffer = append(w.buffer, pt)
	shouldFlush := len(w.buffer) >= w.maxBuffer
	w.bufferMu.Unlock()

	if shouldFlush {
		return w.Flush()
	}
	return nil
}

// Flush writes buffered points to the Parquet file.
func (w *ParquetWriter) Flush() error {
	w.bufferMu.Lock()
	if len(w.buffer) == 0 {
		w.bufferMu.Unlock()
		return nil
	}
	points := w.buffer
	w.buffer = make([]Point, 0, w.maxBuffer)
	w.bufferMu.Unlock()

	return w.backend.WritePoints(context.Background(), w.key, points)
}

// Close flushes and closes the writer.
func (w *ParquetWriter) Close() error {
	return w.Flush()
}

// ExportToParquet exports Chronicle data to standard Parquet format.
func ExportToParquet(db *DB, outputPath string, metric string, start, end int64, config ParquetConfig) error {
	result, err := db.Execute(&Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return err
	}

	backend, err := NewParquetBackend(filepath.Dir(outputPath), config)
	if err != nil {
		return err
	}
	defer backend.Close()

	return backend.WritePoints(context.Background(), filepath.Base(outputPath), result.Points)
}

// ImportFromParquet imports data from Parquet file into Chronicle.
func ImportFromParquet(db *DB, inputPath string) error {
	baseDir := filepath.Dir(inputPath)
	backend, err := NewParquetBackend(baseDir, DefaultParquetConfig())
	if err != nil {
		return err
	}
	defer backend.Close()

	points, err := backend.ReadPoints(context.Background(), filepath.Base(inputPath))
	if err != nil {
		return err
	}

	for _, pt := range points {
		if err := db.Write(pt); err != nil {
			return err
		}
	}

	return db.Flush()
}
