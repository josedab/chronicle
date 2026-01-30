package chronicle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	// SQLite driver using pure Go implementation
	_ "modernc.org/sqlite"
)

// SQLiteBackendConfig configures the SQLite storage backend.
type SQLiteBackendConfig struct {
	// Path to the SQLite database file
	Path string

	// CacheSize is the SQLite page cache size in KB (default: 2000 = 2MB)
	CacheSize int

	// JournalMode sets the SQLite journal mode (WAL, DELETE, TRUNCATE, etc.)
	JournalMode string

	// Synchronous sets the synchronous flag (OFF, NORMAL, FULL, EXTRA)
	Synchronous string

	// BusyTimeout is the timeout for acquiring locks in milliseconds
	BusyTimeout int

	// EnableFTS enables full-text search for metadata queries
	EnableFTS bool

	// EnableMetricIndex creates indexes for metric name queries
	EnableMetricIndex bool

	// MaxConnections is the max number of database connections
	MaxConnections int
}

// DefaultSQLiteBackendConfig returns default configuration.
func DefaultSQLiteBackendConfig() SQLiteBackendConfig {
	return SQLiteBackendConfig{
		Path:              "chronicle.db",
		CacheSize:         2000,
		JournalMode:       "WAL",
		Synchronous:       "NORMAL",
		BusyTimeout:       5000,
		EnableFTS:         false,
		EnableMetricIndex: true,
		MaxConnections:    10,
	}
}

// SQLiteBackend implements StorageBackend using SQLite.
// This allows Chronicle data to be accessed using standard SQLite tools.
type SQLiteBackend struct {
	db     *sql.DB
	config SQLiteBackendConfig
	mu     sync.RWMutex
	closed bool

	// Prepared statements for common operations
	insertStmt  *sql.Stmt
	selectStmt  *sql.Stmt
	deleteStmt  *sql.Stmt
	existsStmt  *sql.Stmt
	insertPoint *sql.Stmt
	selectRange *sql.Stmt
}

// NewSQLiteBackend creates a new SQLite-based storage backend.
func NewSQLiteBackend(config SQLiteBackendConfig) (*SQLiteBackend, error) {
	if config.Path == "" {
		config.Path = "chronicle.db"
	}
	if config.CacheSize <= 0 {
		config.CacheSize = 2000
	}
	if config.JournalMode == "" {
		config.JournalMode = "WAL"
	}
	if config.Synchronous == "" {
		config.Synchronous = "NORMAL"
	}
	if config.BusyTimeout <= 0 {
		config.BusyTimeout = 5000
	}
	if config.MaxConnections <= 0 {
		config.MaxConnections = 10
	}

	// Build connection string with pragmas
	dsn := fmt.Sprintf("%s?_cache_size=%d&_journal_mode=%s&_synchronous=%s&_busy_timeout=%d",
		config.Path, config.CacheSize, config.JournalMode, config.Synchronous, config.BusyTimeout)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	db.SetMaxOpenConns(config.MaxConnections)
	db.SetMaxIdleConns(config.MaxConnections / 2)

	backend := &SQLiteBackend{
		db:     db,
		config: config,
	}

	if err := backend.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	if err := backend.prepareStatements(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	return backend, nil
}

// initSchema creates the database schema.
func (s *SQLiteBackend) initSchema() error {
	schema := `
		-- Partition storage (blob storage compatible with StorageBackend interface)
		CREATE TABLE IF NOT EXISTS partitions (
			key TEXT PRIMARY KEY,
			data BLOB NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			size INTEGER NOT NULL
		);

		-- Time-series data with direct SQL access
		CREATE TABLE IF NOT EXISTS time_series (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			metric TEXT NOT NULL,
			timestamp INTEGER NOT NULL,
			value REAL NOT NULL,
			tags TEXT,  -- JSON encoded tags
			partition_id TEXT
		);

		-- Metrics metadata
		CREATE TABLE IF NOT EXISTS metrics (
			name TEXT PRIMARY KEY,
			description TEXT,
			unit TEXT,
			type TEXT,
			labels TEXT,  -- JSON encoded label names
			created_at INTEGER NOT NULL,
			point_count INTEGER DEFAULT 0,
			last_value REAL,
			last_timestamp INTEGER
		);

		-- Create indexes for common queries
		CREATE INDEX IF NOT EXISTS idx_partitions_created ON partitions(created_at);
		CREATE INDEX IF NOT EXISTS idx_timeseries_metric_ts ON time_series(metric, timestamp);
		CREATE INDEX IF NOT EXISTS idx_timeseries_timestamp ON time_series(timestamp);
	`

	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Create additional indexes if enabled
	if s.config.EnableMetricIndex {
		indexSQL := `
			CREATE INDEX IF NOT EXISTS idx_timeseries_metric ON time_series(metric);
			CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name);
		`
		if _, err := s.db.Exec(indexSQL); err != nil {
			return fmt.Errorf("failed to create metric indexes: %w", err)
		}
	}

	// Create FTS tables if enabled
	if s.config.EnableFTS {
		ftsSQL := `
			CREATE VIRTUAL TABLE IF NOT EXISTS metrics_fts USING fts5(
				name, description, content=metrics, content_rowid=rowid
			);
		`
		if _, err := s.db.Exec(ftsSQL); err != nil {
			// FTS might not be available, log but continue
		}
	}

	return nil
}

// prepareStatements prepares common SQL statements for better performance.
func (s *SQLiteBackend) prepareStatements() error {
	var err error

	s.insertStmt, err = s.db.Prepare(`
		INSERT OR REPLACE INTO partitions (key, data, created_at, updated_at, size)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}

	s.selectStmt, err = s.db.Prepare(`SELECT data FROM partitions WHERE key = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}

	s.deleteStmt, err = s.db.Prepare(`DELETE FROM partitions WHERE key = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare delete statement: %w", err)
	}

	s.existsStmt, err = s.db.Prepare(`SELECT 1 FROM partitions WHERE key = ? LIMIT 1`)
	if err != nil {
		return fmt.Errorf("failed to prepare exists statement: %w", err)
	}

	s.insertPoint, err = s.db.Prepare(`
		INSERT INTO time_series (metric, timestamp, value, tags, partition_id)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert point statement: %w", err)
	}

	s.selectRange, err = s.db.Prepare(`
		SELECT timestamp, value, tags FROM time_series
		WHERE metric = ? AND timestamp >= ? AND timestamp < ?
		ORDER BY timestamp
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare select range statement: %w", err)
	}

	return nil
}

// Read reads a partition from storage.
func (s *SQLiteBackend) Read(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	var data []byte
	err := s.selectStmt.QueryRowContext(ctx, key).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("partition not found: %s", key)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read partition: %w", err)
	}
	return data, nil
}

// Write writes a partition to storage.
func (s *SQLiteBackend) Write(ctx context.Context, key string, data []byte) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("backend is closed")
	}
	s.mu.RUnlock()

	now := time.Now().UnixNano()
	_, err := s.insertStmt.ExecContext(ctx, key, data, now, now, len(data))
	if err != nil {
		return fmt.Errorf("failed to write partition: %w", err)
	}
	return nil
}

// Delete removes a partition from storage.
func (s *SQLiteBackend) Delete(ctx context.Context, key string) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("backend is closed")
	}
	s.mu.RUnlock()

	_, err := s.deleteStmt.ExecContext(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete partition: %w", err)
	}
	return nil
}

// List returns all partition keys matching a prefix.
func (s *SQLiteBackend) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	query := `SELECT key FROM partitions WHERE key LIKE ? ORDER BY key`
	rows, err := s.db.QueryContext(ctx, query, prefix+"%")
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("failed to scan key: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// Exists checks if a partition exists.
func (s *SQLiteBackend) Exists(ctx context.Context, key string) (bool, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return false, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	var exists int
	err := s.existsStmt.QueryRowContext(ctx, key).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check existence: %w", err)
	}
	return true, nil
}

// Close releases any resources.
func (s *SQLiteBackend) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	// Close prepared statements
	if s.insertStmt != nil {
		s.insertStmt.Close()
	}
	if s.selectStmt != nil {
		s.selectStmt.Close()
	}
	if s.deleteStmt != nil {
		s.deleteStmt.Close()
	}
	if s.existsStmt != nil {
		s.existsStmt.Close()
	}
	if s.insertPoint != nil {
		s.insertPoint.Close()
	}
	if s.selectRange != nil {
		s.selectRange.Close()
	}

	return s.db.Close()
}

// WritePoint writes a time-series point directly to the SQL table.
// This enables direct SQL access to time-series data.
func (s *SQLiteBackend) WritePoint(ctx context.Context, point Point) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("backend is closed")
	}
	s.mu.RUnlock()

	var tagsJSON []byte
	if len(point.Tags) > 0 {
		var err error
		tagsJSON, err = json.Marshal(point.Tags)
		if err != nil {
			return fmt.Errorf("failed to marshal tags: %w", err)
		}
	}

	_, err := s.insertPoint.ExecContext(ctx, point.Metric, point.Timestamp, point.Value, tagsJSON, "")
	if err != nil {
		return fmt.Errorf("failed to write point: %w", err)
	}

	// Update metrics metadata
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO metrics (name, created_at, point_count, last_value, last_timestamp)
		VALUES (?, ?, 1, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			point_count = point_count + 1,
			last_value = excluded.last_value,
			last_timestamp = excluded.last_timestamp
	`, point.Metric, time.Now().UnixNano(), point.Value, point.Timestamp)

	return err
}

// WritePoints writes multiple points in a transaction.
func (s *SQLiteBackend) WritePoints(ctx context.Context, points []Point) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("backend is closed")
	}
	s.mu.RUnlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt := tx.StmtContext(ctx, s.insertPoint)
	metricCounts := make(map[string]int)

	for _, point := range points {
		var tagsJSON []byte
		if len(point.Tags) > 0 {
			tagsJSON, err = json.Marshal(point.Tags)
			if err != nil {
				return fmt.Errorf("failed to marshal tags: %w", err)
			}
		}

		_, err = stmt.ExecContext(ctx, point.Metric, point.Timestamp, point.Value, tagsJSON, "")
		if err != nil {
			return fmt.Errorf("failed to write point: %w", err)
		}
		metricCounts[point.Metric]++
	}

	// Update metrics metadata
	for metric, count := range metricCounts {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO metrics (name, created_at, point_count)
			VALUES (?, ?, ?)
			ON CONFLICT(name) DO UPDATE SET
				point_count = point_count + ?
		`, metric, time.Now().UnixNano(), count, count)
		if err != nil {
			return fmt.Errorf("failed to update metric metadata: %w", err)
		}
	}

	return tx.Commit()
}

// QueryRange queries time-series data for a metric within a time range.
func (s *SQLiteBackend) QueryRange(ctx context.Context, metric string, start, end int64) ([]Point, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	rows, err := s.selectRange.QueryContext(ctx, metric, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query range: %w", err)
	}
	defer rows.Close()

	var points []Point
	for rows.Next() {
		var timestamp int64
		var value float64
		var tagsJSON []byte

		if err := rows.Scan(&timestamp, &value, &tagsJSON); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		point := Point{
			Metric:    metric,
			Timestamp: timestamp,
			Value:     value,
		}

		if len(tagsJSON) > 0 {
			if err := json.Unmarshal(tagsJSON, &point.Tags); err != nil {
				return nil, fmt.Errorf("failed to unmarshal tags: %w", err)
			}
		}

		points = append(points, point)
	}

	return points, rows.Err()
}

// ExecuteSQL executes a raw SQL query and returns the results.
// This enables direct SQL access using standard SQLite tools.
func (s *SQLiteBackend) ExecuteSQL(ctx context.Context, query string, args ...interface{}) (*SQLResult, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	// Detect query type
	trimmedQuery := strings.TrimSpace(strings.ToUpper(query))
	isSelect := strings.HasPrefix(trimmedQuery, "SELECT")

	if isSelect {
		return s.executeSelect(ctx, query, args...)
	}
	return s.executeModify(ctx, query, args...)
}

// SQLResult represents the result of an SQL query.
type SQLResult struct {
	Columns      []string        `json:"columns,omitempty"`
	Rows         [][]interface{} `json:"rows,omitempty"`
	RowsAffected int64           `json:"rows_affected,omitempty"`
	LastInsertID int64           `json:"last_insert_id,omitempty"`
}

func (s *SQLiteBackend) executeSelect(ctx context.Context, query string, args ...interface{}) (*SQLResult, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	result := &SQLResult{
		Columns: columns,
		Rows:    make([][]interface{}, 0),
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert []byte to string for JSON serialization
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		result.Rows = append(result.Rows, values)
	}

	return result, rows.Err()
}

func (s *SQLiteBackend) executeModify(ctx context.Context, query string, args ...interface{}) (*SQLResult, error) {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()

	return &SQLResult{
		RowsAffected: rowsAffected,
		LastInsertID: lastInsertID,
	}, nil
}

// GetMetrics returns all registered metrics.
func (s *SQLiteBackend) GetMetrics(ctx context.Context) ([]MetricMetadata, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	rows, err := s.db.QueryContext(ctx, `
		SELECT name, description, unit, type, point_count, last_value, last_timestamp
		FROM metrics ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	var metrics []MetricMetadata
	for rows.Next() {
		var m MetricMetadata
		var desc, unit, mtype sql.NullString
		var lastVal sql.NullFloat64
		var lastTs sql.NullInt64
		var pointCount sql.NullInt64

		if err := rows.Scan(&m.Name, &desc, &unit, &mtype, &pointCount, &lastVal, &lastTs); err != nil {
			return nil, fmt.Errorf("failed to scan metric: %w", err)
		}

		m.Description = desc.String
		m.Unit = unit.String
		m.Type = mtype.String
		if pointCount.Valid {
			m.PointCount = pointCount.Int64
		}
		if lastVal.Valid {
			m.LastValue = lastVal.Float64
		}
		if lastTs.Valid {
			m.LastTimestamp = lastTs.Int64
		}

		metrics = append(metrics, m)
	}

	return metrics, rows.Err()
}

// MetricMetadata contains metadata about a metric.
type MetricMetadata struct {
	Name          string  `json:"name"`
	Description   string  `json:"description,omitempty"`
	Unit          string  `json:"unit,omitempty"`
	Type          string  `json:"type,omitempty"`
	PointCount    int64   `json:"point_count"`
	LastValue     float64 `json:"last_value,omitempty"`
	LastTimestamp int64   `json:"last_timestamp,omitempty"`
}

// Aggregate performs aggregation queries on time-series data.
func (s *SQLiteBackend) Aggregate(ctx context.Context, metric string, start, end int64, fn string, interval int64) ([]AggregateResult, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	// Map aggregation function
	aggFn := strings.ToUpper(fn)
	switch aggFn {
	case "AVG", "SUM", "MIN", "MAX", "COUNT":
		// valid
	default:
		return nil, fmt.Errorf("unsupported aggregation function: %s", fn)
	}

	query := fmt.Sprintf(`
		SELECT 
			(timestamp / ?) * ? AS bucket,
			%s(value) AS agg_value,
			COUNT(*) AS count
		FROM time_series
		WHERE metric = ? AND timestamp >= ? AND timestamp < ?
		GROUP BY bucket
		ORDER BY bucket
	`, aggFn)

	rows, err := s.db.QueryContext(ctx, query, interval, interval, metric, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to execute aggregate query: %w", err)
	}
	defer rows.Close()

	var results []AggregateResult
	for rows.Next() {
		var r AggregateResult
		if err := rows.Scan(&r.Bucket, &r.Value, &r.Count); err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// AggregateResult represents an aggregated bucket.
type AggregateResult struct {
	Bucket int64   `json:"bucket"`
	Value  float64 `json:"value"`
	Count  int64   `json:"count"`
}

// GetDB returns the underlying database connection for advanced use cases.
func (s *SQLiteBackend) GetDB() *sql.DB {
	return s.db
}

// Vacuum performs database maintenance.
func (s *SQLiteBackend) Vacuum(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("backend is closed")
	}

	_, err := s.db.ExecContext(ctx, "VACUUM")
	return err
}

// Stats returns database statistics.
func (s *SQLiteBackend) Stats(ctx context.Context) (*SQLiteStats, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("backend is closed")
	}
	s.mu.RUnlock()

	stats := &SQLiteStats{}

	// Get partition count and size
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM partitions`)
	if err := row.Scan(&stats.PartitionCount, &stats.PartitionSize); err != nil {
		return nil, err
	}

	// Get time-series count
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM time_series`)
	if err := row.Scan(&stats.PointCount); err != nil {
		return nil, err
	}

	// Get metric count
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM metrics`)
	if err := row.Scan(&stats.MetricCount); err != nil {
		return nil, err
	}

	// Get database size using pragma
	row = s.db.QueryRowContext(ctx, `SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()`)
	if err := row.Scan(&stats.DatabaseSize); err != nil {
		// Ignore error, might not work on all SQLite versions
	}

	return stats, nil
}

// SQLiteStats contains database statistics.
type SQLiteStats struct {
	PartitionCount int64 `json:"partition_count"`
	PartitionSize  int64 `json:"partition_size"`
	PointCount     int64 `json:"point_count"`
	MetricCount    int64 `json:"metric_count"`
	DatabaseSize   int64 `json:"database_size"`
}

// EnableReadReplica configures read-only access for analytics.
func (s *SQLiteBackend) EnableReadReplica(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?mode=ro&_query_only=true", path)
	return sql.Open("sqlite", dsn)
}

// ExportToCSV exports a metric's data to CSV format.
func (s *SQLiteBackend) ExportToCSV(ctx context.Context, metric string, start, end int64) (string, error) {
	points, err := s.QueryRange(ctx, metric, start, end)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("timestamp,metric,value,tags\n")

	for _, p := range points {
		tagsStr := ""
		if len(p.Tags) > 0 {
			tagsJSON, _ := json.Marshal(p.Tags)
			tagsStr = string(tagsJSON)
		}
		sb.WriteString(fmt.Sprintf("%d,%s,%f,%s\n", p.Timestamp, p.Metric, p.Value, tagsStr))
	}

	return sb.String(), nil
}
