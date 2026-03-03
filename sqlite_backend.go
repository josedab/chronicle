package chronicle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

	// Prepared aggregate statements keyed by function name
	aggStmts map[string]*sql.Stmt
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

	// Prepare aggregate statements for each supported function
	s.aggStmts = make(map[string]*sql.Stmt)
	for _, fn := range []string{"AVG", "SUM", "MIN", "MAX", "COUNT"} {
		stmt, err := s.db.Prepare(fmt.Sprintf(`
			SELECT
				(timestamp / ?) * ? AS bucket,
				%s(value) AS agg_value,
				COUNT(*) AS count
			FROM time_series
			WHERE metric = ? AND timestamp >= ? AND timestamp < ?
			GROUP BY bucket
			ORDER BY bucket
		`, fn))
		if err != nil {
			return fmt.Errorf("failed to prepare %s aggregate statement: %w", fn, err)
		}
		s.aggStmts[fn] = stmt
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
	for _, stmt := range s.aggStmts {
		stmt.Close()
	}

	return s.db.Close()
}

// Time-series query methods, SQL execution, aggregation, metrics metadata,
// and utility methods are in sqlite_backend_query.go.
