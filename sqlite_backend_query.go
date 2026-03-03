package chronicle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

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
func (s *SQLiteBackend) ExecuteSQL(ctx context.Context, query string, args ...any) (*SQLResult, error) {
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
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
	RowsAffected int64    `json:"rows_affected,omitempty"`
	LastInsertID int64    `json:"last_insert_id,omitempty"`
}

func (s *SQLiteBackend) executeSelect(ctx context.Context, query string, args ...any) (*SQLResult, error) {
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
		Rows:    make([][]any, 0),
	}

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
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

func (s *SQLiteBackend) executeModify(ctx context.Context, query string, args ...any) (*SQLResult, error) {
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

	stmt, ok := s.aggStmts[aggFn]
	if !ok {
		return nil, fmt.Errorf("no prepared statement for aggregation: %s", aggFn)
	}

	rows, err := stmt.QueryContext(ctx, interval, interval, metric, start, end)
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
