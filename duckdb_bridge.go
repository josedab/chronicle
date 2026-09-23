package chronicle

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DuckDBBridge provides interoperability between Chronicle and DuckDB.
type DuckDBBridge struct {
	db *DB
}

// NewDuckDBBridge creates a new DuckDB integration bridge.
func NewDuckDBBridge(db *DB) *DuckDBBridge {
	return &DuckDBBridge{db: db}
}

// DuckDBTableSchema returns SQL DDL compatible with DuckDB external scanner.
func (b *DuckDBBridge) DuckDBTableSchema(tableName string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (metric VARCHAR, timestamp BIGINT, value DOUBLE, tags MAP(VARCHAR, VARCHAR));", tableName)
}

// ExportToDuckDB loads time series data directly into a DuckDB SQL connection.
func (b *DuckDBBridge) ExportToDuckDB(ctx context.Context, sqlDB *sql.DB, tableName string, metric string, start, end int64) (int64, error) {
	if b.db == nil {
		return 0, fmt.Errorf("chronicle db is nil")
	}

	res, err := b.db.ExecuteContext(ctx, &Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return 0, err
	}

	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (metric, timestamp, value) VALUES (?, ?, ?)", tableName))
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var count int64
	for _, pt := range res.Points {
		if _, err := stmt.ExecContext(ctx, pt.Metric, pt.Timestamp, pt.Value); err != nil {
			return count, err
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return count, err
	}
	return count, nil
}
