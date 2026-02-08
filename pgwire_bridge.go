// Bridge: pgwire_bridge.go
//
// This file bridges internal/pgwire/ into the public chronicle package.
// It re-exports types so that callers can use the PostgreSQL wire protocol
// server via the top-level chronicle API.
//
// Pattern: internal/pgwire/ (implementation) → pgwire_bridge.go (public API)

package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/pgwire"
)

// Type aliases re-export PostgreSQL wire protocol types.
type PGServer = pgwire.PGServer
type PGWireConfig = pgwire.PGWireConfig
type PGServerStats = pgwire.PGServerStats

// Re-export constructors.
var DefaultPGWireConfig = pgwire.DefaultPGWireConfig

// NewPGServer creates a new PostgreSQL wire protocol server wrapping a Chronicle DB.
func NewPGServer(db *DB, config *pgwire.PGWireConfig) (*pgwire.PGServer, error) {
	return pgwire.NewPGServer(&dbPGAdapter{db: db}, config)
}

// dbPGAdapter makes *DB satisfy pgwire.PGDB.
type dbPGAdapter struct {
	db *DB
}

func (a *dbPGAdapter) Execute(metric string, start, end int64, tags map[string]string, limit int) (*pgwire.QueryResult, error) {
	q := &Query{Metric: metric, Start: start, End: end, Tags: tags, Limit: limit}
	result, err := a.db.Execute(q)
	if err != nil {
		return nil, err
	}
	points := make([]pgwire.Point, len(result.Points))
	for i, p := range result.Points {
		points[i] = pgwire.Point{Metric: p.Metric, Timestamp: p.Timestamp, Value: p.Value, Tags: p.Tags}
	}
	return &pgwire.QueryResult{Points: points}, nil
}

func (a *dbPGAdapter) Write(p pgwire.Point) error {
	return a.db.Write(Point{Metric: p.Metric, Timestamp: p.Timestamp, Value: p.Value, Tags: p.Tags})
}

func (a *dbPGAdapter) Metrics() []string {
	return a.db.Metrics()
}
