// Bridge: pgwire_bridge.go
//
// PostgreSQL wire protocol v3 implementation.
// Supports startup/auth (cleartext, MD5, SCRAM-SHA-256), simple + extended query
// protocol, COPY IN for bulk ingestion, and prepared statements.
// See FEATURE_MATURITY.md for details.

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

func (a *dbPGAdapter) WriteBatch(points []pgwire.Point) error {
	for _, p := range points {
		if err := a.db.Write(Point{Metric: p.Metric, Timestamp: p.Timestamp, Value: p.Value, Tags: p.Tags}); err != nil {
			return err
		}
	}
	return nil
}

func (a *dbPGAdapter) Metrics() []string {
	return a.db.Metrics()
}
