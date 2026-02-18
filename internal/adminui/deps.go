package adminui

import (
	"encoding/json"
	"log"
	"net/http"
)

// AdminDB abstracts all database access needed by the admin UI.
type AdminDB interface {
	Metrics() []string
	Execute(q *Query) (*Result, error)
	WriteBatch(points []Point) error
	Flush() error
	GetSchema(metric string) *MetricSchema
	Info() DBInfo
	IsClosed() bool
	ParseQuery(queryStr string) (*Query, error)
}

// Point represents a time-series data point.
type Point struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
}

// Query represents a parsed query.
type Query struct {
	Metric     string
	Tags       map[string]string
	TagFilters []TagFilter
	Start      int64
	End        int64
	Limit      int
}

// TagFilter represents a tag predicate.
type TagFilter struct {
	Key    string
	Values []string
}

// Result represents query results.
type Result struct {
	Points []Point
}

// TagSchema defines constraints for a tag.
type TagSchema struct {
	Name string `json:"name"`
}

// MetricSchema defines the schema for a metric.
type MetricSchema struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Tags        []TagSchema `json:"tags,omitempty"`
}

// DBInfo contains database configuration info for display.
type DBInfo struct {
	Path              string
	BufferSize        int
	PartitionDuration string
	RetentionDuration string
	WALSyncInterval   string
	PartitionCount    int
}

// QueryParser parses a limited SQL-like query syntax.
// This wraps the root package's QueryParser functionality.
type QueryParser interface {
	Parse(queryStr string) (*Query, error)
}

// writeJSON encodes data as JSON and writes it to the response.
func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("chronicle: failed to encode JSON response: %v", err)
	}
}
