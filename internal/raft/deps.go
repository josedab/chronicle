package raft

import "context"

// Point represents a single time-series data point.
type Point struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// QueryResult holds the results of a storage query.
type QueryResult struct {
	Points []Point
}

// StorageEngine abstracts the database operations needed by Raft.
type StorageEngine interface {
	Write(p Point) error
	ExecuteQuery(ctx context.Context, metric string, start, end int64) (*QueryResult, error)
	Metrics() []string
}
