package chronicle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// QueryEngine provides additional query functionality on top of the DB.
// It wraps the DB's Execute methods with additional validation and cost estimation.
type QueryEngine struct {
	db *DB
}

// NewQueryEngine creates a new query engine wrapping the given database.
func NewQueryEngine(db *DB) *QueryEngine {
	return &QueryEngine{db: db}
}

// Execute runs a query and returns results. Delegates to DB.Execute.
func (qe *QueryEngine) Execute(q *Query) (*Result, error) {
	if err := qe.ValidateQuery(q); err != nil {
		return nil, err
	}
	return qe.db.Execute(q)
}

// ExecuteContext runs a query with context support. Delegates to DB.ExecuteContext.
func (qe *QueryEngine) ExecuteContext(ctx context.Context, q *Query) (*Result, error) {
	if err := qe.ValidateQuery(q); err != nil {
		return nil, err
	}
	return qe.db.ExecuteContext(ctx, q)
}

// ValidateQuery checks if a query is valid before execution.
func (qe *QueryEngine) ValidateQuery(q *Query) error {
	if q == nil {
		return errors.New("query is nil")
	}
	if q.Metric == "" {
		return errors.New("metric is required")
	}
	if q.End > 0 && q.Start > q.End {
		return fmt.Errorf("start time (%d) must be before end time (%d)", q.Start, q.End)
	}
	if q.Limit < 0 {
		return errors.New("limit must be non-negative")
	}
	return nil
}

// EstimateQueryCost provides a rough estimate of query cost without executing.
func (qe *QueryEngine) EstimateQueryCost(q *Query) QueryCost {
	qe.db.mu.RLock()
	defer qe.db.mu.RUnlock()

	partitions := qe.db.index.FindPartitions(q.Start, q.End)

	var estimatedSeries int64
	for _, p := range partitions {
		p.mu.RLock()
		estimatedSeries += int64(len(p.Series))
		p.mu.RUnlock()
	}

	return QueryCost{
		EstimatedPartitions: len(partitions),
		EstimatedSeries:     estimatedSeries,
		TimeRange:           time.Duration(q.End-q.Start) * time.Nanosecond,
	}
}

// QueryCost represents the estimated cost of executing a query.
type QueryCost struct {
	EstimatedPartitions int
	EstimatedSeries     int64
	TimeRange           time.Duration
}
