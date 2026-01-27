package chronicle

import (
	"context"
	"time"
)

// Query represents a parsed query.
type Query struct {
	Metric      string
	Tags        map[string]string
	TagFilters  []TagFilter
	Start       int64
	End         int64
	Aggregation *Aggregation
	GroupBy     []string
	Limit       int
}

// Aggregation defines an aggregation operation.
type Aggregation struct {
	Function AggFunc
	Window   time.Duration
}

// AggFunc enumerates aggregation functions.
type AggFunc int

const (
	AggNone AggFunc = iota
	AggCount
	AggSum
	AggMean
	AggMin
	AggMax
	AggStddev
	AggPercentile
	AggRate
	AggFirst
	AggLast
)

// TagOp enumerates tag operators.
type TagOp int

const (
	TagOpEq TagOp = iota
	TagOpNotEq
	TagOpIn
)

// TagFilter represents a tag predicate.
type TagFilter struct {
	Key    string
	Op     TagOp
	Values []string
}

// Execute runs a query and returns results.
// This is a convenience wrapper around ExecuteContext with a background context.
func (db *DB) Execute(q *Query) (*Result, error) {
	ctx := context.Background()
	if db.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.config.QueryTimeout)
		defer cancel()
	}
	return db.ExecuteContext(ctx, q)
}

// ExecuteContext runs a query with context support for cancellation and timeout.
func (db *DB) ExecuteContext(ctx context.Context, q *Query) (*Result, error) {
	if q == nil {
		return &Result{}, nil
	}

	q = db.tryMaterialized(q)

	db.mu.RLock()
	partitions := db.findPartitionsLocked(q.Start, q.End)
	allowed := db.index.FilterSeries(q.Metric, q.Tags)
	db.mu.RUnlock()

	if q.Aggregation != nil {
		buckets := newAggBuckets(db.config.MaxMemory)
		window := q.Aggregation.Window
		if window <= 0 {
			window = time.Second
		}
		for _, p := range partitions {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return nil, newQueryError(QueryErrorTypeTimeout, "query timeout", q, ctx.Err())
				}
				return nil, newQueryError(QueryErrorTypeCanceled, "query canceled", q, ctx.Err())
			default:
			}

			if err := p.aggregateIntoContext(ctx, db, q, allowed, buckets); err != nil {
				return nil, err
			}
		}
		points := buckets.finalize(q.Aggregation.Function, window)
		if q.Limit > 0 && len(points) > q.Limit {
			points = points[:q.Limit]
		}
		return &Result{Points: points}, nil
	}

	var points []Point
	var usedBytes int64
	for _, p := range partitions {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return nil, newQueryError(QueryErrorTypeTimeout, "query timeout", q, ctx.Err())
			}
			return nil, newQueryError(QueryErrorTypeCanceled, "query canceled", q, ctx.Err())
		default:
		}

		pts, err := p.queryContext(ctx, db, q, allowed)
		if err != nil {
			return nil, err
		}
		points = append(points, pts...)
		if db.config.MaxMemory > 0 {
			usedBytes += int64(len(pts)) * 48
			if usedBytes > db.config.MaxMemory {
				return nil, newQueryError(QueryErrorTypeMemory, "query memory budget exceeded", q, nil)
			}
		}
	}

	if q.Limit > 0 && len(points) > q.Limit {
		points = points[:q.Limit]
	}

	return &Result{Points: points}, nil
}
