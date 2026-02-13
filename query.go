package chronicle

import (
	"context"
	"time"
)

// Query describes a time-series data retrieval request. At minimum, set Metric
// to select a series. Use Start/End (Unix nanoseconds) to bound the time range,
// TagFilters to narrow by labels, and Aggregation to reduce results.
type Query struct {
	Metric      string
	Tags        map[string]string
	TagFilters  []TagFilter
	Start       int64 // inclusive lower bound, Unix nanoseconds; 0 means unbounded
	End         int64 // exclusive upper bound, Unix nanoseconds; 0 means unbounded
	Aggregation *Aggregation
	GroupBy     []string
	Limit       int
}

// Aggregation specifies a downsampling operation applied during query execution.
// Function selects the reducer (sum, mean, etc.) and Window sets the bucket width.
type Aggregation struct {
	Function AggFunc
	Window   time.Duration
}

// AggFunc selects the aggregation function applied to each time window.
type AggFunc int

const (
	AggNone       AggFunc = iota // No aggregation (raw points)
	AggCount                     // Number of points per window
	AggSum                       // Sum of values per window
	AggMean                      // Arithmetic mean per window
	AggMin                       // Minimum value per window
	AggMax                       // Maximum value per window
	AggStddev                    // Standard deviation per window
	AggPercentile                // Percentile per window
	AggRate                      // Rate of change per window
	AggFirst                     // First value per window
	AggLast                      // Last value per window
)

// TagOp selects the comparison operator used in a TagFilter.
type TagOp int

const (
	TagOpEq    TagOp = iota // Exact match (tag == value)
	TagOpNotEq              // Exclusion (tag != value)
	TagOpIn                 // Set membership (tag in [values...])
)

// TagFilter restricts query results to series whose tag Key satisfies
// the comparison Op against the given Values.
type TagFilter struct {
	Key    string
	Op     TagOp
	Values []string
}

// Execute runs a query and returns results.
// This is a convenience wrapper around ExecuteContext with a background context.
func (db *DB) Execute(q *Query) (*Result, error) {
	ctx := context.Background()
	if db.config.Query.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.config.Query.QueryTimeout)
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
		buckets := newAggBuckets(db.config.Storage.MaxMemory)
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
		if db.config.Storage.MaxMemory > 0 {
			usedBytes += int64(len(pts)) * 48
			if usedBytes > db.config.Storage.MaxMemory {
				return nil, newQueryError(QueryErrorTypeMemory, "query memory budget exceeded", q, nil)
			}
		}
	}

	if q.Limit > 0 && len(points) > q.Limit {
		points = points[:q.Limit]
	}

	return &Result{Points: points}, nil
}
