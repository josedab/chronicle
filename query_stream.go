package chronicle

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// ExecuteStream executes a query and streams matching points to the emit callback.
func (db *DB) ExecuteStream(q *Query, emit func(Point) error) error {
	return db.ExecuteStreamContext(context.Background(), q, emit)
}

// ExecuteStreamContext executes a query with context cancellation and streams points to emit.
// This allows streaming large query result sets via chunked HTTP responses without
// buffering all points in memory.
func (db *DB) ExecuteStreamContext(ctx context.Context, q *Query, emit func(Point) error) error {
	queryStart := time.Now()
	err := db.executeStreamInternal(ctx, q, emit)

	// Record query metrics for self-instrumentation
	if db.features != nil {
		if si := db.features.SelfInstrumentation(); si != nil {
			si.RecordQuery(time.Since(queryStart).Nanoseconds(), err != nil)
		}
	}

	return err
}

func (db *DB) executeStreamInternal(ctx context.Context, q *Query, emit func(Point) error) error {
	if q == nil {
		return nil
	}

	if q.Metric == "" {
		return newQueryError(QueryErrorTypeInvalid, "metric name is required", q, nil)
	}

	if emit == nil {
		return newQueryError(QueryErrorTypeInvalid, "emit callback is required", q, nil)
	}

	// Pre-compile regex tag filters once before scanning partitions
	if len(q.TagFilters) > 0 {
		if err := prepareTagFilters(q.TagFilters); err != nil {
			return newQueryError(QueryErrorTypeInvalid, err.Error(), q, err)
		}
	}

	// Check query cost budget (only if estimator already initialized)
	if db.features != nil {
		if ce := db.features.queryCost; ce != nil {
			if allowed, est, err := ce.ShouldExecute(q); err == nil && !allowed {
				return newQueryError(QueryErrorTypeMemory,
					fmt.Sprintf("query rejected by cost estimator: estimated cost %.2f exceeds threshold (verdict: %s)",
						est.EstimatedCost, est.Verdict), q, nil)
			}
		}
	}

	q = db.tryMaterialized(q)

	db.mu.RLock()
	partitions := db.findPartitionsLocked(q.Start, q.End)
	allowed := db.index.FilterSeries(q.Metric, q.Tags)
	db.mu.RUnlock()

	slog.Debug("query stream plan",
		"metric", q.Metric,
		"partitions", len(partitions),
		"series", len(allowed),
		"has_aggregation", q.Aggregation != nil,
	)

	// Immediate context check
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return newQueryError(QueryErrorTypeTimeout, "query timeout", q, ctx.Err())
		}
		return newQueryError(QueryErrorTypeCanceled, "query canceled", q, ctx.Err())
	default:
	}

	// Aggregated query execution
	if q.Aggregation != nil {
		buckets := newAggBuckets(db.config.Storage.MaxMemory)
		window := q.Aggregation.Window
		if window <= 0 {
			return newQueryError(QueryErrorTypeInvalid,
				"aggregation window must be positive", q, nil)
		}
		for _, p := range partitions {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return newQueryError(QueryErrorTypeTimeout, "query timeout", q, ctx.Err())
				}
				return newQueryError(QueryErrorTypeCanceled, "query canceled", q, ctx.Err())
			default:
			}

			if err := p.aggregateIntoContext(ctx, db, q, allowed, buckets); err != nil {
				return err
			}
		}
		points := buckets.finalize(q.Aggregation.Function, window, q.Aggregation.Percentile)
		if q.Limit > 0 && len(points) > q.Limit {
			points = points[:q.Limit]
		}
		for _, pt := range points {
			if err := emit(pt); err != nil {
				return err
			}
		}
		return nil
	}

	// Non-aggregated query streaming
	var usedBytes int64
	emitted := 0

	for _, p := range partitions {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return newQueryError(QueryErrorTypeTimeout, "query timeout", q, ctx.Err())
			}
			return newQueryError(QueryErrorTypeCanceled, "query canceled", q, ctx.Err())
		default:
		}

		pts, err := p.queryContext(ctx, db, q, allowed)
		if err != nil {
			return err
		}

		if db.config.Storage.MaxMemory > 0 {
			usedBytes += int64(len(pts)) * 48
			if usedBytes > db.config.Storage.MaxMemory {
				return newQueryError(QueryErrorTypeMemory, "query memory budget exceeded", q, nil)
			}
		}

		for _, pt := range pts {
			if q.Limit > 0 && emitted >= q.Limit {
				return nil
			}
			if err := emit(pt); err != nil {
				return err
			}
			emitted++
		}

		if q.Limit > 0 && emitted >= q.Limit {
			return nil
		}
	}

	return nil
}
