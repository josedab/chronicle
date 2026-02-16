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
		estimatedSeries += int64(len(p.series))
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

// ExecuteVectorized attempts to use the vectorized execution path for aggregation queries.
// For large aggregation queries, it collects raw values and applies vectorized operations.
func (qe *QueryEngine) ExecuteVectorized(ctx context.Context, q *Query) (*Result, ExecutionPath, error) {
	if err := qe.ValidateQuery(q); err != nil {
		return nil, PathRowOriented, err
	}

	if q.Aggregation == nil {
		result, err := qe.db.ExecuteContext(ctx, q)
		return result, PathRowOriented, err
	}

	cost := qe.EstimateQueryCost(q)
	estimatedPoints := int(cost.EstimatedSeries) * cost.EstimatedPartitions

	// Small queries: standard path
	if estimatedPoints < 1000 {
		result, err := qe.db.ExecuteContext(ctx, q)
		return result, PathRowOriented, err
	}

	// Large queries: collect raw points then apply vectorized aggregation
	rawQ := &Query{
		Metric: q.Metric,
		Tags:   q.Tags,
		Start:  q.Start,
		End:    q.End,
	}
	rawResult, err := qe.db.ExecuteContext(ctx, rawQ)
	if err != nil {
		return nil, PathRowOriented, err
	}

	if len(rawResult.Points) == 0 {
		return &Result{}, PathVectorized, nil
	}

	// Extract values for vectorized processing
	values := make([]float64, len(rawResult.Points))
	for i, p := range rawResult.Points {
		values[i] = p.Value
	}

	agg := NewVectorizedAggregator()
	var aggOp VectorAggOp
	switch q.Aggregation.Function {
	case AggSum:
		aggOp = VectorSum
	case AggMin:
		aggOp = VectorMin
	case AggMax:
		aggOp = VectorMax
	case AggMean:
		aggOp = VectorAvg
	case AggCount:
		aggOp = VectorCount
	default:
		// Fall back to standard for unsupported agg functions
		result, err := qe.db.ExecuteContext(ctx, q)
		return result, PathRowOriented, err
	}

	path := PathVectorized
	var aggVal float64
	if len(values) >= 100000 {
		path = PathParallelScan
		pe := NewParallelVectorizedExecutor(4)
		chunkSize := len(values) / 4
		if chunkSize < 1000 {
			chunkSize = 1000
		}
		var chunks [][]float64
		for i := 0; i < len(values); i += chunkSize {
			end := i + chunkSize
			if end > len(values) {
				end = len(values)
			}
			chunks = append(chunks, values[i:end])
		}
		aggVal, err = pe.ExecuteParallel(chunks, aggOp)
	} else {
		aggVal, err = agg.Aggregate(aggOp, values)
	}
	if err != nil {
		return nil, path, err
	}

	ts := rawResult.Points[len(rawResult.Points)-1].Timestamp
	return &Result{
		Points: []Point{{
			Metric:    q.Metric,
			Value:     aggVal,
			Timestamp: ts,
		}},
	}, path, nil
}
