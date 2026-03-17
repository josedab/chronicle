package chronicle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// QueryEngineConfig configures the query engine behavior.
type QueryEngineConfig struct {
	// MaxQueryDuration is the maximum time a query is allowed to run.
	// When exceeded, the query is canceled with ErrQueryTimeout.
	// Zero means no timeout (uses the context's deadline if any).
	MaxQueryDuration time.Duration
}

// QueryEngine provides additional query functionality on top of the DB.
// It wraps the DB's Execute methods with additional validation and cost estimation.
type QueryEngine struct {
	db     *DB
	cbo    *CostBasedOptimizer
	config QueryEngineConfig
}

// NewQueryEngine creates a new query engine wrapping the given database.
func NewQueryEngine(db *DB) *QueryEngine {
	return &QueryEngine{db: db}
}

// NewQueryEngineWithConfig creates a query engine with configuration.
func NewQueryEngineWithConfig(db *DB, cfg QueryEngineConfig) *QueryEngine {
	return &QueryEngine{db: db, config: cfg}
}

// NewQueryEngineWithCBO creates a query engine with cost-based optimizer integration.
func NewQueryEngineWithCBO(db *DB, cbo *CostBasedOptimizer) *QueryEngine {
	return &QueryEngine{db: db, cbo: cbo}
}

// Execute runs a query and returns results. Delegates to DB.Execute.
func (qe *QueryEngine) Execute(q *Query) (*Result, error) {
	if err := qe.ValidateQuery(q); err != nil {
		return nil, err
	}
	if qe.config.MaxQueryDuration > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), qe.config.MaxQueryDuration)
		defer cancel()
		return qe.db.ExecuteContext(ctx, q)
	}
	return qe.db.Execute(q)
}

// ExecuteContext runs a query with context support. Delegates to DB.ExecuteContext.
// If MaxQueryDuration is configured and the context has no earlier deadline,
// a timeout is applied.
func (qe *QueryEngine) ExecuteContext(ctx context.Context, q *Query) (*Result, error) {
	if err := qe.ValidateQuery(q); err != nil {
		return nil, err
	}
	if qe.config.MaxQueryDuration > 0 {
		_, hasDeadline := ctx.Deadline()
		if !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, qe.config.MaxQueryDuration)
			defer cancel()
		}
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

// --- Predicate Pushdown & Adaptive Path Selection ---

// PredicateFilter defines a filter that can be pushed down to the storage scan.
type PredicateFilter struct {
	MinValue *float64          // only return points >= this value
	MaxValue *float64          // only return points <= this value
	TagMatch map[string]string // exact tag match
}

// ScanWithPredicate performs a storage-layer scan with predicate pushdown,
// filtering points during scan rather than after retrieval.
func (qe *QueryEngine) ScanWithPredicate(ctx context.Context, q *Query, pred *PredicateFilter) (*Result, error) {
	if err := qe.ValidateQuery(q); err != nil {
		return nil, err
	}

	if pred != nil && len(pred.TagMatch) > 0 {
		if q.Tags == nil {
			q.Tags = make(map[string]string)
		}
		for k, v := range pred.TagMatch {
			q.Tags[k] = v
		}
	}

	result, err := qe.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, err
	}

	if pred == nil || (pred.MinValue == nil && pred.MaxValue == nil) {
		return result, nil
	}

	filtered := make([]Point, 0, len(result.Points))
	for _, p := range result.Points {
		if pred.MinValue != nil && p.Value < *pred.MinValue {
			continue
		}
		if pred.MaxValue != nil && p.Value > *pred.MaxValue {
			continue
		}
		filtered = append(filtered, p)
	}
	return &Result{Points: filtered}, nil
}

// ExecuteAdaptive selects the best execution path based on data characteristics.
// When a CostBasedOptimizer is configured, it uses CBO plan selection.
func (qe *QueryEngine) ExecuteAdaptive(ctx context.Context, q *Query) (*Result, ExecutionPath, error) {
	if err := qe.ValidateQuery(q); err != nil {
		return nil, PathRowOriented, err
	}

	if q.Aggregation == nil {
		result, err := qe.db.ExecuteContext(ctx, q)
		return result, PathRowOriented, err
	}

	// Use CBO-driven path selection when available
	if qe.cbo != nil {
		return qe.executeCBOAdaptive(ctx, q)
	}

	cost := qe.EstimateQueryCost(q)
	estimatedPoints := int(cost.EstimatedSeries) * cost.EstimatedPartitions

	switch {
	case estimatedPoints < 500:
		result, err := qe.db.ExecuteContext(ctx, q)
		return result, PathRowOriented, err
	case estimatedPoints < 50000:
		return qe.ExecuteVectorized(ctx, q)
	default:
		return qe.executeParallelColumnar(ctx, q)
	}
}

// executeCBOAdaptive uses the cost-based optimizer to choose the execution path.
func (qe *QueryEngine) executeCBOAdaptive(ctx context.Context, q *Query) (*Result, ExecutionPath, error) {
	plan, err := qe.cbo.ChooseBestPlan(q)
	if err != nil {
		// Fallback to heuristic path on CBO error
		cost := qe.EstimateQueryCost(q)
		estimatedPoints := int(cost.EstimatedSeries) * cost.EstimatedPartitions
		if estimatedPoints < 500 {
			result, execErr := qe.db.ExecuteContext(ctx, q)
			return result, PathRowOriented, execErr
		}
		return qe.ExecuteVectorized(ctx, q)
	}

	switch plan.Plan {
	case PlanColumnarBatch:
		return qe.executeColumnarBatch(ctx, q, plan)
	case PlanIndexScan, PlanIndexOnly:
		return qe.ExecuteVectorized(ctx, q)
	default:
		result, execErr := qe.db.ExecuteContext(ctx, q)
		return result, PathRowOriented, execErr
	}
}

// executeColumnarBatch uses columnar batch processing driven by CBO plan.
func (qe *QueryEngine) executeColumnarBatch(ctx context.Context, q *Query, plan *CBOPlanCandidate) (*Result, ExecutionPath, error) {
	rawQ := &Query{
		Metric: q.Metric,
		Tags:   q.Tags,
		Start:  q.Start,
		End:    q.End,
	}
	rawResult, err := qe.db.ExecuteContext(ctx, rawQ)
	if err != nil {
		return nil, PathColumnar, err
	}
	if len(rawResult.Points) == 0 {
		return &Result{}, PathColumnar, nil
	}

	// Determine worker count based on estimated rows
	workers := int(plan.EstimatedRows) / 50000
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}

	pa := NewParallelBatchAggregator(workers, DefaultBatchSize)
	aggVal := pa.AggregateBatches(rawResult.Points, q.Aggregation.Function)

	ts := rawResult.Points[len(rawResult.Points)-1].Timestamp
	return &Result{
		Points: []Point{{
			Metric:    q.Metric,
			Value:     aggVal,
			Timestamp: ts,
		}},
	}, PathColumnar, nil
}

func (qe *QueryEngine) executeParallelColumnar(ctx context.Context, q *Query) (*Result, ExecutionPath, error) {
	rawQ := &Query{
		Metric: q.Metric,
		Tags:   q.Tags,
		Start:  q.Start,
		End:    q.End,
	}
	rawResult, err := qe.db.ExecuteContext(ctx, rawQ)
	if err != nil {
		return nil, PathParallelScan, err
	}
	if len(rawResult.Points) == 0 {
		return &Result{}, PathParallelScan, nil
	}

	values := make([]float64, len(rawResult.Points))
	for i, p := range rawResult.Points {
		values[i] = p.Value
	}

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
		result, err := qe.db.ExecuteContext(ctx, q)
		return result, PathRowOriented, err
	}

	workers := len(values) / 100000
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}

	pe := NewParallelVectorizedExecutor(workers)
	chunkSize := len(values) / workers
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

	aggVal, err := pe.ExecuteParallel(chunks, aggOp)
	if err != nil {
		return nil, PathParallelScan, err
	}

	ts := rawResult.Points[len(rawResult.Points)-1].Timestamp
	return &Result{
		Points: []Point{{
			Metric:    q.Metric,
			Value:     aggVal,
			Timestamp: ts,
		}},
	}, PathParallelScan, nil
}
