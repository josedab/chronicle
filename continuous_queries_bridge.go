// Bridge: continuous_queries_bridge.go
//
// This file bridges internal/continuousquery/ into the public chronicle package.
// It re-exports types via type aliases and provides adapter constructors so that
// callers use the top-level chronicle API while implementation stays private.
//
// Pattern: internal/continuousquery/ (implementation) → continuous_queries_bridge.go (public API)

package chronicle

import (
	"context"

	cq "github.com/chronicle-db/chronicle/internal/continuousquery"
)

// ContinuousQueryConfig holds configuration for the continuous query engine.
type ContinuousQueryConfig = cq.ContinuousQueryConfig

// StateBackendType identifies the storage backend for query state.
type StateBackendType = cq.StateBackendType

// ContinuousQueryEngine manages the lifecycle of continuous queries.
type ContinuousQueryEngine = cq.ContinuousQueryEngine

// ContinuousQueryV2 represents a v2 continuous query definition.
type ContinuousQueryV2 = cq.ContinuousQueryV2

// CQState is the lifecycle state of a continuous query.
type CQState = cq.CQState

// CQConfig holds per-query configuration for a continuous query.
type CQConfig = cq.CQConfig

// OutputMode controls how query results are emitted (append, complete, update).
type OutputMode = cq.OutputMode

// TriggerConfig configures when a continuous query fires.
type TriggerConfig = cq.TriggerConfig

// TriggerType specifies the trigger strategy for a continuous query.
type TriggerType = cq.TriggerType

// SinkConfig configures the output destination for query results.
type SinkConfig = cq.SinkConfig

// CQStats contains runtime statistics for a continuous query.
type CQStats = cq.CQStats

// QueryPlan is the logical execution plan for a continuous query.
type QueryPlan = cq.QueryPlan

// PlanNode is a single node in a query execution plan tree.
type PlanNode = cq.PlanNode

// PlanNodeType identifies the operation type of a plan node.
type PlanNodeType = cq.PlanNodeType

// CQOperator processes records in a continuous query pipeline.
type CQOperator = cq.CQOperator

// Record is a single data record flowing through the query pipeline.
type Record = cq.Record

// MaterializedView is a continuously maintained query result set.
type MaterializedView = cq.MaterializedView

// CQColumnDef defines a column in a materialized view.
type CQColumnDef = cq.CQColumnDef

// RefreshMode controls how a materialized view is refreshed (incremental or full).
type RefreshMode = cq.RefreshMode

// ViewData holds the materialized data for a view.
type ViewData = cq.ViewData

// QueryStateManager manages persistent state for continuous queries.
type QueryStateManager = cq.QueryStateManager

// WatermarkTracker tracks event-time watermarks for out-of-order handling.
type WatermarkTracker = cq.WatermarkTracker

// CQQueryOptimizer optimizes continuous query execution plans.
type CQQueryOptimizer = cq.CQQueryOptimizer

// OptimizationRule is a plan transformation rule applied by the optimizer.
type OptimizationRule = cq.OptimizationRule

// CQMetrics collects metrics about continuous query execution.
type CQMetrics = cq.CQMetrics

// QueryCheckpoint is a saved checkpoint for fault-tolerant query recovery.
type QueryCheckpoint = cq.QueryCheckpoint

// PredicatePushdownRule pushes filter predicates closer to data sources.
type PredicatePushdownRule = cq.PredicatePushdownRule

// ProjectionPushdownRule pushes column projections closer to data sources.
type ProjectionPushdownRule = cq.ProjectionPushdownRule

// JoinReorderRule reorders join operations for better performance.
type JoinReorderRule = cq.JoinReorderRule

// AggregationOptimizationRule optimizes aggregation operations.
type AggregationOptimizationRule = cq.AggregationOptimizationRule

// ScanOperator reads records from a data source.
type ScanOperator = cq.ScanOperator

// FilterOperator removes records that do not match a predicate.
type FilterOperator = cq.FilterOperator

// ProjectOperator selects or transforms columns in each record.
type ProjectOperator = cq.ProjectOperator

// AggregateOperator groups and aggregates records.
type AggregateOperator = cq.AggregateOperator

// WindowOperator applies windowed computations over record streams.
type WindowOperator = cq.WindowOperator

// WindowBuffer holds records for a single window instance.
type WindowBuffer = cq.WindowBuffer

// SinkOperator writes processed records to an output destination.
type SinkOperator = cq.SinkOperator

// PassthroughOperator forwards records without modification.
type PassthroughOperator = cq.PassthroughOperator

// CQEngineStats contains aggregate statistics for the query engine.
type CQEngineStats = cq.CQEngineStats

// StreamJoinConfig configures a stream-to-stream join operation.
type StreamJoinConfig = cq.StreamJoinConfig

// StreamJoinType specifies the type of stream join (inner, left, outer).
type StreamJoinType = cq.StreamJoinType

// PartitionedQuery is a query that executes across data partitions.
type PartitionedQuery = cq.PartitionedQuery

// QueryPartition represents one partition of a distributed query.
type QueryPartition = cq.QueryPartition

// KeyRange defines a contiguous key range for partitioned queries.
type KeyRange = cq.KeyRange

// Constant aliases.
const (
	StateBackendMemory    = cq.StateBackendMemory
	StateBackendRocksDB   = cq.StateBackendRocksDB
	StateBackendChronicle = cq.StateBackendChronicle

	CQStateCreated       = cq.CQStateCreated
	CQStateRunning       = cq.CQStateRunning
	CQStatePaused        = cq.CQStatePaused
	CQStateStopped       = cq.CQStateStopped
	CQStateFailed        = cq.CQStateFailed
	CQStateCheckpointing = cq.CQStateCheckpointing

	OutputModeAppend   = cq.OutputModeAppend
	OutputModeComplete = cq.OutputModeComplete
	OutputModeUpdate   = cq.OutputModeUpdate

	TriggerTypeProcessingTime = cq.TriggerTypeProcessingTime
	TriggerTypeEventTime      = cq.TriggerTypeEventTime
	TriggerTypeOnce           = cq.TriggerTypeOnce
	TriggerTypeContinuous     = cq.TriggerTypeContinuous

	PlanNodeScan      = cq.PlanNodeScan
	PlanNodeFilter    = cq.PlanNodeFilter
	PlanNodeProject   = cq.PlanNodeProject
	PlanNodeAggregate = cq.PlanNodeAggregate
	PlanNodeJoin      = cq.PlanNodeJoin
	PlanNodeWindow    = cq.PlanNodeWindow
	PlanNodeSink      = cq.PlanNodeSink
	PlanNodeExchange  = cq.PlanNodeExchange

	RefreshModeIncremental = cq.RefreshModeIncremental
	RefreshModeFull        = cq.RefreshModeFull

	StreamJoinInner = cq.StreamJoinInner
	StreamJoinLeft  = cq.StreamJoinLeft
	StreamJoinOuter = cq.StreamJoinOuter
)

// Function aliases.
var DefaultContinuousQueryConfig = cq.DefaultContinuousQueryConfig

// dbPointWriter adapts *DB to cq.PointWriter.
type dbPointWriter struct {
	db *DB
}

func (w *dbPointWriter) WritePoint(p cq.Point) error {
	return w.db.Write(Point{
		Metric:    p.Metric,
		Tags:      p.Tags,
		Value:     p.Value,
		Timestamp: p.Timestamp,
	})
}

// streamHubBus adapts *StreamHub to cq.StreamBus.
type streamHubBus struct {
	hub *StreamHub
	ctx context.Context
}

// streamSubAdapter adapts *Subscription to cq.StreamSubscription.
type streamSubAdapter struct {
	sub *Subscription
	ctx context.Context
}

func (a *streamSubAdapter) C() <-chan cq.Point {
	// The channel types are identical in layout; use a goroutine to bridge.
	ch := make(chan cq.Point, cap(a.sub.ch))
	go func(ctx context.Context) {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case p, ok := <-a.sub.ch:
				if !ok {
					return
				}
				ch <- cq.Point{
					Metric:    p.Metric,
					Timestamp: p.Timestamp,
					Value:     p.Value,
					Tags:      p.Tags,
				}
			}
		}
	}(a.ctx)
	return ch
}

func (a *streamSubAdapter) Close() {
	a.sub.Close()
}

func (b *streamHubBus) Subscribe(topic string, tags map[string]string) cq.StreamSubscription {
	sub := b.hub.Subscribe(topic, tags)
	if sub == nil {
		return nil
	}
	return &streamSubAdapter{sub: sub, ctx: b.ctx}
}

func (b *streamHubBus) Publish(p cq.Point) {
	b.hub.Publish(Point{
		Metric:    p.Metric,
		Tags:      p.Tags,
		Value:     p.Value,
		Timestamp: p.Timestamp,
	})
}

// NewContinuousQueryEngine creates a new continuous query engine.
// It wraps *DB and *StreamHub into the interfaces expected by the
// internal continuousquery package.
func NewContinuousQueryEngine(db *DB, hub *StreamHub, config ContinuousQueryConfig) *ContinuousQueryEngine {
	var pw cq.PointWriter
	if db != nil {
		pw = &dbPointWriter{db: db}
	}

	var bus cq.StreamBus
	if hub != nil {
		bus = &streamHubBus{hub: hub, ctx: context.Background()}
	}

	return cq.NewContinuousQueryEngine(pw, bus, config)
}

// Compile-time interface compliance checks for bridge adapters.
var (
	_ cq.PointWriter        = (*dbPointWriter)(nil)
	_ cq.StreamBus          = (*streamHubBus)(nil)
	_ cq.StreamSubscription = (*streamSubAdapter)(nil)
)
