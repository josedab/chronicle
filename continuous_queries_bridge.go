// Bridge: continuous_queries_bridge.go
//
// This file bridges internal/continuousquery/ into the public chronicle package.
// It re-exports types via type aliases and provides adapter constructors so that
// callers use the top-level chronicle API while implementation stays private.
//
// Pattern: internal/continuousquery/ (implementation) → continuous_queries_bridge.go (public API)

package chronicle

import (
	"time"

	cq "github.com/chronicle-db/chronicle/internal/continuousquery"
)

// Type aliases for backward compatibility.
type ContinuousQueryConfig = cq.ContinuousQueryConfig
type StateBackendType = cq.StateBackendType
type ContinuousQueryEngine = cq.ContinuousQueryEngine
type ContinuousQueryV2 = cq.ContinuousQueryV2
type CQState = cq.CQState
type CQConfig = cq.CQConfig
type OutputMode = cq.OutputMode
type TriggerConfig = cq.TriggerConfig
type TriggerType = cq.TriggerType
type SinkConfig = cq.SinkConfig
type CQStats = cq.CQStats
type QueryPlan = cq.QueryPlan
type PlanNode = cq.PlanNode
type PlanNodeType = cq.PlanNodeType
type CQOperator = cq.CQOperator
type Record = cq.Record
type MaterializedView = cq.MaterializedView
type CQColumnDef = cq.CQColumnDef
type RefreshMode = cq.RefreshMode
type ViewData = cq.ViewData
type QueryStateManager = cq.QueryStateManager
type WatermarkTracker = cq.WatermarkTracker
type CQQueryOptimizer = cq.CQQueryOptimizer
type OptimizationRule = cq.OptimizationRule
type CQMetrics = cq.CQMetrics
type QueryCheckpoint = cq.QueryCheckpoint
type PredicatePushdownRule = cq.PredicatePushdownRule
type ProjectionPushdownRule = cq.ProjectionPushdownRule
type JoinReorderRule = cq.JoinReorderRule
type AggregationOptimizationRule = cq.AggregationOptimizationRule
type ScanOperator = cq.ScanOperator
type FilterOperator = cq.FilterOperator
type ProjectOperator = cq.ProjectOperator
type AggregateOperator = cq.AggregateOperator
type WindowOperator = cq.WindowOperator
type WindowBuffer = cq.WindowBuffer
type SinkOperator = cq.SinkOperator
type PassthroughOperator = cq.PassthroughOperator
type CQEngineStats = cq.CQEngineStats
type StreamJoinConfig = cq.StreamJoinConfig
type StreamJoinType = cq.StreamJoinType
type PartitionedQuery = cq.PartitionedQuery
type QueryPartition = cq.QueryPartition
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
}

// streamSubAdapter adapts *Subscription to cq.StreamSubscription.
type streamSubAdapter struct {
	sub *Subscription
}

func (a *streamSubAdapter) C() <-chan cq.Point {
	// The channel types are identical in layout; use a goroutine to bridge.
	ch := make(chan cq.Point, cap(a.sub.ch))
	go func() {
		defer close(ch)
		for p := range a.sub.ch {
			ch <- cq.Point{
				Metric:    p.Metric,
				Timestamp: p.Timestamp,
				Value:     p.Value,
				Tags:      p.Tags,
			}
		}
	}()
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
	return &streamSubAdapter{sub: sub}
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
		bus = &streamHubBus{hub: hub}
	}

	return cq.NewContinuousQueryEngine(pw, bus, config)
}

// Ensure DefaultContinuousQueryConfig stays in sync with the internal package
// by verifying the default values at init time.
func init() {
	cfg := DefaultContinuousQueryConfig()
	if cfg.DefaultWindow != time.Minute {
		panic("DefaultContinuousQueryConfig mismatch")
	}
}
