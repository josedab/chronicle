package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ContinuousQueryConfig configures the continuous query engine v2.
type ContinuousQueryConfig struct {
	// Enabled enables continuous queries.
	Enabled bool

	// MaxQueries limits concurrent continuous queries.
	MaxQueries int

	// DefaultWindow is the default window size.
	DefaultWindow time.Duration

	// CheckpointInterval is how often to checkpoint state.
	CheckpointInterval time.Duration

	// StateBackend configures where to store query state.
	StateBackend StateBackendType

	// EnableWatermarks enables event-time watermarks.
	EnableWatermarks bool

	// WatermarkDelay is the allowed late arrival time.
	WatermarkDelay time.Duration

	// EnableExactlyOnce enables exactly-once processing semantics.
	EnableExactlyOnce bool

	// ParallelismHint is the suggested parallelism for queries.
	ParallelismHint int

	// EnableQueryOptimizer enables query optimization.
	EnableQueryOptimizer bool

	// MetricsEnabled enables internal metrics collection.
	MetricsEnabled bool

	// RetryOnFailure enables automatic retry on transient failures.
	RetryOnFailure bool

	// MaxRetries is the maximum number of retries.
	MaxRetries int
}

// StateBackendType identifies the state backend.
type StateBackendType int

const (
	// StateBackendMemory uses in-memory state.
	StateBackendMemory StateBackendType = iota
	// StateBackendRocksDB uses RocksDB for state.
	StateBackendRocksDB
	// StateBackendChronicle uses Chronicle DB for state.
	StateBackendChronicle
)

// DefaultContinuousQueryConfig returns default configuration.
func DefaultContinuousQueryConfig() ContinuousQueryConfig {
	return ContinuousQueryConfig{
		Enabled:              true,
		MaxQueries:           100,
		DefaultWindow:        time.Minute,
		CheckpointInterval:   time.Minute,
		StateBackend:         StateBackendMemory,
		EnableWatermarks:     true,
		WatermarkDelay:       10 * time.Second,
		EnableExactlyOnce:    false,
		ParallelismHint:      4,
		EnableQueryOptimizer: true,
		MetricsEnabled:       true,
		RetryOnFailure:       true,
		MaxRetries:           3,
	}
}

// ContinuousQueryEngine provides Flink-style continuous query processing.
type ContinuousQueryEngine struct {
	db      *DB
	hub     *StreamHub
	config  ContinuousQueryConfig

	// Query registry
	queries   map[string]*ContinuousQueryV2
	queryMu   sync.RWMutex

	// Materialized views
	views     map[string]*MaterializedView
	viewMu    sync.RWMutex

	// State management
	stateManager *QueryStateManager

	// Watermark tracking
	watermarkTracker *WatermarkTracker

	// Query optimizer
	optimizer *CQQueryOptimizer

	// Metrics
	metrics *CQMetrics

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ContinuousQueryV2 represents an enhanced continuous query definition.
type ContinuousQueryV2 struct {
	ID             string                `json:"id"`
	Name           string                `json:"name"`
	SQL            string                `json:"sql"`
	Plan           *QueryPlan            `json:"plan"`
	State          CQState               `json:"state"`
	Config         CQConfig              `json:"config"`
	Created        time.Time             `json:"created"`
	LastCheckpoint time.Time             `json:"last_checkpoint"`
	Stats          CQStats               `json:"stats"`

	// Runtime
	ctx       context.Context
	cancel    context.CancelFunc
	operators []CQOperator
	mu        sync.Mutex
}

// CQState represents continuous query state.
type CQState int

const (
	CQStateCreated CQState = iota
	CQStateRunning
	CQStatePaused
	CQStateStopped
	CQStateFailed
	CQStateCheckpointing
)

func (s CQState) String() string {
	switch s {
	case CQStateCreated:
		return "created"
	case CQStateRunning:
		return "running"
	case CQStatePaused:
		return "paused"
	case CQStateStopped:
		return "stopped"
	case CQStateFailed:
		return "failed"
	case CQStateCheckpointing:
		return "checkpointing"
	default:
		return "unknown"
	}
}

// CQConfig contains per-query configuration.
type CQConfig struct {
	// Parallelism for this query.
	Parallelism int `json:"parallelism"`

	// OutputMode specifies how results are emitted.
	OutputMode OutputMode `json:"output_mode"`

	// Watermark configuration.
	WatermarkColumn string        `json:"watermark_column"`
	WatermarkDelay  time.Duration `json:"watermark_delay"`

	// Trigger configuration.
	Trigger TriggerConfig `json:"trigger"`

	// Sink configuration.
	Sinks []SinkConfig `json:"sinks"`
}

// OutputMode specifies result output behavior.
type OutputMode int

const (
	// OutputModeAppend only outputs new rows.
	OutputModeAppend OutputMode = iota
	// OutputModeComplete outputs all rows on each trigger.
	OutputModeComplete
	// OutputModeUpdate outputs only changed rows.
	OutputModeUpdate
)

// TriggerConfig configures when to emit results.
type TriggerConfig struct {
	Type     TriggerType   `json:"type"`
	Interval time.Duration `json:"interval,omitempty"`
}

// TriggerType identifies trigger types.
type TriggerType int

const (
	// TriggerTypeProcessingTime triggers based on processing time.
	TriggerTypeProcessingTime TriggerType = iota
	// TriggerTypeEventTime triggers based on watermarks.
	TriggerTypeEventTime
	// TriggerTypeOnce triggers once when data is available.
	TriggerTypeOnce
	// TriggerTypeContinuous triggers continuously.
	TriggerTypeContinuous
)

// SinkConfig configures query output sink.
type SinkConfig struct {
	Type   string                 `json:"type"`
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

// CQStats contains query statistics.
type CQStats struct {
	InputRecords     int64         `json:"input_records"`
	OutputRecords    int64         `json:"output_records"`
	ProcessingTime   time.Duration `json:"processing_time"`
	Watermark        int64         `json:"watermark"`
	LastTriggerTime  time.Time     `json:"last_trigger_time"`
	CheckpointCount  int64         `json:"checkpoint_count"`
	FailureCount     int64         `json:"failure_count"`
	RecoveryCount    int64         `json:"recovery_count"`
}

// QueryPlan represents an optimized query execution plan.
type QueryPlan struct {
	Root       *PlanNode          `json:"root"`
	Sources    []string           `json:"sources"`
	Sinks      []string           `json:"sinks"`
	Partitions int                `json:"partitions"`
	Cost       float64            `json:"cost"`
}

// PlanNode represents a node in the query plan.
type PlanNode struct {
	ID         string                 `json:"id"`
	Type       PlanNodeType           `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Children   []*PlanNode            `json:"children,omitempty"`
}

// PlanNodeType identifies plan node types.
type PlanNodeType int

const (
	PlanNodeScan PlanNodeType = iota
	PlanNodeFilter
	PlanNodeProject
	PlanNodeAggregate
	PlanNodeJoin
	PlanNodeWindow
	PlanNodeSink
	PlanNodeExchange
)

func (t PlanNodeType) String() string {
	switch t {
	case PlanNodeScan:
		return "Scan"
	case PlanNodeFilter:
		return "Filter"
	case PlanNodeProject:
		return "Project"
	case PlanNodeAggregate:
		return "Aggregate"
	case PlanNodeJoin:
		return "Join"
	case PlanNodeWindow:
		return "Window"
	case PlanNodeSink:
		return "Sink"
	case PlanNodeExchange:
		return "Exchange"
	default:
		return "Unknown"
	}
}

// CQOperator represents a query operator.
type CQOperator interface {
	Open(ctx context.Context) error
	Process(record *Record) ([]*Record, error)
	Close() error
	GetState() ([]byte, error)
	RestoreState([]byte) error
}

// Record represents a data record flowing through operators.
type Record struct {
	Key       string                 `json:"key"`
	Value     map[string]interface{} `json:"value"`
	Timestamp int64                  `json:"timestamp"`
	Watermark int64                  `json:"watermark"`
	Metadata  map[string]string      `json:"metadata"`
}

// MaterializedView represents a materialized continuous query result.
type MaterializedView struct {
	Name        string             `json:"name"`
	Query       string             `json:"query"`
	Schema      []CQColumnDef      `json:"schema"`
	RefreshMode RefreshMode        `json:"refresh_mode"`
	Data        *ViewData          `json:"data"`
	QueryID     string             `json:"query_id"`
	Created     time.Time          `json:"created"`
	Updated     time.Time          `json:"updated"`
	mu          sync.RWMutex
}

// CQColumnDef defines a column in the view schema.
type CQColumnDef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// RefreshMode specifies how the view is refreshed.
type RefreshMode int

const (
	// RefreshModeIncremental updates incrementally.
	RefreshModeIncremental RefreshMode = iota
	// RefreshModeFull rebuilds the entire view.
	RefreshModeFull
)

// ViewData holds materialized view data.
type ViewData struct {
	Rows      []map[string]interface{} `json:"rows"`
	RowCount  int                      `json:"row_count"`
	UpdatedAt time.Time                `json:"updated_at"`
}

// QueryStateManager manages state for continuous queries.
type QueryStateManager struct {
	backend StateBackendType
	states  map[string][]byte
	mu      sync.RWMutex
}

// WatermarkTracker tracks watermarks for event-time processing.
type WatermarkTracker struct {
	watermarks map[string]int64
	mu         sync.RWMutex
}

// CQQueryOptimizer optimizes continuous query plans.
type CQQueryOptimizer struct {
	rules []OptimizationRule
}

// OptimizationRule represents a query optimization rule.
type OptimizationRule interface {
	Name() string
	Apply(*QueryPlan) (*QueryPlan, bool)
}

// CQMetrics tracks continuous query metrics.
type CQMetrics struct {
	QueriesCreated    int64
	QueriesRunning    int64
	QueriesFailed     int64
	RecordsProcessed  int64
	RecordsEmitted    int64
	CheckpointsTotal  int64
	CheckpointsFailed int64
	ProcessingLatency time.Duration
	mu                sync.RWMutex
}

// NewContinuousQueryEngine creates a new continuous query engine.
func NewContinuousQueryEngine(db *DB, hub *StreamHub, config ContinuousQueryConfig) *ContinuousQueryEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &ContinuousQueryEngine{
		db:      db,
		hub:     hub,
		config:  config,
		queries: make(map[string]*ContinuousQueryV2),
		views:   make(map[string]*MaterializedView),
		stateManager: &QueryStateManager{
			backend: config.StateBackend,
			states:  make(map[string][]byte),
		},
		watermarkTracker: &WatermarkTracker{
			watermarks: make(map[string]int64),
		},
		optimizer: newQueryOptimizer(),
		metrics:   &CQMetrics{},
		ctx:       ctx,
		cancel:    cancel,
	}

	return engine
}

// newQueryOptimizer creates a query optimizer with default rules.
func newQueryOptimizer() *CQQueryOptimizer {
	return &CQQueryOptimizer{
		rules: []OptimizationRule{
			&PredicatePushdownRule{},
			&ProjectionPushdownRule{},
			&JoinReorderRule{},
			&AggregationOptimizationRule{},
		},
	}
}

// Start starts the continuous query engine.
func (e *ContinuousQueryEngine) Start() error {
	// Start checkpoint loop
	e.wg.Add(1)
	go e.checkpointLoop()

	// Start watermark advancement
	if e.config.EnableWatermarks {
		e.wg.Add(1)
		go e.watermarkLoop()
	}

	// Start metrics collection
	if e.config.MetricsEnabled {
		e.wg.Add(1)
		go e.metricsLoop()
	}

	return nil
}

// Stop stops the continuous query engine.
func (e *ContinuousQueryEngine) Stop() error {
	e.cancel()

	// Stop all queries
	e.queryMu.Lock()
	for _, q := range e.queries {
		q.cancel()
	}
	e.queryMu.Unlock()

	e.wg.Wait()
	return nil
}

func (e *ContinuousQueryEngine) checkpointLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.triggerCheckpoint()
		}
	}
}

func (e *ContinuousQueryEngine) triggerCheckpoint() {
	e.queryMu.RLock()
	queries := make([]*ContinuousQueryV2, 0, len(e.queries))
	for _, q := range e.queries {
		if q.State == CQStateRunning {
			queries = append(queries, q)
		}
	}
	e.queryMu.RUnlock()

	for _, q := range queries {
		if err := e.checkpointQuery(q); err != nil {
			atomic.AddInt64(&e.metrics.CheckpointsFailed, 1)
		} else {
			atomic.AddInt64(&e.metrics.CheckpointsTotal, 1)
		}
	}
}

func (e *ContinuousQueryEngine) checkpointQuery(q *ContinuousQueryV2) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.State != CQStateRunning {
		return nil
	}

	q.State = CQStateCheckpointing
	defer func() { q.State = CQStateRunning }()

	// Collect state from all operators
	var states [][]byte
	for _, op := range q.operators {
		state, err := op.GetState()
		if err != nil {
			return err
		}
		states = append(states, state)
	}

	// Store checkpoint
	checkpoint := &QueryCheckpoint{
		QueryID:     q.ID,
		Timestamp:   time.Now().UnixNano(),
		Watermark:   q.Stats.Watermark,
		States:      states,
		InputOffset: q.Stats.InputRecords,
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	e.stateManager.mu.Lock()
	e.stateManager.states[q.ID] = data
	e.stateManager.mu.Unlock()

	q.LastCheckpoint = time.Now()
	atomic.AddInt64(&q.Stats.CheckpointCount, 1)

	return nil
}

// QueryCheckpoint represents a query checkpoint.
type QueryCheckpoint struct {
	QueryID     string   `json:"query_id"`
	Timestamp   int64    `json:"timestamp"`
	Watermark   int64    `json:"watermark"`
	States      [][]byte `json:"states"`
	InputOffset int64    `json:"input_offset"`
}

func (e *ContinuousQueryEngine) watermarkLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.advanceWatermarks()
		}
	}
}

func (e *ContinuousQueryEngine) advanceWatermarks() {
	now := time.Now().UnixNano()
	watermark := now - int64(e.config.WatermarkDelay)

	e.watermarkTracker.mu.Lock()
	for source := range e.watermarkTracker.watermarks {
		e.watermarkTracker.watermarks[source] = watermark
	}
	e.watermarkTracker.mu.Unlock()
}

func (e *ContinuousQueryEngine) metricsLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.collectMetrics()
		}
	}
}

func (e *ContinuousQueryEngine) collectMetrics() {
	e.queryMu.RLock()
	running := 0
	for _, q := range e.queries {
		if q.State == CQStateRunning {
			running++
		}
	}
	e.queryMu.RUnlock()

	e.metrics.mu.Lock()
	e.metrics.QueriesRunning = int64(running)
	e.metrics.mu.Unlock()
}

// CreateQuery creates a new continuous query.
func (e *ContinuousQueryEngine) CreateQuery(name, sql string, config CQConfig) (*ContinuousQueryV2, error) {
	e.queryMu.Lock()
	if len(e.queries) >= e.config.MaxQueries {
		e.queryMu.Unlock()
		return nil, errors.New("max queries reached")
	}
	e.queryMu.Unlock()

	// Parse SQL
	plan, err := e.parseAndPlan(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Optimize plan
	if e.config.EnableQueryOptimizer {
		plan = e.optimizer.Optimize(plan)
	}

	ctx, cancel := context.WithCancel(e.ctx)

	query := &ContinuousQueryV2{
		ID:      fmt.Sprintf("cq-%d", time.Now().UnixNano()),
		Name:    name,
		SQL:     sql,
		Plan:    plan,
		State:   CQStateCreated,
		Config:  config,
		Created: time.Now(),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Build operators
	query.operators, err = e.buildOperators(plan)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("build operators: %w", err)
	}

	e.queryMu.Lock()
	e.queries[query.ID] = query
	e.queryMu.Unlock()

	atomic.AddInt64(&e.metrics.QueriesCreated, 1)

	return query, nil
}

// StartQuery starts a continuous query.
func (e *ContinuousQueryEngine) StartQuery(queryID string) error {
	e.queryMu.RLock()
	query, ok := e.queries[queryID]
	e.queryMu.RUnlock()

	if !ok {
		return errors.New("query not found")
	}

	query.mu.Lock()
	if query.State == CQStateRunning {
		query.mu.Unlock()
		return nil
	}
	query.State = CQStateRunning
	query.mu.Unlock()

	// Open operators
	for _, op := range query.operators {
		if err := op.Open(query.ctx); err != nil {
			return err
		}
	}

	// Start query execution
	e.wg.Add(1)
	go e.runQuery(query)

	return nil
}

// StopQuery stops a continuous query.
func (e *ContinuousQueryEngine) StopQuery(queryID string) error {
	e.queryMu.RLock()
	query, ok := e.queries[queryID]
	e.queryMu.RUnlock()

	if !ok {
		return errors.New("query not found")
	}

	query.cancel()
	query.mu.Lock()
	query.State = CQStateStopped
	query.mu.Unlock()

	// Close operators
	for _, op := range query.operators {
		op.Close()
	}

	return nil
}

// DeleteQuery deletes a continuous query.
func (e *ContinuousQueryEngine) DeleteQuery(queryID string) error {
	if err := e.StopQuery(queryID); err != nil && err.Error() != "query not found" {
		return err
	}

	e.queryMu.Lock()
	delete(e.queries, queryID)
	e.queryMu.Unlock()

	e.stateManager.mu.Lock()
	delete(e.stateManager.states, queryID)
	e.stateManager.mu.Unlock()

	return nil
}

func (e *ContinuousQueryEngine) runQuery(query *ContinuousQueryV2) {
	defer e.wg.Done()

	// Subscribe to source
	sources := query.Plan.Sources
	if len(sources) == 0 {
		return
	}

	if e.hub == nil {
		query.mu.Lock()
		query.State = CQStateFailed
		query.mu.Unlock()
		return
	}

	sub := e.hub.Subscribe(sources[0], nil)
	if sub == nil {
		query.mu.Lock()
		query.State = CQStateFailed
		query.mu.Unlock()
		return
	}
	defer sub.Close()

	// Trigger ticker
	var triggerTicker *time.Ticker
	triggerInterval := time.Second
	if query.Config.Trigger.Interval > 0 {
		triggerInterval = query.Config.Trigger.Interval
	}
	triggerTicker = time.NewTicker(triggerInterval)
	defer triggerTicker.Stop()

	// Pending results buffer
	var pendingResults []*Record
	var pendingMu sync.Mutex

	for {
		select {
		case <-query.ctx.Done():
			return

		case point, ok := <-sub.C():
			if !ok {
				return
			}

			// Convert point to record
			record := &Record{
				Key:       point.Metric,
				Value:     make(map[string]interface{}),
				Timestamp: point.Timestamp,
			}
			record.Value["value"] = point.Value
			record.Value["metric"] = point.Metric
			for k, v := range point.Tags {
				record.Value[k] = v
			}

			// Process through operators
			results := []*Record{record}
			for _, op := range query.operators {
				var newResults []*Record
				for _, r := range results {
					output, err := op.Process(r)
					if err != nil {
						atomic.AddInt64(&query.Stats.FailureCount, 1)
						continue
					}
					newResults = append(newResults, output...)
				}
				results = newResults
			}

			// Buffer results
			pendingMu.Lock()
			pendingResults = append(pendingResults, results...)
			pendingMu.Unlock()

			atomic.AddInt64(&query.Stats.InputRecords, 1)
			atomic.AddInt64(&e.metrics.RecordsProcessed, 1)

		case <-triggerTicker.C:
			// Emit buffered results
			pendingMu.Lock()
			toEmit := pendingResults
			pendingResults = nil
			pendingMu.Unlock()

			for _, result := range toEmit {
				e.emitResult(query, result)
			}

			query.mu.Lock()
			query.Stats.LastTriggerTime = time.Now()
			query.mu.Unlock()
		}
	}
}

func (e *ContinuousQueryEngine) emitResult(query *ContinuousQueryV2, record *Record) {
	// Emit to configured sinks
	for _, sink := range query.Config.Sinks {
		switch sink.Type {
		case "stream":
			e.emitToStream(sink.Name, record)
		case "view":
			e.updateView(sink.Name, record)
		case "chronicle":
			e.emitToChronicle(record)
		}
	}

	atomic.AddInt64(&query.Stats.OutputRecords, 1)
	atomic.AddInt64(&e.metrics.RecordsEmitted, 1)
}

func (e *ContinuousQueryEngine) emitToStream(streamName string, record *Record) {
	if e.hub == nil {
		return
	}

	point := Point{
		Metric:    streamName,
		Timestamp: record.Timestamp,
		Tags:      make(map[string]string),
	}

	if v, ok := record.Value["value"].(float64); ok {
		point.Value = v
	}

	for k, v := range record.Value {
		if s, ok := v.(string); ok {
			point.Tags[k] = s
		}
	}

	point.Metric = streamName
	e.hub.Publish(point)
}

func (e *ContinuousQueryEngine) updateView(viewName string, record *Record) {
	e.viewMu.RLock()
	view, ok := e.views[viewName]
	e.viewMu.RUnlock()

	if !ok {
		return
	}

	view.mu.Lock()
	defer view.mu.Unlock()

	if view.Data == nil {
		view.Data = &ViewData{
			Rows: make([]map[string]interface{}, 0),
		}
	}

	// For incremental mode, append
	if view.RefreshMode == RefreshModeIncremental {
		view.Data.Rows = append(view.Data.Rows, record.Value)
		view.Data.RowCount = len(view.Data.Rows)
	}

	view.Data.UpdatedAt = time.Now()
	view.Updated = time.Now()
}

func (e *ContinuousQueryEngine) emitToChronicle(record *Record) {
	if e.db == nil {
		return
	}

	point := Point{
		Metric:    record.Key,
		Timestamp: record.Timestamp,
		Tags:      make(map[string]string),
	}

	if v, ok := record.Value["value"].(float64); ok {
		point.Value = v
	}

	for k, v := range record.Value {
		if s, ok := v.(string); ok {
			point.Tags[k] = s
		}
	}

	e.db.Write(point)
}

// parseAndPlan parses SQL and creates an execution plan.
func (e *ContinuousQueryEngine) parseAndPlan(sql string) (*QueryPlan, error) {
	upper := strings.ToUpper(sql)

	plan := &QueryPlan{
		Partitions: e.config.ParallelismHint,
	}

	// Extract sources
	if fromIdx := strings.Index(upper, "FROM"); fromIdx != -1 {
		remaining := sql[fromIdx+4:]
		parts := strings.Fields(remaining)
		if len(parts) > 0 {
			source := strings.Trim(parts[0], " \t\n,;")
			plan.Sources = []string{source}
		}
	}

	// Build plan tree
	plan.Root = &PlanNode{
		ID:   "sink-0",
		Type: PlanNodeSink,
		Children: []*PlanNode{
			e.buildPlanFromSQL(sql),
		},
	}

	// Calculate cost estimate
	plan.Cost = e.estimateCost(plan)

	return plan, nil
}

func (e *ContinuousQueryEngine) buildPlanFromSQL(sql string) *PlanNode {
	upper := strings.ToUpper(sql)
	var nodes []*PlanNode

	// Check for aggregations
	hasAgg := strings.Contains(upper, "SUM(") || strings.Contains(upper, "AVG(") ||
		strings.Contains(upper, "COUNT(") || strings.Contains(upper, "MIN(") ||
		strings.Contains(upper, "MAX(")

	// Build from bottom up
	scanNode := &PlanNode{
		ID:         "scan-0",
		Type:       PlanNodeScan,
		Properties: make(map[string]interface{}),
	}
	nodes = append(nodes, scanNode)

	// Filter (WHERE)
	if strings.Contains(upper, "WHERE") {
		filterNode := &PlanNode{
			ID:         "filter-0",
			Type:       PlanNodeFilter,
			Properties: make(map[string]interface{}),
			Children:   []*PlanNode{nodes[len(nodes)-1]},
		}
		nodes = append(nodes, filterNode)
	}

	// Window
	if strings.Contains(upper, "WINDOW") || strings.Contains(upper, "GROUP BY") {
		windowNode := &PlanNode{
			ID:         "window-0",
			Type:       PlanNodeWindow,
			Properties: make(map[string]interface{}),
			Children:   []*PlanNode{nodes[len(nodes)-1]},
		}
		nodes = append(nodes, windowNode)
	}

	// Aggregation
	if hasAgg {
		aggNode := &PlanNode{
			ID:         "agg-0",
			Type:       PlanNodeAggregate,
			Properties: make(map[string]interface{}),
			Children:   []*PlanNode{nodes[len(nodes)-1]},
		}
		nodes = append(nodes, aggNode)
	}

	// Projection
	projectNode := &PlanNode{
		ID:         "project-0",
		Type:       PlanNodeProject,
		Properties: make(map[string]interface{}),
		Children:   []*PlanNode{nodes[len(nodes)-1]},
	}
	nodes = append(nodes, projectNode)

	return nodes[len(nodes)-1]
}

func (e *ContinuousQueryEngine) estimateCost(plan *QueryPlan) float64 {
	var cost float64

	var walkPlan func(*PlanNode)
	walkPlan = func(node *PlanNode) {
		if node == nil {
			return
		}

		switch node.Type {
		case PlanNodeScan:
			cost += 1.0
		case PlanNodeFilter:
			cost += 0.5
		case PlanNodeProject:
			cost += 0.3
		case PlanNodeAggregate:
			cost += 2.0
		case PlanNodeJoin:
			cost += 5.0
		case PlanNodeWindow:
			cost += 1.5
		}

		for _, child := range node.Children {
			walkPlan(child)
		}
	}

	walkPlan(plan.Root)
	return cost
}

// buildOperators creates operators from a query plan.
func (e *ContinuousQueryEngine) buildOperators(plan *QueryPlan) ([]CQOperator, error) {
	var operators []CQOperator

	var buildFromNode func(*PlanNode) error
	buildFromNode = func(node *PlanNode) error {
		if node == nil {
			return nil
		}

		// Process children first
		for _, child := range node.Children {
			if err := buildFromNode(child); err != nil {
				return err
			}
		}

		// Create operator for this node
		var op CQOperator
		switch node.Type {
		case PlanNodeScan:
			op = &ScanOperator{id: node.ID}
		case PlanNodeFilter:
			op = &FilterOperator{id: node.ID}
		case PlanNodeProject:
			op = &ProjectOperator{id: node.ID}
		case PlanNodeAggregate:
			op = &AggregateOperator{id: node.ID}
		case PlanNodeWindow:
			op = &WindowOperator{id: node.ID}
		case PlanNodeSink:
			op = &SinkOperator{id: node.ID}
		default:
			op = &PassthroughOperator{id: node.ID}
		}

		operators = append(operators, op)
		return nil
	}

	if err := buildFromNode(plan.Root); err != nil {
		return nil, err
	}

	return operators, nil
}

// Optimize optimizes a query plan.
func (o *CQQueryOptimizer) Optimize(plan *QueryPlan) *QueryPlan {
	optimized := plan
	for _, rule := range o.rules {
		newPlan, changed := rule.Apply(optimized)
		if changed {
			optimized = newPlan
		}
	}
	return optimized
}

// ========== Optimization Rules ==========

// PredicatePushdownRule pushes filters closer to sources.
type PredicatePushdownRule struct{}

func (r *PredicatePushdownRule) Name() string { return "PredicatePushdown" }

func (r *PredicatePushdownRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	// Simple implementation - in production, would analyze predicates
	return plan, false
}

// ProjectionPushdownRule pushes projections closer to sources.
type ProjectionPushdownRule struct{}

func (r *ProjectionPushdownRule) Name() string { return "ProjectionPushdown" }

func (r *ProjectionPushdownRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	return plan, false
}

// JoinReorderRule reorders joins for efficiency.
type JoinReorderRule struct{}

func (r *JoinReorderRule) Name() string { return "JoinReorder" }

func (r *JoinReorderRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	return plan, false
}

// AggregationOptimizationRule optimizes aggregations.
type AggregationOptimizationRule struct{}

func (r *AggregationOptimizationRule) Name() string { return "AggregationOptimization" }

func (r *AggregationOptimizationRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	return plan, false
}

// ========== Operator Implementations ==========

// ScanOperator reads from a source.
type ScanOperator struct {
	id    string
	state []byte
}

func (o *ScanOperator) Open(ctx context.Context) error { return nil }
func (o *ScanOperator) Process(record *Record) ([]*Record, error) {
	return []*Record{record}, nil
}
func (o *ScanOperator) Close() error            { return nil }
func (o *ScanOperator) GetState() ([]byte, error) { return o.state, nil }
func (o *ScanOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}

// FilterOperator filters records.
type FilterOperator struct {
	id        string
	predicate func(*Record) bool
	state     []byte
}

func (o *FilterOperator) Open(ctx context.Context) error { return nil }
func (o *FilterOperator) Process(record *Record) ([]*Record, error) {
	if o.predicate == nil || o.predicate(record) {
		return []*Record{record}, nil
	}
	return nil, nil
}
func (o *FilterOperator) Close() error            { return nil }
func (o *FilterOperator) GetState() ([]byte, error) { return o.state, nil }
func (o *FilterOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}

// ProjectOperator projects fields.
type ProjectOperator struct {
	id     string
	fields []string
	state  []byte
}

func (o *ProjectOperator) Open(ctx context.Context) error { return nil }
func (o *ProjectOperator) Process(record *Record) ([]*Record, error) {
	if len(o.fields) == 0 {
		return []*Record{record}, nil
	}

	projected := &Record{
		Key:       record.Key,
		Timestamp: record.Timestamp,
		Value:     make(map[string]interface{}),
	}

	for _, f := range o.fields {
		if v, ok := record.Value[f]; ok {
			projected.Value[f] = v
		}
	}

	return []*Record{projected}, nil
}
func (o *ProjectOperator) Close() error            { return nil }
func (o *ProjectOperator) GetState() ([]byte, error) { return o.state, nil }
func (o *ProjectOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}

// AggregateOperator performs aggregations.
type AggregateOperator struct {
	id          string
	aggregates  map[string]float64
	counts      map[string]int64
	mu          sync.Mutex
	state       []byte
}

func (o *AggregateOperator) Open(ctx context.Context) error {
	o.aggregates = make(map[string]float64)
	o.counts = make(map[string]int64)
	return nil
}

func (o *AggregateOperator) Process(record *Record) ([]*Record, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := record.Key
	if v, ok := record.Value["value"].(float64); ok {
		o.aggregates[key+"_sum"] += v
		o.counts[key+"_count"]++

		if _, exists := o.aggregates[key+"_min"]; !exists || v < o.aggregates[key+"_min"] {
			o.aggregates[key+"_min"] = v
		}
		if v > o.aggregates[key+"_max"] {
			o.aggregates[key+"_max"] = v
		}
	}

	// Return aggregated record
	result := &Record{
		Key:       record.Key,
		Timestamp: record.Timestamp,
		Value:     make(map[string]interface{}),
	}

	for k, v := range o.aggregates {
		result.Value[k] = v
	}
	for k, v := range o.counts {
		result.Value[k] = v
	}

	return []*Record{result}, nil
}

func (o *AggregateOperator) Close() error { return nil }

func (o *AggregateOperator) GetState() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	state := map[string]interface{}{
		"aggregates": o.aggregates,
		"counts":     o.counts,
	}
	return json.Marshal(state)
}

func (o *AggregateOperator) RestoreState(state []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	var data map[string]interface{}
	if err := json.Unmarshal(state, &data); err != nil {
		return err
	}

	if agg, ok := data["aggregates"].(map[string]interface{}); ok {
		o.aggregates = make(map[string]float64)
		for k, v := range agg {
			if f, ok := v.(float64); ok {
				o.aggregates[k] = f
			}
		}
	}

	return nil
}

// WindowOperator manages windowed state.
type WindowOperator struct {
	id       string
	windows  map[string]*WindowBuffer
	mu       sync.Mutex
	state    []byte
}

// WindowBuffer holds window data.
type WindowBuffer struct {
	Start   int64
	End     int64
	Records []*Record
}

func (o *WindowOperator) Open(ctx context.Context) error {
	o.windows = make(map[string]*WindowBuffer)
	return nil
}

func (o *WindowOperator) Process(record *Record) ([]*Record, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Calculate window key
	windowSize := int64(time.Minute)
	windowStart := (record.Timestamp / windowSize) * windowSize
	windowKey := fmt.Sprintf("%s:%d", record.Key, windowStart)

	// Get or create window
	window, ok := o.windows[windowKey]
	if !ok {
		window = &WindowBuffer{
			Start:   windowStart,
			End:     windowStart + windowSize,
			Records: make([]*Record, 0),
		}
		o.windows[windowKey] = window
	}

	// Add record to window
	window.Records = append(window.Records, record)

	// Return the record (pass through)
	return []*Record{record}, nil
}

func (o *WindowOperator) Close() error { return nil }

func (o *WindowOperator) GetState() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return json.Marshal(o.windows)
}

func (o *WindowOperator) RestoreState(state []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return json.Unmarshal(state, &o.windows)
}

// SinkOperator outputs records.
type SinkOperator struct {
	id    string
	state []byte
}

func (o *SinkOperator) Open(ctx context.Context) error { return nil }
func (o *SinkOperator) Process(record *Record) ([]*Record, error) {
	return []*Record{record}, nil
}
func (o *SinkOperator) Close() error            { return nil }
func (o *SinkOperator) GetState() ([]byte, error) { return o.state, nil }
func (o *SinkOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}

// PassthroughOperator passes records unchanged.
type PassthroughOperator struct {
	id    string
	state []byte
}

func (o *PassthroughOperator) Open(ctx context.Context) error { return nil }
func (o *PassthroughOperator) Process(record *Record) ([]*Record, error) {
	return []*Record{record}, nil
}
func (o *PassthroughOperator) Close() error            { return nil }
func (o *PassthroughOperator) GetState() ([]byte, error) { return o.state, nil }
func (o *PassthroughOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}

// ========== Materialized Views ==========

// CreateMaterializedView creates a materialized view.
func (e *ContinuousQueryEngine) CreateMaterializedView(name, sql string, refreshMode RefreshMode) (*MaterializedView, error) {
	// Create underlying query
	config := CQConfig{
		Parallelism: e.config.ParallelismHint,
		OutputMode:  OutputModeUpdate,
		Sinks: []SinkConfig{
			{Type: "view", Name: name},
		},
	}

	query, err := e.CreateQuery(name+"_query", sql, config)
	if err != nil {
		return nil, err
	}

	view := &MaterializedView{
		Name:        name,
		Query:       sql,
		RefreshMode: refreshMode,
		QueryID:     query.ID,
		Created:     time.Now(),
		Data: &ViewData{
			Rows: make([]map[string]interface{}, 0),
		},
	}

	e.viewMu.Lock()
	e.views[name] = view
	e.viewMu.Unlock()

	// Start the query
	if err := e.StartQuery(query.ID); err != nil {
		return nil, err
	}

	return view, nil
}

// GetView returns a materialized view by name.
func (e *ContinuousQueryEngine) GetView(name string) (*MaterializedView, bool) {
	e.viewMu.RLock()
	defer e.viewMu.RUnlock()
	v, ok := e.views[name]
	return v, ok
}

// QueryView queries a materialized view.
func (e *ContinuousQueryEngine) QueryView(name string, filter func(map[string]interface{}) bool) ([]map[string]interface{}, error) {
	e.viewMu.RLock()
	view, ok := e.views[name]
	e.viewMu.RUnlock()

	if !ok {
		return nil, errors.New("view not found")
	}

	view.mu.RLock()
	defer view.mu.RUnlock()

	if view.Data == nil {
		return nil, nil
	}

	if filter == nil {
		return view.Data.Rows, nil
	}

	var result []map[string]interface{}
	for _, row := range view.Data.Rows {
		if filter(row) {
			result = append(result, row)
		}
	}

	return result, nil
}

// DropView drops a materialized view.
func (e *ContinuousQueryEngine) DropView(name string) error {
	e.viewMu.Lock()
	view, ok := e.views[name]
	if !ok {
		e.viewMu.Unlock()
		return errors.New("view not found")
	}
	delete(e.views, name)
	e.viewMu.Unlock()

	// Stop underlying query
	return e.DeleteQuery(view.QueryID)
}

// ========== Query Management ==========

// GetQuery returns a query by ID.
func (e *ContinuousQueryEngine) GetQuery(queryID string) (*ContinuousQueryV2, bool) {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()
	q, ok := e.queries[queryID]
	return q, ok
}

// ListQueries returns all queries.
func (e *ContinuousQueryEngine) ListQueries() []*ContinuousQueryV2 {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()

	queries := make([]*ContinuousQueryV2, 0, len(e.queries))
	for _, q := range e.queries {
		queries = append(queries, q)
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Created.Before(queries[j].Created)
	})

	return queries
}

// GetStats returns engine statistics.
func (e *ContinuousQueryEngine) GetStats() CQEngineStats {
	e.metrics.mu.RLock()
	defer e.metrics.mu.RUnlock()

	e.queryMu.RLock()
	queryCount := len(e.queries)
	e.queryMu.RUnlock()

	e.viewMu.RLock()
	viewCount := len(e.views)
	e.viewMu.RUnlock()

	return CQEngineStats{
		QueriesTotal:      queryCount,
		QueriesCreated:    e.metrics.QueriesCreated,
		QueriesRunning:    e.metrics.QueriesRunning,
		QueriesFailed:     e.metrics.QueriesFailed,
		ViewsTotal:        viewCount,
		RecordsProcessed:  e.metrics.RecordsProcessed,
		RecordsEmitted:    e.metrics.RecordsEmitted,
		CheckpointsTotal:  e.metrics.CheckpointsTotal,
		CheckpointsFailed: e.metrics.CheckpointsFailed,
	}
}

// CQEngineStats contains engine statistics.
type CQEngineStats struct {
	QueriesTotal      int   `json:"queries_total"`
	QueriesCreated    int64 `json:"queries_created"`
	QueriesRunning    int64 `json:"queries_running"`
	QueriesFailed     int64 `json:"queries_failed"`
	ViewsTotal        int   `json:"views_total"`
	RecordsProcessed  int64 `json:"records_processed"`
	RecordsEmitted    int64 `json:"records_emitted"`
	CheckpointsTotal  int64 `json:"checkpoints_total"`
	CheckpointsFailed int64 `json:"checkpoints_failed"`
}

// ========== Stream-Stream Joins ==========

// StreamJoinConfig configures a stream-stream join.
type StreamJoinConfig struct {
	LeftStream  string
	RightStream string
	JoinType    StreamJoinType
	JoinKey     string
	Window      time.Duration
}

// StreamJoinType identifies join types.
type StreamJoinType int

const (
	StreamJoinInner StreamJoinType = iota
	StreamJoinLeft
	StreamJoinOuter
)

// CreateStreamJoin creates a stream-stream join.
func (e *ContinuousQueryEngine) CreateStreamJoin(name string, config StreamJoinConfig) (*ContinuousQueryV2, error) {
	sql := fmt.Sprintf(
		"SELECT * FROM %s JOIN %s ON %s.%s = %s.%s WINDOW TUMBLING SIZE %d SECONDS",
		config.LeftStream, config.RightStream,
		config.LeftStream, config.JoinKey,
		config.RightStream, config.JoinKey,
		int(config.Window.Seconds()),
	)

	return e.CreateQuery(name, sql, CQConfig{
		Parallelism: e.config.ParallelismHint,
		OutputMode:  OutputModeUpdate,
	})
}

// ========== Partitioned Queries ==========

// PartitionedQuery represents a query partitioned across multiple workers.
type PartitionedQuery struct {
	*ContinuousQueryV2
	Partitions []*QueryPartition
}

// QueryPartition represents a query partition.
type QueryPartition struct {
	ID        int
	KeyRange  KeyRange
	Operators []CQOperator
	State     []byte
}

// KeyRange represents a key range for partitioning.
type KeyRange struct {
	Start uint64
	End   uint64
}

// CreatePartitionedQuery creates a partitioned query.
func (e *ContinuousQueryEngine) CreatePartitionedQuery(name, sql string, numPartitions int) (*PartitionedQuery, error) {
	query, err := e.CreateQuery(name, sql, CQConfig{
		Parallelism: numPartitions,
	})
	if err != nil {
		return nil, err
	}

	partitions := make([]*QueryPartition, numPartitions)
	rangeSize := uint64(1<<64-1) / uint64(numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitions[i] = &QueryPartition{
			ID: i,
			KeyRange: KeyRange{
				Start: uint64(i) * rangeSize,
				End:   uint64(i+1) * rangeSize,
			},
		}
	}

	return &PartitionedQuery{
		ContinuousQueryV2: query,
		Partitions:        partitions,
	}, nil
}

// GetPartitionForKey returns the partition for a given key.
func (pq *PartitionedQuery) GetPartitionForKey(key string) *QueryPartition {
	h := fnv.New64a()
	h.Write([]byte(key))
	hash := h.Sum64()

	for _, p := range pq.Partitions {
		if hash >= p.KeyRange.Start && hash < p.KeyRange.End {
			return p
		}
	}

	return pq.Partitions[0]
}
