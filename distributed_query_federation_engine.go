package chronicle

// distributed_query_federation_engine.go contains the partial result assembler,
// federated query plan, query federation engine, and HTTP routes.
// See distributed_query_federation.go for configuration, predicate push-down,
// and data locality routing.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Partial Result Assembler
// ---------------------------------------------------------------------------

// FederatedPartialResult is a partial result from a single federation node.
type FederatedPartialResult struct {
	NodeID      string        `json:"node_id"`
	Points      []Point       `json:"points"`
	Error       string        `json:"error,omitempty"`
	Duration    time.Duration `json:"duration"`
	IsPartial   bool          `json:"is_partial"`
	Aggregation *Aggregation  `json:"aggregation,omitempty"`
}

// AssembledResult is the output of merging multiple partial results.
type AssembledResult struct {
	Points         []Point  `json:"points"`
	TotalPoints    int      `json:"total_points"`
	NodesQueried   int      `json:"nodes_queried"`
	NodesResponded int      `json:"nodes_responded"`
	NodesFailed    int      `json:"nodes_failed"`
	FailedNodes    []string `json:"failed_nodes,omitempty"`
	IsPartial      bool     `json:"is_partial"`
}

// PartialResultAssembler merges partial results from multiple federation nodes.
type PartialResultAssembler struct {
	maxSizeMB int
}

// NewPartialResultAssembler creates an assembler with a maximum result size.
func NewPartialResultAssembler(maxSizeMB int) *PartialResultAssembler {
	if maxSizeMB <= 0 {
		maxSizeMB = 64
	}
	return &PartialResultAssembler{maxSizeMB: maxSizeMB}
}

// Assemble merges partial results using sort-merge, deduplication, and aggregation merge.
func (a *PartialResultAssembler) Assemble(partials []FederatedPartialResult, agg *Aggregation) AssembledResult {
	result := AssembledResult{
		NodesQueried: len(partials),
	}
	var allPoints []Point
	for _, p := range partials {
		if p.Error != "" {
			result.NodesFailed++
			result.FailedNodes = append(result.FailedNodes, p.NodeID)
			continue
		}
		result.NodesResponded++
		allPoints = append(allPoints, p.Points...)
	}
	if result.NodesFailed > 0 && result.NodesResponded > 0 {
		result.IsPartial = true
	}

	if agg != nil && isDistributiveAgg(agg.Function) {
		allPoints = a.mergeAggregations(allPoints, agg)
	}

	// Sort by timestamp.
	sort.Slice(allPoints, func(i, j int) bool {
		if allPoints[i].Timestamp != allPoints[j].Timestamp {
			return allPoints[i].Timestamp < allPoints[j].Timestamp
		}
		return allPoints[i].Metric < allPoints[j].Metric
	})

	// Deduplicate.
	allPoints = a.deduplicate(allPoints)

	// Enforce size limit (~128 bytes per point estimate).
	maxPoints := a.maxSizeMB * 1024 * 1024 / 128
	if maxPoints > 0 && len(allPoints) > maxPoints {
		allPoints = allPoints[:maxPoints]
		result.IsPartial = true
	}

	result.Points = allPoints
	result.TotalPoints = len(allPoints)
	return result
}

func (a *PartialResultAssembler) deduplicate(pts []Point) []Point {
	if len(pts) == 0 {
		return pts
	}
	out := make([]Point, 0, len(pts))
	out = append(out, pts[0])
	for i := 1; i < len(pts); i++ {
		prev := out[len(out)-1]
		cur := pts[i]
		if cur.Metric == prev.Metric && cur.Timestamp == prev.Timestamp && tagsEqual(cur.Tags, prev.Tags) {
			continue // duplicate
		}
		out = append(out, cur)
	}
	return out
}

func (a *PartialResultAssembler) mergeAggregations(pts []Point, agg *Aggregation) []Point {
	type bucketKey struct {
		metric string
		ts     int64
		tags   string
	}
	type accum struct {
		sum   float64
		count float64
		min   float64
		max   float64
		tags  map[string]string
	}
	buckets := make(map[bucketKey]*accum)
	for _, p := range pts {
		key := bucketKey{metric: p.Metric, ts: p.Timestamp, tags: sortedTagString(p.Tags)}
		b, ok := buckets[key]
		if !ok {
			b = &accum{min: math.MaxFloat64, max: -math.MaxFloat64, tags: p.Tags}
			buckets[key] = b
		}
		b.sum += p.Value
		b.count++
		if p.Value < b.min {
			b.min = p.Value
		}
		if p.Value > b.max {
			b.max = p.Value
		}
	}

	merged := make([]Point, 0, len(buckets))
	for key, b := range buckets {
		var val float64
		switch agg.Function {
		case AggSum, AggCount:
			val = b.sum
		case AggMin:
			val = b.min
		case AggMax:
			val = b.max
		case AggMean:
			if b.count > 0 {
				val = b.sum / b.count
			}
		default:
			val = b.sum
		}
		merged = append(merged, Point{
			Metric:    key.metric,
			Timestamp: key.ts,
			Tags:      b.tags,
			Value:     val,
		})
	}
	return merged
}

func isDistributiveAgg(f AggFunc) bool {
	switch f {
	case AggSum, AggCount, AggMin, AggMax, AggMean:
		return true
	default:
		return false
	}
}

func tagsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func sortedTagString(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(tags[k])
	}
	return sb.String()
}

// ---------------------------------------------------------------------------
// Federated Query Plan
// ---------------------------------------------------------------------------

// FederatedNodeAssignment maps a node to its portion of the query.
type FederatedNodeAssignment struct {
	NodeID             string              `json:"node_id"`
	Address            string              `json:"address"`
	RemoteQuery        *Query              `json:"remote_query"`
	PushedPredicates   []PushablePredicate `json:"pushed_predicates"`
	ExpectedResultSize int                 `json:"expected_result_size"`
	Score              float64             `json:"score"`
}

// FedAggregationStrategy describes how aggregation is performed.
type FedAggregationStrategy string

const (
	FedAggStrategyNone    FedAggregationStrategy = "none"
	FedAggStrategyPartial FedAggregationStrategy = "partial" // partial at remote, merge locally
	FedAggStrategyFull    FedAggregationStrategy = "full"    // full aggregation locally
)

// FederatedQueryPlan describes how a query will be distributed across nodes.
type FederatedQueryPlan struct {
	QueryID                string                    `json:"query_id"`
	OriginalQuery          *Query                    `json:"original_query"`
	Assignments            []FederatedNodeAssignment `json:"assignments"`
	PredicateAnalysis      PredicateAnalysis         `json:"predicate_analysis"`
	FedAggregationStrategy FedAggregationStrategy    `json:"aggregation_strategy"`
	EstimatedFanOut        int                       `json:"estimated_fan_out"`
	CreatedAt              time.Time                 `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Query Federation Engine
// ---------------------------------------------------------------------------

// QueryFederationEngine orchestrates distributed query execution with
// predicate push-down, locality-aware routing, circuit breakers, and
// partial result assembly.
type QueryFederationEngine struct {
	db     *DB
	config DistributedFederationConfig

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}

	pushDown  *PredicatePushDown
	router    *DataLocalityRouter
	assembler *PartialResultAssembler

	breakers map[string]*FedCircuitBreaker
	breakMu  sync.RWMutex

	executor DistributedQueryExecutor
	querySeq int64

	// stats
	totalQueries   int64
	successQueries int64
	failedQueries  int64
	partialQueries int64
}

// NewQueryFederationEngine creates a new federation engine.
func NewQueryFederationEngine(db *DB, cfg DistributedFederationConfig) *QueryFederationEngine {
	e := &QueryFederationEngine{
		db:        db,
		config:    cfg,
		stopCh:    make(chan struct{}),
		pushDown:  NewPredicatePushDown(cfg.EnablePredicatePushDown),
		router:    NewDataLocalityRouter(),
		assembler: NewPartialResultAssembler(cfg.MaxPartialResultSizeMB),
		breakers:  make(map[string]*FedCircuitBreaker),
	}
	// Default executor: local DB.
	e.executor = func(ctx context.Context, nodeID string, q *Query) (*Result, error) {
		return db.ExecuteContext(ctx, q)
	}
	return e
}

// SetExecutor overrides the remote execution function.
func (e *QueryFederationEngine) SetExecutor(exec DistributedQueryExecutor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executor = exec
}

// Start begins background health checking.
func (e *QueryFederationEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return nil
	}
	e.running = true
	e.stopCh = make(chan struct{})
	go e.healthCheckLoop()
	return nil
}

// Stop halts the engine.
func (e *QueryFederationEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// RegisterNode adds a node to the federation.
func (e *QueryFederationEngine) RegisterNode(node FederationNodeInfo) {
	e.router.RegisterNode(node.NodeID, node.Address, node.Metrics, node.TimeRanges)
	if node.Meta != nil {
		e.router.mu.Lock()
		if n, ok := e.router.nodes[node.NodeID]; ok {
			n.Meta = node.Meta
			n.Load = node.Load
		}
		e.router.mu.Unlock()
	}
	e.breakMu.Lock()
	if _, ok := e.breakers[node.NodeID]; !ok {
		e.breakers[node.NodeID] = NewFedCircuitBreaker(
			e.config.CircuitBreakerThreshold,
			e.config.CircuitBreakerTimeout,
			e.config.CircuitBreakerHalfOpenMax,
		)
	}
	e.breakMu.Unlock()
}

// DeregisterNode removes a node from the federation.
func (e *QueryFederationEngine) DeregisterNode(nodeID string) {
	e.router.RemoveNode(nodeID)
	e.breakMu.Lock()
	delete(e.breakers, nodeID)
	e.breakMu.Unlock()
}

// GetNodeHealth returns health information for all registered nodes.
func (e *QueryFederationEngine) GetNodeHealth() []FederationNodeInfo {
	return e.router.AllNodes()
}

// GetCircuitBreakerState returns the circuit breaker state for a node.
func (e *QueryFederationEngine) GetCircuitBreakerState(nodeID string) (FedCircuitState, bool) {
	e.breakMu.RLock()
	defer e.breakMu.RUnlock()
	cb, ok := e.breakers[nodeID]
	if !ok {
		return FedCircuitClosed, false
	}
	return cb.State(), true
}

func (e *QueryFederationEngine) getBreaker(nodeID string) *FedCircuitBreaker {
	e.breakMu.RLock()
	cb, ok := e.breakers[nodeID]
	e.breakMu.RUnlock()
	if ok {
		return cb
	}
	e.breakMu.Lock()
	defer e.breakMu.Unlock()
	cb, ok = e.breakers[nodeID]
	if ok {
		return cb
	}
	cb = NewFedCircuitBreaker(
		e.config.CircuitBreakerThreshold,
		e.config.CircuitBreakerTimeout,
		e.config.CircuitBreakerHalfOpenMax,
	)
	e.breakers[nodeID] = cb
	return cb
}

// ExplainFederated returns the execution plan for a query without executing it.
func (e *QueryFederationEngine) ExplainFederated(q *Query) *FederatedQueryPlan {
	return e.buildPlan(q)
}

func (e *QueryFederationEngine) buildPlan(q *Query) *FederatedQueryPlan {
	queryID := fmt.Sprintf("fed-%d", atomic.AddInt64(&e.querySeq, 1))
	analysis := e.pushDown.AnalyzePredicates(q)

	plan := &FederatedQueryPlan{
		QueryID:           queryID,
		OriginalQuery:     q,
		PredicateAnalysis: analysis,
		CreatedAt:         time.Now(),
	}

	// Determine aggregation strategy.
	if q.Aggregation != nil && isDistributiveAgg(q.Aggregation.Function) {
		plan.FedAggregationStrategy = FedAggStrategyPartial
	} else if q.Aggregation != nil {
		plan.FedAggregationStrategy = FedAggStrategyFull
	} else {
		plan.FedAggregationStrategy = FedAggStrategyNone
	}

	// Route query to nodes.
	var scores []NodeScore
	if e.config.EnableLocalityRouting {
		scores = e.router.RouteQuery(q)
	} else {
		for _, n := range e.router.AllNodes() {
			if n.Healthy {
				scores = append(scores, NodeScore{Node: n, Score: 1})
			}
		}
	}

	// Limit fan-out.
	if len(scores) > e.config.MaxFanOut {
		scores = scores[:e.config.MaxFanOut]
	}

	for _, ns := range scores {
		remoteQ := e.pushDown.CreateRemoteQuery(q, analysis.Pushable)
		plan.Assignments = append(plan.Assignments, FederatedNodeAssignment{
			NodeID:           ns.Node.NodeID,
			Address:          ns.Node.Address,
			RemoteQuery:      remoteQ,
			PushedPredicates: analysis.Pushable,
			Score:            ns.Score,
		})
	}
	plan.EstimatedFanOut = len(plan.Assignments)
	return plan
}

// ExecuteFederated runs a query across all federated nodes and merges results.
func (e *QueryFederationEngine) ExecuteFederated(ctx context.Context, q *Query) (*AssembledResult, error) {
	atomic.AddInt64(&e.totalQueries, 1)

	plan := e.buildPlan(q)
	if len(plan.Assignments) == 0 {
		// No nodes — execute locally.
		res, err := e.db.ExecuteContext(ctx, q)
		if err != nil {
			atomic.AddInt64(&e.failedQueries, 1)
			return nil, err
		}
		atomic.AddInt64(&e.successQueries, 1)
		return &AssembledResult{
			Points:         res.Points,
			TotalPoints:    len(res.Points),
			NodesQueried:   1,
			NodesResponded: 1,
		}, nil
	}

	// Execute across nodes concurrently.
	timeout := e.config.PartialResultTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < timeout {
			timeout = d
		}
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	partials := make([]FederatedPartialResult, len(plan.Assignments))
	var wg sync.WaitGroup
	for i, assign := range plan.Assignments {
		wg.Add(1)
		go func(idx int, a FederatedNodeAssignment) {
			defer wg.Done()
			partial := FederatedPartialResult{NodeID: a.NodeID}
			start := time.Now()

			cb := e.getBreaker(a.NodeID)
			err := cb.Execute(func() error {
				return e.executeWithRetry(execCtx, a.NodeID, a.RemoteQuery, &partial)
			})
			partial.Duration = time.Since(start)
			if err != nil {
				partial.Error = err.Error()
			}
			partials[idx] = partial
		}(i, assign)
	}
	wg.Wait()

	assembled := e.assembler.Assemble(partials, q.Aggregation)

	// Apply local-only predicates (limit).
	if q.Limit > 0 && len(assembled.Points) > q.Limit {
		assembled.Points = assembled.Points[:q.Limit]
		assembled.TotalPoints = q.Limit
	}

	if assembled.NodesFailed == assembled.NodesQueried {
		atomic.AddInt64(&e.failedQueries, 1)
		return &assembled, fmt.Errorf("all %d federation nodes failed", assembled.NodesQueried)
	}
	if assembled.IsPartial {
		atomic.AddInt64(&e.partialQueries, 1)
	} else {
		atomic.AddInt64(&e.successQueries, 1)
	}
	return &assembled, nil
}

func (e *QueryFederationEngine) executeWithRetry(ctx context.Context, nodeID string, q *Query, partial *FederatedPartialResult) error {
	var lastErr error
	retries := e.config.RetryPolicy.MaxRetries
	backoff := e.config.RetryPolicy.InitialInterval

	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = time.Duration(float64(backoff) * e.config.RetryPolicy.Multiplier)
			if backoff > e.config.RetryPolicy.MaxInterval {
				backoff = e.config.RetryPolicy.MaxInterval
			}
		}

		e.mu.RLock()
		exec := e.executor
		e.mu.RUnlock()

		res, err := exec(ctx, nodeID, q)
		if err != nil {
			lastErr = err
			continue
		}
		if res != nil {
			partial.Points = res.Points
		}
		return nil
	}
	return lastErr
}

func (e *QueryFederationEngine) healthCheckLoop() {
	ticker := time.NewTicker(e.config.HealthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.performHealthChecks()
		}
	}
}

func (e *QueryFederationEngine) performHealthChecks() {
	nodes := e.router.AllNodes()
	for _, n := range nodes {
		e.breakMu.RLock()
		cb, ok := e.breakers[n.NodeID]
		e.breakMu.RUnlock()
		if !ok {
			continue
		}
		state := cb.State()
		healthy := state != FedCircuitOpen
		e.router.UpdateHealth(n.NodeID, healthy, n.Load)
	}
}

// Stats returns current federation engine statistics.
func (e *QueryFederationEngine) Stats() map[string]int64 {
	return map[string]int64{
		"total_queries":   atomic.LoadInt64(&e.totalQueries),
		"success_queries": atomic.LoadInt64(&e.successQueries),
		"failed_queries":  atomic.LoadInt64(&e.failedQueries),
		"partial_queries": atomic.LoadInt64(&e.partialQueries),
	}
}

// ---------------------------------------------------------------------------
// HTTP Routes
// ---------------------------------------------------------------------------

// setupFederationRoutes registers HTTP endpoints for the federation engine.
func setupFederationRoutes(mux *http.ServeMux, engine *QueryFederationEngine, wrap middlewareWrapper) {
	mux.HandleFunc("/api/v1/federation/query", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		var q Query
		if err := json.Unmarshal(body, &q); err != nil {
			http.Error(w, "invalid query JSON", http.StatusBadRequest)
			return
		}
		result, err := engine.ExecuteFederated(r.Context(), &q)
		if err != nil && result == nil {
			internalError(w, err, "internal error")
			return
		}
		resp := map[string]any{"result": result}
		if err != nil {
			resp["error"] = err.Error()
		}
		writeJSON(w, resp)
	}))

	mux.HandleFunc("/api/v1/federation/explain", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		var q Query
		if err := json.Unmarshal(body, &q); err != nil {
			http.Error(w, "invalid query JSON", http.StatusBadRequest)
			return
		}
		plan := engine.ExplainFederated(&q)
		writeJSON(w, plan)
	}))

	mux.HandleFunc("/api/v1/federation/nodes", wrap(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}
			var node FederationNodeInfo
			if err := json.Unmarshal(body, &node); err != nil {
				http.Error(w, "invalid node JSON", http.StatusBadRequest)
				return
			}
			if node.NodeID == "" {
				http.Error(w, "node_id required", http.StatusBadRequest)
				return
			}
			engine.RegisterNode(node)
			w.WriteHeader(http.StatusCreated)
			writeJSON(w, map[string]string{"status": "registered", "node_id": node.NodeID})
		case http.MethodGet:
			writeJSON(w, engine.GetNodeHealth())
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	mux.HandleFunc("/api/v1/federation/nodes/", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		nodeID := strings.TrimPrefix(r.URL.Path, "/api/v1/federation/nodes/")
		if nodeID == "" {
			http.Error(w, "node_id required", http.StatusBadRequest)
			return
		}
		engine.DeregisterNode(nodeID)
		writeJSON(w, map[string]string{"status": "deregistered", "node_id": nodeID})
	}))

	mux.HandleFunc("/api/v1/federation/health", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		nodes := engine.GetNodeHealth()
		type nodeHealth struct {
			FederationNodeInfo
			CircuitBreaker string `json:"circuit_breaker"`
		}
		result := make([]nodeHealth, 0, len(nodes))
		for _, n := range nodes {
			nh := nodeHealth{FederationNodeInfo: n}
			if state, ok := engine.GetCircuitBreakerState(n.NodeID); ok {
				nh.CircuitBreaker = state.String()
			}
			result = append(result, nh)
		}
		writeJSON(w, result)
	}))

	mux.HandleFunc("/api/v1/federation/status", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		engine.mu.RLock()
		running := engine.running
		engine.mu.RUnlock()
		writeJSON(w, map[string]any{
			"running": running,
			"config":  engine.config,
			"stats":   engine.Stats(),
			"nodes":   len(engine.GetNodeHealth()),
		})
	}))
}
