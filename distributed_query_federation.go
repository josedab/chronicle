package chronicle

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
// Configuration
// ---------------------------------------------------------------------------

// RetryBackoffConfig configures exponential backoff for retries.
type RetryBackoffConfig struct {
	InitialInterval time.Duration `json:"initial_interval"`
	MaxInterval     time.Duration `json:"max_interval"`
	Multiplier      float64       `json:"multiplier"`
	MaxRetries      int           `json:"max_retries"`
}

// DistributedFederationConfig controls the distributed query federation engine.
type DistributedFederationConfig struct {
	Enabled             bool          `json:"enabled"`
	MaxFanOut           int           `json:"max_fan_out"`
	PartialResultTimeout time.Duration `json:"partial_result_timeout"`

	EnablePredicatePushDown bool `json:"enable_predicate_push_down"`
	EnableLocalityRouting   bool `json:"enable_locality_routing"`

	CircuitBreakerThreshold int           `json:"circuit_breaker_threshold"`
	CircuitBreakerTimeout   time.Duration `json:"circuit_breaker_timeout"`
	CircuitBreakerHalfOpenMax int         `json:"circuit_breaker_half_open_max"`

	MaxPartialResultSizeMB int  `json:"max_partial_result_size_mb"`
	EnablePartialResults   bool `json:"enable_partial_results"`

	RetryPolicy         RetryBackoffConfig `json:"retry_policy"`
	HealthCheckInterval time.Duration      `json:"health_check_interval"`
}

// DefaultDistributedFederationConfig returns a production-ready default configuration.
func DefaultDistributedFederationConfig() DistributedFederationConfig {
	return DistributedFederationConfig{
		Enabled:                 true,
		MaxFanOut:               10,
		PartialResultTimeout:    5 * time.Second,
		EnablePredicatePushDown: true,
		EnableLocalityRouting:   true,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   30 * time.Second,
		CircuitBreakerHalfOpenMax: 3,
		MaxPartialResultSizeMB:  64,
		EnablePartialResults:    true,
		RetryPolicy: RetryBackoffConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     5 * time.Second,
			Multiplier:      2.0,
			MaxRetries:      3,
		},
		HealthCheckInterval: 10 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// Predicate Push-Down
// ---------------------------------------------------------------------------

// FedPredicateKind classifies how a predicate can be pushed.
type FedPredicateKind int

const (
	FedPredicateTimeRange FedPredicateKind = iota
	FedPredicateTagEquality
	FedPredicateTagFilter
	FedPredicateAggregation
	FedPredicateLimit
)

// PushablePredicate describes a single predicate that can be pushed to a remote node.
type PushablePredicate struct {
	Kind  FedPredicateKind `json:"kind"`
	Field string        `json:"field"`
	Value string        `json:"value,omitempty"`
	Safe  bool          `json:"safe"` // always safe to push without changing semantics
}

// PredicateAnalysis is the result of analysing a query for push-down.
type PredicateAnalysis struct {
	Pushable []PushablePredicate `json:"pushable"`
	Local    []PushablePredicate `json:"local"`
}

// PredicatePushDown analyses queries and creates optimised remote queries.
type PredicatePushDown struct {
	enabled bool
}

// NewPredicatePushDown returns a new push-down analyser.
func NewPredicatePushDown(enabled bool) *PredicatePushDown {
	return &PredicatePushDown{enabled: enabled}
}

// AnalyzePredicates splits a query's predicates into pushable and local sets.
func (pp *PredicatePushDown) AnalyzePredicates(q *Query) PredicateAnalysis {
	var analysis PredicateAnalysis
	if !pp.enabled || q == nil {
		return analysis
	}

	// Time range — always safe to push.
	if q.Start != 0 || q.End != 0 {
		analysis.Pushable = append(analysis.Pushable, PushablePredicate{
			Kind:  FedPredicateTimeRange,
			Field: "time",
			Value: fmt.Sprintf("%d-%d", q.Start, q.End),
			Safe:  true,
		})
	}

	// Tag equality predicates — always safe.
	for k, v := range q.Tags {
		analysis.Pushable = append(analysis.Pushable, PushablePredicate{
			Kind:  FedPredicateTagEquality,
			Field: k,
			Value: v,
			Safe:  true,
		})
	}

	// TagFilters: equality/in are safe; regex stays local.
	for _, tf := range q.TagFilters {
		p := PushablePredicate{
			Kind:  FedPredicateTagFilter,
			Field: tf.Key,
		}
		switch tf.Op {
		case TagOpEq, TagOpNotEq, TagOpIn:
			p.Safe = true
			p.Value = strings.Join(tf.Values, ",")
			analysis.Pushable = append(analysis.Pushable, p)
		default:
			p.Safe = false
			analysis.Local = append(analysis.Local, p)
		}
	}

	// Aggregation — partial aggregation is pushable for distributive funcs.
	if q.Aggregation != nil {
		p := PushablePredicate{
			Kind:  FedPredicateAggregation,
			Field: fmt.Sprintf("agg_%d", q.Aggregation.Function),
		}
		switch q.Aggregation.Function {
		case AggSum, AggCount, AggMin, AggMax:
			p.Safe = true
			analysis.Pushable = append(analysis.Pushable, p)
		case AggMean:
			// Mean needs sum+count locally, push partial.
			p.Safe = false
			analysis.Pushable = append(analysis.Pushable, p)
		default:
			p.Safe = false
			analysis.Local = append(analysis.Local, p)
		}
	}

	// Limit stays local (applied after merge).
	if q.Limit > 0 {
		analysis.Local = append(analysis.Local, PushablePredicate{
			Kind:  FedPredicateLimit,
			Field: "limit",
			Value: fmt.Sprintf("%d", q.Limit),
			Safe:  false,
		})
	}

	return analysis
}

// CreateRemoteQuery builds a query suitable for a remote node by applying pushable predicates.
func (pp *PredicatePushDown) CreateRemoteQuery(q *Query, pushable []PushablePredicate) *Query {
	if q == nil {
		return nil
	}
	remote := &Query{
		Metric: q.Metric,
		Start:  q.Start,
		End:    q.End,
	}
	for _, p := range pushable {
		switch p.Kind {
		case FedPredicateTagEquality:
			if remote.Tags == nil {
				remote.Tags = make(map[string]string)
			}
			remote.Tags[p.Field] = p.Value
		case FedPredicateTagFilter:
			for _, tf := range q.TagFilters {
				if tf.Key == p.Field {
					remote.TagFilters = append(remote.TagFilters, tf)
					break
				}
			}
		case FedPredicateAggregation:
			if q.Aggregation != nil {
				agg := *q.Aggregation
				remote.Aggregation = &agg
				remote.GroupBy = q.GroupBy
			}
		}
	}
	return remote
}

// ---------------------------------------------------------------------------
// Data Locality Router
// ---------------------------------------------------------------------------

// FederationNodeInfo describes a node's capabilities.
type FederationNodeInfo struct {
	NodeID     string            `json:"node_id"`
	Address    string            `json:"address"`
	Metrics    []string          `json:"metrics"`
	TimeRanges []TimeRange       `json:"time_ranges"`
	Load       float64           `json:"load"`        // 0.0–1.0
	Healthy    bool              `json:"healthy"`
	LastSeen   time.Time         `json:"last_seen"`
	Meta       map[string]string `json:"meta,omitempty"`
}

// NodeScore pairs a node with a routing score.
type NodeScore struct {
	Node  FederationNodeInfo `json:"node"`
	Score float64            `json:"score"`
}

// DataLocalityRouter routes queries to optimal nodes based on data locality.
type DataLocalityRouter struct {
	mu    sync.RWMutex
	nodes map[string]*FederationNodeInfo
}

// NewDataLocalityRouter creates a new router.
func NewDataLocalityRouter() *DataLocalityRouter {
	return &DataLocalityRouter{nodes: make(map[string]*FederationNodeInfo)}
}

// RegisterNode adds or updates a node's capabilities.
func (r *DataLocalityRouter) RegisterNode(nodeID, address string, metrics []string, timeRanges []TimeRange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[nodeID] = &FederationNodeInfo{
		NodeID:     nodeID,
		Address:    address,
		Metrics:    metrics,
		TimeRanges: timeRanges,
		Healthy:    true,
		LastSeen:   time.Now(),
		Meta:       make(map[string]string),
	}
}

// RemoveNode deregisters a node.
func (r *DataLocalityRouter) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
}

// UpdateHealth sets a node's health status and load.
func (r *DataLocalityRouter) UpdateHealth(nodeID string, healthy bool, load float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[nodeID]; ok {
		n.Healthy = healthy
		n.Load = load
		n.LastSeen = time.Now()
	}
}

// GetNode returns info for a single node.
func (r *DataLocalityRouter) GetNode(nodeID string) (FederationNodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[nodeID]
	if !ok {
		return FederationNodeInfo{}, false
	}
	return *n, true
}

// AllNodes returns all registered nodes.
func (r *DataLocalityRouter) AllNodes() []FederationNodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]FederationNodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	return out
}

// RouteQuery returns a ranked list of nodes best suited for the query.
func (r *DataLocalityRouter) RouteQuery(q *Query) []NodeScore {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var scores []NodeScore
	for _, node := range r.nodes {
		if !node.Healthy {
			continue
		}
		score := r.scoreNode(node, q)
		if score > 0 {
			scores = append(scores, NodeScore{Node: *node, Score: score})
		}
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i].Score > scores[j].Score })
	return scores
}

func (r *DataLocalityRouter) scoreNode(node *FederationNodeInfo, q *Query) float64 {
	var score float64

	// Metric presence: highest weight.
	hasMetric := false
	for _, m := range node.Metrics {
		if m == q.Metric {
			hasMetric = true
			break
		}
	}
	if !hasMetric && len(node.Metrics) > 0 {
		return 0 // node doesn't have data
	}
	if hasMetric {
		score += 50
	}

	// Time range overlap.
	if q.Start != 0 || q.End != 0 {
		for _, tr := range node.TimeRanges {
			overlap := r.timeOverlap(q.Start, q.End, tr.Start, tr.End)
			score += overlap * 30 // up to 30 points
		}
	} else {
		score += 15 // unbounded query: moderate score
	}

	// Load penalty: prefer less-loaded nodes.
	score += (1.0 - node.Load) * 20

	return score
}

func (r *DataLocalityRouter) timeOverlap(qStart, qEnd, nStart, nEnd int64) float64 {
	if qEnd != 0 && qEnd < nStart {
		return 0
	}
	if qStart != 0 && nEnd != 0 && qStart > nEnd {
		return 0
	}
	// Compute overlap fraction relative to query range.
	qS, qE := qStart, qEnd
	if qS == 0 {
		qS = nStart
	}
	if qE == 0 {
		qE = nEnd
	}
	if qE == 0 || qS == 0 {
		return 0.5 // can't determine range
	}
	overlapStart := qS
	if nStart > overlapStart {
		overlapStart = nStart
	}
	overlapEnd := qE
	if nEnd != 0 && nEnd < overlapEnd {
		overlapEnd = nEnd
	}
	if overlapEnd <= overlapStart {
		return 0
	}
	qRange := float64(qE - qS)
	if qRange == 0 {
		return 1.0
	}
	return float64(overlapEnd-overlapStart) / qRange
}

// ---------------------------------------------------------------------------
// Circuit Breaker
// ---------------------------------------------------------------------------

// FedCircuitState represents the state of a circuit breaker.
type FedCircuitState int

const (
	FedCircuitClosed   FedCircuitState = iota // normal operation
	FedCircuitOpen                         // failing, reject fast
	FedCircuitHalfOpen                     // testing recovery
)

// String returns a human-readable circuit state.
func (s FedCircuitState) String() string {
	switch s {
	case FedCircuitClosed:
		return "closed"
	case FedCircuitOpen:
		return "open"
	case FedCircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements a per-node circuit breaker with three states.
type FedCircuitBreaker struct {
	mu               sync.Mutex
	state            FedCircuitState
	failures         int
	successes        int
	threshold        int
	timeout          time.Duration
	halfOpenMax      int
	lastFailure      time.Time
	halfOpenAttempts int
}

// NewFedCircuitBreaker creates a circuit breaker with the given thresholds.
func NewFedCircuitBreaker(threshold int, timeout time.Duration, halfOpenMax int) *FedCircuitBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if halfOpenMax <= 0 {
		halfOpenMax = 3
	}
	return &FedCircuitBreaker{
		state:       FedCircuitClosed,
		threshold:   threshold,
		timeout:     timeout,
		halfOpenMax: halfOpenMax,
	}
}

// State returns the current circuit breaker state.
func (cb *FedCircuitBreaker) State() FedCircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.checkStateTransition()
	return cb.state
}

// Execute wraps fn with circuit breaker logic. Returns an error immediately if
// the circuit is open.
func (cb *FedCircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	cb.checkStateTransition()

	switch cb.state {
	case FedCircuitOpen:
		cb.mu.Unlock()
		return fmt.Errorf("circuit breaker open")
	case FedCircuitHalfOpen:
		if cb.halfOpenAttempts >= cb.halfOpenMax {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker half-open limit reached")
		}
		cb.halfOpenAttempts++
	}
	cb.mu.Unlock()

	err := fn()

	if err != nil {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return err
}

// RecordSuccess records a successful call.
func (cb *FedCircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.successes++
	if cb.state == FedCircuitHalfOpen {
		// Recovered.
		cb.state = FedCircuitClosed
		cb.failures = 0
		cb.successes = 0
		cb.halfOpenAttempts = 0
	}
}

// RecordFailure records a failed call.
func (cb *FedCircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case FedCircuitClosed:
		if cb.failures >= cb.threshold {
			cb.state = FedCircuitOpen
		}
	case FedCircuitHalfOpen:
		// Any failure in half-open reopens.
		cb.state = FedCircuitOpen
		cb.halfOpenAttempts = 0
	}
}

// Reset forces the circuit breaker back to closed.
func (cb *FedCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = FedCircuitClosed
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenAttempts = 0
}

// checkStateTransition must be called with mu held.
func (cb *FedCircuitBreaker) checkStateTransition() {
	if cb.state == FedCircuitOpen && time.Since(cb.lastFailure) >= cb.timeout {
		cb.state = FedCircuitHalfOpen
		cb.halfOpenAttempts = 0
	}
}

// ---------------------------------------------------------------------------
// Partial Result Assembler
// ---------------------------------------------------------------------------

// FederatedPartialResult is a partial result from a single federation node.
type FederatedPartialResult struct {
	NodeID     string        `json:"node_id"`
	Points     []Point       `json:"points"`
	Error      string        `json:"error,omitempty"`
	Duration   time.Duration `json:"duration"`
	IsPartial  bool          `json:"is_partial"`
	Aggregation *Aggregation `json:"aggregation,omitempty"`
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
	QueryID             string                    `json:"query_id"`
	OriginalQuery       *Query                    `json:"original_query"`
	Assignments         []FederatedNodeAssignment  `json:"assignments"`
	PredicateAnalysis   PredicateAnalysis          `json:"predicate_analysis"`
	FedAggregationStrategy FedAggregationStrategy        `json:"aggregation_strategy"`
	EstimatedFanOut     int                        `json:"estimated_fan_out"`
	CreatedAt           time.Time                  `json:"created_at"`
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
			"running":  running,
			"config":   engine.config,
			"stats":    engine.Stats(),
			"nodes":    len(engine.GetNodeHealth()),
		})
	}))
}
