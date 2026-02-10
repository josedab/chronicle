package chronicle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// QueryCompilerConfig configures the unified query compiler.
type QueryCompilerConfig struct {
	// EnablePushDown enables predicate push-down optimization.
	EnablePushDown bool `json:"enable_push_down"`

	// EnableVectorized enables vectorized aggregation.
	EnableVectorized bool `json:"enable_vectorized"`

	// EnableCostOptimizer enables cost-based plan selection.
	EnableCostOptimizer bool `json:"enable_cost_optimizer"`

	// MaxIRNodes limits IR complexity to prevent resource exhaustion.
	MaxIRNodes int `json:"max_ir_nodes"`

	// CacheSize is the number of compiled plans to cache.
	CacheSize int `json:"cache_size"`

	// DefaultTimeout for query execution.
	DefaultTimeout time.Duration `json:"default_timeout"`
}

// DefaultQueryCompilerConfig returns sensible defaults.
func DefaultQueryCompilerConfig() QueryCompilerConfig {
	return QueryCompilerConfig{
		EnablePushDown:      true,
		EnableVectorized:    true,
		EnableCostOptimizer: true,
		MaxIRNodes:          256,
		CacheSize:           1024,
		DefaultTimeout:      30 * time.Second,
	}
}

// --- Intermediate Representation (IR) ---

// IRNodeType identifies the type of IR node.
type IRNodeType int

const (
	IRScan IRNodeType = iota
	IRFilter
	IRProject
	IRAggregation
	IRSort
	IRLimit
	IRJoin
	IRUnion
	IRWindow
	IRGapFill
)

func (t IRNodeType) String() string {
	names := [...]string{"Scan", "Filter", "Project", "Aggregation", "Sort", "Limit", "Join", "Union", "Window", "GapFill"}
	if int(t) < len(names) {
		return names[t]
	}
	return "Unknown"
}

// IRNode represents a node in the intermediate representation tree.
type IRNode struct {
	Type       IRNodeType `json:"type"`
	Children   []*IRNode  `json:"children,omitempty"`
	Properties IRProps    `json:"properties"`
}

// IRProps holds the properties that vary by node type.
type IRProps struct {
	// Scan
	Metric string            `json:"metric,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
	Start  int64             `json:"start,omitempty"`
	End    int64             `json:"end,omitempty"`

	// Filter
	Predicate *IRPredicate `json:"predicate,omitempty"`

	// Aggregation
	AggFunc AggFunc       `json:"agg_func,omitempty"`
	Window  time.Duration `json:"window,omitempty"`
	GroupBy []string      `json:"group_by,omitempty"`

	// Sort/Limit
	SortField string `json:"sort_field,omitempty"`
	SortAsc   bool   `json:"sort_asc,omitempty"`
	LimitN    int    `json:"limit_n,omitempty"`

	// Project
	Fields []string `json:"fields,omitempty"`
}

// IRPredicate represents a filter predicate in the IR.
type IRPredicate struct {
	Field string         `json:"field"`
	Op    IRPredicateOp  `json:"op"`
	Value any            `json:"value"`
	And   []*IRPredicate `json:"and,omitempty"`
	Or    []*IRPredicate `json:"or,omitempty"`
}

// IRPredicateOp is a predicate comparison operator.
type IRPredicateOp string

const (
	IROpEq   IRPredicateOp = "="
	IROpNe   IRPredicateOp = "!="
	IROpGt   IRPredicateOp = ">"
	IROpGte  IRPredicateOp = ">="
	IROpLt   IRPredicateOp = "<"
	IROpLte  IRPredicateOp = "<="
	IROpIn   IRPredicateOp = "IN"
	IROpLike IRPredicateOp = "LIKE"
)

// CompiledPlan is the result of compiling a query through the unified pipeline.
type CompiledPlan struct {
	Root           *IRNode   `json:"root"`
	SourceLanguage string    `json:"source_language"`
	OriginalQuery  string    `json:"original_query"`
	EstimatedCost  float64   `json:"estimated_cost"`
	Optimizations  []string  `json:"optimizations"`
	CompiledAt     time.Time `json:"compiled_at"`
}

// QueryCompiler provides unified compilation from SQL/PromQL/CQL/NL to IR.
type QueryCompiler struct {
	config QueryCompilerConfig
	db     *DB

	// Plan cache
	planCache map[string]*CompiledPlan
	cacheMu   sync.RWMutex

	// Stats
	compilations  int64
	cacheHits     int64
	cacheMisses   int64
	optimizations int64
	mu            sync.RWMutex
}

// QueryCompilerStats tracks compiler statistics.
type QueryCompilerStats struct {
	Compilations  int64 `json:"compilations"`
	CacheHits     int64 `json:"cache_hits"`
	CacheMisses   int64 `json:"cache_misses"`
	Optimizations int64 `json:"optimizations"`
	CacheSize     int   `json:"cache_size"`
}

// NewQueryCompiler creates a new unified query compiler.
func NewQueryCompiler(db *DB, config QueryCompilerConfig) *QueryCompiler {
	return &QueryCompiler{
		config:    config,
		db:        db,
		planCache: make(map[string]*CompiledPlan),
	}
}

// Compile parses and optimizes a query string from any supported language.
func (qc *QueryCompiler) Compile(query string) (*CompiledPlan, error) {
	if query == "" {
		return nil, errors.New("query_compiler: empty query")
	}

	// Check cache
	qc.cacheMu.RLock()
	if plan, ok := qc.planCache[query]; ok {
		qc.cacheMu.RUnlock()
		qc.mu.Lock()
		qc.cacheHits++
		qc.mu.Unlock()
		return plan, nil
	}
	qc.cacheMu.RUnlock()

	qc.mu.Lock()
	qc.compilations++
	qc.cacheMisses++
	qc.mu.Unlock()

	// Detect language and parse to IR
	lang := detectQueryLanguage(query)
	var root *IRNode
	var err error

	switch lang {
	case "sql":
		root, err = qc.parseSQL(query)
	case "promql":
		root, err = qc.parsePromQL(query)
	case "cql":
		root, err = qc.parseCQL(query)
	default:
		root, err = qc.parseSQL(query)
		lang = "sql"
	}

	if err != nil {
		return nil, fmt.Errorf("query_compiler: parse error (%s): %w", lang, err)
	}

	if err := qc.validateIR(root, 0); err != nil {
		return nil, fmt.Errorf("query_compiler: IR validation: %w", err)
	}

	// Optimize
	var optimizations []string
	if qc.config.EnablePushDown {
		if opt := qc.pushDownPredicates(root); opt {
			optimizations = append(optimizations, "predicate_push_down")
		}
	}
	if qc.config.EnableCostOptimizer {
		cost := qc.estimateCost(root)
		optimizations = append(optimizations, fmt.Sprintf("cost_estimate=%.2f", cost))
	}

	plan := &CompiledPlan{
		Root:           root,
		SourceLanguage: lang,
		OriginalQuery:  query,
		EstimatedCost:  qc.estimateCost(root),
		Optimizations:  optimizations,
		CompiledAt:     time.Now(),
	}

	// Cache the plan
	qc.cacheMu.Lock()
	if len(qc.planCache) >= qc.config.CacheSize {
		// Evict oldest entry
		var oldest string
		var oldestTime time.Time
		for k, v := range qc.planCache {
			if oldest == "" || v.CompiledAt.Before(oldestTime) {
				oldest = k
				oldestTime = v.CompiledAt
			}
		}
		delete(qc.planCache, oldest)
	}
	qc.planCache[query] = plan
	qc.cacheMu.Unlock()

	return plan, nil
}

// Execute compiles and executes a query.
func (qc *QueryCompiler) Execute(ctx context.Context, query string) (*Result, error) {
	plan, err := qc.Compile(query)
	if err != nil {
		return nil, err
	}
	return qc.executePlan(ctx, plan)
}

// Stats returns compiler statistics.
func (qc *QueryCompiler) Stats() QueryCompilerStats {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	qc.cacheMu.RLock()
	cacheSize := len(qc.planCache)
	qc.cacheMu.RUnlock()

	return QueryCompilerStats{
		Compilations:  qc.compilations,
		CacheHits:     qc.cacheHits,
		CacheMisses:   qc.cacheMisses,
		Optimizations: qc.optimizations,
		CacheSize:     cacheSize,
	}
}

// ClearCache empties the plan cache.
func (qc *QueryCompiler) ClearCache() {
	qc.cacheMu.Lock()
	qc.planCache = make(map[string]*CompiledPlan)
	qc.cacheMu.Unlock()
}

// --- Language Detection ---

func detectQueryLanguage(query string) string {
	q := strings.TrimSpace(strings.ToUpper(query))

	// PromQL patterns: metric_name{label="value"}, rate(...), sum(...)
	if strings.Contains(query, "{") && strings.Contains(query, "}") &&
		!strings.HasPrefix(q, "SELECT") && !strings.HasPrefix(q, "FROM") {
		return "promql"
	}

	// CQL patterns: WINDOW, GAP_FILL, ALIGN, ASOF
	if strings.Contains(q, "WINDOW") || strings.Contains(q, "GAP_FILL") ||
		strings.Contains(q, "ALIGN") || strings.Contains(q, "ASOF") {
		return "cql"
	}

	// SQL patterns
	if strings.HasPrefix(q, "SELECT") || strings.HasPrefix(q, "FROM") ||
		strings.HasPrefix(q, "SHOW") || strings.HasPrefix(q, "DESCRIBE") {
		return "sql"
	}

	return "sql"
}

// --- SQL Parser to IR ---

func (qc *QueryCompiler) parseSQL(query string) (*IRNode, error) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("expected SELECT, got: %s", q[:compilerMin(20, len(q))])
	}

	// Parse: SELECT [agg(field)] FROM metric [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT n]
	parts := sqlSplitClauses(q)

	metric := extractFromClause(parts["FROM"])
	if metric == "" {
		return nil, errors.New("missing FROM clause")
	}

	// Build scan node
	scan := &IRNode{
		Type: IRScan,
		Properties: IRProps{
			Metric: metric,
		},
	}

	current := scan

	// WHERE clause → filter node
	if where, ok := parts["WHERE"]; ok && where != "" {
		pred := parseWherePredicate(where)
		if pred != nil {
			filter := &IRNode{
				Type:       IRFilter,
				Children:   []*IRNode{current},
				Properties: IRProps{Predicate: pred},
			}
			current = filter
		}
	}

	// Check for aggregation in SELECT
	selectClause := parts["SELECT"]
	aggFunc, hasAgg := parseAggFunction(selectClause)

	if hasAgg {
		aggNode := &IRNode{
			Type:     IRAggregation,
			Children: []*IRNode{current},
			Properties: IRProps{
				AggFunc: aggFunc,
			},
		}

		// GROUP BY
		if gb, ok := parts["GROUP BY"]; ok && gb != "" {
			aggNode.Properties.GroupBy = splitAndTrim(gb, ",")
		}

		current = aggNode
	}

	// ORDER BY
	if ob, ok := parts["ORDER BY"]; ok && ob != "" {
		sortNode := &IRNode{
			Type:     IRSort,
			Children: []*IRNode{current},
			Properties: IRProps{
				SortField: strings.TrimSpace(ob),
				SortAsc:   !strings.Contains(strings.ToUpper(ob), "DESC"),
			},
		}
		current = sortNode
	}

	// LIMIT
	if lim, ok := parts["LIMIT"]; ok && lim != "" {
		var n int
		fmt.Sscanf(strings.TrimSpace(lim), "%d", &n)
		if n > 0 {
			limitNode := &IRNode{
				Type:     IRLimit,
				Children: []*IRNode{current},
				Properties: IRProps{
					LimitN: n,
				},
			}
			current = limitNode
		}
	}

	return current, nil
}

// --- PromQL Parser to IR ---

func (qc *QueryCompiler) parsePromQL(query string) (*IRNode, error) {
	q := strings.TrimSpace(query)

	// Handle aggregation functions: sum(metric{...}), avg(metric{...}), etc.
	aggFunc, inner, hasAgg := parsePromQLAggregation(q)

	var metricQuery string
	if hasAgg {
		metricQuery = inner
	} else {
		metricQuery = q
	}

	// Parse metric_name{label="value", ...}[duration]
	metric, tags, err := parsePromQLSelector(metricQuery)
	if err != nil {
		return nil, err
	}

	scan := &IRNode{
		Type: IRScan,
		Properties: IRProps{
			Metric: metric,
			Tags:   tags,
		},
	}

	current := scan

	if hasAgg {
		aggNode := &IRNode{
			Type:     IRAggregation,
			Children: []*IRNode{current},
			Properties: IRProps{
				AggFunc: aggFunc,
			},
		}
		current = aggNode
	}

	return current, nil
}

// --- CQL Parser to IR ---

func (qc *QueryCompiler) parseCQL(query string) (*IRNode, error) {
	// CQL extends SQL with WINDOW, GAP_FILL, ALIGN, ASOF JOIN
	root, err := qc.parseSQL(query)
	if err != nil {
		return nil, err
	}

	upper := strings.ToUpper(query)

	// Add WINDOW semantics
	if idx := strings.Index(upper, "WINDOW"); idx >= 0 {
		windowClause := query[idx+6:]
		if endIdx := strings.IndexAny(windowClause, " \t\n)"); endIdx > 0 {
			windowClause = windowClause[:endIdx]
		}
		dur, err := time.ParseDuration(strings.TrimSpace(windowClause))
		if err == nil {
			windowNode := &IRNode{
				Type:     IRWindow,
				Children: []*IRNode{root},
				Properties: IRProps{
					Window: dur,
				},
			}
			root = windowNode
		}
	}

	// Add GAP_FILL
	if strings.Contains(upper, "GAP_FILL") {
		gapFillNode := &IRNode{
			Type:     IRGapFill,
			Children: []*IRNode{root},
		}
		root = gapFillNode
	}

	return root, nil
}

// --- IR Optimization ---

func (qc *QueryCompiler) pushDownPredicates(node *IRNode) bool {
	if node == nil {
		return false
	}

	optimized := false

	// Push filters below aggregations when possible
	if node.Type == IRAggregation && len(node.Children) > 0 {
		child := node.Children[0]
		if child.Type == IRFilter && len(child.Children) > 0 {
			// Filter is already below aggregation — good
		}
	}

	// Push time range from filter into scan
	if node.Type == IRFilter && node.Properties.Predicate != nil {
		for _, child := range node.Children {
			if child.Type == IRScan {
				if pushTimePredicate(node.Properties.Predicate, &child.Properties) {
					optimized = true
					qc.mu.Lock()
					qc.optimizations++
					qc.mu.Unlock()
				}
			}
		}
	}

	for _, child := range node.Children {
		if qc.pushDownPredicates(child) {
			optimized = true
		}
	}

	return optimized
}

func pushTimePredicate(pred *IRPredicate, props *IRProps) bool {
	if pred == nil {
		return false
	}
	pushed := false

	if pred.Field == "timestamp" || pred.Field == "time" {
		if val, ok := pred.Value.(float64); ok {
			ts := int64(val)
			switch pred.Op {
			case IROpGt, IROpGte:
				if ts > props.Start {
					props.Start = ts
					pushed = true
				}
			case IROpLt, IROpLte:
				if props.End == 0 || ts < props.End {
					props.End = ts
					pushed = true
				}
			}
		}
	}

	for _, p := range pred.And {
		if pushTimePredicate(p, props) {
			pushed = true
		}
	}
	return pushed
}

func (qc *QueryCompiler) estimateCost(node *IRNode) float64 {
	if node == nil {
		return 0
	}

	baseCost := 1.0
	switch node.Type {
	case IRScan:
		baseCost = 10.0
	case IRFilter:
		baseCost = 2.0
	case IRAggregation:
		baseCost = 5.0
	case IRSort:
		baseCost = 3.0
	case IRJoin:
		baseCost = 20.0
	}

	childCost := 0.0
	for _, child := range node.Children {
		childCost += qc.estimateCost(child)
	}

	return baseCost + childCost
}

func (qc *QueryCompiler) validateIR(node *IRNode, depth int) error {
	if node == nil {
		return errors.New("nil IR node")
	}
	if depth > qc.config.MaxIRNodes {
		return fmt.Errorf("IR tree exceeds max depth %d", qc.config.MaxIRNodes)
	}
	for _, child := range node.Children {
		if err := qc.validateIR(child, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// --- IR Execution ---

func (qc *QueryCompiler) executePlan(ctx context.Context, plan *CompiledPlan) (*Result, error) {
	if plan == nil || plan.Root == nil {
		return nil, errors.New("query_compiler: nil plan")
	}

	return qc.executeNode(ctx, plan.Root)
}

func (qc *QueryCompiler) executeNode(ctx context.Context, node *IRNode) (*Result, error) {
	switch node.Type {
	case IRScan:
		return qc.executeScan(ctx, node)
	case IRFilter:
		return qc.executeFilter(ctx, node)
	case IRAggregation:
		return qc.executeAggregation(ctx, node)
	case IRSort:
		return qc.executeSort(ctx, node)
	case IRLimit:
		return qc.executeLimit(ctx, node)
	case IRWindow:
		return qc.executeWindow(ctx, node)
	case IRGapFill:
		return qc.executeGapFill(ctx, node)
	default:
		// For unhandled types, execute children and return first result
		if len(node.Children) > 0 {
			return qc.executeNode(ctx, node.Children[0])
		}
		return &Result{}, nil
	}
}

func (qc *QueryCompiler) executeScan(ctx context.Context, node *IRNode) (*Result, error) {
	q := &Query{
		Metric: node.Properties.Metric,
		Tags:   node.Properties.Tags,
		Start:  node.Properties.Start,
		End:    node.Properties.End,
	}
	return qc.db.ExecuteContext(ctx, q)
}

func (qc *QueryCompiler) executeFilter(ctx context.Context, node *IRNode) (*Result, error) {
	if len(node.Children) == 0 {
		return &Result{}, nil
	}

	result, err := qc.executeNode(ctx, node.Children[0])
	if err != nil {
		return nil, err
	}

	if node.Properties.Predicate == nil {
		return result, nil
	}

	filtered := make([]Point, 0, len(result.Points))
	for _, p := range result.Points {
		if evaluatePredicate(node.Properties.Predicate, p) {
			filtered = append(filtered, p)
		}
	}
	result.Points = filtered
	return result, nil
}

func (qc *QueryCompiler) executeAggregation(ctx context.Context, node *IRNode) (*Result, error) {
	if len(node.Children) == 0 {
		return &Result{}, nil
	}

	result, err := qc.executeNode(ctx, node.Children[0])
	if err != nil {
		return nil, err
	}

	if len(result.Points) == 0 {
		return result, nil
	}

	if qc.config.EnableVectorized {
		return qc.vectorizedAggregate(result, node.Properties.AggFunc)
	}

	return qc.scalarAggregate(result, node.Properties.AggFunc)
}

func (qc *QueryCompiler) vectorizedAggregate(result *Result, fn AggFunc) (*Result, error) {
	values := make([]float64, len(result.Points))
	for i, p := range result.Points {
		values[i] = p.Value
	}

	var aggValue float64
	switch fn {
	case AggSum:
		for _, v := range values {
			aggValue += v
		}
	case AggMean:
		for _, v := range values {
			aggValue += v
		}
		aggValue /= float64(len(values))
	case AggMin:
		aggValue = math.MaxFloat64
		for _, v := range values {
			if v < aggValue {
				aggValue = v
			}
		}
	case AggMax:
		aggValue = -math.MaxFloat64
		for _, v := range values {
			if v > aggValue {
				aggValue = v
			}
		}
	case AggCount:
		aggValue = float64(len(values))
	default:
		aggValue = float64(len(values))
	}

	return &Result{
		Points: []Point{{
			Metric:    result.Points[0].Metric,
			Value:     aggValue,
			Timestamp: result.Points[len(result.Points)-1].Timestamp,
		}},
	}, nil
}

func (qc *QueryCompiler) scalarAggregate(result *Result, fn AggFunc) (*Result, error) {
	return qc.vectorizedAggregate(result, fn)
}

func (qc *QueryCompiler) executeSort(ctx context.Context, node *IRNode) (*Result, error) {
	if len(node.Children) == 0 {
		return &Result{}, nil
	}

	result, err := qc.executeNode(ctx, node.Children[0])
	if err != nil {
		return nil, err
	}

	asc := node.Properties.SortAsc
	sort.Slice(result.Points, func(i, j int) bool {
		if asc {
			return result.Points[i].Timestamp < result.Points[j].Timestamp
		}
		return result.Points[i].Timestamp > result.Points[j].Timestamp
	})

	return result, nil
}

func (qc *QueryCompiler) executeLimit(ctx context.Context, node *IRNode) (*Result, error) {
	if len(node.Children) == 0 {
		return &Result{}, nil
	}

	result, err := qc.executeNode(ctx, node.Children[0])
	if err != nil {
		return nil, err
	}

	if node.Properties.LimitN > 0 && len(result.Points) > node.Properties.LimitN {
		result.Points = result.Points[:node.Properties.LimitN]
	}
	return result, nil
}

func (qc *QueryCompiler) executeWindow(ctx context.Context, node *IRNode) (*Result, error) {
	if len(node.Children) == 0 {
		return &Result{}, nil
	}
	return qc.executeNode(ctx, node.Children[0])
}

func (qc *QueryCompiler) executeGapFill(ctx context.Context, node *IRNode) (*Result, error) {
	if len(node.Children) == 0 {
		return &Result{}, nil
	}
	return qc.executeNode(ctx, node.Children[0])
}

// --- Helper parsers ---

func evaluatePredicate(pred *IRPredicate, p Point) bool {
	if pred == nil {
		return true
	}

	// Tag-based predicates
	if tagVal, ok := p.Tags[pred.Field]; ok {
		strVal, isStr := pred.Value.(string)
		if isStr {
			switch pred.Op {
			case IROpEq:
				return tagVal == strVal
			case IROpNe:
				return tagVal != strVal
			}
		}
	}

	// Value-based predicates
	if pred.Field == "value" {
		numVal, ok := pred.Value.(float64)
		if ok {
			switch pred.Op {
			case IROpGt:
				return p.Value > numVal
			case IROpGte:
				return p.Value >= numVal
			case IROpLt:
				return p.Value < numVal
			case IROpLte:
				return p.Value <= numVal
			case IROpEq:
				return p.Value == numVal
			case IROpNe:
				return p.Value != numVal
			}
		}
	}

	// AND/OR logic
	if len(pred.And) > 0 {
		for _, ap := range pred.And {
			if !evaluatePredicate(ap, p) {
				return false
			}
		}
	}
	if len(pred.Or) > 0 {
		for _, op := range pred.Or {
			if evaluatePredicate(op, p) {
				return true
			}
		}
		return false
	}

	return true
}

func sqlSplitClauses(q string) map[string]string {
	upper := strings.ToUpper(q)
	clauses := map[string]string{}

	keywords := []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "WINDOW", "GAP_FILL"}
	type kwPos struct {
		kw  string
		pos int
	}
	var found []kwPos
	for _, kw := range keywords {
		idx := strings.Index(upper, kw)
		if idx >= 0 {
			found = append(found, kwPos{kw, idx})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].pos < found[j].pos })

	for i, f := range found {
		start := f.pos + len(f.kw)
		var end int
		if i+1 < len(found) {
			end = found[i+1].pos
		} else {
			end = len(q)
		}
		clauses[f.kw] = strings.TrimSpace(q[start:end])
	}

	return clauses
}

func extractFromClause(fromClause string) string {
	if fromClause == "" {
		return ""
	}
	parts := strings.Fields(fromClause)
	if len(parts) == 0 {
		return ""
	}
	return strings.Trim(parts[0], `"'`)
}

func parseAggFunction(selectClause string) (AggFunc, bool) {
	upper := strings.ToUpper(strings.TrimSpace(selectClause))
	aggMap := map[string]AggFunc{
		"COUNT": AggCount,
		"SUM":   AggSum,
		"AVG":   AggMean,
		"MEAN":  AggMean,
		"MIN":   AggMin,
		"MAX":   AggMax,
		"FIRST": AggFirst,
		"LAST":  AggLast,
	}

	for name, fn := range aggMap {
		if strings.Contains(upper, name+"(") {
			return fn, true
		}
	}
	return AggNone, false
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func parsePromQLAggregation(q string) (AggFunc, string, bool) {
	aggMap := map[string]AggFunc{
		"sum":   AggSum,
		"avg":   AggMean,
		"min":   AggMin,
		"max":   AggMax,
		"count": AggCount,
	}

	for name, fn := range aggMap {
		if strings.HasPrefix(q, name+"(") && strings.HasSuffix(q, ")") {
			inner := q[len(name)+1 : len(q)-1]
			return fn, inner, true
		}
	}
	return AggNone, "", false
}

func parsePromQLSelector(q string) (string, map[string]string, error) {
	q = strings.TrimSpace(q)

	braceIdx := strings.Index(q, "{")
	if braceIdx < 0 {
		// Just metric name, no labels
		metric := strings.TrimRight(q, "[]0-9smhd")
		return strings.TrimSpace(metric), nil, nil
	}

	metric := strings.TrimSpace(q[:braceIdx])
	endBrace := strings.Index(q, "}")
	if endBrace < 0 {
		return "", nil, errors.New("unclosed label selector")
	}

	labelStr := q[braceIdx+1 : endBrace]
	tags := make(map[string]string)

	for _, pair := range strings.Split(labelStr, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		eqIdx := strings.Index(pair, "=")
		if eqIdx < 0 {
			continue
		}
		key := strings.TrimSpace(pair[:eqIdx])
		val := strings.Trim(strings.TrimSpace(pair[eqIdx+1:]), `"'`)
		tags[key] = val
	}

	return metric, tags, nil
}

func parseWherePredicate(where string) *IRPredicate {
	where = strings.TrimSpace(where)
	if where == "" {
		return nil
	}

	// Handle simple: field op value
	ops := []struct {
		sym string
		op  IRPredicateOp
	}{
		{">=", IROpGte}, {"<=", IROpLte},
		{"!=", IROpNe}, {"=", IROpEq},
		{">", IROpGt}, {"<", IROpLt},
	}

	for _, o := range ops {
		idx := strings.Index(where, o.sym)
		if idx > 0 {
			field := strings.TrimSpace(where[:idx])
			val := strings.Trim(strings.TrimSpace(where[idx+len(o.sym):]), `"'`)

			// Try numeric
			var value any = val
			var f float64
			if _, err := fmt.Sscanf(val, "%f", &f); err == nil {
				value = f
			}

			return &IRPredicate{
				Field: field,
				Op:    o.op,
				Value: value,
			}
		}
	}

	return nil
}

func compilerMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
