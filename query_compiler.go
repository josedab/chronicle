package chronicle

import (
	"context"
	"errors"
	"fmt"
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

	// UDF aggregation
	UDFName string `json:"udf_name,omitempty"` // custom WASM UDF function name

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
