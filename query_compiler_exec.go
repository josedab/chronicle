package chronicle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// IR execution, cost estimation, helper parsers, and query plan generation for the query compiler.

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

	// Check for UDF aggregation
	if node.Properties.UDFName != "" && qc.db != nil {
		return qc.executeUDFAggregation(ctx, result, node.Properties.UDFName)
	}

	if qc.config.EnableVectorized {
		return qc.vectorizedAggregate(result, node.Properties.AggFunc)
	}

	return qc.scalarAggregate(result, node.Properties.AggFunc)
}

// executeUDFAggregation invokes a registered WASM UDF for aggregation.
func (qc *QueryCompiler) executeUDFAggregation(ctx context.Context, result *Result, udfName string) (*Result, error) {
	values := make([]float64, len(result.Points))
	for i, p := range result.Points {
		values[i] = p.Value
	}

	// Look up UDF engine from DB features (if available)
	udfEngine := findUDFEngine(qc.db)
	if udfEngine == nil {
		return nil, fmt.Errorf("UDF engine not available for function %q", udfName)
	}

	udfResult, err := udfEngine.Invoke(udfName, map[string]interface{}{"values": values})
	if err != nil {
		return nil, fmt.Errorf("UDF %q execution failed: %w", udfName, err)
	}

	// Extract scalar result
	var aggValue float64
	switch v := udfResult.Output.(type) {
	case float64:
		aggValue = v
	case map[string]float64:
		if sum, ok := v["sum"]; ok {
			aggValue = sum
		}
	case []float64:
		if len(v) > 0 {
			aggValue = v[0]
		}
	}

	return &Result{
		Points: []Point{{
			Metric:    result.Points[0].Metric,
			Value:     aggValue,
			Timestamp: result.Points[len(result.Points)-1].Timestamp,
			Tags:      map[string]string{"__udf__": udfName},
		}},
	}, nil
}

// findUDFEngine locates the WASM UDF engine from the DB's feature registry.
func findUDFEngine(db *DB) *WASMUDFEngine {
	if db == nil {
		return nil
	}
	// The UDF engine would typically be registered in the feature manager
	// For now, return nil - callers should set up the engine explicitly
	return nil
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
