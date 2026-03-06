package chronicle

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// --- PromQL Evaluator Engine ---

// PromQLEvaluator evaluates PromQL queries against the Chronicle database.
type PromQLEvaluator struct {
	db     *DB
	parser *PromQLCompleteParser
}

// NewPromQLEvaluator creates a new PromQL evaluator.
func NewPromQLEvaluator(db *DB) *PromQLEvaluator {
	return &PromQLEvaluator{
		db:     db,
		parser: NewPromQLCompleteParser(),
	}
}

// PromQLEvalResult holds the result of a PromQL evaluation.
type PromQLEvalResult struct {
	Series []PromQLResultSeries `json:"series"`
	Scalar *float64             `json:"scalar,omitempty"`
}

// PromQLResultSeries is a labeled series in a PromQL result.
type PromQLResultSeries struct {
	Metric  string            `json:"metric"`
	Labels  map[string]string `json:"labels"`
	Samples []PromQLSample    `json:"samples"`
	Value   float64           `json:"value"`
}

// Evaluate parses and evaluates a PromQL expression at the given time range.
func (e *PromQLEvaluator) Evaluate(expr string, start, end int64) (*PromQLEvalResult, error) {
	pq, err := e.parser.ParseComplete(expr)
	if err != nil {
		return nil, fmt.Errorf("promql parse: %w", err)
	}

	// Handle binary expressions
	if pq.BinaryExpr != nil {
		return e.evalBinaryExpr(pq.BinaryExpr, start, end)
	}

	// Fetch base data from the DB
	// For extended agg ops (topk, bottomk, quantile, stdvar, group, count_values),
	// fetch raw data since the DB doesn't know how to aggregate these.
	cq := pq.ToChronicleQuery(start, end)
	if pq.Aggregation != nil && isExtendedAggOp(pq.Aggregation.Op) {
		cq.Aggregation = nil // fetch raw points
	}
	dbResult, err := e.db.Execute(cq)
	if err != nil {
		return nil, fmt.Errorf("promql execute: %w", err)
	}

	points := dbResult.Points
	if len(points) == 0 {
		// absent() returns 1 when no series match
		if pq.Aggregation != nil && pq.Aggregation.Op == PromQLFuncAbsent {
			return &PromQLEvalResult{
				Scalar: floatPtr(1),
			}, nil
		}
		if pq.Aggregation != nil && pq.Aggregation.Op == PromQLFuncAbsentOverTime {
			return &PromQLEvalResult{
				Scalar: floatPtr(1),
			}, nil
		}
		// vector(n) always returns a value regardless of data presence
		if pq.Aggregation != nil && pq.Aggregation.Op == PromQLFuncVector {
			return &PromQLEvalResult{Scalar: floatPtr(1)}, nil
		}
		return &PromQLEvalResult{}, nil
	}

	// Apply extended aggregation operations
	if pq.Aggregation != nil {
		return e.applyExtendedAgg(pq, points, start, end)
	}

	// Apply scalar functions (sgn, clamp, sort, etc.)
	result := pointsToEvalResult(points)
	if pq.Function != 0 {
		result = e.applyScalarFunction(pq.Function, result)
	}

	return result, nil
}

func floatPtr(v float64) *float64 { return &v }

func isExtendedAggOp(op PromQLAggOp) bool {
	switch op {
	case PromQLAggTopk, PromQLAggBottomk, PromQLAggQuantile,
		PromQLAggStdvar, PromQLAggGroup, PromQLAggCountValues,
		PromQLFuncAbsent, PromQLFuncAbsentOverTime,
		PromQLFuncVector, PromQLFuncScalar:
		return true
	}
	return false
}

func pointsToEvalResult(points []Point) *PromQLEvalResult {
	seriesMap := make(map[string]*PromQLResultSeries)
	for _, p := range points {
		key := p.Metric
		s, ok := seriesMap[key]
		if !ok {
			s = &PromQLResultSeries{Metric: p.Metric, Labels: p.Tags}
			seriesMap[key] = s
		}
		s.Samples = append(s.Samples, PromQLSample{Timestamp: p.Timestamp, Value: p.Value})
	}
	result := &PromQLEvalResult{}
	for _, s := range seriesMap {
		if len(s.Samples) > 0 {
			s.Value = s.Samples[len(s.Samples)-1].Value
		}
		result.Series = append(result.Series, *s)
	}
	return result
}

func (e *PromQLEvaluator) applyExtendedAgg(pq *PromQLQuery, points []Point, start, end int64) (*PromQLEvalResult, error) {
	op := pq.Aggregation.Op

	switch op {
	case PromQLFuncAbsent:
		// absent returns empty when series exist
		return &PromQLEvalResult{}, nil
	case PromQLFuncAbsentOverTime:
		return &PromQLEvalResult{}, nil
	case PromQLFuncVector:
		return &PromQLEvalResult{Scalar: floatPtr(1)}, nil
	case PromQLFuncScalar:
		if len(points) > 0 {
			return &PromQLEvalResult{Scalar: floatPtr(points[len(points)-1].Value)}, nil
		}
		return &PromQLEvalResult{Scalar: floatPtr(math.NaN())}, nil
	}

	// Group points by series for group-by aggregations
	groups := groupPointsByTags(points, pq.Aggregation.By, pq.Aggregation.Without)

	switch op {
	case PromQLAggTopk:
		return e.evalTopk(groups, 10, false), nil
	case PromQLAggBottomk:
		return e.evalTopk(groups, 10, true), nil
	case PromQLAggQuantile:
		return e.evalQuantile(groups, 0.5), nil
	case PromQLAggCountValues:
		return e.evalCountValues(groups), nil
	case PromQLAggStdvar:
		return e.evalStdvar(groups), nil
	case PromQLAggGroup:
		return e.evalGroup(groups), nil
	default:
		// For range functions (rate, irate, delta, etc.), apply function to each series
		if pq.Function != 0 {
			return e.evalRangeFunc(pq.Function, points, start, end), nil
		}
		// For standard aggregations, the DB already handled it via ToChronicleQuery
		return pointsToEvalResult(points), nil
	}
}

// evalRangeFunc applies a PromQL range function to points grouped by series.
func (e *PromQLEvaluator) evalRangeFunc(fn PromQLAggOp, points []Point, start, end int64) *PromQLEvalResult {
	// Group points by series key
	seriesMap := make(map[string]*PromQLResultSeries)
	seriesOrder := make([]string, 0)
	for _, p := range points {
		key := p.Metric
		s, ok := seriesMap[key]
		if !ok {
			s = &PromQLResultSeries{Metric: p.Metric, Labels: p.Tags}
			seriesMap[key] = s
			seriesOrder = append(seriesOrder, key)
		}
		s.Samples = append(s.Samples, PromQLSample{Timestamp: p.Timestamp, Value: p.Value})
	}

	rangeMs := end - start
	if rangeMs <= 0 {
		rangeMs = 300000 // default 5m
	}

	rangeFn := mapFuncToRangeFunc(fn)

	result := &PromQLEvalResult{}
	for _, key := range seriesOrder {
		s := seriesMap[key]
		val := EvalRangeFunction(rangeFn, s.Samples, rangeMs)
		result.Series = append(result.Series, PromQLResultSeries{
			Metric:  s.Metric,
			Labels:  s.Labels,
			Value:   val,
			Samples: []PromQLSample{{Timestamp: end, Value: val}},
		})
	}
	return result
}

// mapFuncToRangeFunc maps a PromQLAggOp function ID to a PromQLRangeFunc.
func mapFuncToRangeFunc(fn PromQLAggOp) PromQLRangeFunc {
	switch fn {
	case PromQLAggRate:
		return RangeFuncRate
	case PromQLFuncIrate:
		return RangeFuncIrate
	case PromQLFuncIncrease:
		return RangeFuncIncrease
	case PromQLFuncDelta:
		return RangeFuncDelta
	case PromQLFuncIdelta:
		return RangeFuncIdelta
	case PromQLFuncDeriv:
		return RangeFuncDeriv
	case PromQLFuncChanges:
		return RangeFuncChanges
	case PromQLFuncResets:
		return RangeFuncResets
	case PromQLFuncPredictLinear:
		return RangeFuncPredictLinear
	case PromQLFuncLastOverTime:
		return RangeFuncLastOverTime
	case PromQLFuncHoltWinters:
		return RangeFuncHoltWinters
	default:
		return RangeFuncRate
	}
}

type pointGroup struct {
	key    string
	labels map[string]string
	values []float64
}

func groupPointsByTags(points []Point, by []string, without []string) []pointGroup {
	groupMap := make(map[string]*pointGroup)
	for _, p := range points {
		key := promqlBuildGroupKey(p.Tags, by, without)
		g, ok := groupMap[key]
		if !ok {
			labels := make(map[string]string)
			if len(by) > 0 {
				for _, k := range by {
					if v, ok := p.Tags[k]; ok {
						labels[k] = v
					}
				}
			} else {
				for k, v := range p.Tags {
					labels[k] = v
				}
			}
			g = &pointGroup{key: key, labels: labels}
			groupMap[key] = g
		}
		g.values = append(g.values, p.Value)
	}
	var groups []pointGroup
	for _, g := range groupMap {
		groups = append(groups, *g)
	}
	return groups
}

func promqlBuildGroupKey(tags map[string]string, by []string, without []string) string {
	if len(by) > 0 {
		var parts []string
		for _, k := range by {
			parts = append(parts, k+"="+tags[k])
		}
		sort.Strings(parts)
		return strings.Join(parts, ",")
	}
	if len(without) > 0 {
		skip := make(map[string]bool, len(without))
		for _, k := range without {
			skip[k] = true
		}
		var parts []string
		for k, v := range tags {
			if !skip[k] {
				parts = append(parts, k+"="+v)
			}
		}
		sort.Strings(parts)
		return strings.Join(parts, ",")
	}
	return "__all__"
}

func (e *PromQLEvaluator) evalTopk(groups []pointGroup, k int, reverse bool) *PromQLEvalResult {
	type groupVal struct {
		group pointGroup
		last  float64
	}
	var gvals []groupVal
	for _, g := range groups {
		if len(g.values) == 0 {
			continue
		}
		gvals = append(gvals, groupVal{group: g, last: g.values[len(g.values)-1]})
	}

	sort.Slice(gvals, func(i, j int) bool {
		if reverse {
			return gvals[i].last < gvals[j].last
		}
		return gvals[i].last > gvals[j].last
	})
	if k > len(gvals) {
		k = len(gvals)
	}

	result := &PromQLEvalResult{}
	for _, gv := range gvals[:k] {
		result.Series = append(result.Series, PromQLResultSeries{
			Labels: gv.group.labels,
			Value:  gv.last,
		})
	}
	return result
}

func (e *PromQLEvaluator) evalQuantile(groups []pointGroup, q float64) *PromQLEvalResult {
	result := &PromQLEvalResult{}
	for _, g := range groups {
		if len(g.values) == 0 {
			continue
		}
		sorted := make([]float64, len(g.values))
		copy(sorted, g.values)
		sort.Float64s(sorted)
		val := evalQuantileOverTime([]PromQLSample{}, q)
		if len(sorted) > 0 {
			idx := q * float64(len(sorted)-1)
			lower := int(math.Floor(idx))
			upper := int(math.Ceil(idx))
			if lower == upper || upper >= len(sorted) {
				val = sorted[lower]
			} else {
				frac := idx - float64(lower)
				val = sorted[lower]*(1-frac) + sorted[upper]*frac
			}
		}
		result.Series = append(result.Series, PromQLResultSeries{
			Labels: g.labels,
			Value:  val,
		})
	}
	return result
}

func (e *PromQLEvaluator) evalCountValues(groups []pointGroup) *PromQLEvalResult {
	result := &PromQLEvalResult{}
	for _, g := range groups {
		counts := make(map[float64]int)
		for _, v := range g.values {
			counts[v]++
		}
		for val, cnt := range counts {
			labels := make(map[string]string)
			for k, v := range g.labels {
				labels[k] = v
			}
			labels["value"] = fmt.Sprintf("%g", val)
			result.Series = append(result.Series, PromQLResultSeries{
				Labels: labels,
				Value:  float64(cnt),
			})
		}
	}
	return result
}

func (e *PromQLEvaluator) evalStdvar(groups []pointGroup) *PromQLEvalResult {
	result := &PromQLEvalResult{}
	for _, g := range groups {
		if len(g.values) < 2 {
			result.Series = append(result.Series, PromQLResultSeries{Labels: g.labels, Value: 0})
			continue
		}
		var sum, sumSq float64
		for _, v := range g.values {
			sum += v
			sumSq += v * v
		}
		n := float64(len(g.values))
		mean := sum / n
		variance := sumSq/n - mean*mean
		result.Series = append(result.Series, PromQLResultSeries{Labels: g.labels, Value: variance})
	}
	return result
}

func (e *PromQLEvaluator) evalGroup(groups []pointGroup) *PromQLEvalResult {
	result := &PromQLEvalResult{}
	for _, g := range groups {
		result.Series = append(result.Series, PromQLResultSeries{Labels: g.labels, Value: 1})
	}
	return result
}

func (e *PromQLEvaluator) evalBinaryExpr(expr *PromQLBinaryExpr, start, end int64) (*PromQLEvalResult, error) {
	leftResult, err := e.Evaluate(exprToString(expr.Left), start, end)
	if err != nil {
		return nil, err
	}
	rightResult, err := e.Evaluate(exprToString(expr.Right), start, end)
	if err != nil {
		return nil, err
	}

	// Scalar-scalar
	if leftResult.Scalar != nil && rightResult.Scalar != nil {
		val := evalBinaryScalar(expr.Op, *leftResult.Scalar, *rightResult.Scalar)
		return &PromQLEvalResult{Scalar: &val}, nil
	}

	// Handle set operations (and, or, unless) with label matching
	switch expr.Op {
	case PromQLBinAnd:
		return e.evalSetAnd(leftResult, rightResult), nil
	case PromQLBinOr:
		return e.evalSetOr(leftResult, rightResult), nil
	case PromQLBinUnless:
		return e.evalSetUnless(leftResult, rightResult), nil
	}

	// Vector-scalar: apply scalar to each left series
	if rightResult.Scalar != nil {
		result := &PromQLEvalResult{}
		for _, ls := range leftResult.Series {
			val := evalBinaryScalar(expr.Op, ls.Value, *rightResult.Scalar)
			if !math.IsNaN(val) || !isComparisonOp(expr.Op) {
				result.Series = append(result.Series, PromQLResultSeries{
					Metric: ls.Metric, Labels: ls.Labels, Value: val,
				})
			}
		}
		return result, nil
	}
	if leftResult.Scalar != nil {
		result := &PromQLEvalResult{}
		for _, rs := range rightResult.Series {
			val := evalBinaryScalar(expr.Op, *leftResult.Scalar, rs.Value)
			if !math.IsNaN(val) || !isComparisonOp(expr.Op) {
				result.Series = append(result.Series, PromQLResultSeries{
					Metric: rs.Metric, Labels: rs.Labels, Value: val,
				})
			}
		}
		return result, nil
	}

	// Vector-vector: match on labels
	rightByLabels := make(map[string]PromQLResultSeries)
	for _, rs := range rightResult.Series {
		key := seriesLabelKey(rs.Labels)
		rightByLabels[key] = rs
	}

	result := &PromQLEvalResult{}
	for _, ls := range leftResult.Series {
		key := seriesLabelKey(ls.Labels)
		if rs, ok := rightByLabels[key]; ok {
			val := evalBinaryScalar(expr.Op, ls.Value, rs.Value)
			if !math.IsNaN(val) || !isComparisonOp(expr.Op) {
				result.Series = append(result.Series, PromQLResultSeries{
					Metric: ls.Metric, Labels: ls.Labels, Value: val,
				})
			}
		} else if !isComparisonOp(expr.Op) {
			// For arithmetic ops with no match, use 0 as right side
			val := evalBinaryScalar(expr.Op, ls.Value, 0)
			result.Series = append(result.Series, PromQLResultSeries{
				Metric: ls.Metric, Labels: ls.Labels, Value: val,
			})
		}
	}
	return result, nil
}

func isComparisonOp(op PromQLBinaryOp) bool {
	switch op {
	case PromQLBinEqual, PromQLBinNotEqual, PromQLBinGreaterThan,
		PromQLBinLessThan, PromQLBinGreaterOrEqual, PromQLBinLessOrEqual:
		return true
	}
	return false
}

func seriesLabelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// evalSetAnd returns series present in both left and right (intersection by labels).
func (e *PromQLEvaluator) evalSetAnd(left, right *PromQLEvalResult) *PromQLEvalResult {
	rightKeys := make(map[string]bool)
	for _, rs := range right.Series {
		rightKeys[seriesLabelKey(rs.Labels)] = true
	}
	result := &PromQLEvalResult{}
	for _, ls := range left.Series {
		if rightKeys[seriesLabelKey(ls.Labels)] {
			result.Series = append(result.Series, ls)
		}
	}
	return result
}

// evalSetOr returns all series from left, plus series from right not in left (union).
func (e *PromQLEvaluator) evalSetOr(left, right *PromQLEvalResult) *PromQLEvalResult {
	result := &PromQLEvalResult{}
	leftKeys := make(map[string]bool)
	for _, ls := range left.Series {
		result.Series = append(result.Series, ls)
		leftKeys[seriesLabelKey(ls.Labels)] = true
	}
	for _, rs := range right.Series {
		if !leftKeys[seriesLabelKey(rs.Labels)] {
			result.Series = append(result.Series, rs)
		}
	}
	return result
}

// evalSetUnless returns series from left not present in right (difference).
func (e *PromQLEvaluator) evalSetUnless(left, right *PromQLEvalResult) *PromQLEvalResult {
	rightKeys := make(map[string]bool)
	for _, rs := range right.Series {
		rightKeys[seriesLabelKey(rs.Labels)] = true
	}
	result := &PromQLEvalResult{}
	for _, ls := range left.Series {
		if !rightKeys[seriesLabelKey(ls.Labels)] {
			result.Series = append(result.Series, ls)
		}
	}
	return result
}

func exprToString(q *PromQLQuery) string {
	if q == nil {
		return ""
	}
	return q.Metric
}

// applyScalarFunction applies a scalar function to all series values in a result.
func (e *PromQLEvaluator) applyScalarFunction(fn PromQLAggOp, result *PromQLEvalResult) *PromQLEvalResult {
	switch fn {
	case PromQLFuncSgn:
		for i := range result.Series {
			result.Series[i].Value = EvalSgn(result.Series[i].Value)
			for j := range result.Series[i].Samples {
				result.Series[i].Samples[j].Value = EvalSgn(result.Series[i].Samples[j].Value)
			}
		}
	case PromQLFuncClamp:
		for i := range result.Series {
			result.Series[i].Value = EvalClamp(result.Series[i].Value, 0, 100)
			for j := range result.Series[i].Samples {
				result.Series[i].Samples[j].Value = EvalClamp(result.Series[i].Samples[j].Value, 0, 100)
			}
		}
	case PromQLFuncClampMin:
		for i := range result.Series {
			result.Series[i].Value = EvalClampMin(result.Series[i].Value, 0)
			for j := range result.Series[i].Samples {
				result.Series[i].Samples[j].Value = EvalClampMin(result.Series[i].Samples[j].Value, 0)
			}
		}
	case PromQLFuncClampMax:
		for i := range result.Series {
			result.Series[i].Value = EvalClampMax(result.Series[i].Value, 100)
			for j := range result.Series[i].Samples {
				result.Series[i].Samples[j].Value = EvalClampMax(result.Series[i].Samples[j].Value, 100)
			}
		}
	case PromQLFuncCeil:
		for i := range result.Series {
			result.Series[i].Value = math.Ceil(result.Series[i].Value)
		}
	case PromQLFuncFloor:
		for i := range result.Series {
			result.Series[i].Value = math.Floor(result.Series[i].Value)
		}
	case PromQLFuncRound:
		for i := range result.Series {
			result.Series[i].Value = math.Round(result.Series[i].Value)
		}
	case PromQLFuncAbs:
		for i := range result.Series {
			result.Series[i].Value = math.Abs(result.Series[i].Value)
		}
	case PromQLFuncSortAsc:
		sort.Slice(result.Series, func(a, b int) bool {
			return result.Series[a].Value < result.Series[b].Value
		})
	case PromQLFuncSortDesc:
		sort.Slice(result.Series, func(a, b int) bool {
			return result.Series[a].Value > result.Series[b].Value
		})
	}
	return result
}
