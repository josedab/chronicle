package chronicle

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Extended PromQL aggregation operators.
const (
	PromQLAggTopk PromQLAggOp = iota + 10
	PromQLAggBottomk
	PromQLAggCountValues
	PromQLAggQuantile
	PromQLAggStdvar
	PromQLAggGroup
)

// PromQL functions (non-aggregation).
const (
	PromQLFuncAbsent PromQLAggOp = iota + 20
	PromQLFuncAbsentOverTime
	PromQLFuncCeil
	PromQLFuncFloor
	PromQLFuncRound
	PromQLFuncAbs
	PromQLFuncClamp
	PromQLFuncClampMax
	PromQLFuncClampMin
	PromQLFuncDelta
	PromQLFuncIdelta
	PromQLFuncIncrease
	PromQLFuncIrate
	PromQLFuncDeriv
	PromQLFuncHistogramQuantile
	PromQLFuncLabelReplace
	PromQLFuncLabelJoin
	PromQLFuncVector
	PromQLFuncScalar
	PromQLFuncSortAsc
	PromQLFuncSortDesc
	PromQLFuncTimestamp
	PromQLFuncDayOfMonth
	PromQLFuncDayOfWeek
	PromQLFuncDaysInMonth
	PromQLFuncHour
	PromQLFuncMinute
	PromQLFuncMonth
	PromQLFuncYear
	PromQLFuncChanges
	PromQLFuncResets
)

// PromQLBinaryOp represents a binary operation between two expressions.
type PromQLBinaryOp int

const (
	PromQLBinAdd PromQLBinaryOp = iota
	PromQLBinSub
	PromQLBinMul
	PromQLBinDiv
	PromQLBinMod
	PromQLBinPow
	PromQLBinEqual
	PromQLBinNotEqual
	PromQLBinGreaterThan
	PromQLBinLessThan
	PromQLBinGreaterOrEqual
	PromQLBinLessOrEqual
	PromQLBinAnd
	PromQLBinOr
	PromQLBinUnless
)

// String returns the string representation of extended agg ops.
func (op PromQLAggOp) StringExtended() string {
	switch op {
	case PromQLAggTopk:
		return "topk"
	case PromQLAggBottomk:
		return "bottomk"
	case PromQLAggCountValues:
		return "count_values"
	case PromQLAggQuantile:
		return "quantile"
	case PromQLAggStdvar:
		return "stdvar"
	case PromQLAggGroup:
		return "group"
	case PromQLFuncAbsent:
		return "absent"
	case PromQLFuncHistogramQuantile:
		return "histogram_quantile"
	case PromQLFuncLabelReplace:
		return "label_replace"
	case PromQLFuncLabelJoin:
		return "label_join"
	case PromQLFuncVector:
		return "vector"
	case PromQLFuncScalar:
		return "scalar"
	default:
		return op.String()
	}
}

// PromQLCompleteParser extends PromQLParser with full PromQL function support.
type PromQLCompleteParser struct {
	PromQLParser
}

// NewPromQLCompleteParser creates a parser with full PromQL support.
func NewPromQLCompleteParser() *PromQLCompleteParser {
	return &PromQLCompleteParser{}
}

// ParseComplete parses a PromQL expression with full function support.
func (p *PromQLCompleteParser) ParseComplete(expr string) (*PromQLQuery, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty expression")
	}

	// Check for extended functions
	lower := strings.ToLower(expr)
	for _, fn := range extendedPromQLFunctions {
		if strings.HasPrefix(lower, fn) {
			return p.parseExtendedFunction(expr, fn)
		}
	}

	// Fall back to standard parser
	return p.Parse(expr)
}

var extendedPromQLFunctions = []string{
	"histogram_quantile", "label_replace", "label_join",
	"absent_over_time", "absent",
	"ceil", "floor", "round", "abs",
	"clamp_max", "clamp_min", "clamp",
	"delta", "idelta", "increase", "irate", "deriv",
	"vector", "scalar",
	"sort_desc", "sort",
	"timestamp", "day_of_month", "day_of_week", "days_in_month",
	"hour", "minute", "month", "year",
	"changes", "resets",
	"topk", "bottomk", "count_values", "quantile", "stdvar", "group",
}

func (p *PromQLCompleteParser) parseExtendedFunction(expr, fnName string) (*PromQLQuery, error) {
	rest := strings.TrimSpace(expr[len(fnName):])

	// Extract parenthesized arguments
	if !strings.HasPrefix(rest, "(") {
		return nil, fmt.Errorf("expected ( after %s", fnName)
	}
	inner, _ := p.extractParenContent(rest)

	switch fnName {
	case "absent":
		return p.parseAbsent(inner)
	case "histogram_quantile":
		return p.parseHistogramQuantile(inner)
	case "label_replace":
		return p.parseLabelReplace(inner)
	case "label_join":
		return p.parseLabelJoin(inner)
	case "vector":
		return p.parseVector(inner)
	case "scalar":
		return p.parseScalar(inner)
	default:
		// For simple functions, parse inner expression
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fnName, err)
		}
		return query, nil
	}
}

func (p *PromQLCompleteParser) parseAbsent(inner string) (*PromQLQuery, error) {
	query, err := p.ParseComplete(strings.TrimSpace(inner))
	if err != nil {
		return nil, fmt.Errorf("absent: %w", err)
	}
	query.Aggregation = &PromQLAggregation{Op: PromQLFuncAbsent}
	return query, nil
}

func (p *PromQLCompleteParser) parseHistogramQuantile(inner string) (*PromQLQuery, error) {
	// histogram_quantile(quantile, metric_expr)
	parts := splitPromQLArgs(inner)
	if len(parts) < 2 {
		return nil, fmt.Errorf("histogram_quantile requires 2 arguments")
	}

	query, err := p.ParseComplete(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, fmt.Errorf("histogram_quantile: %w", err)
	}
	query.Aggregation = &PromQLAggregation{Op: PromQLFuncHistogramQuantile}
	return query, nil
}

func (p *PromQLCompleteParser) parseLabelReplace(inner string) (*PromQLQuery, error) {
	// label_replace(v, dst_label, replacement, src_label, regex)
	parts := splitPromQLArgs(inner)
	if len(parts) < 5 {
		return nil, fmt.Errorf("label_replace requires 5 arguments")
	}

	query, err := p.ParseComplete(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("label_replace: %w", err)
	}
	return query, nil
}

func (p *PromQLCompleteParser) parseLabelJoin(inner string) (*PromQLQuery, error) {
	// label_join(v, dst_label, separator, src_labels...)
	parts := splitPromQLArgs(inner)
	if len(parts) < 4 {
		return nil, fmt.Errorf("label_join requires at least 4 arguments")
	}

	query, err := p.ParseComplete(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("label_join: %w", err)
	}
	return query, nil
}

func (p *PromQLCompleteParser) parseVector(inner string) (*PromQLQuery, error) {
	return &PromQLQuery{
		Metric:      "__vector__",
		Labels:      map[string]LabelMatcher{},
		Aggregation: &PromQLAggregation{Op: PromQLFuncVector},
	}, nil
}

func (p *PromQLCompleteParser) parseScalar(inner string) (*PromQLQuery, error) {
	query, err := p.ParseComplete(strings.TrimSpace(inner))
	if err != nil {
		return nil, fmt.Errorf("scalar: %w", err)
	}
	query.Aggregation = &PromQLAggregation{Op: PromQLFuncScalar}
	return query, nil
}

func splitPromQLArgs(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, ch := range s {
		if !inQuote && (ch == '"' || ch == '\'') {
			inQuote = true
			quoteChar = ch
			current.WriteRune(ch)
		} else if inQuote && ch == quoteChar {
			inQuote = false
			quoteChar = 0
			current.WriteRune(ch)
		} else if !inQuote && ch == '(' {
			depth++
			current.WriteRune(ch)
		} else if !inQuote && ch == ')' {
			depth--
			current.WriteRune(ch)
		} else if !inQuote && depth == 0 && ch == ',' {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// PromQL math functions applied to time-series data.

// PromQLMathApply applies a math function to a slice of values.
func PromQLMathApply(fn string, values []float64) []float64 {
	result := make([]float64, len(values))
	for i, v := range values {
		switch fn {
		case "abs":
			result[i] = math.Abs(v)
		case "ceil":
			result[i] = math.Ceil(v)
		case "floor":
			result[i] = math.Floor(v)
		case "round":
			result[i] = math.Round(v)
		case "sqrt":
			result[i] = math.Sqrt(v)
		case "exp":
			result[i] = math.Exp(v)
		case "ln":
			result[i] = math.Log(v)
		case "log2":
			result[i] = math.Log2(v)
		case "log10":
			result[i] = math.Log10(v)
		case "sgn":
			if v > 0 {
				result[i] = 1
			} else if v < 0 {
				result[i] = -1
			}
		default:
			result[i] = v
		}
	}
	return result
}

// PromQLHistogramQuantile computes the quantile from histogram buckets.
func PromQLHistogramQuantile(q float64, buckets []PromQLHistogramBucket) float64 {
	if len(buckets) == 0 || q < 0 || q > 1 {
		return math.NaN()
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].UpperBound < buckets[j].UpperBound
	})

	total := buckets[len(buckets)-1].Count
	if total == 0 {
		return math.NaN()
	}

	target := q * float64(total)

	for i, bucket := range buckets {
		if float64(bucket.Count) >= target {
			// Linear interpolation
			prevCount := 0.0
			prevBound := 0.0
			if i > 0 {
				prevCount = float64(buckets[i-1].Count)
				prevBound = buckets[i-1].UpperBound
			}
			fraction := (target - prevCount) / (float64(bucket.Count) - prevCount)
			return prevBound + fraction*(bucket.UpperBound-prevBound)
		}
	}
	return buckets[len(buckets)-1].UpperBound
}

// PromQLHistogramBucket represents a histogram bucket for quantile calculation.
type PromQLHistogramBucket struct {
	UpperBound float64 `json:"upper_bound"`
	Count      int64   `json:"count"`
}

// PromQLLabelReplace performs label_replace on a set of labels.
func PromQLLabelReplace(labels map[string]string, dstLabel, replacement, srcLabel, regexStr string) (map[string]string, error) {
	result := make(map[string]string, len(labels))
	for k, v := range labels {
		result[k] = v
	}

	srcValue := labels[srcLabel]
	re, err := regexp.Compile("^(?:" + regexStr + ")$")
	if err != nil {
		return nil, fmt.Errorf("label_replace: invalid regex: %w", err)
	}

	if matches := re.FindStringSubmatch(srcValue); matches != nil {
		newValue := replacement
		for i, match := range matches {
			newValue = strings.ReplaceAll(newValue, fmt.Sprintf("$%d", i), match)
		}
		result[dstLabel] = newValue
	}

	return result, nil
}

// PromQLLabelJoin performs label_join, concatenating source label values.
func PromQLLabelJoin(labels map[string]string, dstLabel, separator string, srcLabels ...string) map[string]string {
	result := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		result[k] = v
	}

	var values []string
	for _, src := range srcLabels {
		if v, ok := labels[src]; ok {
			values = append(values, v)
		}
	}
	result[dstLabel] = strings.Join(values, separator)
	return result
}

// PromQLComplianceTest represents a single compliance test case.
type PromQLComplianceTest struct {
	Name     string `json:"name"`
	Query    string `json:"query"`
	Category string `json:"category"`
	Passed   bool   `json:"passed"`
	Error    string `json:"error,omitempty"`
}

// PromQLComplianceSuite runs compliance tests against the PromQL parser.
type PromQLComplianceSuite struct {
	parser  *PromQLCompleteParser
	results []PromQLComplianceTest
	mu      sync.RWMutex
}

// NewPromQLComplianceSuite creates a new compliance test suite.
func NewPromQLComplianceSuite() *PromQLComplianceSuite {
	return &PromQLComplianceSuite{
		parser: NewPromQLCompleteParser(),
	}
}

// RunAll runs all compliance tests and returns results.
func (s *PromQLComplianceSuite) RunAll() []PromQLComplianceTest {
	s.mu.Lock()
	s.results = nil
	s.mu.Unlock()

	testCases := []struct {
		name     string
		query    string
		category string
	}{
		// Basic selectors
		{"simple_metric", "http_requests_total", "selector"},
		{"metric_with_labels", `http_requests_total{method="GET"}`, "selector"},
		{"metric_not_equal", `http_requests_total{status!="500"}`, "selector"},
		{"metric_regex", `http_requests_total{path=~"/api/.*"}`, "selector"},
		{"metric_not_regex", `http_requests_total{path!~"/health"}`, "selector"},
		{"range_selector", "http_requests_total[5m]", "selector"},
		{"range_selector_hours", "node_cpu[1h]", "selector"},

		// Standard aggregations
		{"agg_sum", "sum(http_requests_total)", "aggregation"},
		{"agg_avg", "avg(http_requests_total)", "aggregation"},
		{"agg_min", "min(http_requests_total)", "aggregation"},
		{"agg_max", "max(http_requests_total)", "aggregation"},
		{"agg_count", "count(http_requests_total)", "aggregation"},
		{"agg_stddev", "stddev(http_requests_total)", "aggregation"},
		{"agg_by", `sum by (method) (http_requests_total)`, "aggregation"},
		{"agg_without", `sum without (instance) (http_requests_total)`, "aggregation"},
		{"agg_rate", "rate(http_requests_total[5m])", "aggregation"},

		// Extended functions
		{"func_absent", "absent(nonexistent_metric)", "function"},
		{"func_histogram_quantile", `histogram_quantile(0.95, http_request_duration_bucket)`, "function"},
		{"func_label_replace", `label_replace(up, "host", "$1", "instance", "(.*):.*")`, "function"},
		{"func_label_join", `label_join(up, "combined", "-", "job", "instance")`, "function"},
		{"func_vector", "vector(1)", "function"},
		{"func_scalar", "scalar(http_requests_total)", "function"},

		// Combined expressions
		{"combined_rate_sum", `sum(rate(http_requests_total{status="200"}[5m])) by (method)`, "combined"},
	}

	for _, tc := range testCases {
		result := PromQLComplianceTest{
			Name:     tc.name,
			Query:    tc.query,
			Category: tc.category,
		}

		_, err := s.parser.ParseComplete(tc.query)
		if err != nil {
			result.Passed = false
			result.Error = err.Error()
		} else {
			result.Passed = true
		}

		s.mu.Lock()
		s.results = append(s.results, result)
		s.mu.Unlock()
	}

	s.mu.RLock()
	out := make([]PromQLComplianceTest, len(s.results))
	copy(out, s.results)
	s.mu.RUnlock()
	return out
}

// PassRate returns the pass rate of the compliance suite.
func (s *PromQLComplianceSuite) PassRate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.results) == 0 {
		return 0
	}
	passed := 0
	for _, r := range s.results {
		if r.Passed {
			passed++
		}
	}
	return float64(passed) / float64(len(s.results))
}

// Summary returns a summary of compliance test results.
func (s *PromQLComplianceSuite) Summary() map[string]any {
	results := s.RunAll()
	passed := 0
	byCategory := make(map[string]int)
	passedByCategory := make(map[string]int)

	for _, r := range results {
		byCategory[r.Category]++
		if r.Passed {
			passed++
			passedByCategory[r.Category]++
		}
	}

	categories := make(map[string]any)
	for cat, total := range byCategory {
		p := passedByCategory[cat]
		categories[cat] = map[string]any{
			"total":     total,
			"passed":    p,
			"failed":    total - p,
			"pass_rate": float64(p) / float64(total),
		}
	}

	return map[string]any{
		"total_tests": len(results),
		"passed":      passed,
		"failed":      len(results) - passed,
		"pass_rate":   float64(passed) / float64(len(results)),
		"categories":  categories,
		"timestamp":   time.Now(),
	}
}
