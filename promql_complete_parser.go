package chronicle

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

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

	// Check for extended functions (only if followed by '(')
	lower := strings.ToLower(expr)
	for _, fn := range extendedPromQLFunctions {
		if strings.HasPrefix(lower, fn) {
			rest := strings.TrimSpace(expr[len(fn):])
			if strings.HasPrefix(rest, "(") {
				return p.parseExtendedFunction(expr, fn)
			}
		}
	}

	// Check for binary expressions (e.g., "metric_a + metric_b")
	if q, err := p.parseBinaryExpr(expr); q != nil || err != nil {
		return q, err
	}

	// Fall back to standard parser
	return p.Parse(expr)
}

var extendedPromQLFunctions = []string{
	"histogram_quantile", "label_replace", "label_join",
	"absent_over_time", "absent",
	"avg_over_time", "min_over_time", "max_over_time",
	"sum_over_time", "count_over_time",
	"stddev_over_time", "stdvar_over_time",
	"quantile_over_time", "last_over_time",
	"predict_linear", "holt_winters",
	"histogram_count", "histogram_sum", "histogram_avg", "histogram_fraction",
	"ceil", "floor", "round", "abs", "sgn",
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
	case "rate":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("rate: %w", err)
		}
		query.Function = PromQLAggRate
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "irate":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("irate: %w", err)
		}
		query.Function = PromQLFuncIrate
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "increase":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("increase: %w", err)
		}
		query.Function = PromQLFuncIncrease
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "delta", "idelta", "deriv":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fnName, err)
		}
		switch fnName {
		case "delta":
			query.Function = PromQLFuncDelta
		case "idelta":
			query.Function = PromQLFuncIdelta
		case "deriv":
			query.Function = PromQLFuncDeriv
		}
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "changes":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("changes: %w", err)
		}
		query.Function = PromQLFuncChanges
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "resets":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("resets: %w", err)
		}
		query.Function = PromQLFuncResets
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "avg_over_time", "min_over_time", "max_over_time",
		"sum_over_time", "count_over_time",
		"stddev_over_time", "stdvar_over_time":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fnName, err)
		}
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "quantile_over_time":
		parts := splitPromQLArgs(inner)
		if len(parts) < 2 {
			return nil, fmt.Errorf("quantile_over_time requires 2 arguments")
		}
		query, err := p.ParseComplete(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("quantile_over_time: %w", err)
		}
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "absent_over_time":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("absent_over_time: %w", err)
		}
		query.Aggregation = &PromQLAggregation{Op: PromQLFuncAbsentOverTime}
		return query, nil
	case "predict_linear":
		parts := splitPromQLArgs(inner)
		if len(parts) < 2 {
			return nil, fmt.Errorf("predict_linear requires 2 arguments")
		}
		query, err := p.ParseComplete(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("predict_linear: %w", err)
		}
		query.Function = PromQLFuncPredictLinear
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "last_over_time":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("last_over_time: %w", err)
		}
		query.Function = PromQLFuncLastOverTime
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "holt_winters":
		parts := splitPromQLArgs(inner)
		if len(parts) < 3 {
			return nil, fmt.Errorf("holt_winters requires 3 arguments (vector, sf, tf)")
		}
		query, err := p.ParseComplete(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("holt_winters: %w", err)
		}
		query.Function = PromQLFuncHoltWinters
		if query.Aggregation == nil {
			query.Aggregation = &PromQLAggregation{Op: PromQLAggRate}
		}
		return query, nil
	case "clamp":
		parts := splitPromQLArgs(inner)
		if len(parts) < 3 {
			return nil, fmt.Errorf("clamp requires 3 arguments (vector, min, max)")
		}
		query, err := p.ParseComplete(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("clamp: %w", err)
		}
		query.Function = PromQLFuncClamp
		return query, nil
	case "clamp_min":
		parts := splitPromQLArgs(inner)
		if len(parts) < 2 {
			return nil, fmt.Errorf("clamp_min requires 2 arguments")
		}
		query, err := p.ParseComplete(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("clamp_min: %w", err)
		}
		query.Function = PromQLFuncClampMin
		return query, nil
	case "clamp_max":
		parts := splitPromQLArgs(inner)
		if len(parts) < 2 {
			return nil, fmt.Errorf("clamp_max requires 2 arguments")
		}
		query, err := p.ParseComplete(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("clamp_max: %w", err)
		}
		query.Function = PromQLFuncClampMax
		return query, nil
	case "sgn":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("sgn: %w", err)
		}
		query.Function = PromQLFuncSgn
		return query, nil
	case "histogram_count":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("histogram_count: %w", err)
		}
		query.Function = PromQLFuncHistogramCount
		return query, nil
	case "histogram_sum":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("histogram_sum: %w", err)
		}
		query.Function = PromQLFuncHistogramSum
		return query, nil
	case "histogram_avg":
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("histogram_avg: %w", err)
		}
		query.Function = PromQLFuncHistogramAvg
		return query, nil
	case "histogram_fraction":
		parts := splitPromQLArgs(inner)
		if len(parts) < 3 {
			return nil, fmt.Errorf("histogram_fraction requires 3 arguments (lower, upper, vector)")
		}
		query, err := p.ParseComplete(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, fmt.Errorf("histogram_fraction: %w", err)
		}
		query.Function = PromQLFuncHistogramFraction
		return query, nil
	default:
		// For aggregation operators and simple functions, parse inner expression
		query, err := p.ParseComplete(strings.TrimSpace(inner))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fnName, err)
		}
		// Set aggregation op for extended aggregation operators
		switch fnName {
		case "topk":
			query.Aggregation = &PromQLAggregation{Op: PromQLAggTopk}
		case "bottomk":
			query.Aggregation = &PromQLAggregation{Op: PromQLAggBottomk}
		case "quantile":
			query.Aggregation = &PromQLAggregation{Op: PromQLAggQuantile}
		case "stdvar":
			query.Aggregation = &PromQLAggregation{Op: PromQLAggStdvar}
		case "group":
			query.Aggregation = &PromQLAggregation{Op: PromQLAggGroup}
		case "count_values":
			query.Aggregation = &PromQLAggregation{Op: PromQLAggCountValues}
		case "sort":
			query.Function = PromQLFuncSortAsc
		case "sort_desc":
			query.Function = PromQLFuncSortDesc
		case "abs":
			query.Function = PromQLFuncAbs
		case "ceil":
			query.Function = PromQLFuncCeil
		case "floor":
			query.Function = PromQLFuncFloor
		case "round":
			query.Function = PromQLFuncRound
		case "timestamp":
			query.Function = PromQLFuncTimestamp
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
			if math.IsNaN(v) {
				result[i] = math.NaN()
			} else if v > 0 {
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

	// Ensure we have a +Inf bucket (required by Prometheus spec)
	hasInf := false
	for _, b := range buckets {
		if math.IsInf(b.UpperBound, 1) {
			hasInf = true
			break
		}
	}
	if !hasInf && len(buckets) > 0 {
		// Use the last bucket as total
		buckets = append(buckets, PromQLHistogramBucket{
			UpperBound: math.Inf(1),
			Count:      buckets[len(buckets)-1].Count,
		})
	}

	// Total is from +Inf bucket or last bucket
	total := float64(buckets[len(buckets)-1].Count)
	if total == 0 {
		return math.NaN()
	}

	// Handle edge cases per Prometheus spec
	if q == 0 {
		// Return lowest non-zero boundary
		for _, b := range buckets {
			if b.Count > 0 && !math.IsInf(b.UpperBound, 1) {
				return 0 // q=0 returns lower bound of lowest populated bucket
			}
		}
		return math.NaN()
	}
	if q == 1 {
		// Return the upper bound of the second-highest bucket (before +Inf)
		for i := len(buckets) - 1; i >= 0; i-- {
			if !math.IsInf(buckets[i].UpperBound, 1) {
				return buckets[i].UpperBound
			}
		}
		return math.NaN()
	}

	target := q * total

	for i, bucket := range buckets {
		if math.IsInf(bucket.UpperBound, 1) {
			continue
		}
		if float64(bucket.Count) >= target {
			// Linear interpolation
			prevCount := 0.0
			prevBound := 0.0
			if i > 0 {
				prevCount = float64(buckets[i-1].Count)
				prevBound = buckets[i-1].UpperBound
			}
			countDiff := float64(bucket.Count) - prevCount
			if countDiff == 0 {
				return prevBound
			}
			fraction := (target - prevCount) / countDiff
			return prevBound + fraction*(bucket.UpperBound-prevBound)
		}
	}
	// If target falls into +Inf bucket, return last finite bound
	for i := len(buckets) - 1; i >= 0; i-- {
		if !math.IsInf(buckets[i].UpperBound, 1) {
			return buckets[i].UpperBound
		}
	}
	return math.NaN()
}
