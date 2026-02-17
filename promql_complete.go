package chronicle

import (
	"errors"
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
	PromQLFuncPredictLinear
	PromQLFuncLastOverTime
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
	"predict_linear",
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

// PromQLHistogramBucket represents a histogram bucket for quantile calculation.
type PromQLHistogramBucket struct {
	UpperBound float64 `json:"upper_bound"`
	Count      int64   `json:"count"`
}

// --- Range Function Engine ---

// StalenessWindow is the default Prometheus staleness period (5 minutes).
const StalenessWindow = 5 * time.Minute

// PromQLSample represents a timestamped sample value.
type PromQLSample struct {
	Timestamp int64   // unix milliseconds
	Value     float64
}

// PromQLRangeFunc identifies which range-vector function to apply.
type PromQLRangeFunc int

const (
	RangeFuncRate PromQLRangeFunc = iota
	RangeFuncIncrease
	RangeFuncIrate
	RangeFuncDelta
	RangeFuncIdelta
	RangeFuncDeriv
	RangeFuncChanges
	RangeFuncResets
	RangeFuncAvgOverTime
	RangeFuncMinOverTime
	RangeFuncMaxOverTime
	RangeFuncSumOverTime
	RangeFuncCountOverTime
	RangeFuncStddevOverTime
	RangeFuncStdvarOverTime
	RangeFuncQuantileOverTime
	RangeFuncPredictLinear
	RangeFuncLastOverTime
)

// EvalRangeFunction evaluates a range function over a window of samples.
// Handles counter resets for rate/increase, NaN propagation, and staleness.
func EvalRangeFunction(fn PromQLRangeFunc, samples []PromQLSample, rangeMs int64) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}

	// Filter stale samples
	samples = filterStaleSamples(samples)
	if len(samples) == 0 {
		return math.NaN()
	}

	// NaN propagation: if any sample is NaN, certain functions propagate it
	for _, s := range samples {
		if math.IsNaN(s.Value) {
			switch fn {
			case RangeFuncRate, RangeFuncIncrease, RangeFuncIrate, RangeFuncDelta, RangeFuncIdelta, RangeFuncDeriv:
				return math.NaN()
			case RangeFuncChanges, RangeFuncResets:
				// changes/resets can skip NaN
			default:
				return math.NaN()
			}
		}
	}

	switch fn {
	case RangeFuncRate:
		return evalRate(samples, rangeMs, true)
	case RangeFuncIncrease:
		return evalRate(samples, rangeMs, false)
	case RangeFuncIrate:
		return evalIrate(samples, true)
	case RangeFuncDelta:
		return evalDelta(samples)
	case RangeFuncIdelta:
		return evalIrate(samples, false)
	case RangeFuncDeriv:
		return evalDeriv(samples)
	case RangeFuncChanges:
		return evalChanges(samples)
	case RangeFuncResets:
		return evalResets(samples)
	case RangeFuncAvgOverTime:
		return evalAvgOverTime(samples)
	case RangeFuncMinOverTime:
		return evalMinOverTime(samples)
	case RangeFuncMaxOverTime:
		return evalMaxOverTime(samples)
	case RangeFuncSumOverTime:
		return evalSumOverTime(samples)
	case RangeFuncCountOverTime:
		return float64(len(samples))
	case RangeFuncStddevOverTime:
		return evalStddevOverTime(samples)
	case RangeFuncStdvarOverTime:
		return evalStdvarOverTime(samples)
	case RangeFuncQuantileOverTime:
		return evalQuantileOverTime(samples, 0.5) // default to median
	case RangeFuncPredictLinear:
		return evalPredictLinear(samples, rangeMs)
	case RangeFuncLastOverTime:
		return evalLastOverTime(samples)
	default:
		return math.NaN()
	}
}

// EvalRangeFunctionWithParam evaluates a parameterized range function (e.g., quantile_over_time).
func EvalRangeFunctionWithParam(fn PromQLRangeFunc, samples []PromQLSample, rangeMs int64, param float64) float64 {
	if fn == RangeFuncQuantileOverTime {
		samples = filterStaleSamples(samples)
		if len(samples) == 0 {
			return math.NaN()
		}
		return evalQuantileOverTime(samples, param)
	}
	return EvalRangeFunction(fn, samples, rangeMs)
}

// evalRate computes rate (per-second) or increase (total) over the sample window.
// Handles counter resets by detecting decreases and compensating.
// Uses Prometheus-compatible extrapolation accounting for sample boundaries.
func evalRate(samples []PromQLSample, rangeMs int64, perSecond bool) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}

	var counterResetCompensation float64
	prev := samples[0].Value
	for _, s := range samples[1:] {
		if s.Value < prev {
			// Counter reset detected: add the previous value as compensation
			counterResetCompensation += prev
		}
		prev = s.Value
	}

	totalIncrease := (samples[len(samples)-1].Value - samples[0].Value) + counterResetCompensation

	if totalIncrease < 0 {
		totalIncrease = 0
	}

	// Prometheus-compatible extrapolation: account for partial coverage
	// of the requested range window relative to the actual sample span.
	sampleSpanMs := float64(samples[len(samples)-1].Timestamp - samples[0].Timestamp)
	if sampleSpanMs == 0 {
		return math.NaN()
	}

	// Estimate the average interval between samples
	avgIntervalMs := sampleSpanMs / float64(len(samples)-1)

	// Extend the sample span by half an interval on each side to estimate
	// where the "true" first and last samples would be
	extrapolateToMs := sampleSpanMs + avgIntervalMs

	// Cap extrapolation: don't extrapolate beyond 110% of the range
	requestedMs := float64(rangeMs)
	if extrapolateToMs > requestedMs*1.1 {
		extrapolateToMs = requestedMs * 1.1
	}

	extrapolationFactor := extrapolateToMs / sampleSpanMs
	totalIncrease *= extrapolationFactor

	if perSecond {
		rangeSec := extrapolateToMs / 1000.0
		if rangeSec == 0 {
			return math.NaN()
		}
		return totalIncrease / rangeSec
	}
	return totalIncrease
}

// evalIrate computes instantaneous rate from the last two samples.
func evalIrate(samples []PromQLSample, perSecond bool) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	last := samples[len(samples)-1]
	prev := samples[len(samples)-2]

	diff := last.Value - prev.Value
	if diff < 0 {
		diff = last.Value // counter reset
	}

	if perSecond {
		dtSec := float64(last.Timestamp-prev.Timestamp) / 1000.0
		if dtSec == 0 {
			return math.NaN()
		}
		return diff / dtSec
	}
	return diff
}

// evalDelta computes the difference between last and first sample.
func evalDelta(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	return samples[len(samples)-1].Value - samples[0].Value
}

// evalDeriv uses least-squares linear regression to compute derivative.
func evalDeriv(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	n := float64(len(samples))
	var sumX, sumY, sumXY, sumX2 float64
	for _, s := range samples {
		x := float64(s.Timestamp) / 1000.0
		sumX += x
		sumY += s.Value
		sumXY += x * s.Value
		sumX2 += x * x
	}
	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return math.NaN()
	}
	return (n*sumXY - sumX*sumY) / denom
}

func evalChanges(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	changes := 0.0
	for i := 1; i < len(samples); i++ {
		if !math.IsNaN(samples[i].Value) && !math.IsNaN(samples[i-1].Value) && samples[i].Value != samples[i-1].Value {
			changes++
		}
	}
	return changes
}

func evalResets(samples []PromQLSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	resets := 0.0
	for i := 1; i < len(samples); i++ {
		if !math.IsNaN(samples[i].Value) && !math.IsNaN(samples[i-1].Value) && samples[i].Value < samples[i-1].Value {
			resets++
		}
	}
	return resets
}

func evalAvgOverTime(samples []PromQLSample) float64 {
	sum := 0.0
	count := 0
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			sum += s.Value
			count++
		}
	}
	if count == 0 {
		return math.NaN()
	}
	return sum / float64(count)
}

func evalMinOverTime(samples []PromQLSample) float64 {
	min := math.Inf(1)
	for _, s := range samples {
		if !math.IsNaN(s.Value) && s.Value < min {
			min = s.Value
		}
	}
	if math.IsInf(min, 1) {
		return math.NaN()
	}
	return min
}

func evalMaxOverTime(samples []PromQLSample) float64 {
	max := math.Inf(-1)
	for _, s := range samples {
		if !math.IsNaN(s.Value) && s.Value > max {
			max = s.Value
		}
	}
	if math.IsInf(max, -1) {
		return math.NaN()
	}
	return max
}

func evalSumOverTime(samples []PromQLSample) float64 {
	sum := 0.0
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			sum += s.Value
		}
	}
	return sum
}

func evalStddevOverTime(samples []PromQLSample) float64 {
	v := evalStdvarOverTime(samples)
	if math.IsNaN(v) {
		return math.NaN()
	}
	return math.Sqrt(v)
}

func evalStdvarOverTime(samples []PromQLSample) float64 {
	var sum, sumSq float64
	count := 0
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			sum += s.Value
			sumSq += s.Value * s.Value
			count++
		}
	}
	if count < 2 {
		return math.NaN()
	}
	mean := sum / float64(count)
	return sumSq/float64(count) - mean*mean
}

func evalQuantileOverTime(samples []PromQLSample, q float64) float64 {
	if q < 0 || q > 1 {
		return math.NaN()
	}
	vals := make([]float64, 0, len(samples))
	for _, s := range samples {
		if !math.IsNaN(s.Value) {
			vals = append(vals, s.Value)
		}
	}
	if len(vals) == 0 {
		return math.NaN()
	}
	sort.Float64s(vals)
	if q == 0 {
		return vals[0]
	}
	if q == 1 {
		return vals[len(vals)-1]
	}
	idx := q * float64(len(vals)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(vals) {
		return vals[lower]
	}
	frac := idx - float64(lower)
	return vals[lower]*(1-frac) + vals[upper]*frac
}

// EvalSubquery evaluates a subquery by computing the inner expression at each step within the range.
// Returns a slice of samples representing evaluation results at each step.
func EvalSubquery(innerFn func(ts int64) float64, endMs int64, rangeMs int64, stepMs int64) []PromQLSample {
	if stepMs <= 0 || rangeMs <= 0 {
		return nil
	}
	startMs := endMs - rangeMs
	var results []PromQLSample
	for ts := startMs; ts <= endMs; ts += stepMs {
		val := innerFn(ts)
		results = append(results, PromQLSample{Timestamp: ts, Value: val})
	}
	return results
}

// evalPredictLinear uses linear regression to predict the value at rangeMs into the future.
func evalPredictLinear(samples []PromQLSample, rangeMs int64) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	slope := evalDeriv(samples)
	if math.IsNaN(slope) {
		return math.NaN()
	}
	lastVal := samples[len(samples)-1].Value
	predSec := float64(rangeMs) / 1000.0
	return lastVal + slope*predSec
}

// evalLastOverTime returns the most recent sample value.
func evalLastOverTime(samples []PromQLSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	return samples[len(samples)-1].Value
}

// filterStaleSamples removes samples that are older than StalenessWindow
// relative to the newest sample in the window.
func filterStaleSamples(samples []PromQLSample) []PromQLSample {
	if len(samples) == 0 {
		return nil
	}
	newest := samples[len(samples)-1].Timestamp
	cutoff := newest - StalenessWindow.Milliseconds()
	start := 0
	for i, s := range samples {
		if s.Timestamp >= cutoff {
			start = i
			break
		}
	}
	return samples[start:]
}

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
		return &PromQLEvalResult{}, nil
	}

	// Apply extended aggregation operations
	if pq.Aggregation != nil {
		return e.applyExtendedAgg(pq, points, start, end)
	}

	// Return raw points as series
	return pointsToEvalResult(points), nil
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
		// For standard aggregations, the DB already handled it via ToChronicleQuery
		return pointsToEvalResult(points), nil
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

// --- Recording Rules ---

// PromQLRecordingRule defines a PromQL recording rule that periodically evaluates
// an expression and writes the result as a new time series.
type PromQLRecordingRule struct {
	Name       string            `json:"name"`
	Expression string            `json:"expression"`
	Labels     map[string]string `json:"labels,omitempty"`
	Interval   time.Duration     `json:"interval"`
}

// PromQLRecordingRuleEngine evaluates PromQL recording rules on a schedule.
type PromQLRecordingRuleEngine struct {
	db     *DB
	rules  []PromQLRecordingRule
	parser *PromQLCompleteParser
	mu     sync.RWMutex
	stopCh chan struct{}
}

// NewPromQLRecordingRuleEngine creates a new recording rule engine.
func NewPromQLRecordingRuleEngine(db *DB) *PromQLRecordingRuleEngine {
	return &PromQLRecordingRuleEngine{
		db:     db,
		parser: NewPromQLCompleteParser(),
		stopCh: make(chan struct{}),
	}
}

// AddRule registers a recording rule.
func (e *PromQLRecordingRuleEngine) AddRule(rule PromQLRecordingRule) error {
	if rule.Name == "" {
		return errors.New("recording rule name is required")
	}
	if rule.Expression == "" {
		return errors.New("recording rule expression is required")
	}
	if rule.Interval <= 0 {
		rule.Interval = time.Minute
	}

	// Validate the expression parses
	if _, err := e.parser.ParseComplete(rule.Expression); err != nil {
		return fmt.Errorf("invalid recording rule expression: %w", err)
	}

	e.mu.Lock()
	e.rules = append(e.rules, rule)
	e.mu.Unlock()
	return nil
}

// Rules returns a copy of all registered recording rules.
func (e *PromQLRecordingRuleEngine) Rules() []PromQLRecordingRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]PromQLRecordingRule, len(e.rules))
	copy(out, e.rules)
	return out
}

// EvaluateRule evaluates a single recording rule and writes the result.
func (e *PromQLRecordingRuleEngine) EvaluateRule(rule PromQLRecordingRule) error {
	now := time.Now()
	pq, err := e.parser.ParseComplete(rule.Expression)
	if err != nil {
		return fmt.Errorf("parse recording rule %q: %w", rule.Name, err)
	}

	cq := pq.ToChronicleQuery(now.Add(-rule.Interval).UnixNano(), now.UnixNano())
	result, err := e.db.Execute(cq)
	if err != nil {
		return fmt.Errorf("evaluate recording rule %q: %w", rule.Name, err)
	}

	// Write aggregated result as the new metric
	if result != nil && len(result.Points) > 0 {
		tags := make(map[string]string)
		for k, v := range rule.Labels {
			tags[k] = v
		}
		lastPoint := result.Points[len(result.Points)-1]
		return e.db.Write(Point{
			Metric:    rule.Name,
			Value:     lastPoint.Value,
			Tags:      tags,
			Timestamp: now.UnixNano(),
		})
	}
	return nil
}

// Start begins periodic evaluation of all recording rules.
func (e *PromQLRecordingRuleEngine) Start() {
	go func() {
		ticker := time.NewTicker(time.Second * 15)
		defer ticker.Stop()
		for {
			select {
			case <-e.stopCh:
				return
			case <-ticker.C:
				e.mu.RLock()
				rules := make([]PromQLRecordingRule, len(e.rules))
				copy(rules, e.rules)
				e.mu.RUnlock()

				for _, rule := range rules {
					_ = e.EvaluateRule(rule)
				}
			}
		}
	}()
}

// Stop halts the recording rule engine.
func (e *PromQLRecordingRuleEngine) Stop() {
	close(e.stopCh)
}

// PromQLLabelReplace performs label_replace on a set of labels.
func PromQLLabelReplace(labels map[string]string, dstLabel, replacement, srcLabel, regexStr string) (map[string]string, error) {
	if labels == nil {
		labels = map[string]string{}
	}
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
	if labels == nil {
		labels = map[string]string{}
	}
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
		{"range_selector_seconds", "node_cpu[30s]", "selector"},
		{"range_selector_days", "node_cpu[7d]", "selector"},

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
		{"agg_by_multi", `sum by (method, status) (http_requests_total)`, "aggregation"},
		{"agg_without_multi", `avg without (instance, pod) (http_requests_total)`, "aggregation"},

		// Extended aggregation functions
		{"agg_topk", `topk(5, http_requests_total)`, "aggregation"},
		{"agg_bottomk", `bottomk(3, http_requests_total)`, "aggregation"},
		{"agg_count_values", `count_values("version", build_info)`, "aggregation"},
		{"agg_quantile", `quantile(0.9, http_request_duration)`, "aggregation"},
		{"agg_stdvar", `stdvar(http_request_duration)`, "aggregation"},
		{"agg_group", `group(http_requests_total)`, "aggregation"},

		// Extended functions
		{"func_absent", "absent(nonexistent_metric)", "function"},
		{"func_absent_over_time", "absent_over_time(nonexistent_metric[5m])", "function"},
		{"func_histogram_quantile", `histogram_quantile(0.95, http_request_duration_bucket)`, "function"},
		{"func_label_replace", `label_replace(up, "host", "$1", "instance", "(.*):.*")`, "function"},
		{"func_label_join", `label_join(up, "combined", "-", "job", "instance")`, "function"},
		{"func_vector", "vector(1)", "function"},
		{"func_scalar", "scalar(http_requests_total)", "function"},

		// Range functions
		{"func_rate", "rate(http_requests_total[5m])", "range_function"},
		{"func_irate", "irate(http_requests_total[5m])", "range_function"},
		{"func_increase", "increase(http_requests_total[1h])", "range_function"},
		{"func_delta", "delta(temperature[10m])", "range_function"},
		{"func_idelta", "idelta(temperature[5m])", "range_function"},
		{"func_deriv", "deriv(predictions_total[30m])", "range_function"},
		{"func_changes", "changes(up[10m])", "range_function"},
		{"func_resets", "resets(http_requests_total[1h])", "range_function"},

		// Over-time range functions
		{"func_avg_over_time", "avg_over_time(http_requests_total[5m])", "range_function"},
		{"func_min_over_time", "min_over_time(temperature[10m])", "range_function"},
		{"func_max_over_time", "max_over_time(temperature[10m])", "range_function"},
		{"func_sum_over_time", "sum_over_time(http_requests_total[1h])", "range_function"},
		{"func_count_over_time", "count_over_time(up[5m])", "range_function"},
		{"func_stddev_over_time", "stddev_over_time(temperature[1h])", "range_function"},
		{"func_stdvar_over_time", "stdvar_over_time(temperature[1h])", "range_function"},
		{"func_quantile_over_time", "quantile_over_time(0.95, http_request_duration[5m])", "range_function"},

		// Math functions
		{"func_abs", "abs(temperature)", "function"},
		{"func_ceil", "ceil(latency)", "function"},
		{"func_floor", "floor(latency)", "function"},
		{"func_round", "round(latency)", "function"},
		{"func_clamp", "clamp(temperature, 0, 100)", "function"},
		{"func_clamp_min", "clamp_min(temperature, 0)", "function"},
		{"func_clamp_max", "clamp_max(temperature, 100)", "function"},

		// Time functions
		{"func_timestamp", "timestamp(up)", "function"},
		{"func_day_of_month", "day_of_month(timestamp(up))", "function"},
		{"func_day_of_week", "day_of_week(timestamp(up))", "function"},
		{"func_days_in_month", "days_in_month(timestamp(up))", "function"},
		{"func_hour", "hour(timestamp(up))", "function"},
		{"func_minute", "minute(timestamp(up))", "function"},
		{"func_month", "month(timestamp(up))", "function"},
		{"func_year", "year(timestamp(up))", "function"},

		// Sort functions
		{"func_sort", "sort(http_requests_total)", "function"},
		{"func_sort_desc", "sort_desc(http_requests_total)", "function"},

		// Binary operations
		{"bin_add", "metric_a + metric_b", "binary"},
		{"bin_sub", "metric_a - metric_b", "binary"},
		{"bin_mul", "metric_a * metric_b", "binary"},
		{"bin_div", "metric_a / metric_b", "binary"},
		{"bin_mod", "metric_a % metric_b", "binary"},
		{"bin_pow", "metric_a ^ metric_b", "binary"},
		{"bin_gt", "metric_a > metric_b", "binary"},
		{"bin_lt", "metric_a < metric_b", "binary"},
		{"bin_gte", "metric_a >= metric_b", "binary"},
		{"bin_lte", "metric_a <= metric_b", "binary"},
		{"bin_eq", "metric_a == metric_b", "binary"},
		{"bin_neq", "metric_a != metric_b", "binary"},
		{"bin_and", "metric_a and metric_b", "binary"},
		{"bin_or", "metric_a or metric_b", "binary"},
		{"bin_unless", "metric_a unless metric_b", "binary"},
		{"bin_scalar_add", "http_requests_total + 5", "binary"},
		{"bin_scalar_mul", "http_requests_total * 2", "binary"},

		// Offset modifier
		{"offset_positive", "http_requests_total offset 5m", "modifier"},
		{"offset_negative", "http_requests_total offset -5m", "modifier"},

		// @ modifier
		{"at_modifier", "http_requests_total @ 1609459200", "modifier"},

		// Combined expressions
		{"combined_rate_sum", `sum(rate(http_requests_total{status="200"}[5m])) by (method)`, "combined"},
		{"combined_nested_rate", `increase(http_requests_total{code=~"2.."}[1h])`, "combined"},
		{"combined_rate_div", `rate(http_errors_total[5m]) / rate(http_requests_total[5m])`, "combined"},
		{"combined_histogram_q", `histogram_quantile(0.99, rate(http_request_duration_bucket[5m]))`, "combined"},
		{"combined_absent_label", `absent(up{job="prometheus"})`, "combined"},
		{"combined_nested_agg", `max(rate(http_requests_total[5m])) by (instance)`, "combined"},

		// Subquery
		{"subquery_basic", `http_requests_total[1h:5m]`, "subquery"},

		// Predict linear and last_over_time
		{"func_predict_linear", `predict_linear(http_requests_total[5m], 3600)`, "range_function"},
		{"func_last_over_time", `last_over_time(temperature[10m])`, "range_function"},

		// Nested extended functions
		{"nested_topk_rate", `topk(5, rate(http_requests_total[5m]))`, "combined"},
		{"nested_bottomk_avg", `bottomk(3, avg_over_time(temperature[1h]))`, "combined"},
		{"nested_quantile_rate", `quantile(0.99, rate(http_requests_total[5m]))`, "combined"},
		{"nested_count_values", `count_values("status", http_responses_total)`, "combined"},
		{"nested_stdvar_rate", `stdvar(rate(http_request_duration[5m]))`, "combined"},
		{"nested_group_by", `group(http_requests_total) by (method)`, "combined"},
		{"nested_absent_labels", `absent(nonexistent{job="test",env="prod"})`, "combined"},
		{"nested_absent_over_time", `absent_over_time(nonexistent[5m])`, "combined"},

		// Multi-level nesting
		{"multi_rate_sum_topk", `topk(3, sum(rate(http_requests_total[5m])) by (job))`, "combined"},
		{"multi_histogram_sum", `histogram_quantile(0.95, sum(rate(http_request_duration_bucket[5m])) by (le))`, "combined"},
		{"multi_bool_filter", `http_requests_total > 100`, "combined"},
		{"multi_binary_rate", `rate(errors_total[5m]) / rate(requests_total[5m])`, "combined"},
		{"multi_clamp_rate", `clamp(rate(http_requests_total[5m]), 0, 100)`, "combined"},
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

// PromQLBinaryExpr represents a binary operation between two sub-expressions.
type PromQLBinaryExpr struct {
	Op    PromQLBinaryOp
	Left  *PromQLQuery
	Right *PromQLQuery
}

// String returns the operator symbol for a binary op.
func (op PromQLBinaryOp) String() string {
	switch op {
	case PromQLBinAdd:
		return "+"
	case PromQLBinSub:
		return "-"
	case PromQLBinMul:
		return "*"
	case PromQLBinDiv:
		return "/"
	case PromQLBinMod:
		return "%"
	case PromQLBinPow:
		return "^"
	case PromQLBinEqual:
		return "=="
	case PromQLBinNotEqual:
		return "!="
	case PromQLBinGreaterThan:
		return ">"
	case PromQLBinLessThan:
		return "<"
	case PromQLBinGreaterOrEqual:
		return ">="
	case PromQLBinLessOrEqual:
		return "<="
	case PromQLBinAnd:
		return "and"
	case PromQLBinOr:
		return "or"
	case PromQLBinUnless:
		return "unless"
	default:
		return "?"
	}
}

// binaryOpTokens maps operator strings to PromQLBinaryOp values,
// ordered longest-first so ">=" is matched before ">".
var binaryOpTokens = []struct {
	token string
	op    PromQLBinaryOp
}{
	{">=", PromQLBinGreaterOrEqual},
	{"<=", PromQLBinLessOrEqual},
	{"!=", PromQLBinNotEqual},
	{"==", PromQLBinEqual},
	{"+", PromQLBinAdd},
	{"-", PromQLBinSub},
	{"*", PromQLBinMul},
	{"/", PromQLBinDiv},
	{"%", PromQLBinMod},
	{"^", PromQLBinPow},
	{">", PromQLBinGreaterThan},
	{"<", PromQLBinLessThan},
}

// parseBinaryExpr attempts to split expr on a binary operator outside
// parentheses/braces/brackets. Returns nil, nil if no binary op is found.
func (p *PromQLCompleteParser) parseBinaryExpr(expr string) (*PromQLQuery, error) {
	// Try keyword ops first ("and", "or", "unless")
	for _, kw := range []struct {
		word string
		op   PromQLBinaryOp
	}{
		{" unless ", PromQLBinUnless},
		{" and ", PromQLBinAnd},
		{" or ", PromQLBinOr},
	} {
		if idx := findOpOutsideBrackets(expr, kw.word); idx >= 0 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(kw.word):])
			return p.buildBinaryQuery(left, right, kw.op)
		}
	}

	// Try symbol ops from lowest to highest precedence.
	// findOpOutsideBrackets returns the rightmost match, so we split at the
	// lowest-precedence operator first, giving higher-precedence ops to sub-trees.
	// Precedence (low→high): comparison < add/sub < mul/div/mod < power
	for _, level := range [][]struct {
		token string
		op    PromQLBinaryOp
	}{
		{{"==", PromQLBinEqual}, {"!=", PromQLBinNotEqual}, {">=", PromQLBinGreaterOrEqual}, {"<=", PromQLBinLessOrEqual}, {">", PromQLBinGreaterThan}, {"<", PromQLBinLessThan}},
		{{"+", PromQLBinAdd}, {"-", PromQLBinSub}},
		{{"*", PromQLBinMul}, {"/", PromQLBinDiv}, {"%", PromQLBinMod}},
		{{"^", PromQLBinPow}},
	} {
		for _, tok := range level {
			if idx := findOpOutsideBrackets(expr, tok.token); idx >= 0 {
				left := strings.TrimSpace(expr[:idx])
				right := strings.TrimSpace(expr[idx+len(tok.token):])
				if left == "" || right == "" {
					continue
				}
				return p.buildBinaryQuery(left, right, tok.op)
			}
		}
	}

	return nil, nil
}

// findOpOutsideBrackets finds the last occurrence of op in expr that is not
// inside parentheses, braces, or brackets. Returns -1 if not found.
func findOpOutsideBrackets(expr, op string) int {
	depth := 0
	best := -1
	for i := 0; i <= len(expr)-len(op); i++ {
		switch expr[i] {
		case '(', '{', '[':
			depth++
		case ')', '}', ']':
			depth--
		}
		if depth == 0 && expr[i:i+len(op)] == op {
			// For multi-char ops, avoid matching a substring of a longer op
			if len(op) == 1 && (op[0] == '>' || op[0] == '<' || op[0] == '!' || op[0] == '=') {
				if i+1 < len(expr) && expr[i+1] == '=' {
					continue
				}
			}
			best = i
		}
	}
	return best
}

func (p *PromQLCompleteParser) buildBinaryQuery(leftExpr, rightExpr string, op PromQLBinaryOp) (*PromQLQuery, error) {
	left, err := p.ParseComplete(leftExpr)
	if err != nil {
		return nil, fmt.Errorf("left side of %s: %w", op, err)
	}
	right, err := p.ParseComplete(rightExpr)
	if err != nil {
		return nil, fmt.Errorf("right side of %s: %w", op, err)
	}
	return &PromQLQuery{
		Metric: left.Metric,
		Labels: left.Labels,
		BinaryExpr: &PromQLBinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
		},
	}, nil
}

// ApplyBinaryOp applies a binary operation element-wise to two float slices.
// Slices are aligned by index; the shorter one determines the result length.
func ApplyBinaryOp(op PromQLBinaryOp, left, right []float64) []float64 {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = evalBinaryScalar(op, left[i], right[i])
	}
	return result
}

func evalBinaryScalar(op PromQLBinaryOp, a, b float64) float64 {
	switch op {
	case PromQLBinAdd:
		return a + b
	case PromQLBinSub:
		return a - b
	case PromQLBinMul:
		return a * b
	case PromQLBinDiv:
		if b == 0 {
			return math.NaN()
		}
		return a / b
	case PromQLBinMod:
		if b == 0 {
			return math.NaN()
		}
		return math.Mod(a, b)
	case PromQLBinPow:
		return math.Pow(a, b)
	case PromQLBinEqual:
		if a == b {
			return a
		}
		return math.NaN()
	case PromQLBinNotEqual:
		if a != b {
			return a
		}
		return math.NaN()
	case PromQLBinGreaterThan:
		if a > b {
			return a
		}
		return math.NaN()
	case PromQLBinLessThan:
		if a < b {
			return a
		}
		return math.NaN()
	case PromQLBinGreaterOrEqual:
		if a >= b {
			return a
		}
		return math.NaN()
	case PromQLBinLessOrEqual:
		if a <= b {
			return a
		}
		return math.NaN()
	default:
		return math.NaN()
	}
}
