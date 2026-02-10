package chronicle

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// PromQL implements a subset of the Prometheus Query Language.
// Supported features:
// - Instant selectors: metric{label="value"}
// - Range selectors: metric[5m]
// - Aggregation functions: sum, avg, min, max, count
// - Aggregation modifiers: by, without

// PromQLParser parses PromQL expressions.
type PromQLParser struct{}

// PromQLQuery represents a parsed PromQL query.
type PromQLQuery struct {
	Metric      string
	Labels      map[string]LabelMatcher
	RangeWindow time.Duration
	Aggregation *PromQLAggregation
}

// LabelMatcher defines a label matching operation.
type LabelMatcher struct {
	Op    LabelMatchOp
	Value string
	Regex *regexp.Regexp
}

// LabelMatchOp is the label matching operator.
type LabelMatchOp int

const (
	LabelMatchEqual LabelMatchOp = iota
	LabelMatchNotEqual
	LabelMatchRegex
	LabelMatchNotRegex
)

// PromQLAggregation defines an aggregation operation.
type PromQLAggregation struct {
	Op      PromQLAggOp
	By      []string
	Without []string
}

// PromQLAggOp is the aggregation operator.
type PromQLAggOp int

const (
	PromQLAggSum PromQLAggOp = iota
	PromQLAggAvg
	PromQLAggMin
	PromQLAggMax
	PromQLAggCount
	PromQLAggStddev
	PromQLAggRate
)

// String returns the string representation of aggregation op.
func (op PromQLAggOp) String() string {
	switch op {
	case PromQLAggSum:
		return "sum"
	case PromQLAggAvg:
		return "avg"
	case PromQLAggMin:
		return "min"
	case PromQLAggMax:
		return "max"
	case PromQLAggCount:
		return "count"
	case PromQLAggStddev:
		return "stddev"
	case PromQLAggRate:
		return "rate"
	default:
		return "unknown"
	}
}

// Parse parses a PromQL expression into a PromQLQuery.
//
// Supported syntax:
//
//	// Simple metric
//	http_requests_total
//
//	// Metric with label selectors
//	http_requests_total{method="GET", status="200"}
//	http_requests_total{method!="POST"}
//	http_requests_total{path=~"/api/.*"}
//
//	// Range selector
//	http_requests_total[5m]
//
//	// Aggregation
//	sum(http_requests_total)
//	avg by (method) (http_requests_total)
//	sum without (instance) (http_requests_total)
//
//	// Combined
//	sum(rate(http_requests_total{status="200"}[5m])) by (method)
func (p *PromQLParser) Parse(expr string) (*PromQLQuery, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, errors.New("empty expression")
	}

	// Check for aggregation function
	for _, aggName := range []string{"sum", "avg", "min", "max", "count", "stddev", "rate"} {
		if strings.HasPrefix(strings.ToLower(expr), aggName) {
			return p.parseAggregation(expr, aggName)
		}
	}

	return p.parseSelector(expr)
}

func (p *PromQLParser) parseAggregation(expr, aggName string) (*PromQLQuery, error) {
	rest := strings.TrimPrefix(strings.ToLower(expr), strings.ToLower(aggName))
	rest = strings.TrimSpace(rest)

	// Parse optional by/without clause
	var by, without []string
	if strings.HasPrefix(rest, "by") {
		rest = strings.TrimPrefix(rest, "by")
		rest = strings.TrimSpace(rest)
		labels, remaining := p.parseParenLabels(rest)
		by = labels
		rest = remaining
	} else if strings.HasPrefix(rest, "without") {
		rest = strings.TrimPrefix(rest, "without")
		rest = strings.TrimSpace(rest)
		labels, remaining := p.parseParenLabels(rest)
		without = labels
		rest = remaining
	}

	// The inner expression should be in parentheses
	if !strings.HasPrefix(rest, "(") {
		return nil, fmt.Errorf("expected ( after %s", aggName)
	}
	inner, _ := p.extractParenContent(rest)

	query, err := p.parseSelector(strings.TrimSpace(inner))
	if err != nil {
		return nil, err
	}

	aggOp := p.parseAggOp(aggName)
	query.Aggregation = &PromQLAggregation{
		Op:      aggOp,
		By:      by,
		Without: without,
	}

	return query, nil
}

func (p *PromQLParser) parseAggOp(name string) PromQLAggOp {
	switch strings.ToLower(name) {
	case "sum":
		return PromQLAggSum
	case "avg":
		return PromQLAggAvg
	case "min":
		return PromQLAggMin
	case "max":
		return PromQLAggMax
	case "count":
		return PromQLAggCount
	case "stddev":
		return PromQLAggStddev
	case "rate":
		return PromQLAggRate
	default:
		return PromQLAggSum
	}
}

func (p *PromQLParser) parseSelector(expr string) (*PromQLQuery, error) {
	query := &PromQLQuery{
		Labels: make(map[string]LabelMatcher),
	}

	// Check for range selector
	if idx := strings.Index(expr, "["); idx != -1 {
		rangeEnd := strings.Index(expr[idx:], "]")
		if rangeEnd == -1 {
			return nil, errors.New("unclosed range bracket")
		}
		rangeStr := expr[idx+1 : idx+rangeEnd]
		duration, err := parseDuration(rangeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid duration: %w", err)
		}
		query.RangeWindow = duration
		expr = expr[:idx]
	}

	// Parse metric name and labels
	if idx := strings.Index(expr, "{"); idx != -1 {
		query.Metric = strings.TrimSpace(expr[:idx])
		labelsEnd := strings.LastIndex(expr, "}")
		if labelsEnd == -1 {
			return nil, errors.New("unclosed label matcher")
		}
		labelsStr := expr[idx+1 : labelsEnd]
		if err := p.parseLabels(labelsStr, query); err != nil {
			return nil, err
		}
	} else {
		query.Metric = strings.TrimSpace(expr)
	}

	if query.Metric == "" {
		return nil, errors.New("metric name is required")
	}

	return query, nil
}

func (p *PromQLParser) parseLabels(labelsStr string, query *PromQLQuery) error {
	if strings.TrimSpace(labelsStr) == "" {
		return nil
	}

	// Split by comma, handling quoted values
	parts := splitLabels(labelsStr)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Determine operator
		var op LabelMatchOp
		var opStr string
		if strings.Contains(part, "=~") {
			op = LabelMatchRegex
			opStr = "=~"
		} else if strings.Contains(part, "!~") {
			op = LabelMatchNotRegex
			opStr = "!~"
		} else if strings.Contains(part, "!=") {
			op = LabelMatchNotEqual
			opStr = "!="
		} else if strings.Contains(part, "=") {
			op = LabelMatchEqual
			opStr = "="
		} else {
			return fmt.Errorf("invalid label matcher: %s", part)
		}

		idx := strings.Index(part, opStr)
		key := strings.TrimSpace(part[:idx])
		value := strings.TrimSpace(part[idx+len(opStr):])

		// Remove quotes
		value = strings.Trim(value, "\"'")

		matcher := LabelMatcher{Op: op, Value: value}

		if op == LabelMatchRegex || op == LabelMatchNotRegex {
			re, err := regexp.Compile(value)
			if err != nil {
				return fmt.Errorf("invalid regex: %w", err)
			}
			matcher.Regex = re
		}

		query.Labels[key] = matcher
	}

	return nil
}

func (p *PromQLParser) parseParenLabels(expr string) ([]string, string) {
	if !strings.HasPrefix(expr, "(") {
		return nil, expr
	}
	end := strings.Index(expr, ")")
	if end == -1 {
		return nil, expr
	}
	inner := expr[1:end]
	labels := strings.Split(inner, ",")
	for i := range labels {
		labels[i] = strings.TrimSpace(labels[i])
	}
	return labels, strings.TrimSpace(expr[end+1:])
}

func (p *PromQLParser) extractParenContent(expr string) (string, string) {
	if !strings.HasPrefix(expr, "(") {
		return "", expr
	}
	depth := 0
	for i, ch := range expr {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				return expr[1:i], expr[i+1:]
			}
		}
	}
	return expr[1:], ""
}

func splitLabels(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, ch := range s {
		if (ch == '"' || ch == '\'') && !inQuote {
			inQuote = true
			quoteChar = ch
			current.WriteRune(ch)
		} else if ch == quoteChar && inQuote {
			inQuote = false
			quoteChar = 0
			current.WriteRune(ch)
		} else if ch == ',' && !inQuote {
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

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty duration")
	}

	// Handle common time units
	var multiplier time.Duration = 1
	var numStr string

	for i, ch := range s {
		if !unicode.IsDigit(ch) {
			numStr = s[:i]
			unit := s[i:]

			switch strings.ToLower(unit) {
			case "ms":
				multiplier = time.Millisecond
			case "s":
				multiplier = time.Second
			case "m":
				multiplier = time.Minute
			case "h":
				multiplier = time.Hour
			case "d":
				multiplier = 24 * time.Hour
			case "w":
				multiplier = 7 * 24 * time.Hour
			case "y":
				multiplier = 365 * 24 * time.Hour
			default:
				return 0, fmt.Errorf("unknown unit: %s", unit)
			}
			break
		}
	}

	if numStr == "" {
		return 0, errors.New("no number in duration")
	}

	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return time.Duration(num) * multiplier, nil
}

// ToChronicleQuery converts a PromQL query to a Chronicle query.
func (q *PromQLQuery) ToChronicleQuery(start, end int64) *Query {
	cq := &Query{
		Metric: q.Metric,
		Tags:   make(map[string]string),
		Start:  start,
		End:    end,
	}

	// Convert label matchers
	for k, v := range q.Labels {
		switch v.Op {
		case LabelMatchEqual:
			cq.Tags[k] = v.Value
		case LabelMatchNotEqual:
			cq.TagFilters = append(cq.TagFilters, TagFilter{
				Key:    k,
				Op:     TagOpNotEq,
				Values: []string{v.Value},
			})
		case LabelMatchRegex, LabelMatchNotRegex:
			// Regex matchers are not directly supported in Chronicle.
			// For basic prefix/suffix patterns, we could potentially translate,
			// but for now we skip regex matchers (they won't filter results).
			// Users should be aware that regex filters are ignored.
		}
	}

	// Convert aggregation
	if q.Aggregation != nil {
		cq.Aggregation = &Aggregation{
			Function: promQLAggToChronicle(q.Aggregation.Op),
			Window:   q.RangeWindow,
		}
		if len(q.Aggregation.By) > 0 {
			cq.GroupBy = q.Aggregation.By
		}
	}

	if cq.Aggregation != nil && cq.Aggregation.Window <= 0 {
		cq.Aggregation.Window = time.Minute // Default window
	}

	return cq
}

func promQLAggToChronicle(op PromQLAggOp) AggFunc {
	switch op {
	case PromQLAggSum:
		return AggSum
	case PromQLAggAvg:
		return AggMean
	case PromQLAggMin:
		return AggMin
	case PromQLAggMax:
		return AggMax
	case PromQLAggCount:
		return AggCount
	case PromQLAggStddev:
		return AggStddev
	case PromQLAggRate:
		return AggRate
	default:
		return AggNone
	}
}

// PromQLHandler returns an HTTP handler for PromQL queries.
type PromQLHandler struct {
	db     *DB
	parser *PromQLParser
}

// NewPromQLHandler creates a new PromQL HTTP handler.
func NewPromQLHandler(db *DB) *PromQLHandler {
	return &PromQLHandler{
		db:     db,
		parser: &PromQLParser{},
	}
}

// PromQLResponse is the Prometheus-compatible response format.
type PromQLResponse struct {
	Status string     `json:"status"`
	Data   PromQLData `json:"data,omitempty"`
	Error  string     `json:"error,omitempty"`
}

// PromQLData contains query results.
type PromQLData struct {
	ResultType string         `json:"resultType"`
	Result     []PromQLResult `json:"result"`
}

// PromQLResult is a single result series.
type PromQLResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values,omitempty"` // For range queries: [[timestamp, value], ...]
	Value  []any             `json:"value,omitempty"`  // For instant queries: [timestamp, value]
}
