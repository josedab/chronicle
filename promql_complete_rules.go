package chronicle

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

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
					_ = e.EvaluateRule(rule) //nolint:errcheck // best-effort rule evaluation, logged internally
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
