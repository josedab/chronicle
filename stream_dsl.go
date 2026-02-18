package chronicle

// stream_dsl.go implements V1 of the stream processing DSL.
// V2 (stream_dsl_v2.go) adds windowed operations (Tumbling, Sliding, Session, Count, Global),
// advanced aggregations, and richer trigger semantics.
//
// Deprecated: New callers should prefer the V2 API (StreamDSLV2Engine) for production use.
// This file will be removed in a future major version.

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StreamTriggerOp defines which data mutation triggers a rule.
type StreamTriggerOp int

const (
	StreamTriggerInsert StreamTriggerOp = iota
	StreamTriggerUpdate
	StreamTriggerDelete
	StreamTriggerAny
)

func (op StreamTriggerOp) String() string {
	switch op {
	case StreamTriggerInsert:
		return "INSERT"
	case StreamTriggerUpdate:
		return "UPDATE"
	case StreamTriggerDelete:
		return "DELETE"
	case StreamTriggerAny:
		return "ANY"
	default:
		return "UNKNOWN"
	}
}

// StreamAction defines what happens when a rule fires.
type StreamAction int

const (
	StreamActionEmitMetric StreamAction = iota
	StreamActionEmitAlert
	StreamActionLog
	StreamActionCallback
)

func (a StreamAction) String() string {
	switch a {
	case StreamActionEmitMetric:
		return "EMIT_METRIC"
	case StreamActionEmitAlert:
		return "EMIT_ALERT"
	case StreamActionLog:
		return "LOG"
	case StreamActionCallback:
		return "CALLBACK"
	default:
		return "UNKNOWN"
	}
}

// StreamPredicate evaluates whether a point matches a condition.
type StreamPredicate func(p *Point) bool

// StreamRule defines a stream processing rule.
type StreamRule struct {
	Name         string
	Trigger      StreamTriggerOp
	MetricFilter string // empty = all metrics
	Predicate    StreamPredicate
	Action       StreamAction
	TargetMetric string         // for EMIT_METRIC
	Message      string         // for EMIT_ALERT / LOG
	Callback     func(p *Point) // for CALLBACK
	Window       time.Duration  // for windowed rules
	windowBuf    []windowEntry
	windowMu     sync.Mutex
}

type windowEntry struct {
	point     Point
	timestamp time.Time
}

// StreamRuleBuilder provides a fluent API for constructing rules.
type StreamRuleBuilder struct {
	rule *StreamRule
}

// OnInsert starts building a rule triggered by INSERT operations.
func OnInsert() *StreamRuleBuilder {
	return &StreamRuleBuilder{rule: &StreamRule{Trigger: StreamTriggerInsert}}
}

// OnUpdate starts building a rule triggered by UPDATE operations.
func OnUpdate() *StreamRuleBuilder {
	return &StreamRuleBuilder{rule: &StreamRule{Trigger: StreamTriggerUpdate}}
}

// OnDelete starts building a rule triggered by DELETE operations.
func OnDelete() *StreamRuleBuilder {
	return &StreamRuleBuilder{rule: &StreamRule{Trigger: StreamTriggerDelete}}
}

// OnAny starts building a rule triggered by any operation.
func OnAny() *StreamRuleBuilder {
	return &StreamRuleBuilder{rule: &StreamRule{Trigger: StreamTriggerAny}}
}

// Named sets the rule name.
func (b *StreamRuleBuilder) Named(name string) *StreamRuleBuilder {
	b.rule.Name = name
	return b
}

// IntoMetric filters to a specific metric.
func (b *StreamRuleBuilder) IntoMetric(metric string) *StreamRuleBuilder {
	b.rule.MetricFilter = metric
	return b
}

// Where adds a predicate condition.
func (b *StreamRuleBuilder) Where(pred StreamPredicate) *StreamRuleBuilder {
	b.rule.Predicate = pred
	return b
}

// WhereValueGT is a convenience for value > threshold.
func (b *StreamRuleBuilder) WhereValueGT(threshold float64) *StreamRuleBuilder {
	b.rule.Predicate = func(p *Point) bool { return p.Value > threshold }
	return b
}

// WhereValueLT is a convenience for value < threshold.
func (b *StreamRuleBuilder) WhereValueLT(threshold float64) *StreamRuleBuilder {
	b.rule.Predicate = func(p *Point) bool { return p.Value < threshold }
	return b
}

// WhereTagEquals filters by tag value.
func (b *StreamRuleBuilder) WhereTagEquals(key, value string) *StreamRuleBuilder {
	b.rule.Predicate = func(p *Point) bool { return p.Tags[key] == value }
	return b
}

// WithWindow sets a time window for aggregation.
func (b *StreamRuleBuilder) WithWindow(d time.Duration) *StreamRuleBuilder {
	b.rule.Window = d
	return b
}

// EmitMetric configures the rule to write a derived metric.
func (b *StreamRuleBuilder) EmitMetric(target string) *StreamRuleBuilder {
	b.rule.Action = StreamActionEmitMetric
	b.rule.TargetMetric = target
	return b
}

// EmitAlert configures the rule to fire an alert.
func (b *StreamRuleBuilder) EmitAlert(message string) *StreamRuleBuilder {
	b.rule.Action = StreamActionEmitAlert
	b.rule.Message = message
	return b
}

// EmitLog configures the rule to emit a log message.
func (b *StreamRuleBuilder) EmitLog(message string) *StreamRuleBuilder {
	b.rule.Action = StreamActionLog
	b.rule.Message = message
	return b
}

// Do configures the rule with a custom callback.
func (b *StreamRuleBuilder) Do(fn func(p *Point)) *StreamRuleBuilder {
	b.rule.Action = StreamActionCallback
	b.rule.Callback = fn
	return b
}

// Build returns the constructed rule.
func (b *StreamRuleBuilder) Build() *StreamRule {
	if b.rule.Name == "" {
		b.rule.Name = fmt.Sprintf("rule_%s_%s", b.rule.Trigger, b.rule.Action)
	}
	return b.rule
}

// StreamProcessor evaluates rules against incoming data.
type StreamProcessor struct {
	mu       sync.RWMutex
	rules    []*StreamRule
	stats    StreamProcessorStats
	onAlert  func(name, message string, p *Point)
	onMetric func(metric string, p *Point)
	onLog    func(message string, p *Point)
}

// StreamProcessorStats tracks stream processing metrics.
type StreamProcessorStats struct {
	PointsProcessed int64 `json:"points_processed"`
	RulesEvaluated  int64 `json:"rules_evaluated"`
	RulesFired      int64 `json:"rules_fired"`
	AlertsEmitted   int64 `json:"alerts_emitted"`
	MetricsEmitted  int64 `json:"metrics_emitted"`
}

// NewStreamProcessor creates a new stream processor.
func NewStreamProcessor() *StreamProcessor {
	return &StreamProcessor{
		rules: make([]*StreamRule, 0),
	}
}

// AddRule registers a stream processing rule.
func (sp *StreamProcessor) AddRule(rule *StreamRule) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.rules = append(sp.rules, rule)
}

// OnAlert registers a callback for alert emissions.
func (sp *StreamProcessor) OnAlert(fn func(name, message string, p *Point)) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.onAlert = fn
}

// OnMetric registers a callback for metric emissions.
func (sp *StreamProcessor) OnMetric(fn func(metric string, p *Point)) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.onMetric = fn
}

// OnLog registers a callback for log emissions.
func (sp *StreamProcessor) OnLog(fn func(message string, p *Point)) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.onLog = fn
}

// Process evaluates all rules against a point with the given trigger operation.
func (sp *StreamProcessor) Process(op StreamTriggerOp, p *Point) {
	sp.mu.RLock()
	rules := make([]*StreamRule, len(sp.rules))
	copy(rules, sp.rules)
	onAlert := sp.onAlert
	onMetric := sp.onMetric
	onLog := sp.onLog
	sp.mu.RUnlock()

	sp.mu.Lock()
	sp.stats.PointsProcessed++
	sp.mu.Unlock()

	for _, rule := range rules {

		sp.mu.Lock()
		sp.stats.RulesEvaluated++
		sp.mu.Unlock()

		// Check trigger
		if rule.Trigger != StreamTriggerAny && rule.Trigger != op {
			continue
		}

		// Check metric filter
		if rule.MetricFilter != "" && rule.MetricFilter != p.Metric {
			continue
		}

		// Check predicate
		if rule.Predicate != nil && !rule.Predicate(p) {
			continue
		}

		sp.mu.Lock()
		sp.stats.RulesFired++
		sp.mu.Unlock()

		// Execute action
		switch rule.Action {
		case StreamActionEmitMetric:
			sp.mu.Lock()
			sp.stats.MetricsEmitted++
			sp.mu.Unlock()
			if onMetric != nil {
				onMetric(rule.TargetMetric, p)
			}

		case StreamActionEmitAlert:
			sp.mu.Lock()
			sp.stats.AlertsEmitted++
			sp.mu.Unlock()
			if onAlert != nil {
				onAlert(rule.Name, rule.Message, p)
			}

		case StreamActionLog:
			if onLog != nil {
				onLog(rule.Message, p)
			}

		case StreamActionCallback:
			if rule.Callback != nil {
				rule.Callback(p)
			}
		}
	}
}

// Stats returns processing statistics.
func (sp *StreamProcessor) Stats() StreamProcessorStats {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.stats
}

// RuleCount returns the number of registered rules.
func (sp *StreamProcessor) RuleCount() int {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return len(sp.rules)
}

// ParseStreamRule parses a simple DSL string into a StreamRule.
// Format: ON <INSERT|UPDATE|DELETE> INTO <metric> WHERE value <op> <threshold> EMIT <METRIC|ALERT> <target>
func ParseStreamRule(dsl string) (*StreamRule, error) {
	tokens := strings.Fields(dsl)
	if len(tokens) < 2 {
		return nil, fmt.Errorf("stream dsl: too few tokens")
	}

	rule := &StreamRule{}
	i := 0

	// ON
	if strings.ToUpper(tokens[i]) != "ON" {
		return nil, fmt.Errorf("stream dsl: expected ON, got %q", tokens[i])
	}
	i++

	// Trigger
	switch strings.ToUpper(tokens[i]) {
	case "INSERT":
		rule.Trigger = StreamTriggerInsert
	case "UPDATE":
		rule.Trigger = StreamTriggerUpdate
	case "DELETE":
		rule.Trigger = StreamTriggerDelete
	case "ANY":
		rule.Trigger = StreamTriggerAny
	default:
		return nil, fmt.Errorf("stream dsl: unknown trigger %q", tokens[i])
	}
	i++

	// Optional INTO <metric>
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "INTO" {
		i++
		if i >= len(tokens) {
			return nil, fmt.Errorf("stream dsl: expected metric after INTO")
		}
		rule.MetricFilter = tokens[i]
		i++
	}

	// Optional WHERE value <op> <threshold>
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "WHERE" {
		i++
		if i+2 >= len(tokens) {
			return nil, fmt.Errorf("stream dsl: incomplete WHERE clause")
		}
		field := strings.ToLower(tokens[i])
		i++
		op := tokens[i]
		i++
		threshStr := tokens[i]
		i++

		threshold, err := strconv.ParseFloat(threshStr, 64)
		if err != nil {
			return nil, fmt.Errorf("stream dsl: invalid threshold %q: %w", threshStr, err)
		}

		if field == "value" {
			switch op {
			case ">":
				rule.Predicate = func(p *Point) bool { return p.Value > threshold }
			case "<":
				rule.Predicate = func(p *Point) bool { return p.Value < threshold }
			case ">=":
				rule.Predicate = func(p *Point) bool { return p.Value >= threshold }
			case "<=":
				rule.Predicate = func(p *Point) bool { return p.Value <= threshold }
			case "==", "=":
				rule.Predicate = func(p *Point) bool { return p.Value == threshold }
			default:
				return nil, fmt.Errorf("stream dsl: unknown operator %q", op)
			}
		} else {
			return nil, fmt.Errorf("stream dsl: WHERE only supports 'value' field, got %q", field)
		}
	}

	// Optional EMIT <type> <target>
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "EMIT" {
		i++
		if i >= len(tokens) {
			return nil, fmt.Errorf("stream dsl: expected action after EMIT")
		}
		switch strings.ToUpper(tokens[i]) {
		case "METRIC":
			rule.Action = StreamActionEmitMetric
			i++
			if i < len(tokens) {
				rule.TargetMetric = tokens[i]
			}
		case "ALERT":
			rule.Action = StreamActionEmitAlert
			i++
			if i < len(tokens) {
				rule.Message = strings.Join(tokens[i:], " ")
			}
		case "LOG":
			rule.Action = StreamActionLog
			i++
			if i < len(tokens) {
				rule.Message = strings.Join(tokens[i:], " ")
			}
		default:
			return nil, fmt.Errorf("stream dsl: unknown action %q", tokens[i])
		}
	}

	if rule.Name == "" {
		rule.Name = fmt.Sprintf("parsed_%s_%s", rule.Trigger, rule.MetricFilter)
	}

	return rule, nil
}
