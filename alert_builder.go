package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// AlertBuilder provides a low-code visual interface for creating alert rules.
// It supports drag-and-drop style rule composition with condition chaining,
// preview capabilities, and validation.
type AlertBuilder struct {
	db       *DB
	rules    map[string]*BuilderRule
	triggers map[string]*AlertTrigger
	mu       sync.RWMutex
	config   AlertBuilderConfig
}

// AlertBuilderConfig configures the alert builder.
type AlertBuilderConfig struct {
	// MaxRulesPerUser limits rules per user
	MaxRulesPerUser int `json:"max_rules_per_user"`

	// MaxConditionsPerRule limits condition complexity
	MaxConditionsPerRule int `json:"max_conditions_per_rule"`

	// EnablePreview allows testing rules before saving
	EnablePreview bool `json:"enable_preview"`

	// PreviewLookback is the time window for preview
	PreviewLookback time.Duration `json:"preview_lookback"`

	// DefaultEvalInterval is the default evaluation interval
	DefaultEvalInterval time.Duration `json:"default_eval_interval"`
}

// DefaultAlertBuilderConfig returns sensible defaults.
func DefaultAlertBuilderConfig() AlertBuilderConfig {
	return AlertBuilderConfig{
		MaxRulesPerUser:      100,
		MaxConditionsPerRule: 20,
		EnablePreview:        true,
		PreviewLookback:      time.Hour,
		DefaultEvalInterval:  time.Minute,
	}
}

// AlertRule represents a visual alert rule built from conditions.
type BuilderRule struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	Enabled     bool               `json:"enabled"`
	Severity    AlertSeverity      `json:"severity"`
	Conditions  []BuilderCondition `json:"conditions"`
	Logic       ConditionLogic     `json:"logic"` // AND/OR between conditions
	Actions     []AlertAction      `json:"actions"`
	Labels      map[string]string  `json:"labels,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`

	// Timing
	EvalInterval time.Duration `json:"eval_interval"`
	ForDuration  time.Duration `json:"for_duration"` // How long condition must be true
	RepeatAfter  time.Duration `json:"repeat_after"` // Re-alert interval

	// Metadata
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	CreatedBy string    `json:"created_by,omitempty"`
	Version   int       `json:"version"`

	// State
	lastEval   time.Time
	lastFired  time.Time
	firingFrom *time.Time
}

// AlertSeverity indicates the alert severity level.
type AlertSeverity string

const (
	AlertSeverityInfo     AlertSeverity = "info"
	AlertSeverityWarning  AlertSeverity = "warning"
	AlertSeverityCritical AlertSeverity = "critical"
)

// ConditionLogic defines how conditions are combined.
type ConditionLogic string

const (
	ConditionLogicAND ConditionLogic = "and"
	ConditionLogicOR  ConditionLogic = "or"
)

// AlertCondition represents a single condition in a rule.
type BuilderCondition struct {
	ID       string        `json:"id"`
	Type     ConditionType `json:"type"`
	Metric   string        `json:"metric"`
	Operator Operator      `json:"operator"`
	Value    float64       `json:"value"`

	// For aggregation conditions
	Aggregation AggregationType `json:"aggregation,omitempty"`
	Window      time.Duration   `json:"window,omitempty"`

	// For rate conditions
	RateInterval time.Duration `json:"rate_interval,omitempty"`

	// For comparison conditions
	CompareMetric string `json:"compare_metric,omitempty"`

	// Tag filters
	TagFilters map[string]string `json:"tag_filters,omitempty"`

	// Nested conditions for complex logic
	SubConditions []BuilderCondition `json:"sub_conditions,omitempty"`
	SubLogic      ConditionLogic     `json:"sub_logic,omitempty"`
}

// ConditionType defines the type of condition.
type ConditionType string

const (
	ConditionTypeThreshold   ConditionType = "threshold"   // Simple value comparison
	ConditionTypeRate        ConditionType = "rate"        // Rate of change
	ConditionTypeAggregation ConditionType = "aggregation" // Aggregated value
	ConditionTypeAbsence     ConditionType = "absence"     // Missing data
	ConditionTypeAnomaly     ConditionType = "anomaly"     // Anomaly detection
	ConditionTypeComparison  ConditionType = "comparison"  // Compare two metrics
	ConditionTypeComplex     ConditionType = "complex"     // Nested conditions
)

// Operator defines comparison operators.
type Operator string

const (
	OperatorGT  Operator = ">"
	OperatorGTE Operator = ">="
	OperatorLT  Operator = "<"
	OperatorLTE Operator = "<="
	OperatorEQ  Operator = "=="
	OperatorNE  Operator = "!="
)

// AggregationType defines aggregation functions.
type BuilderAggregationType string

const (
	BuilderAggregationAvg   AggregationType = "avg"
	BuilderAggregationSum   AggregationType = "sum"
	BuilderAggregationMin   AggregationType = "min"
	BuilderAggregationMax   AggregationType = "max"
	BuilderAggregationCount AggregationType = "count"
	BuilderAggregationP50   AggregationType = "p50"
	BuilderAggregationP90   AggregationType = "p90"
	BuilderAggregationP99   AggregationType = "p99"
)

// AlertAction defines what happens when an alert fires.
type AlertAction struct {
	Type   ActionType        `json:"type"`
	Config map[string]string `json:"config"`
}

// ActionType defines action types.
type ActionType string

const (
	ActionTypeWebhook   ActionType = "webhook"
	ActionTypeEmail     ActionType = "email"
	ActionTypeSlack     ActionType = "slack"
	ActionTypePagerDuty ActionType = "pagerduty"
	ActionTypeLog       ActionType = "log"
)

// AlertTrigger represents a triggered alert instance.
type AlertTrigger struct {
	ID            string            `json:"id"`
	RuleID        string            `json:"rule_id"`
	RuleName      string            `json:"rule_name"`
	Severity      AlertSeverity     `json:"severity"`
	FiredAt       time.Time         `json:"fired_at"`
	ResolvedAt    *time.Time        `json:"resolved_at,omitempty"`
	State         TriggerState      `json:"state"`
	Value         float64           `json:"value"`
	Threshold     float64           `json:"threshold"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
	ConditionMet  string            `json:"condition_met"`
	ActionResults []ActionResult    `json:"action_results,omitempty"`
}

// TriggerState represents the state of a trigger.
type TriggerState string

const (
	TriggerStatePending  TriggerState = "pending"
	TriggerStateFiring   TriggerState = "firing"
	TriggerStateResolved TriggerState = "resolved"
)

// ActionResult records the result of an action.
type ActionResult struct {
	ActionType ActionType `json:"action_type"`
	Success    bool       `json:"success"`
	Message    string     `json:"message,omitempty"`
	Timestamp  time.Time  `json:"timestamp"`
}

// NewAlertBuilder creates a new alert builder.
func NewAlertBuilder(db *DB, config AlertBuilderConfig) *AlertBuilder {
	if config.MaxRulesPerUser <= 0 {
		config.MaxRulesPerUser = 100
	}
	if config.MaxConditionsPerRule <= 0 {
		config.MaxConditionsPerRule = 20
	}
	if config.DefaultEvalInterval <= 0 {
		config.DefaultEvalInterval = time.Minute
	}
	if config.PreviewLookback <= 0 {
		config.PreviewLookback = time.Hour
	}

	return &AlertBuilder{
		db:       db,
		rules:    make(map[string]*BuilderRule),
		triggers: make(map[string]*AlertTrigger),
		config:   config,
	}
}

// CreateRule creates a new alert rule from visual builder input.
func (ab *AlertBuilder) CreateRule(rule *BuilderRule) error {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	// Validate
	if err := ab.validateRule(rule); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Check limits
	userRules := 0
	for _, r := range ab.rules {
		if r.CreatedBy == rule.CreatedBy {
			userRules++
		}
	}
	if userRules >= ab.config.MaxRulesPerUser {
		return fmt.Errorf("maximum rules per user (%d) exceeded", ab.config.MaxRulesPerUser)
	}

	// Set defaults
	if rule.ID == "" {
		rule.ID = generateAlertID()
	}
	if rule.EvalInterval <= 0 {
		rule.EvalInterval = ab.config.DefaultEvalInterval
	}
	rule.CreatedAt = time.Now()
	rule.UpdatedAt = time.Now()
	rule.Version = 1

	ab.rules[rule.ID] = rule
	return nil
}

// UpdateRule updates an existing rule.
func (ab *AlertBuilder) UpdateRule(id string, updates *BuilderRule) error {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	existing, ok := ab.rules[id]
	if !ok {
		return fmt.Errorf("rule %q not found", id)
	}

	// Validate updates
	if err := ab.validateRule(updates); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Preserve metadata
	updates.ID = id
	updates.CreatedAt = existing.CreatedAt
	updates.CreatedBy = existing.CreatedBy
	updates.UpdatedAt = time.Now()
	updates.Version = existing.Version + 1

	ab.rules[id] = updates
	return nil
}

// DeleteRule removes a rule.
func (ab *AlertBuilder) DeleteRule(id string) error {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	if _, ok := ab.rules[id]; !ok {
		return fmt.Errorf("rule %q not found", id)
	}

	delete(ab.rules, id)
	return nil
}

// GetRule retrieves a rule by ID.
func (ab *AlertBuilder) GetRule(id string) (*BuilderRule, error) {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	rule, ok := ab.rules[id]
	if !ok {
		return nil, fmt.Errorf("rule %q not found", id)
	}

	return rule, nil
}

// ListRules returns all rules.
func (ab *AlertBuilder) ListRules() []*BuilderRule {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	rules := make([]*BuilderRule, 0, len(ab.rules))
	for _, r := range ab.rules {
		rules = append(rules, r)
	}

	// Sort by name
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Name < rules[j].Name
	})

	return rules
}

// EnableRule enables a rule.
func (ab *AlertBuilder) EnableRule(id string) error {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	rule, ok := ab.rules[id]
	if !ok {
		return fmt.Errorf("rule %q not found", id)
	}

	rule.Enabled = true
	rule.UpdatedAt = time.Now()
	return nil
}

// DisableRule disables a rule.
func (ab *AlertBuilder) DisableRule(id string) error {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	rule, ok := ab.rules[id]
	if !ok {
		return fmt.Errorf("rule %q not found", id)
	}

	rule.Enabled = false
	rule.UpdatedAt = time.Now()
	return nil
}

// PreviewRule tests a rule against historical data.
func (ab *AlertBuilder) PreviewRule(rule *BuilderRule) (*PreviewResult, error) {
	if !ab.config.EnablePreview {
		return nil, fmt.Errorf("preview is disabled")
	}

	if err := ab.validateRule(rule); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	result := &PreviewResult{
		RuleID:    rule.ID,
		RuleName:  rule.Name,
		StartTime: time.Now().Add(-ab.config.PreviewLookback),
		EndTime:   time.Now(),
		Matches:   []PreviewMatch{},
	}

	// Evaluate conditions against historical data
	for _, cond := range rule.Conditions {
		matches, err := ab.evaluateConditionPreview(cond, result.StartTime, result.EndTime)
		if err != nil {
			return nil, fmt.Errorf("preview evaluation failed: %w", err)
		}
		result.Matches = append(result.Matches, matches...)
	}

	// Calculate statistics
	result.TotalMatches = len(result.Matches)
	if result.TotalMatches > 0 {
		for _, m := range result.Matches {
			result.TotalDuration += m.Duration
		}
		result.AvgMatchDuration = result.TotalDuration / time.Duration(result.TotalMatches)
	}

	return result, nil
}

// PreviewResult contains the results of a rule preview.
type PreviewResult struct {
	RuleID           string         `json:"rule_id"`
	RuleName         string         `json:"rule_name"`
	StartTime        time.Time      `json:"start_time"`
	EndTime          time.Time      `json:"end_time"`
	TotalMatches     int            `json:"total_matches"`
	Matches          []PreviewMatch `json:"matches"`
	TotalDuration    time.Duration  `json:"total_duration"`
	AvgMatchDuration time.Duration  `json:"avg_match_duration"`
	WouldHaveFired   bool           `json:"would_have_fired"`
}

// PreviewMatch represents a single match in preview.
type PreviewMatch struct {
	Timestamp time.Time     `json:"timestamp"`
	Value     float64       `json:"value"`
	Threshold float64       `json:"threshold"`
	Condition string        `json:"condition"`
	Duration  time.Duration `json:"duration"`
}

// evaluateConditionPreview evaluates a condition against historical data.
func (ab *AlertBuilder) evaluateConditionPreview(cond BuilderCondition, start, end time.Time) ([]PreviewMatch, error) {
	var matches []PreviewMatch

	// Query historical data
	result, err := ab.db.Execute(&Query{
		Metric: cond.Metric,
		Start:  start.UnixNano(),
		End:    end.UnixNano(),
		Tags:   cond.TagFilters,
	})
	if err != nil {
		return nil, err
	}

	if result == nil || len(result.Points) == 0 {
		return matches, nil
	}

	for _, p := range result.Points {
		var matched bool
		switch cond.Type {
		case ConditionTypeThreshold:
			matched = ab.evaluateOperator(p.Value, cond.Operator, cond.Value)
		case ConditionTypeAggregation:
			// For aggregation, we'd need window calculation
			// Simplified for preview
			matched = ab.evaluateOperator(p.Value, cond.Operator, cond.Value)
		default:
			matched = ab.evaluateOperator(p.Value, cond.Operator, cond.Value)
		}

		if matched {
			matches = append(matches, PreviewMatch{
				Timestamp: time.Unix(0, p.Timestamp),
				Value:     p.Value,
				Threshold: cond.Value,
				Condition: fmt.Sprintf("%s %s %v", cond.Metric, cond.Operator, cond.Value),
				Duration:  time.Second, // Estimated
			})
		}
	}

	return matches, nil
}

// evaluateOperator evaluates a comparison operator.
func (ab *AlertBuilder) evaluateOperator(value float64, op Operator, threshold float64) bool {
	switch op {
	case OperatorGT:
		return value > threshold
	case OperatorGTE:
		return value >= threshold
	case OperatorLT:
		return value < threshold
	case OperatorLTE:
		return value <= threshold
	case OperatorEQ:
		return value == threshold
	case OperatorNE:
		return value != threshold
	default:
		return false
	}
}

// validateRule validates an alert rule.
func (ab *AlertBuilder) validateRule(rule *BuilderRule) error {
	if rule.Name == "" {
		return fmt.Errorf("rule name is required")
	}

	if len(rule.Conditions) == 0 {
		return fmt.Errorf("at least one condition is required")
	}

	if len(rule.Conditions) > ab.config.MaxConditionsPerRule {
		return fmt.Errorf("too many conditions (max %d)", ab.config.MaxConditionsPerRule)
	}

	// Validate each condition
	for i, cond := range rule.Conditions {
		if err := ab.validateCondition(cond); err != nil {
			return fmt.Errorf("condition %d: %w", i, err)
		}
	}

	// Validate severity
	switch rule.Severity {
	case AlertSeverityInfo, AlertSeverityWarning, AlertSeverityCritical:
		// Valid
	case "":
		rule.Severity = AlertSeverityWarning // Default
	default:
		return fmt.Errorf("invalid severity: %s", rule.Severity)
	}

	// Validate logic
	switch rule.Logic {
	case ConditionLogicAND, ConditionLogicOR:
		// Valid
	case "":
		rule.Logic = ConditionLogicAND // Default
	default:
		return fmt.Errorf("invalid logic: %s", rule.Logic)
	}

	return nil
}

// validateCondition validates a single condition.
func (ab *AlertBuilder) validateCondition(cond BuilderCondition) error {
	if cond.Metric == "" && cond.Type != ConditionTypeComplex {
		return fmt.Errorf("metric is required")
	}

	// Validate operator
	switch cond.Operator {
	case OperatorGT, OperatorGTE, OperatorLT, OperatorLTE, OperatorEQ, OperatorNE:
		// Valid
	case "":
		return fmt.Errorf("operator is required")
	default:
		return fmt.Errorf("invalid operator: %s", cond.Operator)
	}

	// Type-specific validation
	switch cond.Type {
	case ConditionTypeThreshold:
		// Basic threshold - no extra validation

	case ConditionTypeRate:
		if cond.RateInterval <= 0 {
			return fmt.Errorf("rate_interval is required for rate conditions")
		}

	case ConditionTypeAggregation:
		if cond.Window <= 0 {
			return fmt.Errorf("window is required for aggregation conditions")
		}
		switch cond.Aggregation {
		case BuilderAggregationAvg, BuilderAggregationSum, BuilderAggregationMin,
			BuilderAggregationMax, BuilderAggregationCount, BuilderAggregationP50,
			BuilderAggregationP90, BuilderAggregationP99:
			// Valid
		case "":
			return fmt.Errorf("aggregation type is required")
		default:
			return fmt.Errorf("invalid aggregation type: %s", cond.Aggregation)
		}

	case ConditionTypeAbsence:
		if cond.Window <= 0 {
			return fmt.Errorf("window is required for absence conditions")
		}

	case ConditionTypeComparison:
		if cond.CompareMetric == "" {
			return fmt.Errorf("compare_metric is required for comparison conditions")
		}

	case ConditionTypeComplex:
		if len(cond.SubConditions) == 0 {
			return fmt.Errorf("sub_conditions required for complex conditions")
		}
		for i, sub := range cond.SubConditions {
			if err := ab.validateCondition(sub); err != nil {
				return fmt.Errorf("sub_condition %d: %w", i, err)
			}
		}

	case "":
		// Default to threshold
	default:
		return fmt.Errorf("invalid condition type: %s", cond.Type)
	}

	return nil
}

// EvaluateRule evaluates a rule against current data.
func (ab *AlertBuilder) EvaluateRule(ruleID string) (*EvaluationResult, error) {
	ab.mu.RLock()
	rule, ok := ab.rules[ruleID]
	ab.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("rule %q not found", ruleID)
	}

	if !rule.Enabled {
		return &EvaluationResult{
			RuleID:  ruleID,
			Skipped: true,
			Reason:  "rule is disabled",
		}, nil
	}

	result := &EvaluationResult{
		RuleID:        ruleID,
		EvaluatedAt:   time.Now(),
		ConditionsMet: make([]ConditionResult, 0, len(rule.Conditions)),
	}

	// Evaluate each condition
	for _, cond := range rule.Conditions {
		condResult := ab.evaluateCondition(cond)
		result.ConditionsMet = append(result.ConditionsMet, condResult)
	}

	// Apply logic
	switch rule.Logic {
	case ConditionLogicAND:
		result.Firing = true
		for _, cr := range result.ConditionsMet {
			if !cr.Met {
				result.Firing = false
				break
			}
		}
	case ConditionLogicOR:
		result.Firing = false
		for _, cr := range result.ConditionsMet {
			if cr.Met {
				result.Firing = true
				break
			}
		}
	}

	// Handle for_duration
	if result.Firing {
		if rule.firingFrom == nil {
			now := time.Now()
			rule.firingFrom = &now
		}
		if time.Since(*rule.firingFrom) < rule.ForDuration {
			result.Firing = false
			result.Pending = true
		}
	} else {
		rule.firingFrom = nil
	}

	return result, nil
}

// EvaluationResult contains the results of rule evaluation.
type EvaluationResult struct {
	RuleID        string            `json:"rule_id"`
	EvaluatedAt   time.Time         `json:"evaluated_at"`
	Firing        bool              `json:"firing"`
	Pending       bool              `json:"pending"`
	Skipped       bool              `json:"skipped"`
	Reason        string            `json:"reason,omitempty"`
	ConditionsMet []ConditionResult `json:"conditions_met"`
}

// ConditionResult contains the result of a single condition evaluation.
type ConditionResult struct {
	ConditionID string  `json:"condition_id"`
	Met         bool    `json:"met"`
	Value       float64 `json:"value"`
	Threshold   float64 `json:"threshold"`
	Error       string  `json:"error,omitempty"`
}

// evaluateCondition evaluates a single condition.
func (ab *AlertBuilder) evaluateCondition(cond BuilderCondition) ConditionResult {
	result := ConditionResult{
		ConditionID: cond.ID,
		Threshold:   cond.Value,
	}

	// Query current data
	now := time.Now()
	window := cond.Window
	if window <= 0 {
		window = time.Minute
	}

	queryResult, err := ab.db.Execute(&Query{
		Metric: cond.Metric,
		Start:  now.Add(-window).UnixNano(),
		End:    now.UnixNano(),
		Tags:   cond.TagFilters,
	})
	if err != nil {
		result.Error = err.Error()
		return result
	}

	if queryResult == nil || len(queryResult.Points) == 0 {
		if cond.Type == ConditionTypeAbsence {
			result.Met = true
		}
		return result
	}

	// Calculate value based on condition type
	switch cond.Type {
	case ConditionTypeThreshold:
		// Use latest value
		result.Value = queryResult.Points[len(queryResult.Points)-1].Value

	case ConditionTypeAggregation:
		result.Value = ab.calculateAggregation(queryResult.Points, cond.Aggregation)

	case ConditionTypeRate:
		result.Value = ab.calculateRate(queryResult.Points, cond.RateInterval)

	default:
		result.Value = queryResult.Points[len(queryResult.Points)-1].Value
	}

	result.Met = ab.evaluateOperator(result.Value, cond.Operator, cond.Value)
	return result
}

// calculateAggregation calculates an aggregation over points.
func (ab *AlertBuilder) calculateAggregation(points []Point, aggType AggregationType) float64 {
	if len(points) == 0 {
		return 0
	}

	var values []float64
	for _, p := range points {
		values = append(values, p.Value)
	}

	switch aggType {
	case BuilderAggregationAvg:
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case BuilderAggregationSum:
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum

	case BuilderAggregationMin:
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min

	case BuilderAggregationMax:
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max

	case BuilderAggregationCount:
		return float64(len(values))

	case BuilderAggregationP50:
		return ab.percentile(values, 50)

	case BuilderAggregationP90:
		return ab.percentile(values, 90)

	case BuilderAggregationP99:
		return ab.percentile(values, 99)

	default:
		return values[len(values)-1]
	}
}

// percentile calculates the p-th percentile.
func (ab *AlertBuilder) percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	idx := (p / 100.0) * float64(len(sorted)-1)
	lower := int(idx)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[lower]
	}

	weight := idx - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// calculateRate calculates the rate of change.
func (ab *AlertBuilder) calculateRate(points []Point, interval time.Duration) float64 {
	if len(points) < 2 {
		return 0
	}

	// Use first and last points
	first := points[0]
	last := points[len(points)-1]

	timeDiff := float64(last.Timestamp-first.Timestamp) / float64(time.Second)
	if timeDiff <= 0 {
		return 0
	}

	valueDiff := last.Value - first.Value
	return valueDiff / timeDiff
}

// GetTrigger retrieves a trigger by ID.
func (ab *AlertBuilder) GetTrigger(id string) (*AlertTrigger, error) {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	trigger, ok := ab.triggers[id]
	if !ok {
		return nil, fmt.Errorf("trigger %q not found", id)
	}

	return trigger, nil
}

// ListTriggers returns all triggers.
func (ab *AlertBuilder) ListTriggers(state TriggerState) []*AlertTrigger {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	triggers := make([]*AlertTrigger, 0)
	for _, t := range ab.triggers {
		if state == "" || t.State == state {
			triggers = append(triggers, t)
		}
	}

	// Sort by fired time (newest first)
	sort.Slice(triggers, func(i, j int) bool {
		return triggers[i].FiredAt.After(triggers[j].FiredAt)
	})

	return triggers
}

// AcknowledgeTrigger acknowledges a trigger.
func (ab *AlertBuilder) AcknowledgeTrigger(id string) error {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	trigger, ok := ab.triggers[id]
	if !ok {
		return fmt.Errorf("trigger %q not found", id)
	}

	// Mark as resolved
	now := time.Now()
	trigger.ResolvedAt = &now
	trigger.State = TriggerStateResolved

	return nil
}

// GetAvailableMetrics returns metrics that can be used in conditions.
func (ab *AlertBuilder) GetAvailableMetrics() []string {
	metrics := ab.db.Metrics()
	sort.Strings(metrics)
	return metrics
}

// GetConditionTemplates returns predefined condition templates.
func (ab *AlertBuilder) GetConditionTemplates() []ConditionTemplate {
	return []ConditionTemplate{
		{
			Name:        "High CPU Usage",
			Description: "Alert when CPU usage exceeds threshold",
			Type:        ConditionTypeThreshold,
			MetricHint:  "cpu.usage",
			Operator:    OperatorGT,
			Value:       80.0,
		},
		{
			Name:        "Memory Pressure",
			Description: "Alert when memory usage is high",
			Type:        ConditionTypeThreshold,
			MetricHint:  "memory.percent",
			Operator:    OperatorGT,
			Value:       90.0,
		},
		{
			Name:        "High Error Rate",
			Description: "Alert when error rate exceeds threshold",
			Type:        ConditionTypeRate,
			MetricHint:  "errors.count",
			Operator:    OperatorGT,
			Value:       10.0,
		},
		{
			Name:        "Latency Spike (P99)",
			Description: "Alert when P99 latency exceeds threshold",
			Type:        ConditionTypeAggregation,
			MetricHint:  "request.latency",
			Operator:    OperatorGT,
			Value:       500.0,
			Aggregation: BuilderAggregationP99,
		},
		{
			Name:        "Missing Data",
			Description: "Alert when data is missing",
			Type:        ConditionTypeAbsence,
			MetricHint:  "",
			Operator:    OperatorEQ,
			Value:       1.0,
		},
	}
}

// ConditionTemplate provides a predefined condition setup.
type ConditionTemplate struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Type        ConditionType   `json:"type"`
	MetricHint  string          `json:"metric_hint"`
	Operator    Operator        `json:"operator"`
	Value       float64         `json:"value"`
	Aggregation AggregationType `json:"aggregation,omitempty"`
	Window      time.Duration   `json:"window,omitempty"`
}

// generateAlertID generates a unique alert ID.
func generateAlertID() string {
	return fmt.Sprintf("alert-%d", time.Now().UnixNano())
}

// ExportRules exports rules as JSON.
func (ab *AlertBuilder) ExportRules() ([]byte, error) {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	rules := make([]*BuilderRule, 0, len(ab.rules))
	for _, r := range ab.rules {
		rules = append(rules, r)
	}

	return json.Marshal(rules)
}

// ImportRules imports rules from JSON.
func (ab *AlertBuilder) ImportRules(data []byte) error {
	var rules []*BuilderRule
	if err := json.Unmarshal(data, &rules); err != nil {
		return fmt.Errorf("failed to unmarshal rules: %w", err)
	}

	ab.mu.Lock()
	defer ab.mu.Unlock()

	for _, rule := range rules {
		if err := ab.validateRule(rule); err != nil {
			return fmt.Errorf("invalid rule %q: %w", rule.Name, err)
		}
		ab.rules[rule.ID] = rule
	}

	return nil
}

// ValidateExpression validates a rule expression string.
func (ab *AlertBuilder) ValidateExpression(expr string) error {
	// Simple validation for common patterns
	if expr == "" {
		return fmt.Errorf("expression cannot be empty")
	}

	// Check for valid metric pattern
	metricPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*\s*(>|>=|<|<=|==|!=)\s*[\d.]+$`)
	if !metricPattern.MatchString(strings.TrimSpace(expr)) {
		return fmt.Errorf("invalid expression format: expected 'metric operator value'")
	}

	return nil
}

// HTTP Handlers

// setupAlertBuilderRoutes sets up HTTP routes for the alert builder.
func setupAlertBuilderRoutes(mux *http.ServeMux, ab *AlertBuilder, wrap func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("/api/v1/alerts/rules", wrap(ab.handleRules))
	mux.HandleFunc("/api/v1/alerts/rules/", wrap(ab.handleRule))
	mux.HandleFunc("/api/v1/alerts/preview", wrap(ab.handlePreview))
	mux.HandleFunc("/api/v1/alerts/triggers", wrap(ab.handleTriggers))
	mux.HandleFunc("/api/v1/alerts/triggers/", wrap(ab.handleTrigger))
	mux.HandleFunc("/api/v1/alerts/metrics", wrap(ab.handleMetrics))
	mux.HandleFunc("/api/v1/alerts/templates", wrap(ab.handleTemplates))
	mux.HandleFunc("/api/v1/alerts/validate", wrap(ab.handleValidate))
}

func (ab *AlertBuilder) handleRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rules := ab.ListRules()
		writeJSONStatus(w, http.StatusOK, rules)

	case http.MethodPost:
		var rule BuilderRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		if err := ab.CreateRule(&rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		writeJSONStatus(w, http.StatusCreated, rule)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ab *AlertBuilder) handleRule(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/alerts/rules/")
	if id == "" {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "rule ID required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		rule, err := ab.GetRule(id)
		if err != nil {
			writeJSONStatus(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSONStatus(w, http.StatusOK, rule)

	case http.MethodPut:
		var rule BuilderRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		if err := ab.UpdateRule(id, &rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		writeJSONStatus(w, http.StatusOK, rule)

	case http.MethodDelete:
		if err := ab.DeleteRule(id); err != nil {
			writeJSONStatus(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ab *AlertBuilder) handlePreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var rule BuilderRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := ab.PreviewRule(&rule)
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	writeJSONStatus(w, http.StatusOK, result)
}

func (ab *AlertBuilder) handleTriggers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	state := TriggerState(r.URL.Query().Get("state"))
	triggers := ab.ListTriggers(state)
	writeJSONStatus(w, http.StatusOK, triggers)
}

func (ab *AlertBuilder) handleTrigger(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/alerts/triggers/")
	if id == "" {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "trigger ID required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		trigger, err := ab.GetTrigger(id)
		if err != nil {
			writeJSONStatus(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSONStatus(w, http.StatusOK, trigger)

	case http.MethodPost:
		// Acknowledge trigger
		action := r.URL.Query().Get("action")
		if action == "acknowledge" {
			if err := ab.AcknowledgeTrigger(id); err != nil {
				writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			w.WriteHeader(http.StatusOK)
		} else {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "unknown action"})
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ab *AlertBuilder) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	metrics := ab.GetAvailableMetrics()
	writeJSONStatus(w, http.StatusOK, metrics)
}

func (ab *AlertBuilder) handleTemplates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	templates := ab.GetConditionTemplates()
	writeJSONStatus(w, http.StatusOK, templates)
}

func (ab *AlertBuilder) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Expression string       `json:"expression"`
		Rule       *BuilderRule `json:"rule,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if req.Expression != "" {
		if err := ab.ValidateExpression(req.Expression); err != nil {
			writeJSONStatus(w, http.StatusOK, map[string]any{
				"valid": false,
				"error": err.Error(),
			})
			return
		}
	}

	if req.Rule != nil {
		if err := ab.validateRule(req.Rule); err != nil {
			writeJSONStatus(w, http.StatusOK, map[string]any{
				"valid": false,
				"error": err.Error(),
			})
			return
		}
	}

	writeJSONStatus(w, http.StatusOK, map[string]any{
		"valid": true,
	})
}
