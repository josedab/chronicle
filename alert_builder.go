package chronicle

import (
	"fmt"
	"sort"
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
