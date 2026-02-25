package chronicle

// alert_builder_eval.go contains the alert evaluation engine and aggregation
// logic for the AlertBuilder. See alert_builder.go for types and CRUD operations.

import (
	"fmt"
	"sort"
	"time"
)

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
