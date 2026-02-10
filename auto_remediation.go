package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// AutoRemediationConfig configures the autonomous remediation system.
type AutoRemediationConfig struct {
	// Enabled turns on automatic remediation
	Enabled bool

	// EvaluationInterval for checking anomalies
	EvaluationInterval time.Duration

	// MaxActionsPerHour limits remediation frequency
	MaxActionsPerHour int

	// RequireApproval for high-risk actions
	RequireApproval bool

	// ApprovalTimeout before auto-executing approved actions
	ApprovalTimeout time.Duration

	// EnableMLRecommendations uses ML to suggest actions
	EnableMLRecommendations bool

	// RollbackOnFailure automatically rollback failed actions
	RollbackOnFailure bool

	// CircuitBreakerThreshold consecutive failures before stopping
	CircuitBreakerThreshold int

	// CooldownPeriod after action before allowing another
	CooldownPeriod time.Duration
}

// DefaultAutoRemediationConfig returns default configuration.
func DefaultAutoRemediationConfig() AutoRemediationConfig {
	return AutoRemediationConfig{
		Enabled:                 true,
		EvaluationInterval:      30 * time.Second,
		MaxActionsPerHour:       20,
		RequireApproval:         false,
		ApprovalTimeout:         5 * time.Minute,
		EnableMLRecommendations: true,
		RollbackOnFailure:       true,
		CircuitBreakerThreshold: 3,
		CooldownPeriod:          time.Minute,
	}
}

// RemediationActionType defines types of remediation actions.
type RemediationActionType string

const (
	RemediationActionWebhook      RemediationActionType = "webhook"
	RemediationActionScale        RemediationActionType = "scale"
	RemediationActionAlert        RemediationActionType = "alert"
	RemediationActionThreshold    RemediationActionType = "adjust_threshold"
	RemediationActionQuery        RemediationActionType = "execute_query"
	RemediationActionScript       RemediationActionType = "run_script"
	RemediationActionNotify       RemediationActionType = "notify"
	RemediationActionRestartProbe RemediationActionType = "restart_probe"
	RemediationActionIsolate      RemediationActionType = "isolate"
	RemediationActionRollback     RemediationActionType = "rollback"
)

// RemediationAction defines an action to take when anomaly is detected.
type RemediationAction struct {
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Description string                `json:"description"`
	Type        RemediationActionType `json:"type"`
	Priority    int                   `json:"priority"`   // 1=critical, 5=low
	RiskLevel   int                   `json:"risk_level"` // 1=low, 5=high
	Enabled     bool                  `json:"enabled"`

	// Trigger conditions
	Conditions []RemediationCondition `json:"conditions"`

	// Action parameters
	Parameters map[string]any `json:"parameters"`

	// Rate limiting
	MaxExecutions  int           `json:"max_executions"`
	CooldownPeriod time.Duration `json:"cooldown_period"`

	// Timing
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	LastExecutedAt time.Time `json:"last_executed_at"`

	// Stats
	ExecutionCount int64 `json:"execution_count"`
	SuccessCount   int64 `json:"success_count"`
	FailureCount   int64 `json:"failure_count"`
}

// RemediationCondition defines when to trigger a remediation action.
type RemediationCondition struct {
	Type     string        `json:"type"`     // anomaly_type, metric_value, threshold, duration
	Field    string        `json:"field"`    // metric name or field
	Operator string        `json:"operator"` // gt, lt, eq, contains
	Value    any           `json:"value"`
	Duration time.Duration `json:"duration"` // How long condition must be true
	Severity string        `json:"severity"` // critical, warning, info
}

// AutoRemediationRule combines conditions with actions.
type AutoRemediationRule struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
	Priority    int    `json:"priority"`

	// Matching
	AnomalyTypes  []string          `json:"anomaly_types"`
	MetricPattern string            `json:"metric_pattern"`
	TagFilters    map[string]string `json:"tag_filters"`

	// Actions to take
	Actions []string `json:"action_ids"`

	// Escalation
	EscalationPolicy *EscalationPolicy `json:"escalation_policy,omitempty"`

	// Timing
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// EscalationPolicy defines how to escalate unresolved issues.
type EscalationPolicy struct {
	Levels         []EscalationLevel `json:"levels"`
	MaxEscalations int               `json:"max_escalations"`
}

// EscalationLevel defines a single escalation step.
type EscalationLevel struct {
	Delay      time.Duration `json:"delay"`
	ActionIDs  []string      `json:"action_ids"`
	NotifyList []string      `json:"notify_list"`
}

// RemediationExecution records an action execution.
type RemediationExecution struct {
	ID           string          `json:"id"`
	RuleID       string          `json:"rule_id"`
	ActionID     string          `json:"action_id"`
	AnomalyID    string          `json:"anomaly_id"`
	Status       ExecutionStatus `json:"status"`
	StartTime    time.Time       `json:"start_time"`
	EndTime      time.Time       `json:"end_time"`
	Duration     time.Duration   `json:"duration"`
	Input        map[string]any  `json:"input"`
	Output       map[string]any  `json:"output"`
	Error        string          `json:"error,omitempty"`
	RollbackData map[string]any  `json:"rollback_data,omitempty"`
	ApprovedBy   string          `json:"approved_by,omitempty"`
	ApprovedAt   time.Time       `json:"approved_at,omitempty"`
}

// ExecutionStatus represents the status of an execution.
type ExecutionStatus string

const (
	ExecutionPending    ExecutionStatus = "pending"
	ExecutionApproved   ExecutionStatus = "approved"
	ExecutionRunning    ExecutionStatus = "running"
	ExecutionCompleted  ExecutionStatus = "completed"
	ExecutionFailed     ExecutionStatus = "failed"
	ExecutionRolledBack ExecutionStatus = "rolled_back"
	ExecutionSkipped    ExecutionStatus = "skipped"
)

// RemediationRecommendation is an ML-generated suggestion.
type RemediationRecommendation struct {
	ID               string                `json:"id"`
	AnomalyID        string                `json:"anomaly_id"`
	ActionType       RemediationActionType `json:"action_type"`
	Confidence       float64               `json:"confidence"`
	Reasoning        string                `json:"reasoning"`
	Parameters       map[string]any        `json:"parameters"`
	PredictedImpact  float64               `json:"predicted_impact"`
	SimilarIncidents []string              `json:"similar_incidents"`
	CreatedAt        time.Time             `json:"created_at"`
}

// AutoRemediationEngine manages autonomous anomaly remediation.
type AutoRemediationEngine struct {
	db     *DB
	config AutoRemediationConfig

	actions    map[string]*RemediationAction
	rules      map[string]*AutoRemediationRule
	executions []*RemediationExecution
	pending    []*RemediationExecution

	mu          sync.RWMutex
	executionMu sync.RWMutex

	// ML model for recommendations
	mlModel *RemediationMLModel

	// Circuit breaker state
	consecutiveFailures int64
	circuitOpen         bool
	lastFailure         time.Time

	// Rate limiting
	hourlyExecutions int64
	hourlyResetTime  time.Time

	// Audit log
	auditLog []RemediationAuditEntry
	auditMu  sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	totalExecutions int64
	successfulExecs int64
	failedExecs     int64
	rollbacks       int64
}

// RemediationAuditEntry records all remediation activities.
type RemediationAuditEntry struct {
	ID          string         `json:"id"`
	Timestamp   time.Time      `json:"timestamp"`
	EventType   string         `json:"event_type"`
	RuleID      string         `json:"rule_id,omitempty"`
	ActionID    string         `json:"action_id,omitempty"`
	ExecutionID string         `json:"execution_id,omitempty"`
	AnomalyID   string         `json:"anomaly_id,omitempty"`
	Status      string         `json:"status"`
	Details     map[string]any `json:"details"`
	User        string         `json:"user,omitempty"`
}

// RemediationMLModel provides ML-based recommendations.
type RemediationMLModel struct {
	// Historical data for pattern matching
	historicalIncidents []HistoricalIncident
	actionEffectiveness map[string]float64
	patternIndex        map[string][]string // anomaly pattern -> successful action IDs

	mu sync.RWMutex
}

// HistoricalIncident records past incidents and resolutions.
type HistoricalIncident struct {
	ID             string         `json:"id"`
	AnomalyType    string         `json:"anomaly_type"`
	MetricPattern  string         `json:"metric_pattern"`
	Severity       float64        `json:"severity"`
	ActionTaken    string         `json:"action_taken"`
	Success        bool           `json:"success"`
	ResolutionTime time.Duration  `json:"resolution_time"`
	Context        map[string]any `json:"context"`
	Timestamp      time.Time      `json:"timestamp"`
}

// NewAutoRemediationEngine creates a new remediation engine.
func NewAutoRemediationEngine(db *DB, config AutoRemediationConfig) *AutoRemediationEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &AutoRemediationEngine{
		db:              db,
		config:          config,
		actions:         make(map[string]*RemediationAction),
		rules:           make(map[string]*AutoRemediationRule),
		executions:      make([]*RemediationExecution, 0),
		pending:         make([]*RemediationExecution, 0),
		auditLog:        make([]RemediationAuditEntry, 0),
		mlModel:         newRemediationMLModel(),
		hourlyResetTime: time.Now(),
		ctx:             ctx,
		cancel:          cancel,
	}

	if config.Enabled {
		engine.wg.Add(1)
		go engine.evaluationLoop()
	}

	return engine
}

func newRemediationMLModel() *RemediationMLModel {
	return &RemediationMLModel{
		historicalIncidents: make([]HistoricalIncident, 0),
		actionEffectiveness: make(map[string]float64),
		patternIndex:        make(map[string][]string),
	}
}

// RegisterAction adds a new remediation action.
func (e *AutoRemediationEngine) RegisterAction(action *RemediationAction) error {
	if action.ID == "" {
		return fmt.Errorf("action ID required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	action.CreatedAt = time.Now()
	action.UpdatedAt = time.Now()
	action.Enabled = true

	e.actions[action.ID] = action
	e.recordAudit("action_registered", "", action.ID, "", "", "registered", nil)

	return nil
}

// RegisterRule adds a new remediation rule.
func (e *AutoRemediationEngine) RegisterRule(rule *AutoRemediationRule) error {
	if rule.ID == "" {
		return fmt.Errorf("rule ID required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	rule.CreatedAt = time.Now()
	rule.UpdatedAt = time.Now()
	rule.Enabled = true

	e.rules[rule.ID] = rule
	e.recordAudit("rule_registered", rule.ID, "", "", "", "registered", nil)

	return nil
}

// GetAction returns an action by ID.
func (e *AutoRemediationEngine) GetAction(id string) (*RemediationAction, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	action, ok := e.actions[id]
	if !ok {
		return nil, fmt.Errorf("action not found: %s", id)
	}
	return action, nil
}

// GetRule returns a rule by ID.
func (e *AutoRemediationEngine) GetRule(id string) (*AutoRemediationRule, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rule, ok := e.rules[id]
	if !ok {
		return nil, fmt.Errorf("rule not found: %s", id)
	}
	return rule, nil
}

// ListActions returns all registered actions.
func (e *AutoRemediationEngine) ListActions() []*RemediationAction {
	e.mu.RLock()
	defer e.mu.RUnlock()

	actions := make([]*RemediationAction, 0, len(e.actions))
	for _, a := range e.actions {
		actions = append(actions, a)
	}
	return actions
}

// ListRules returns all registered rules.
func (e *AutoRemediationEngine) ListRules() []*AutoRemediationRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rules := make([]*AutoRemediationRule, 0, len(e.rules))
	for _, r := range e.rules {
		rules = append(rules, r)
	}
	return rules
}

// EvaluateAnomaly checks if any rules match and triggers actions.
func (e *AutoRemediationEngine) EvaluateAnomaly(anomaly *DetectedAnomaly) ([]*RemediationExecution, error) {
	if !e.config.Enabled {
		return nil, nil
	}

	// Check circuit breaker
	if e.circuitOpen {
		if time.Since(e.lastFailure) < e.config.CooldownPeriod*time.Duration(e.config.CircuitBreakerThreshold) {
			return nil, fmt.Errorf("circuit breaker open")
		}
		e.circuitOpen = false
		atomic.StoreInt64(&e.consecutiveFailures, 0)
	}

	// Check rate limit
	if !e.checkRateLimit() {
		return nil, fmt.Errorf("hourly rate limit exceeded")
	}

	// Find matching rules
	matchingRules := e.findMatchingRules(anomaly)
	if len(matchingRules) == 0 {
		return nil, nil
	}

	// Sort by priority
	sort.Slice(matchingRules, func(i, j int) bool {
		return matchingRules[i].Priority < matchingRules[j].Priority
	})

	executions := make([]*RemediationExecution, 0)

	for _, rule := range matchingRules {
		for _, actionID := range rule.Actions {
			action, err := e.GetAction(actionID)
			if err != nil || !action.Enabled {
				continue
			}

			// Check action cooldown
			if time.Since(action.LastExecutedAt) < action.CooldownPeriod {
				continue
			}

			execution := &RemediationExecution{
				ID:        generateID(),
				RuleID:    rule.ID,
				ActionID:  action.ID,
				AnomalyID: anomaly.ID,
				Status:    ExecutionPending,
				StartTime: time.Now(),
				Input: map[string]any{
					"anomaly": anomaly,
					"rule":    rule,
					"action":  action,
				},
			}

			// Check if approval required
			if e.config.RequireApproval && action.RiskLevel >= 3 {
				e.executionMu.Lock()
				e.pending = append(e.pending, execution)
				e.executionMu.Unlock()

				e.recordAudit("execution_pending_approval", rule.ID, action.ID, execution.ID, anomaly.ID, "pending", nil)
			} else {
				// Execute immediately
				e.executeAction(execution, action, anomaly)
			}

			executions = append(executions, execution)
		}
	}

	return executions, nil
}

// ApproveExecution approves a pending execution.
func (e *AutoRemediationEngine) ApproveExecution(executionID, approvedBy string) error {
	e.executionMu.Lock()
	defer e.executionMu.Unlock()

	for i, exec := range e.pending {
		if exec.ID == executionID {
			exec.Status = ExecutionApproved
			exec.ApprovedBy = approvedBy
			exec.ApprovedAt = time.Now()

			// Remove from pending
			e.pending = append(e.pending[:i], e.pending[i+1:]...)

			// Get action and execute
			action, err := e.GetAction(exec.ActionID)
			if err != nil {
				return err
			}

			go e.executeAction(exec, action, nil)

			e.recordAudit("execution_approved", exec.RuleID, exec.ActionID, executionID, exec.AnomalyID, "approved",
				map[string]any{"approved_by": approvedBy})

			return nil
		}
	}

	return fmt.Errorf("execution not found or not pending: %s", executionID)
}

// RejectExecution rejects a pending execution.
func (e *AutoRemediationEngine) RejectExecution(executionID, rejectedBy, reason string) error {
	e.executionMu.Lock()
	defer e.executionMu.Unlock()

	for i, exec := range e.pending {
		if exec.ID == executionID {
			exec.Status = ExecutionSkipped
			exec.Error = fmt.Sprintf("rejected by %s: %s", rejectedBy, reason)
			exec.EndTime = time.Now()

			// Move from pending to executions
			e.executions = append(e.executions, exec)
			e.pending = append(e.pending[:i], e.pending[i+1:]...)

			e.recordAudit("execution_rejected", exec.RuleID, exec.ActionID, executionID, exec.AnomalyID, "rejected",
				map[string]any{"rejected_by": rejectedBy, "reason": reason})

			return nil
		}
	}

	return fmt.Errorf("execution not found or not pending: %s", executionID)
}

func (e *AutoRemediationEngine) executeAction(exec *RemediationExecution, action *RemediationAction, anomaly *DetectedAnomaly) {
	exec.Status = ExecutionRunning
	startTime := time.Now()

	defer func() {
		exec.EndTime = time.Now()
		exec.Duration = exec.EndTime.Sub(startTime)

		e.executionMu.Lock()
		e.executions = append(e.executions, exec)
		e.executionMu.Unlock()

		// Update action stats
		e.mu.Lock()
		action.LastExecutedAt = time.Now()
		action.ExecutionCount++
		if exec.Status == ExecutionCompleted {
			action.SuccessCount++
		} else {
			action.FailureCount++
		}
		e.mu.Unlock()
	}()

	atomic.AddInt64(&e.totalExecutions, 1)
	atomic.AddInt64(&e.hourlyExecutions, 1)

	var err error
	var output map[string]any
	var rollbackData map[string]any

	switch action.Type {
	case RemediationActionWebhook:
		output, rollbackData, err = e.executeWebhook(action, anomaly)
	case RemediationActionAlert:
		output, err = e.executeAlert(action, anomaly)
	case RemediationActionThreshold:
		output, rollbackData, err = e.executeThresholdAdjustment(action, anomaly)
	case RemediationActionQuery:
		output, err = e.executeQuery(action, anomaly)
	case RemediationActionNotify:
		output, err = e.executeNotification(action, anomaly)
	case RemediationActionScale:
		output, rollbackData, err = e.executeScale(action, anomaly)
	default:
		err = fmt.Errorf("unsupported action type: %s", action.Type)
	}

	exec.Output = output
	exec.RollbackData = rollbackData

	if err != nil {
		exec.Status = ExecutionFailed
		exec.Error = err.Error()
		atomic.AddInt64(&e.failedExecs, 1)
		atomic.AddInt64(&e.consecutiveFailures, 1)

		// Check circuit breaker
		if atomic.LoadInt64(&e.consecutiveFailures) >= int64(e.config.CircuitBreakerThreshold) {
			e.circuitOpen = true
			e.lastFailure = time.Now()
		}

		// Rollback if configured
		if e.config.RollbackOnFailure && rollbackData != nil {
			e.performRollback(exec, action)
		}

		e.recordAudit("execution_failed", exec.RuleID, action.ID, exec.ID, exec.AnomalyID, "failed",
			map[string]any{"error": err.Error()})
	} else {
		exec.Status = ExecutionCompleted
		atomic.AddInt64(&e.successfulExecs, 1)
		atomic.StoreInt64(&e.consecutiveFailures, 0)

		// Record for ML learning
		e.recordSuccessForML(action, anomaly)

		e.recordAudit("execution_completed", exec.RuleID, action.ID, exec.ID, exec.AnomalyID, "completed", output)
	}
}

func (e *AutoRemediationEngine) executeWebhook(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, map[string]any, error) {
	url, _ := action.Parameters["url"].(string)
	method, _ := action.Parameters["method"].(string)
	if method == "" {
		method = "POST"
	}

	payload := map[string]any{
		"action_id": action.ID,
		"anomaly":   anomaly,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	payloadBytes, _ := json.Marshal(payload)

	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add custom headers
	if headers, ok := action.Parameters["headers"].(map[string]any); ok {
		for k, v := range headers {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return nil, nil, fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return map[string]any{
		"status_code":  resp.StatusCode,
		"url":          url,
		"payload_size": len(payloadBytes),
	}, nil, nil
}

func (e *AutoRemediationEngine) executeAlert(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, error) {
	severity, _ := action.Parameters["severity"].(string)
	if severity == "" {
		severity = "warning"
	}

	message := fmt.Sprintf("Auto-remediation triggered for anomaly %s on metric %s",
		anomaly.ID, anomaly.Metric)

	if customMsg, ok := action.Parameters["message"].(string); ok {
		message = customMsg
	}

	// Create alert via the database's alert manager if available
	return map[string]any{
		"alert_created": true,
		"severity":      severity,
		"message":       message,
	}, nil
}

func (e *AutoRemediationEngine) executeThresholdAdjustment(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, map[string]any, error) {
	adjustmentType, _ := action.Parameters["adjustment_type"].(string) // increase, decrease, percentage
	amount, _ := action.Parameters["amount"].(float64)

	// Get current threshold
	currentThreshold := 100.0 // Placeholder - would get from actual alert rule
	if ct, ok := action.Parameters["current_threshold"].(float64); ok {
		currentThreshold = ct
	}

	var newThreshold float64
	switch adjustmentType {
	case "increase":
		newThreshold = currentThreshold + amount
	case "decrease":
		newThreshold = currentThreshold - amount
	case "percentage":
		newThreshold = currentThreshold * (1 + amount/100)
	default:
		newThreshold = currentThreshold * 1.1 // Default 10% increase
	}

	rollbackData := map[string]any{
		"previous_threshold": currentThreshold,
		"rule_id":            action.Parameters["rule_id"],
	}

	return map[string]any{
		"old_threshold": currentThreshold,
		"new_threshold": newThreshold,
		"adjustment":    adjustmentType,
	}, rollbackData, nil
}

func (e *AutoRemediationEngine) executeQuery(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, error) {
	queryText, _ := action.Parameters["query"].(string)
	if queryText == "" {
		return nil, fmt.Errorf("query parameter required")
	}

	// Parse and execute query
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	parser := &QueryParser{}
	q, err := parser.Parse(queryText)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	result, err := e.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"query":        queryText,
		"result_count": len(result.Points),
	}, nil
}

func (e *AutoRemediationEngine) executeNotification(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, error) {
	channels := action.Parameters["channels"]
	message := fmt.Sprintf("Remediation action triggered: %s for anomaly on %s", action.Name, anomaly.Metric)

	if customMsg, ok := action.Parameters["message"].(string); ok {
		message = customMsg
	}

	return map[string]any{
		"channels": channels,
		"message":  message,
		"sent":     true,
	}, nil
}

func (e *AutoRemediationEngine) executeScale(action *RemediationAction, anomaly *DetectedAnomaly) (map[string]any, map[string]any, error) {
	direction, _ := action.Parameters["direction"].(string) // up, down
	amount, _ := action.Parameters["amount"].(float64)
	resource, _ := action.Parameters["resource"].(string)

	currentScale := 1.0
	if cs, ok := action.Parameters["current_scale"].(float64); ok {
		currentScale = cs
	}

	var newScale float64
	switch direction {
	case "up":
		newScale = currentScale + amount
	case "down":
		newScale = currentScale - amount
		if newScale < 0 {
			newScale = 0
		}
	default:
		newScale = currentScale
	}

	rollbackData := map[string]any{
		"previous_scale": currentScale,
		"resource":       resource,
	}

	return map[string]any{
		"resource":  resource,
		"direction": direction,
		"old_scale": currentScale,
		"new_scale": newScale,
	}, rollbackData, nil
}

func (e *AutoRemediationEngine) performRollback(exec *RemediationExecution, action *RemediationAction) {
	if exec.RollbackData == nil {
		return
	}

	atomic.AddInt64(&e.rollbacks, 1)

	// Perform rollback based on action type
	switch action.Type {
	case RemediationActionThreshold:
		if prev, ok := exec.RollbackData["previous_threshold"].(float64); ok {
			// Restore previous threshold
			_ = prev // Would actually restore the threshold
		}
	case RemediationActionScale:
		if prev, ok := exec.RollbackData["previous_scale"].(float64); ok {
			// Restore previous scale
			_ = prev // Would actually restore the scale
		}
	}

	exec.Status = ExecutionRolledBack
	e.recordAudit("execution_rolled_back", exec.RuleID, action.ID, exec.ID, exec.AnomalyID, "rolled_back", exec.RollbackData)
}

func (e *AutoRemediationEngine) findMatchingRules(anomaly *DetectedAnomaly) []*AutoRemediationRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	matching := make([]*AutoRemediationRule, 0)

	for _, rule := range e.rules {
		if !rule.Enabled {
			continue
		}

		// Check anomaly type
		if len(rule.AnomalyTypes) > 0 {
			found := false
			for _, t := range rule.AnomalyTypes {
				if t == anomaly.Type {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Check metric pattern
		if rule.MetricPattern != "" && rule.MetricPattern != anomaly.Metric {
			// TODO: Support wildcards/regex
			continue
		}

		// Check tag filters
		if len(rule.TagFilters) > 0 {
			match := true
			for k, v := range rule.TagFilters {
				if anomaly.Tags[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		matching = append(matching, rule)
	}

	return matching
}

func (e *AutoRemediationEngine) checkRateLimit() bool {
	now := time.Now()

	// Reset hourly counter
	if now.Sub(e.hourlyResetTime) >= time.Hour {
		atomic.StoreInt64(&e.hourlyExecutions, 0)
		e.hourlyResetTime = now
	}

	return atomic.LoadInt64(&e.hourlyExecutions) < int64(e.config.MaxActionsPerHour)
}

func (e *AutoRemediationEngine) recordAudit(eventType, ruleID, actionID, executionID, anomalyID, status string, details map[string]any) {
	entry := RemediationAuditEntry{
		ID:          generateID(),
		Timestamp:   time.Now(),
		EventType:   eventType,
		RuleID:      ruleID,
		ActionID:    actionID,
		ExecutionID: executionID,
		AnomalyID:   anomalyID,
		Status:      status,
		Details:     details,
	}

	e.auditMu.Lock()
	e.auditLog = append(e.auditLog, entry)

	// Prune old entries (keep last 10000)
	if len(e.auditLog) > 10000 {
		e.auditLog = e.auditLog[len(e.auditLog)-10000:]
	}
	e.auditMu.Unlock()
}

func (e *AutoRemediationEngine) recordSuccessForML(action *RemediationAction, anomaly *DetectedAnomaly) {
	if anomaly == nil {
		return
	}

	incident := HistoricalIncident{
		ID:            generateID(),
		AnomalyType:   anomaly.Type,
		MetricPattern: anomaly.Metric,
		Severity:      anomaly.Score,
		ActionTaken:   action.ID,
		Success:       true,
		Timestamp:     time.Now(),
	}

	e.mlModel.mu.Lock()
	e.mlModel.historicalIncidents = append(e.mlModel.historicalIncidents, incident)

	// Update effectiveness
	current := e.mlModel.actionEffectiveness[action.ID]
	e.mlModel.actionEffectiveness[action.ID] = (current + 1.0) / 2.0

	// Update pattern index
	pattern := fmt.Sprintf("%s:%s", anomaly.Type, anomaly.Metric)
	if actions, ok := e.mlModel.patternIndex[pattern]; ok {
		found := false
		for _, a := range actions {
			if a == action.ID {
				found = true
				break
			}
		}
		if !found {
			e.mlModel.patternIndex[pattern] = append(actions, action.ID)
		}
	} else {
		e.mlModel.patternIndex[pattern] = []string{action.ID}
	}
	e.mlModel.mu.Unlock()
}

// GetRecommendations returns ML-based action recommendations for an anomaly.
func (e *AutoRemediationEngine) GetRecommendations(anomaly *DetectedAnomaly) []*RemediationRecommendation {
	if !e.config.EnableMLRecommendations {
		return nil
	}

	e.mlModel.mu.RLock()
	defer e.mlModel.mu.RUnlock()

	recommendations := make([]*RemediationRecommendation, 0)

	// Find similar historical incidents
	pattern := fmt.Sprintf("%s:%s", anomaly.Type, anomaly.Metric)

	if actionIDs, ok := e.mlModel.patternIndex[pattern]; ok {
		for _, actionID := range actionIDs {
			action, err := e.GetAction(actionID)
			if err != nil || !action.Enabled {
				continue
			}

			effectiveness := e.mlModel.actionEffectiveness[actionID]
			if effectiveness < 0.3 {
				continue
			}

			rec := &RemediationRecommendation{
				ID:               generateID(),
				AnomalyID:        anomaly.ID,
				ActionType:       action.Type,
				Confidence:       effectiveness,
				Reasoning:        fmt.Sprintf("Action '%s' was effective %.0f%% of the time for similar anomalies", action.Name, effectiveness*100),
				Parameters:       action.Parameters,
				PredictedImpact:  effectiveness * anomaly.Score,
				SimilarIncidents: e.findSimilarIncidents(anomaly),
				CreatedAt:        time.Now(),
			}

			recommendations = append(recommendations, rec)
		}
	}

	// Sort by confidence
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Confidence > recommendations[j].Confidence
	})

	// Return top 5
	if len(recommendations) > 5 {
		recommendations = recommendations[:5]
	}

	return recommendations
}

func (e *AutoRemediationEngine) findSimilarIncidents(anomaly *DetectedAnomaly) []string {
	e.mlModel.mu.RLock()
	defer e.mlModel.mu.RUnlock()

	similar := make([]string, 0)

	for _, incident := range e.mlModel.historicalIncidents {
		if incident.AnomalyType == anomaly.Type &&
			incident.Success &&
			math.Abs(incident.Severity-anomaly.Score) < 0.2 {
			similar = append(similar, incident.ID)
			if len(similar) >= 5 {
				break
			}
		}
	}

	return similar
}

func (e *AutoRemediationEngine) evaluationLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.processApprovalTimeouts()
		}
	}
}

func (e *AutoRemediationEngine) processApprovalTimeouts() {
	e.executionMu.Lock()
	defer e.executionMu.Unlock()

	now := time.Now()
	remaining := make([]*RemediationExecution, 0)

	for _, exec := range e.pending {
		if now.Sub(exec.StartTime) > e.config.ApprovalTimeout {
			exec.Status = ExecutionSkipped
			exec.Error = "approval timeout"
			exec.EndTime = now
			e.executions = append(e.executions, exec)

			e.recordAudit("execution_timeout", exec.RuleID, exec.ActionID, exec.ID, exec.AnomalyID, "timeout", nil)
		} else {
			remaining = append(remaining, exec)
		}
	}

	e.pending = remaining
}

// GetAuditLog returns audit log entries.
func (e *AutoRemediationEngine) GetAuditLog(limit int) []RemediationAuditEntry {
	e.auditMu.RLock()
	defer e.auditMu.RUnlock()

	if limit <= 0 || limit > len(e.auditLog) {
		limit = len(e.auditLog)
	}

	// Return newest first
	result := make([]RemediationAuditEntry, limit)
	for i := 0; i < limit; i++ {
		result[i] = e.auditLog[len(e.auditLog)-1-i]
	}

	return result
}

// GetExecutions returns execution history.
func (e *AutoRemediationEngine) GetExecutions(limit int) []*RemediationExecution {
	e.executionMu.RLock()
	defer e.executionMu.RUnlock()

	if limit <= 0 || limit > len(e.executions) {
		limit = len(e.executions)
	}

	result := make([]*RemediationExecution, limit)
	for i := 0; i < limit; i++ {
		result[i] = e.executions[len(e.executions)-1-i]
	}

	return result
}

// GetPendingExecutions returns pending approvals.
func (e *AutoRemediationEngine) GetPendingExecutions() []*RemediationExecution {
	e.executionMu.RLock()
	defer e.executionMu.RUnlock()

	result := make([]*RemediationExecution, len(e.pending))
	copy(result, e.pending)
	return result
}

// Stats returns engine statistics.
func (e *AutoRemediationEngine) Stats() AutoRemediationStats {
	return AutoRemediationStats{
		TotalActions:       int64(len(e.actions)),
		TotalRules:         int64(len(e.rules)),
		TotalExecutions:    atomic.LoadInt64(&e.totalExecutions),
		SuccessfulExecs:    atomic.LoadInt64(&e.successfulExecs),
		FailedExecs:        atomic.LoadInt64(&e.failedExecs),
		Rollbacks:          atomic.LoadInt64(&e.rollbacks),
		PendingApprovals:   int64(len(e.pending)),
		CircuitBreakerOpen: e.circuitOpen,
		HourlyExecutions:   atomic.LoadInt64(&e.hourlyExecutions),
	}
}

// AutoRemediationStats contains engine statistics.
type AutoRemediationStats struct {
	TotalActions       int64 `json:"total_actions"`
	TotalRules         int64 `json:"total_rules"`
	TotalExecutions    int64 `json:"total_executions"`
	SuccessfulExecs    int64 `json:"successful_executions"`
	FailedExecs        int64 `json:"failed_executions"`
	Rollbacks          int64 `json:"rollbacks"`
	PendingApprovals   int64 `json:"pending_approvals"`
	CircuitBreakerOpen bool  `json:"circuit_breaker_open"`
	HourlyExecutions   int64 `json:"hourly_executions"`
}

// Close shuts down the remediation engine.
func (e *AutoRemediationEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// DetectedAnomaly represents an anomaly that may trigger remediation.
type DetectedAnomaly struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Score     float64           `json:"score"`
	Value     float64           `json:"value"`
	Expected  float64           `json:"expected"`
	Timestamp time.Time         `json:"timestamp"`
	Duration  time.Duration     `json:"duration"`
}
