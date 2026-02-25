package chronicle

import (
	"context"
	"fmt"
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

	// AllowedWebhookDomains restricts webhook URLs to these domains. If empty, all non-private HTTPS URLs are allowed.
	AllowedWebhookDomains []string
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

			genID1, _ := generateID()
			execution := &RemediationExecution{
				ID:        genID1,
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
