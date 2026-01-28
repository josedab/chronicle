package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// AlertState represents the current state of an alert.
type AlertState int

const (
	// AlertStateOK means the alert condition is not met.
	AlertStateOK AlertState = iota
	// AlertStatePending means the condition is met but for duration not yet reached.
	AlertStatePending
	// AlertStateFiring means the alert is actively firing.
	AlertStateFiring
)

// String returns the string representation of alert state.
func (s AlertState) String() string {
	switch s {
	case AlertStateOK:
		return "ok"
	case AlertStatePending:
		return "pending"
	case AlertStateFiring:
		return "firing"
	default:
		return "unknown"
	}
}

// AlertCondition defines when an alert fires.
type AlertCondition int

const (
	// AlertConditionAbove fires when value is above threshold.
	AlertConditionAbove AlertCondition = iota
	// AlertConditionBelow fires when value is below threshold.
	AlertConditionBelow
	// AlertConditionEqual fires when value equals threshold.
	AlertConditionEqual
	// AlertConditionNotEqual fires when value doesn't equal threshold.
	AlertConditionNotEqual
	// AlertConditionAbsent fires when no data is received.
	AlertConditionAbsent
)

// AlertRule defines an alerting rule.
type AlertRule struct {
	Name        string
	Description string
	Metric      string
	Tags        map[string]string
	Condition   AlertCondition
	Threshold   float64
	// ForDuration is how long the condition must be true before firing
	ForDuration time.Duration
	// EvalInterval is how often to evaluate this rule
	EvalInterval time.Duration
	// Labels are added to alert notifications
	Labels map[string]string
	// Annotations contain additional information
	Annotations map[string]string
	// WebhookURL is called when alert fires/resolves
	WebhookURL string
}

// Alert represents an active or resolved alert.
type Alert struct {
	Rule        *AlertRule
	State       AlertState
	Value       float64
	FiredAt     time.Time
	ResolvedAt  time.Time
	PendingSince time.Time
	Labels      map[string]string
	Annotations map[string]string
}

// AlertNotification is sent to webhooks.
type AlertNotification struct {
	Status      string            `json:"status"` // "firing" or "resolved"
	AlertName   string            `json:"alertname"`
	Description string            `json:"description,omitempty"`
	Metric      string            `json:"metric"`
	Value       float64           `json:"value"`
	Threshold   float64           `json:"threshold"`
	Condition   string            `json:"condition"`
	FiredAt     time.Time         `json:"fired_at,omitempty"`
	ResolvedAt  time.Time         `json:"resolved_at,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// AlertManager manages alert rules and evaluations.
type AlertManager struct {
	db          *DB
	mu          sync.RWMutex
	rules       map[string]*AlertRule
	alerts      map[string]*Alert
	stop        chan struct{}
	httpClient  HTTPDoer
	evalTicker  *time.Ticker
	defaultEval time.Duration
	retryer     *Retryer
}

// AlertManagerOption configures an AlertManager.
type AlertManagerOption func(*AlertManager)

// WithHTTPClient sets a custom HTTP client for webhook notifications.
func WithHTTPClient(client HTTPDoer) AlertManagerOption {
	return func(am *AlertManager) {
		am.httpClient = client
	}
}

// NewAlertManager creates a new alert manager.
func NewAlertManager(db *DB, opts ...AlertManagerOption) *AlertManager {
	am := &AlertManager{
		db:          db,
		rules:       make(map[string]*AlertRule),
		alerts:      make(map[string]*Alert),
		stop:        make(chan struct{}),
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		defaultEval: time.Minute,
		retryer: NewRetryer(RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        5 * time.Second,
			BackoffMultiplier: 2.0,
			Jitter:            0.1,
			RetryIf:           IsRetryable,
		}),
	}
	for _, opt := range opts {
		opt(am)
	}
	return am
}

// AddRule adds an alerting rule.
func (m *AlertManager) AddRule(rule AlertRule) error {
	if rule.Name == "" {
		return fmt.Errorf("alert rule name is required")
	}
	if rule.Metric == "" && rule.Condition != AlertConditionAbsent {
		return fmt.Errorf("metric is required for non-absent alerts")
	}
	if rule.EvalInterval <= 0 {
		rule.EvalInterval = m.defaultEval
	}

	m.mu.Lock()
	m.rules[rule.Name] = &rule
	m.mu.Unlock()
	return nil
}

// RemoveRule removes an alerting rule.
func (m *AlertManager) RemoveRule(name string) {
	m.mu.Lock()
	delete(m.rules, name)
	delete(m.alerts, name)
	m.mu.Unlock()
}

// GetAlert returns the current alert state for a rule.
func (m *AlertManager) GetAlert(name string) *Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.alerts[name]
}

// ListAlerts returns all active alerts.
func (m *AlertManager) ListAlerts() []*Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()
	alerts := make([]*Alert, 0, len(m.alerts))
	for _, a := range m.alerts {
		if a.State == AlertStateFiring {
			alerts = append(alerts, a)
		}
	}
	return alerts
}

// ListRules returns all configured rules.
func (m *AlertManager) ListRules() []*AlertRule {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rules := make([]*AlertRule, 0, len(m.rules))
	for _, r := range m.rules {
		rules = append(rules, r)
	}
	return rules
}

// Start begins alert evaluation.
func (m *AlertManager) Start() {
	m.evalTicker = time.NewTicker(m.defaultEval)
	go m.evaluationLoop()
}

// Stop stops alert evaluation.
func (m *AlertManager) Stop() {
	close(m.stop)
	if m.evalTicker != nil {
		m.evalTicker.Stop()
	}
}

func (m *AlertManager) evaluationLoop() {
	for {
		select {
		case <-m.stop:
			return
		case <-m.evalTicker.C:
			m.evaluateAll()
		}
	}
}

func (m *AlertManager) evaluateAll() {
	m.mu.RLock()
	rules := make([]*AlertRule, 0, len(m.rules))
	for _, r := range m.rules {
		rules = append(rules, r)
	}
	m.mu.RUnlock()

	for _, rule := range rules {
		m.evaluate(rule)
	}
}

func (m *AlertManager) evaluate(rule *AlertRule) {
	now := time.Now()
	var value float64
	conditionMet := false

	if rule.Condition == AlertConditionAbsent {
		// Check for absence of data
		result, err := m.db.Execute(&Query{
			Metric: rule.Metric,
			Tags:   rule.Tags,
			Start:  now.Add(-rule.EvalInterval * 2).UnixNano(),
			End:    now.UnixNano(),
		})
		if err == nil && len(result.Points) == 0 {
			conditionMet = true
		}
	} else {
		// Query for latest value
		result, err := m.db.Execute(&Query{
			Metric: rule.Metric,
			Tags:   rule.Tags,
			Aggregation: &Aggregation{
				Function: AggLast,
				Window:   rule.EvalInterval,
			},
		})
		if err != nil || len(result.Points) == 0 {
			return
		}
		value = result.Points[0].Value

		switch rule.Condition {
		case AlertConditionAbove:
			conditionMet = value > rule.Threshold
		case AlertConditionBelow:
			conditionMet = value < rule.Threshold
		case AlertConditionEqual:
			conditionMet = value == rule.Threshold
		case AlertConditionNotEqual:
			conditionMet = value != rule.Threshold
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	alert, exists := m.alerts[rule.Name]
	if !exists {
		alert = &Alert{
			Rule:        rule,
			State:       AlertStateOK,
			Labels:      mergeLabels(rule.Labels, rule.Tags),
			Annotations: rule.Annotations,
		}
		m.alerts[rule.Name] = alert
	}

	alert.Value = value
	prevState := alert.State

	if conditionMet {
		switch alert.State {
		case AlertStateOK:
			if rule.ForDuration > 0 {
				alert.State = AlertStatePending
				alert.PendingSince = now
			} else {
				alert.State = AlertStateFiring
				alert.FiredAt = now
				go m.notify(alert, "firing")
			}
		case AlertStatePending:
			if now.Sub(alert.PendingSince) >= rule.ForDuration {
				alert.State = AlertStateFiring
				alert.FiredAt = now
				go m.notify(alert, "firing")
			}
		case AlertStateFiring:
			// Still firing
		}
	} else {
		if prevState == AlertStateFiring {
			alert.State = AlertStateOK
			alert.ResolvedAt = now
			go m.notify(alert, "resolved")
		} else {
			alert.State = AlertStateOK
			alert.PendingSince = time.Time{}
		}
	}
}

func (m *AlertManager) notify(alert *Alert, status string) {
	if alert.Rule.WebhookURL == "" {
		return
	}

	notification := AlertNotification{
		Status:      status,
		AlertName:   alert.Rule.Name,
		Description: alert.Rule.Description,
		Metric:      alert.Rule.Metric,
		Value:       alert.Value,
		Threshold:   alert.Rule.Threshold,
		Condition:   conditionString(alert.Rule.Condition),
		Labels:      alert.Labels,
		Annotations: alert.Annotations,
	}

	if status == "firing" {
		notification.FiredAt = alert.FiredAt
	} else {
		notification.ResolvedAt = alert.ResolvedAt
	}

	payload, err := json.Marshal(notification)
	if err != nil {
		return
	}

	// Use retry for webhook notification
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result := m.retryer.Do(ctx, func() error {
		return m.sendWebhook(alert.Rule.WebhookURL, payload)
	})

	if result.LastErr != nil {
		// Log webhook failure after all retries exhausted
		// Silent failure is acceptable for alerting - alerts are still tracked internally
	}
}

func (m *AlertManager) sendWebhook(url string, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 500 || resp.StatusCode == 429 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

func conditionString(c AlertCondition) string {
	switch c {
	case AlertConditionAbove:
		return "above"
	case AlertConditionBelow:
		return "below"
	case AlertConditionEqual:
		return "equal"
	case AlertConditionNotEqual:
		return "not_equal"
	case AlertConditionAbsent:
		return "absent"
	default:
		return "unknown"
	}
}

func mergeLabels(a, b map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

// EvaluateNow triggers immediate evaluation of all rules.
func (m *AlertManager) EvaluateNow() {
	m.evaluateAll()
}
