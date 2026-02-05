package chronicle

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// SLOConfig defines a service-level objective.
type SLOConfig struct {
	Name         string
	Metric       string
	Target       float64 // e.g., 0.999 = 99.9%
	Window       time.Duration
	BadThreshold float64 // values above this are "bad"
	Operator     SLOOperator
}

// SLOOperator defines how to compare metric values.
type SLOOperator int

const (
	SLOOperatorLessThan SLOOperator = iota
	SLOOperatorGreaterThan
	SLOOperatorLessThanOrEqual
	SLOOperatorGreaterThanOrEqual
)

func (op SLOOperator) String() string {
	switch op {
	case SLOOperatorLessThan:
		return "<"
	case SLOOperatorGreaterThan:
		return ">"
	case SLOOperatorLessThanOrEqual:
		return "<="
	case SLOOperatorGreaterThanOrEqual:
		return ">="
	default:
		return "?"
	}
}

func (op SLOOperator) evaluate(value, threshold float64) bool {
	switch op {
	case SLOOperatorLessThan:
		return value < threshold
	case SLOOperatorGreaterThan:
		return value > threshold
	case SLOOperatorLessThanOrEqual:
		return value <= threshold
	case SLOOperatorGreaterThanOrEqual:
		return value >= threshold
	default:
		return false
	}
}

// SLOStatus represents the current state of an SLO.
type SLOStatus struct {
	Name           string        `json:"name"`
	Target         float64       `json:"target"`
	Current        float64       `json:"current"`
	ErrorBudget    float64       `json:"error_budget"`
	BudgetConsumed float64       `json:"budget_consumed"`
	IsViolated     bool          `json:"is_violated"`
	Window         time.Duration `json:"window"`
	TotalEvents    int64         `json:"total_events"`
	BadEvents      int64         `json:"bad_events"`
	LastUpdated    time.Time     `json:"last_updated"`
}

// SLOTracker monitors service-level objectives across signals.
type SLOTracker struct {
	mu   sync.RWMutex
	slos map[string]*sloState
}

type sloState struct {
	config SLOConfig
	events []sloEvent
}

type sloEvent struct {
	timestamp time.Time
	isBad     bool
	value     float64
}

// NewSLOTracker creates a new SLO tracker.
func NewSLOTracker() *SLOTracker {
	return &SLOTracker{
		slos: make(map[string]*sloState),
	}
}

// Register adds an SLO definition.
func (t *SLOTracker) Register(config SLOConfig) error {
	if config.Name == "" {
		return fmt.Errorf("slo: name is required")
	}
	if config.Target <= 0 || config.Target > 1 {
		return fmt.Errorf("slo: target must be between 0 and 1")
	}
	if config.Window <= 0 {
		config.Window = 24 * time.Hour
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.slos[config.Name] = &sloState{
		config: config,
		events: make([]sloEvent, 0, 1024),
	}
	return nil
}

// RecordEvent records a metric observation for SLO tracking.
func (t *SLOTracker) RecordEvent(sloName string, value float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.slos[sloName]
	if !ok {
		return
	}

	isBad := state.config.Operator.evaluate(value, state.config.BadThreshold)
	state.events = append(state.events, sloEvent{
		timestamp: time.Now(),
		isBad:     isBad,
		value:     value,
	})

	// Prune old events
	cutoff := time.Now().Add(-state.config.Window)
	pruneIdx := 0
	for pruneIdx < len(state.events) && state.events[pruneIdx].timestamp.Before(cutoff) {
		pruneIdx++
	}
	if pruneIdx > 0 {
		state.events = state.events[pruneIdx:]
	}
}

// Status returns the current status of an SLO.
func (t *SLOTracker) Status(name string) (SLOStatus, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	state, ok := t.slos[name]
	if !ok {
		return SLOStatus{}, fmt.Errorf("slo: %q not found", name)
	}

	return t.computeStatus(state), nil
}

// AllStatuses returns the status of all registered SLOs.
func (t *SLOTracker) AllStatuses() []SLOStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	statuses := make([]SLOStatus, 0, len(t.slos))
	for _, state := range t.slos {
		statuses = append(statuses, t.computeStatus(state))
	}
	return statuses
}

func (t *SLOTracker) computeStatus(state *sloState) SLOStatus {
	total := int64(len(state.events))
	var bad int64
	for _, e := range state.events {
		if e.isBad {
			bad++
		}
	}

	var current float64
	if total > 0 {
		current = 1.0 - float64(bad)/float64(total)
	} else {
		current = 1.0
	}

	target := state.config.Target
	errorBudget := 1.0 - target
	var budgetConsumed float64
	if errorBudget > 0 {
		consumed := float64(bad) / float64(math.Max(float64(total), 1))
		budgetConsumed = consumed / errorBudget
	}

	return SLOStatus{
		Name:           state.config.Name,
		Target:         target,
		Current:        current,
		ErrorBudget:    errorBudget,
		BudgetConsumed: budgetConsumed,
		IsViolated:     current < target,
		Window:         state.config.Window,
		TotalEvents:    total,
		BadEvents:      bad,
		LastUpdated:    time.Now(),
	}
}

// CrossSignalAlert represents an alert that fires based on correlated signals.
type CrossSignalAlert struct {
	Name       string            `json:"name"`
	Signals    []SignalType      `json:"signals"`
	Condition  string            `json:"condition"`
	Severity   AlertSeverity     `json:"severity"`
	Labels     map[string]string `json:"labels,omitempty"`
	CooldownMs int64             `json:"cooldown_ms"`
}

// CrossSignalAlertEngine evaluates alerts that span metrics, logs, and traces.
type CrossSignalAlertEngine struct {
	mu       sync.RWMutex
	alerts   []CrossSignalAlert
	firings  []AlertFiring
	onFiring func(AlertFiring)
}

// AlertFiring records a single alert firing event.
type AlertFiring struct {
	Alert     string        `json:"alert"`
	Severity  AlertSeverity `json:"severity"`
	Timestamp time.Time     `json:"timestamp"`
	Message   string        `json:"message"`
}

// NewCrossSignalAlertEngine creates a new cross-signal alert engine.
func NewCrossSignalAlertEngine() *CrossSignalAlertEngine {
	return &CrossSignalAlertEngine{
		alerts:  make([]CrossSignalAlert, 0),
		firings: make([]AlertFiring, 0, 256),
	}
}

// AddAlert registers a cross-signal alert rule.
func (e *CrossSignalAlertEngine) AddAlert(alert CrossSignalAlert) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.alerts = append(e.alerts, alert)
}

// OnFiring registers a callback for alert firings.
func (e *CrossSignalAlertEngine) OnFiring(fn func(AlertFiring)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onFiring = fn
}

// Fire triggers an alert manually (used by evaluation loops).
func (e *CrossSignalAlertEngine) Fire(alertName, message string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var severity AlertSeverity
	for _, a := range e.alerts {
		if a.Name == alertName {
			severity = a.Severity
			break
		}
	}

	firing := AlertFiring{
		Alert:     alertName,
		Severity:  severity,
		Timestamp: time.Now(),
		Message:   message,
	}

	e.firings = append(e.firings, firing)
	if len(e.firings) > 10000 {
		e.firings = e.firings[5000:]
	}

	if e.onFiring != nil {
		e.onFiring(firing)
	}
}

// Firings returns recent alert firings.
func (e *CrossSignalAlertEngine) Firings() []AlertFiring {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]AlertFiring, len(e.firings))
	copy(out, e.firings)
	return out
}

// AlertCount returns the number of registered alerts.
func (e *CrossSignalAlertEngine) AlertCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.alerts)
}
