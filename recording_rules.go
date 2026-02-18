package chronicle

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// RecordingRule defines a rule for pre-computing queries and storing results.
// Similar to Prometheus recording rules, this enables efficient querying of
// expensive aggregations.
type RecordingRule struct {
	// Name is the unique rule identifier.
	Name string

	// Query is the SQL-like query to execute.
	Query string

	// TargetMetric is where results are stored.
	TargetMetric string

	// Interval is how often to evaluate the rule.
	Interval time.Duration

	// Labels are additional labels added to output.
	Labels map[string]string

	// Enabled controls whether the rule is active.
	Enabled bool
}

// RecordingRulesEngine manages recording rules evaluation.
type RecordingRulesEngine struct {
	db      *DB
	rules   map[string]*ruleState
	mu      sync.RWMutex
	parser  *QueryParser
	stopCh  chan struct{}
	wg      sync.WaitGroup
	started bool
}

type ruleState struct {
	rule         RecordingRule
	lastEval     time.Time
	lastDuration time.Duration
	lastError    error
	evalCount    int64
	errorCount   int64
}

// RecordingRulesConfig configures the recording rules engine.
type RecordingRulesConfig struct {
	// Enabled enables the recording rules engine.
	Enabled bool

	// Rules are the recording rules to evaluate.
	Rules []RecordingRule

	// DefaultInterval is the default evaluation interval.
	DefaultInterval time.Duration

	// ConcurrencyLimit limits concurrent rule evaluations.
	ConcurrencyLimit int
}

// DefaultRecordingRulesConfig returns default configuration.
func DefaultRecordingRulesConfig() RecordingRulesConfig {
	return RecordingRulesConfig{
		Enabled:          true,
		DefaultInterval:  time.Minute,
		ConcurrencyLimit: 4,
	}
}

// NewRecordingRulesEngine creates a recording rules engine.
func NewRecordingRulesEngine(db *DB) *RecordingRulesEngine {
	return &RecordingRulesEngine{
		db:     db,
		rules:  make(map[string]*ruleState),
		parser: &QueryParser{},
		stopCh: make(chan struct{}),
	}
}

// AddRule adds a recording rule.
func (rre *RecordingRulesEngine) AddRule(rule RecordingRule) error {
	if rule.Name == "" {
		return fmt.Errorf("rule name is required")
	}
	if rule.Query == "" {
		return fmt.Errorf("rule query is required")
	}
	if rule.TargetMetric == "" {
		return fmt.Errorf("target metric is required")
	}
	if rule.Interval <= 0 {
		rule.Interval = time.Minute
	}

	// Validate query
	_, err := rre.parser.Parse(rule.Query)
	if err != nil {
		return fmt.Errorf("invalid query: %w", err)
	}

	rre.mu.Lock()
	defer rre.mu.Unlock()

	rre.rules[rule.Name] = &ruleState{
		rule: rule,
	}

	return nil
}

// RemoveRule removes a recording rule.
func (rre *RecordingRulesEngine) RemoveRule(name string) {
	rre.mu.Lock()
	defer rre.mu.Unlock()
	delete(rre.rules, name)
}

// GetRule returns a rule by name.
func (rre *RecordingRulesEngine) GetRule(name string) (RecordingRule, bool) {
	rre.mu.RLock()
	defer rre.mu.RUnlock()

	state, ok := rre.rules[name]
	if !ok {
		return RecordingRule{}, false
	}
	return state.rule, true
}

// ListRules returns all rules.
func (rre *RecordingRulesEngine) ListRules() []RecordingRule {
	rre.mu.RLock()
	defer rre.mu.RUnlock()

	rules := make([]RecordingRule, 0, len(rre.rules))
	for _, state := range rre.rules {
		rules = append(rules, state.rule)
	}
	return rules
}

// Start begins the recording rules evaluation loop.
func (rre *RecordingRulesEngine) Start(ctx context.Context) {
	rre.mu.Lock()
	if rre.started {
		rre.mu.Unlock()
		return
	}
	rre.started = true
	rre.mu.Unlock()

	rre.wg.Add(1)
	go rre.evaluationLoop(ctx)
}

// Stop stops the recording rules engine.
func (rre *RecordingRulesEngine) Stop() {
	close(rre.stopCh)
	rre.wg.Wait()
}

func (rre *RecordingRulesEngine) evaluationLoop(ctx context.Context) {
	defer rre.wg.Done()

	ticker := time.NewTicker(time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rre.stopCh:
			return
		case <-ticker.C:
			rre.evaluateDueRules(ctx)
		}
	}
}

func (rre *RecordingRulesEngine) evaluateDueRules(ctx context.Context) {
	rre.mu.RLock()
	defer rre.mu.RUnlock()

	now := time.Now()

	for _, state := range rre.rules {
		if !state.rule.Enabled {
			continue
		}

		// Check if rule is due
		if now.Sub(state.lastEval) < state.rule.Interval {
			continue
		}

		// Evaluate rule
		go rre.evaluateRule(ctx, state)
	}
}

func (rre *RecordingRulesEngine) evaluateRule(ctx context.Context, state *ruleState) {
	start := time.Now()

	defer func() {
		state.lastEval = time.Now()
		state.lastDuration = time.Since(start)
		state.evalCount++
	}()

	// Parse and execute query
	query, err := rre.parser.Parse(state.rule.Query)
	if err != nil {
		state.lastError = err
		state.errorCount++
		return
	}

	result, err := rre.db.Execute(query)
	if err != nil {
		state.lastError = err
		state.errorCount++
		return
	}

	// Write results to target metric
	now := time.Now().UnixNano()
	for _, point := range result.Points {
		tags := cloneTags(point.Tags)
		// Add rule labels
		for k, v := range state.rule.Labels {
			tags[k] = v
		}

		if err := rre.db.Write(Point{
			Metric:    state.rule.TargetMetric,
			Tags:      tags,
			Value:     point.Value,
			Timestamp: now,
		}); err != nil {
			state.lastError = err
			state.errorCount++
			return
		}
	}

	state.lastError = nil
}

// EvaluateNow forces immediate evaluation of a rule.
func (rre *RecordingRulesEngine) EvaluateNow(ctx context.Context, name string) error {
	rre.mu.RLock()
	state, ok := rre.rules[name]
	rre.mu.RUnlock()

	if !ok {
		return fmt.Errorf("rule not found: %s", name)
	}

	rre.evaluateRule(ctx, state)
	return state.lastError
}

// Stats returns statistics for all rules.
func (rre *RecordingRulesEngine) Stats() []RuleStats {
	rre.mu.RLock()
	defer rre.mu.RUnlock()

	stats := make([]RuleStats, 0, len(rre.rules))
	for name, state := range rre.rules {
		var lastError string
		if state.lastError != nil {
			lastError = state.lastError.Error()
		}

		stats = append(stats, RuleStats{
			Name:         name,
			LastEval:     state.lastEval,
			LastDuration: state.lastDuration,
			LastError:    lastError,
			EvalCount:    state.evalCount,
			ErrorCount:   state.errorCount,
		})
	}
	return stats
}

// RuleStats contains statistics for a recording rule.
type RuleStats struct {
	Name         string
	LastEval     time.Time
	LastDuration time.Duration
	LastError    string
	EvalCount    int64
	ErrorCount   int64
}

// Enable enables a rule.
func (rre *RecordingRulesEngine) Enable(name string) error {
	rre.mu.Lock()
	defer rre.mu.Unlock()

	state, ok := rre.rules[name]
	if !ok {
		return fmt.Errorf("rule not found: %s", name)
	}

	state.rule.Enabled = true
	return nil
}

// Disable disables a rule.
func (rre *RecordingRulesEngine) Disable(name string) error {
	rre.mu.Lock()
	defer rre.mu.Unlock()

	state, ok := rre.rules[name]
	if !ok {
		return fmt.Errorf("rule not found: %s", name)
	}

	state.rule.Enabled = false
	return nil
}
