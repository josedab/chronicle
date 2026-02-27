package chronicle

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
)

// CrossAlertConfig configures the cross-metric alert rules engine.
type CrossAlertConfig struct {
	Enabled      bool          `json:"enabled"`
	MaxRules     int           `json:"max_rules"`
	EvalInterval time.Duration `json:"eval_interval"`
}

// DefaultCrossAlertConfig returns sensible defaults for CrossAlertConfig.
func DefaultCrossAlertConfig() CrossAlertConfig {
	return CrossAlertConfig{
		Enabled:      true,
		MaxRules:     100,
		EvalInterval: 30 * time.Second,
	}
}

// CrossAlertCondition defines a single condition within a cross-alert rule.
type CrossAlertCondition struct {
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"` // gt, lt, eq, gte, lte
	Threshold float64 `json:"threshold"`
}

// CrossAlertRule defines a rule that correlates conditions across multiple metrics.
type CrossAlertRule struct {
	Name       string                `json:"name"`
	Expression string                `json:"expression"`
	Conditions []CrossAlertCondition `json:"conditions"`
	Operator   string                `json:"operator"` // and, or
	State      string                `json:"state"`     // inactive, pending, firing
}

// CrossAlertFired represents an alert that has fired.
type CrossAlertFired struct {
	RuleName          string    `json:"rule_name"`
	FiredAt           time.Time `json:"fired_at"`
	ConditionsMatched int       `json:"conditions_matched"`
}

// CrossAlertStats holds aggregate statistics for the alert engine.
type CrossAlertStats struct {
	TotalRules       int   `json:"total_rules"`
	TotalEvaluations int64 `json:"total_evaluations"`
	TotalFired       int64 `json:"total_fired"`
}

// CrossAlertEngine evaluates cross-metric alert rules.
type CrossAlertEngine struct {
	db      *DB
	config  CrossAlertConfig
	mu      sync.RWMutex
	rules   map[string]*CrossAlertRule
	fired   []CrossAlertFired
	stats   CrossAlertStats
	stopCh  chan struct{}
	running bool
}

// NewCrossAlertEngine creates a new CrossAlertEngine.
func NewCrossAlertEngine(db *DB, cfg CrossAlertConfig) *CrossAlertEngine {
	return &CrossAlertEngine{
		db:     db,
		config: cfg,
		rules:  make(map[string]*CrossAlertRule),
		stopCh: make(chan struct{}),
	}
}

// Start begins the cross-alert engine.
func (e *CrossAlertEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop halts the cross-alert engine.
func (e *CrossAlertEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// AddRule registers a new cross-alert rule.
func (e *CrossAlertEngine) AddRule(rule CrossAlertRule) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.rules) >= e.config.MaxRules {
		return errMaxRulesReached
	}
	if rule.State == "" {
		rule.State = "inactive"
	}
	if rule.Operator == "" {
		rule.Operator = "and"
	}
	cp := rule
	e.rules[rule.Name] = &cp
	e.stats.TotalRules = len(e.rules)
	return nil
}

// RemoveRule deletes a rule by name.
func (e *CrossAlertEngine) RemoveRule(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.rules[name]; !ok {
		return false
	}
	delete(e.rules, name)
	e.stats.TotalRules = len(e.rules)
	return true
}

// Evaluate checks all rules against the provided metrics and returns fired alerts.
func (e *CrossAlertEngine) Evaluate(metrics map[string]float64) []CrossAlertFired {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stats.TotalEvaluations++
	var result []CrossAlertFired
	for _, rule := range e.rules {
		matched := 0
		for _, cond := range rule.Conditions {
			val, ok := metrics[cond.Metric]
			if !ok {
				continue
			}
			if evalCrossAlertCondition(val, cond.Operator, cond.Threshold) {
				matched++
			}
		}
		fired := false
		if rule.Operator == "and" && matched == len(rule.Conditions) && len(rule.Conditions) > 0 {
			fired = true
		} else if rule.Operator == "or" && matched > 0 {
			fired = true
		}
		if fired {
			rule.State = "firing"
			alert := CrossAlertFired{
				RuleName:          rule.Name,
				FiredAt:           time.Now(),
				ConditionsMatched: matched,
			}
			result = append(result, alert)
			e.fired = append(e.fired, alert)
			e.stats.TotalFired++
		} else {
			rule.State = "inactive"
		}
	}
	return result
}

// ListRules returns all registered rules.
func (e *CrossAlertEngine) ListRules() []CrossAlertRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	rules := make([]CrossAlertRule, 0, len(e.rules))
	for _, r := range e.rules {
		rules = append(rules, *r)
	}
	return rules
}

// GetStats returns engine statistics.
func (e *CrossAlertEngine) GetStats() CrossAlertStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func evalCrossAlertCondition(val float64, op string, threshold float64) bool {
	switch op {
	case "gt":
		return val > threshold
	case "lt":
		return val < threshold
	case "eq":
		return val == threshold
	case "gte":
		return val >= threshold
	case "lte":
		return val <= threshold
	default:
		return false
	}
}

var errMaxRulesReached = errors.New("cross alert: max rules reached")

// RegisterHTTPHandlers registers cross-alert HTTP endpoints.
func (e *CrossAlertEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/crossalert/rules", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListRules())
	})
	mux.HandleFunc("/api/v1/crossalert/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/crossalert/evaluate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var metrics map[string]float64
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&metrics); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Evaluate(metrics))
	})
}
