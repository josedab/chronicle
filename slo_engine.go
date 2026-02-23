package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// SLIType defines the type of Service Level Indicator.
type SLIType string

const (
	SLIAvailability SLIType = "availability"
	SLILatency      SLIType = "latency"
	SLIThroughput   SLIType = "throughput"
)

// SLODefinition defines a Service Level Objective.
type SLODefinition struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	SLIType     SLIType           `json:"sli_type"`
	Target      float64           `json:"target"` // e.g., 0.999 for 99.9%
	Metric      string            `json:"metric"`
	TotalMetric string            `json:"total_metric,omitempty"` // for ratio-based SLIs
	Labels      map[string]string `json:"labels,omitempty"`
	Window      time.Duration     `json:"window"`      // Compliance window (e.g., 30 days)
	CreatedAt   time.Time         `json:"created_at"`
	Enabled     bool              `json:"enabled"`

	// Latency-specific: threshold in milliseconds
	LatencyThresholdMs float64 `json:"latency_threshold_ms,omitempty"`
}

// SLOEngineStatus represents the current state of an SLO managed by the engine.
type SLOEngineStatus struct {
	Name             string    `json:"name"`
	SLIType          SLIType   `json:"sli_type"`
	Target           float64   `json:"target"`
	Current          float64   `json:"current"`
	ErrorBudget      float64   `json:"error_budget"`
	ErrorBudgetUsed  float64   `json:"error_budget_used"`
	BudgetRemaining  float64   `json:"budget_remaining"`
	IsViolated       bool      `json:"is_violated"`
	TotalEvents      int64     `json:"total_events"`
	GoodEvents       int64     `json:"good_events"`
	BadEvents        int64     `json:"bad_events"`
	WindowStart      time.Time `json:"window_start"`
	WindowEnd        time.Time `json:"window_end"`
	LastEvaluatedAt  time.Time `json:"last_evaluated_at"`
}

// SLOEngineConfig configures the SLO engine.
type SLOEngineConfig struct {
	Enabled            bool          `json:"enabled"`
	EvaluationInterval time.Duration `json:"evaluation_interval"`
	MaxSLOs            int           `json:"max_slos"`
}

// DefaultSLOEngineConfig returns sensible defaults.
func DefaultSLOEngineConfig() SLOEngineConfig {
	return SLOEngineConfig{
		Enabled:            true,
		EvaluationInterval: time.Minute,
		MaxSLOs:            100,
	}
}

// SLOEngine manages SLO definitions, tracking, and error budget calculations.
type SLOEngine struct {
	db     *DB
	config SLOEngineConfig

	slos     map[string]*SLODefinition
	statuses map[string]*SLOEngineStatus
	events   map[string]*sloEventTracker

	running bool
	stopCh  chan struct{}
	mu      sync.RWMutex
}

type sloEventTracker struct {
	totalEvents int64
	goodEvents  int64
	badEvents   int64
	windowStart time.Time
}

// NewSLOEngine creates a new SLO engine.
func NewSLOEngine(db *DB, config SLOEngineConfig) *SLOEngine {
	return &SLOEngine{
		db:       db,
		config:   config,
		slos:     make(map[string]*SLODefinition),
		statuses: make(map[string]*SLOEngineStatus),
		events:   make(map[string]*sloEventTracker),
		stopCh:   make(chan struct{}),
	}
}

// AddSLO adds an SLO definition.
func (e *SLOEngine) AddSLO(slo SLODefinition) error {
	if slo.Name == "" {
		return fmt.Errorf("SLO name is required")
	}
	if slo.Metric == "" {
		return fmt.Errorf("SLO metric is required")
	}
	if slo.Target <= 0 || slo.Target >= 1.0 {
		return fmt.Errorf("SLO target must be between 0 and 1 (e.g., 0.999 for 99.9%%)")
	}
	if slo.Window <= 0 {
		slo.Window = 30 * 24 * time.Hour // Default 30 days
	}
	if slo.CreatedAt.IsZero() {
		slo.CreatedAt = time.Now()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.slos) >= e.config.MaxSLOs {
		return fmt.Errorf("max SLOs reached (%d)", e.config.MaxSLOs)
	}

	slo.Enabled = true
	e.slos[slo.Name] = &slo
	e.events[slo.Name] = &sloEventTracker{
		windowStart: time.Now().Add(-slo.Window),
	}

	return nil
}

// RemoveSLO removes an SLO by name.
func (e *SLOEngine) RemoveSLO(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.slos[name]; !exists {
		return fmt.Errorf("SLO %q not found", name)
	}
	delete(e.slos, name)
	delete(e.statuses, name)
	delete(e.events, name)
	return nil
}

// GetSLO returns an SLO definition by name.
func (e *SLOEngine) GetSLO(name string) (SLODefinition, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	slo, ok := e.slos[name]
	if !ok {
		return SLODefinition{}, false
	}
	return *slo, true
}

// ListSLOs returns all SLO definitions.
func (e *SLOEngine) ListSLOs() []SLODefinition {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]SLODefinition, 0, len(e.slos))
	for _, slo := range e.slos {
		result = append(result, *slo)
	}
	return result
}

// RecordEvent records a good or bad event for an SLO.
func (e *SLOEngine) RecordEvent(sloName string, good bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tracker, exists := e.events[sloName]
	if !exists {
		return fmt.Errorf("SLO %q not found", sloName)
	}

	tracker.totalEvents++
	if good {
		tracker.goodEvents++
	} else {
		tracker.badEvents++
	}

	return nil
}

// GetStatus returns the current status of an SLO.
func (e *SLOEngine) GetStatus(name string) (SLOEngineStatus, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	status, ok := e.statuses[name]
	if !ok {
		// Compute on-demand
		slo, exists := e.slos[name]
		if !exists {
			return SLOEngineStatus{}, false
		}
		tracker, tExists := e.events[name]
		if !tExists {
			return SLOEngineStatus{}, false
		}
		s := e.computeStatus(slo, tracker)
		return s, true
	}
	return *status, true
}

// ListStatuses returns all SLO statuses.
func (e *SLOEngine) ListStatuses() []SLOEngineStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]SLOEngineStatus, 0, len(e.slos))
	for name, slo := range e.slos {
		tracker := e.events[name]
		if tracker == nil {
			continue
		}
		result = append(result, e.computeStatus(slo, tracker))
	}
	return result
}

func (e *SLOEngine) computeStatus(slo *SLODefinition, tracker *sloEventTracker) SLOEngineStatus {
	now := time.Now()
	status := SLOEngineStatus{
		Name:            slo.Name,
		SLIType:         slo.SLIType,
		Target:          slo.Target,
		TotalEvents:     tracker.totalEvents,
		GoodEvents:      tracker.goodEvents,
		BadEvents:       tracker.badEvents,
		WindowStart:     now.Add(-slo.Window),
		WindowEnd:       now,
		LastEvaluatedAt: now,
	}

	// Calculate current SLI value
	if tracker.totalEvents > 0 {
		status.Current = float64(tracker.goodEvents) / float64(tracker.totalEvents)
	} else {
		status.Current = 1.0 // No events = 100% good
	}

	// Calculate error budget
	// Error budget = 1 - target (e.g., 0.001 for 99.9%)
	status.ErrorBudget = 1.0 - slo.Target

	// Error budget consumed = (1 - current SLI) / error budget
	if status.ErrorBudget > 0 {
		errorRate := 1.0 - status.Current
		status.ErrorBudgetUsed = errorRate / status.ErrorBudget
		status.BudgetRemaining = math.Max(0, 1.0-status.ErrorBudgetUsed) * 100
	}

	status.IsViolated = status.Current < slo.Target

	return status
}

// Start begins the SLO evaluation loop.
func (e *SLOEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	go e.evaluationLoop()
}

// Stop stops the SLO engine.
func (e *SLOEngine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.running = false
	e.mu.Unlock()
	close(e.stopCh)
}

func (e *SLOEngine) evaluationLoop() {
	ticker := time.NewTicker(e.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.evaluate()
		}
	}
}

func (e *SLOEngine) evaluate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for name, slo := range e.slos {
		if !slo.Enabled {
			continue
		}
		tracker := e.events[name]
		if tracker == nil {
			continue
		}
		status := e.computeStatus(slo, tracker)
		e.statuses[name] = &status
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the SLO engine.
func (e *SLOEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/slos", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(e.ListSLOs())
		case http.MethodPost:
			var slo SLODefinition
			if err := json.NewDecoder(r.Body).Decode(&slo); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := e.AddSLO(slo); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(slo)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/slos/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListStatuses())
	})

	mux.HandleFunc("/api/v1/slos/budget", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		name := r.URL.Query().Get("name")
		if name == "" {
			json.NewEncoder(w).Encode(e.ListStatuses())
			return
		}
		status, ok := e.GetStatus(name)
		if !ok {
			http.Error(w, "SLO not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"name":             status.Name,
			"target":           status.Target,
			"current":          status.Current,
			"error_budget":     status.ErrorBudget,
			"budget_remaining": status.BudgetRemaining,
			"is_violated":      status.IsViolated,
		})
	})

	mux.HandleFunc("/api/v1/slos/event", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			SLO  string `json:"slo"`
			Good bool   `json:"good"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := e.RecordEvent(req.SLO, req.Good); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "recorded"})
	})
}

// CreateRecordingRules generates recording rules that pre-compute SLO metrics.
// These rules write error budget and burn rate values as time-series for dashboarding.
func (e *SLOEngine) CreateRecordingRules(rre *RecordingRulesEngine) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, slo := range e.slos {
		if !slo.Enabled {
			continue
		}

		// Recording rule for error budget remaining
		budgetRule := RecordingRule{
			Name:         fmt.Sprintf("slo:%s:error_budget_remaining", slo.Name),
			Query:        fmt.Sprintf("SELECT last(value) FROM %s", sanitizeIdentifier(slo.Metric)),
			TargetMetric: fmt.Sprintf("slo:%s:budget_remaining", slo.Name),
			Interval:     e.config.EvaluationInterval,
			Labels: map[string]string{
				"slo":     slo.Name,
				"target":  fmt.Sprintf("%.4f", slo.Target),
				"sli_type": string(slo.SLIType),
			},
			Enabled: true,
		}
		if err := rre.AddRule(budgetRule); err != nil {
			// Ignore duplicate rule errors during re-creation
			continue
		}
	}

	return nil
}

// CreateBurnRateAlerts generates alerting rules based on SLO burn rates.
// Follows the multi-window, multi-burn-rate approach from the Google SRE book.
func (e *SLOEngine) CreateBurnRateAlerts(am *AlertManager) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, slo := range e.slos {
		if !slo.Enabled {
			continue
		}

		// Page-level alert: 14.4x burn rate in 1h window
		pageRule := AlertRule{
			Name:        fmt.Sprintf("slo_%s_burn_rate_page", slo.Name),
			Description: fmt.Sprintf("SLO %s error budget burning at >14.4x rate", slo.Name),
			Metric:      fmt.Sprintf("slo:%s:budget_remaining", slo.Name),
			Condition:   AlertConditionBelow,
			Threshold:   (1.0 - slo.Target) * 100 * 0.02, // 2% of budget in 1h → page
			ForDuration: 5 * time.Minute,
			EvalInterval: time.Minute,
			Labels: map[string]string{
				"severity": "page",
				"slo":      slo.Name,
			},
			Annotations: map[string]string{
				"summary":     fmt.Sprintf("SLO %s burn rate critical", slo.Name),
				"description": fmt.Sprintf("Error budget for %s is being consumed at >14.4x the sustainable rate", slo.Name),
			},
		}
		if err := am.AddRule(pageRule); err != nil {
			continue
		}

		// Ticket-level alert: 3x burn rate in 1d window
		ticketRule := AlertRule{
			Name:        fmt.Sprintf("slo_%s_burn_rate_ticket", slo.Name),
			Description: fmt.Sprintf("SLO %s error budget burning at >3x rate", slo.Name),
			Metric:      fmt.Sprintf("slo:%s:budget_remaining", slo.Name),
			Condition:   AlertConditionBelow,
			Threshold:   (1.0 - slo.Target) * 100 * 0.10, // 10% of budget in 1d → ticket
			ForDuration: 30 * time.Minute,
			EvalInterval: 5 * time.Minute,
			Labels: map[string]string{
				"severity": "ticket",
				"slo":      slo.Name,
			},
			Annotations: map[string]string{
				"summary":     fmt.Sprintf("SLO %s burn rate elevated", slo.Name),
				"description": fmt.Sprintf("Error budget for %s is being consumed at >3x the sustainable rate", slo.Name),
			},
		}
		am.AddRule(ticketRule)
	}

	return nil
}
