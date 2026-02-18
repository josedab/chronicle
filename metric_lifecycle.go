package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// MetricLifecycleConfig configures the metric lifecycle manager.
type MetricLifecycleConfig struct {
	Enabled            bool          `json:"enabled"`
	EvaluationInterval time.Duration `json:"evaluation_interval"`
	MaxPolicies        int           `json:"max_policies"`
	StaleTimeout       time.Duration `json:"stale_timeout"`       // Time after which active→stale
	ArchiveTimeout     time.Duration `json:"archive_timeout"`     // Time after which stale→archived
	DeleteTimeout      time.Duration `json:"delete_timeout"`      // Time after which archived→deleted
	QueryRetentionBonus time.Duration `json:"query_retention_bonus"` // Extra retention for frequently queried
}

// DefaultMetricLifecycleConfig returns sensible defaults.
func DefaultMetricLifecycleConfig() MetricLifecycleConfig {
	return MetricLifecycleConfig{
		Enabled:             true,
		EvaluationInterval:  5 * time.Minute,
		MaxPolicies:         100,
		StaleTimeout:        30 * time.Minute,
		ArchiveTimeout:      24 * time.Hour,
		DeleteTimeout:       7 * 24 * time.Hour,
		QueryRetentionBonus: 24 * time.Hour,
	}
}

// LifecyclePolicy defines a lifecycle policy for metrics matching a pattern.
type LifecyclePolicy struct {
	MetricPattern string        `json:"metric_pattern"`
	MaxAge        time.Duration `json:"max_age"`
	ArchiveAfter  time.Duration `json:"archive_after"`
	AutoDelete    bool          `json:"auto_delete"`
}

// MetricState tracks the current state of a metric.
type MetricState struct {
	Metric      string             `json:"metric"`
	State       string             `json:"state"` // "discovered", "active", "stale", "archived", "deleted"
	PointCount  int64              `json:"point_count"`
	QueryCount  int64              `json:"query_count"`
	FirstSeen   time.Time          `json:"first_seen"`
	LastSeen    time.Time          `json:"last_seen"`
	LastQueried time.Time          `json:"last_queried,omitempty"`
	StateChangedAt time.Time       `json:"state_changed_at"`
	Deprecated  bool               `json:"deprecated"`
	DeprecationMsg string          `json:"deprecation_msg,omitempty"`
	Replacement string             `json:"replacement,omitempty"` // Suggested replacement metric
	Policies    []LifecyclePolicy  `json:"policies"`
}

// MetricLifecycleStats tracks aggregate lifecycle statistics.
type MetricLifecycleStats struct {
	TotalMetrics      int   `json:"total_metrics"`
	DiscoveredMetrics int   `json:"discovered_metrics"`
	ActiveMetrics     int   `json:"active_metrics"`
	StaleMetrics      int   `json:"stale_metrics"`
	ArchivedMetrics   int   `json:"archived_metrics"`
	DeletedMetrics    int   `json:"deleted_metrics"`
	DeprecatedMetrics int   `json:"deprecated_metrics"`
	PolicyCount       int   `json:"policy_count"`
}

// MetricLifecycleManager manages metric lifecycles.
type MetricLifecycleManager struct {
	db     *DB
	config MetricLifecycleConfig

	policies []LifecyclePolicy
	states   map[string]*MetricState
	running  bool
	stopCh   chan struct{}

	mu sync.RWMutex
}

// NewMetricLifecycleManager creates a new metric lifecycle manager.
func NewMetricLifecycleManager(db *DB, cfg MetricLifecycleConfig) *MetricLifecycleManager {
	return &MetricLifecycleManager{
		db:       db,
		config:   cfg,
		policies: make([]LifecyclePolicy, 0),
		states:   make(map[string]*MetricState),
		stopCh:   make(chan struct{}),
	}
}

// Start starts the metric lifecycle manager.
func (m *MetricLifecycleManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return
	}
	m.running = true
	go m.evaluationLoop()
}

// Stop stops the metric lifecycle manager.
func (m *MetricLifecycleManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running {
		return
	}
	m.running = false
	close(m.stopCh)
}

func (m *MetricLifecycleManager) evaluationLoop() {
	ticker := time.NewTicker(m.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.evaluateTransitions()
		}
	}
}

// evaluateTransitions checks all metrics and transitions them through
// the lifecycle: discovered → active → stale → archived → deleted
func (m *MetricLifecycleManager) evaluateTransitions() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for _, state := range m.states {
		switch state.State {
		case "discovered":
			// Move to active once data is received
			if state.PointCount > 0 {
				state.State = "active"
				state.StateChangedAt = now
			}

		case "active":
			// Move to stale if no recent data
			if m.config.StaleTimeout > 0 && now.Sub(state.LastSeen) > m.config.StaleTimeout {
				// Frequently queried metrics get a bonus
				if state.QueryCount > 0 && now.Sub(state.LastQueried) < m.config.QueryRetentionBonus {
					continue
				}
				state.State = "stale"
				state.StateChangedAt = now
			}

		case "stale":
			// Move to archived after archive timeout
			if m.config.ArchiveTimeout > 0 && now.Sub(state.StateChangedAt) > m.config.ArchiveTimeout {
				state.State = "archived"
				state.StateChangedAt = now
			}
			// If new data arrives, move back to active
			if now.Sub(state.LastSeen) < m.config.StaleTimeout {
				state.State = "active"
				state.StateChangedAt = now
			}

		case "archived":
			// Move to deleted after delete timeout, if auto-delete enabled
			if m.config.DeleteTimeout > 0 && now.Sub(state.StateChangedAt) > m.config.DeleteTimeout {
				for _, p := range state.Policies {
					if p.AutoDelete {
						state.State = "deleted"
						state.StateChangedAt = now
						break
					}
				}
			}
		}
	}
}

// AddPolicy adds a lifecycle policy.
func (m *MetricLifecycleManager) AddPolicy(policy LifecyclePolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.policies) >= m.config.MaxPolicies {
		return fmt.Errorf("max policies (%d) reached", m.config.MaxPolicies)
	}
	m.policies = append(m.policies, policy)
	return nil
}

// TrackMetric records a metric observation, updating its state.
func (m *MetricLifecycleManager) TrackMetric(metric string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	state, exists := m.states[metric]
	if !exists {
		state = &MetricState{
			Metric:         metric,
			State:          "discovered",
			FirstSeen:      now,
			StateChangedAt: now,
		}
		m.states[metric] = state
	}
	state.PointCount++
	state.LastSeen = now

	// Auto-transition from discovered to active
	if state.State == "discovered" {
		state.State = "active"
		state.StateChangedAt = now
	}

	// Reactivate stale metrics on new data
	if state.State == "stale" {
		state.State = "active"
		state.StateChangedAt = now
	}

	// Attach matching policies
	state.Policies = m.matchingPolicies(metric)
}

// TrackQuery records a query for a metric, extending its retention.
func (m *MetricLifecycleManager) TrackQuery(metric string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[metric]
	if !exists {
		return
	}
	state.QueryCount++
	state.LastQueried = time.Now()
}

// DeprecateMetric marks a metric as deprecated with an optional replacement.
func (m *MetricLifecycleManager) DeprecateMetric(metric, message, replacement string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[metric]
	if !exists {
		return fmt.Errorf("metric %q not found", metric)
	}
	state.Deprecated = true
	state.DeprecationMsg = message
	state.Replacement = replacement
	return nil
}

// CheckDeprecated returns deprecation info if a metric is deprecated.
func (m *MetricLifecycleManager) CheckDeprecated(metric string) (bool, string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.states[metric]
	if !exists || !state.Deprecated {
		return false, "", ""
	}
	return true, state.DeprecationMsg, state.Replacement
}

func (m *MetricLifecycleManager) matchingPolicies(metric string) []LifecyclePolicy {
	var matched []LifecyclePolicy
	for _, p := range m.policies {
		if p.MetricPattern == metric || p.MetricPattern == "*" {
			matched = append(matched, p)
		}
	}
	return matched
}

// GetState returns the state of a metric.
func (m *MetricLifecycleManager) GetState(metric string) (MetricState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.states[metric]
	if !exists {
		return MetricState{}, false
	}
	return *state, true
}

// ListMetrics returns all tracked metric states.
func (m *MetricLifecycleManager) ListMetrics() []MetricState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]MetricState, 0, len(m.states))
	for _, s := range m.states {
		out = append(out, *s)
	}
	return out
}

// Archive moves a metric to the archived state.
func (m *MetricLifecycleManager) Archive(metric string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[metric]
	if !exists {
		return fmt.Errorf("metric %q not found", metric)
	}
	if state.State == "deleted" {
		return fmt.Errorf("metric %q is deleted", metric)
	}
	state.State = "archived"
	state.StateChangedAt = time.Now()
	return nil
}

// Tombstone moves a metric to the deleted state.
func (m *MetricLifecycleManager) Tombstone(metric string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[metric]
	if !exists {
		return fmt.Errorf("metric %q not found", metric)
	}
	state.State = "deleted"
	state.StateChangedAt = time.Now()
	return nil
}

// Restore moves a metric back to the active state.
func (m *MetricLifecycleManager) Restore(metric string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[metric]
	if !exists {
		return fmt.Errorf("metric %q not found", metric)
	}
	if state.State == "active" {
		return fmt.Errorf("metric %q is already active", metric)
	}
	state.State = "active"
	state.StateChangedAt = time.Now()
	return nil
}

// Stats returns aggregate lifecycle statistics.
func (m *MetricLifecycleManager) Stats() MetricLifecycleStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := MetricLifecycleStats{
		TotalMetrics: len(m.states),
		PolicyCount:  len(m.policies),
	}
	for _, s := range m.states {
		switch s.State {
		case "discovered":
			stats.DiscoveredMetrics++
		case "active":
			stats.ActiveMetrics++
		case "stale":
			stats.StaleMetrics++
		case "archived":
			stats.ArchivedMetrics++
		case "deleted":
			stats.DeletedMetrics++
		}
		if s.Deprecated {
			stats.DeprecatedMetrics++
		}
	}
	return stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (m *MetricLifecycleManager) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/lifecycle/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.ListMetrics())
	})
	mux.HandleFunc("/api/v1/lifecycle/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.Stats())
	})
	mux.HandleFunc("/api/v1/lifecycle/archive", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string `json:"metric"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := m.Archive(req.Metric); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "archived"})
	})
	mux.HandleFunc("/api/v1/lifecycle/restore", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string `json:"metric"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := m.Restore(req.Metric); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "restored"})
	})
	mux.HandleFunc("/api/v1/lifecycle/deprecate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric      string `json:"metric"`
			Message     string `json:"message"`
			Replacement string `json:"replacement"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := m.DeprecateMetric(req.Metric, req.Message, req.Replacement); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deprecated"})
	})
}
