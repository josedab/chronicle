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
}

// DefaultMetricLifecycleConfig returns sensible defaults.
func DefaultMetricLifecycleConfig() MetricLifecycleConfig {
	return MetricLifecycleConfig{
		Enabled:            true,
		EvaluationInterval: 5 * time.Minute,
		MaxPolicies:        100,
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
	Metric     string             `json:"metric"`
	State      string             `json:"state"` // "active", "archived", "tombstoned"
	PointCount int64              `json:"point_count"`
	FirstSeen  time.Time          `json:"first_seen"`
	LastSeen   time.Time          `json:"last_seen"`
	Policies   []LifecyclePolicy  `json:"policies"`
}

// MetricLifecycleStats tracks aggregate lifecycle statistics.
type MetricLifecycleStats struct {
	TotalMetrics    int   `json:"total_metrics"`
	ActiveMetrics   int   `json:"active_metrics"`
	ArchivedMetrics int   `json:"archived_metrics"`
	TombstonedMetrics int `json:"tombstoned_metrics"`
	PolicyCount     int   `json:"policy_count"`
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
			Metric:    metric,
			State:     "active",
			FirstSeen: now,
		}
		m.states[metric] = state
	}
	state.PointCount++
	state.LastSeen = now

	// Attach matching policies
	state.Policies = m.matchingPolicies(metric)
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
	if state.State == "tombstoned" {
		return fmt.Errorf("metric %q is tombstoned", metric)
	}
	state.State = "archived"
	return nil
}

// Tombstone moves a metric to the tombstoned state.
func (m *MetricLifecycleManager) Tombstone(metric string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[metric]
	if !exists {
		return fmt.Errorf("metric %q not found", metric)
	}
	state.State = "tombstoned"
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
		case "active":
			stats.ActiveMetrics++
		case "archived":
			stats.ArchivedMetrics++
		case "tombstoned":
			stats.TombstonedMetrics++
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
}
