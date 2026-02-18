package chronicle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// RelabelAction defines what to do with a label.
type RelabelAction string

const (
	RelabelDrop    RelabelAction = "drop"
	RelabelRename  RelabelAction = "rename"
	RelabelHash    RelabelAction = "hash"
	RelabelReplace RelabelAction = "replace"
	RelabelKeep    RelabelAction = "keep"
)

// RelabelRule defines a pre-write relabeling rule.
type RelabelRule struct {
	SourceLabel  string        `json:"source_label"`
	TargetLabel  string        `json:"target_label,omitempty"`
	Action       RelabelAction `json:"action"`
	Regex        string        `json:"regex,omitempty"`
	Replacement  string        `json:"replacement,omitempty"`
	MetricFilter string        `json:"metric_filter,omitempty"` // Apply only to matching metrics
	compiled     *regexp.Regexp
}

// RelabelEngine applies relabeling rules to points before writing.
type RelabelEngine struct {
	rules   []RelabelRule
	mu      sync.RWMutex
}

// NewRelabelEngine creates a new relabeling engine.
func NewRelabelEngine() *RelabelEngine {
	return &RelabelEngine{
		rules: make([]RelabelRule, 0),
	}
}

// AddRule adds a relabeling rule.
func (e *RelabelEngine) AddRule(rule RelabelRule) error {
	if rule.SourceLabel == "" && rule.Action != RelabelKeep {
		return fmt.Errorf("source_label is required")
	}
	if rule.Action == "" {
		return fmt.Errorf("action is required")
	}
	if rule.Regex != "" {
		compiled, err := regexp.Compile(rule.Regex)
		if err != nil {
			return fmt.Errorf("invalid regex %q: %w", rule.Regex, err)
		}
		rule.compiled = compiled
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, rule)
	return nil
}

// RemoveRule removes a rule by source label and action.
func (e *RelabelEngine) RemoveRule(sourceLabel string, action RelabelAction) {
	e.mu.Lock()
	defer e.mu.Unlock()

	filtered := make([]RelabelRule, 0, len(e.rules))
	for _, r := range e.rules {
		if r.SourceLabel == sourceLabel && r.Action == action {
			continue
		}
		filtered = append(filtered, r)
	}
	e.rules = filtered
}

// ListRules returns all relabeling rules.
func (e *RelabelEngine) ListRules() []RelabelRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]RelabelRule, len(e.rules))
	copy(result, e.rules)
	return result
}

// Apply applies all relabeling rules to a point.
// Returns the modified point and whether it should be dropped entirely.
func (e *RelabelEngine) Apply(p Point) (Point, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.rules) == 0 {
		return p, false
	}

	tags := cloneTags(p.Tags)

	for _, rule := range e.rules {
		// Check metric filter
		if rule.MetricFilter != "" && !matchMetric(rule.MetricFilter, p.Metric) {
			continue
		}

		switch rule.Action {
		case RelabelDrop:
			if rule.SourceLabel == "__name__" {
				return p, true // Drop the entire point
			}
			delete(tags, rule.SourceLabel)

		case RelabelRename:
			if val, exists := tags[rule.SourceLabel]; exists {
				delete(tags, rule.SourceLabel)
				if rule.TargetLabel != "" {
					tags[rule.TargetLabel] = val
				}
			}

		case RelabelHash:
			if val, exists := tags[rule.SourceLabel]; exists {
				hash := sha256.Sum256([]byte(val))
				tags[rule.SourceLabel] = hex.EncodeToString(hash[:8])
			}

		case RelabelReplace:
			if val, exists := tags[rule.SourceLabel]; exists {
				if rule.compiled != nil {
					val = rule.compiled.ReplaceAllString(val, rule.Replacement)
				} else if rule.Replacement != "" {
					val = rule.Replacement
				}
				target := rule.SourceLabel
				if rule.TargetLabel != "" {
					target = rule.TargetLabel
				}
				tags[target] = val
			}

		case RelabelKeep:
			// Keep only specified labels
			keepLabels := strings.Split(rule.SourceLabel, ",")
			keepSet := make(map[string]bool, len(keepLabels))
			for _, l := range keepLabels {
				keepSet[strings.TrimSpace(l)] = true
			}
			for k := range tags {
				if !keepSet[k] {
					delete(tags, k)
				}
			}
		}
	}

	p.Tags = tags
	return p, false
}

func matchMetric(filter, metric string) bool {
	if filter == "*" {
		return true
	}
	if strings.HasSuffix(filter, "*") {
		return strings.HasPrefix(metric, filter[:len(filter)-1])
	}
	return filter == metric
}

// CardinalityCircuitBreaker monitors cardinality growth rate and trips
// when growth exceeds a threshold.
type CardinalityCircuitBreaker struct {
	tracker       *CardinalityTracker
	maxGrowthRate float64 // Max series growth per second
	windowSize    time.Duration
	tripped       bool
	tripTime      time.Time
	resetAfter    time.Duration

	samples []cardinalitySample
	mu      sync.RWMutex
}

type cardinalitySample struct {
	count     int64
	timestamp time.Time
}

// NewCardinalityCircuitBreaker creates a circuit breaker for cardinality growth.
func NewCardinalityCircuitBreaker(tracker *CardinalityTracker, maxGrowthRate float64) *CardinalityCircuitBreaker {
	return &CardinalityCircuitBreaker{
		tracker:       tracker,
		maxGrowthRate: maxGrowthRate,
		windowSize:    time.Minute,
		resetAfter:    5 * time.Minute,
		samples:       make([]cardinalitySample, 0),
	}
}

// Check returns an error if the circuit breaker is tripped.
func (cb *CardinalityCircuitBreaker) Check() error {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.tripped {
		if time.Since(cb.tripTime) > cb.resetAfter {
			return nil // Allow reset attempt
		}
		return fmt.Errorf("cardinality circuit breaker tripped: growth rate exceeded %.0f series/sec at %s",
			cb.maxGrowthRate, cb.tripTime.Format(time.RFC3339))
	}
	return nil
}

// RecordSample records the current series count for growth rate calculation.
func (cb *CardinalityCircuitBreaker) RecordSample() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	current := cb.tracker.totalSeries.Load()

	cb.samples = append(cb.samples, cardinalitySample{
		count:     current,
		timestamp: now,
	})

	// Keep only samples within window
	cutoff := now.Add(-cb.windowSize)
	filtered := make([]cardinalitySample, 0, len(cb.samples))
	for _, s := range cb.samples {
		if s.timestamp.After(cutoff) {
			filtered = append(filtered, s)
		}
	}
	cb.samples = filtered

	// Calculate growth rate
	if len(cb.samples) >= 2 {
		first := cb.samples[0]
		last := cb.samples[len(cb.samples)-1]
		elapsed := last.timestamp.Sub(first.timestamp).Seconds()
		if elapsed > 0 {
			rate := float64(last.count-first.count) / elapsed
			if rate > cb.maxGrowthRate {
				cb.tripped = true
				cb.tripTime = now
			} else if cb.tripped && time.Since(cb.tripTime) > cb.resetAfter {
				cb.tripped = false
			}
		}
	}
}

// IsTripped returns whether the circuit breaker is currently tripped.
func (cb *CardinalityCircuitBreaker) IsTripped() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.tripped
}

// Reset manually resets the circuit breaker.
func (cb *CardinalityCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.tripped = false
	cb.samples = cb.samples[:0]
}

// StaleSeriesManager handles eviction of stale series that haven't received
// new data within the configured timeout.
type StaleSeriesManager struct {
	config    StaleSeriesConfig
	lastSeen  map[string]time.Time // seriesKey -> last write time
	mu        sync.RWMutex
	stopCh    chan struct{}
}

// StaleSeriesConfig configures stale series detection and eviction.
type StaleSeriesConfig struct {
	Enabled        bool          `json:"enabled"`
	StaleTimeout   time.Duration `json:"stale_timeout"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
	MaxStaleSeries int           `json:"max_stale_series"`
	EvictionPolicy string        `json:"eviction_policy"` // "lru" or "lfu"
}

// DefaultStaleSeriesConfig returns sensible defaults.
func DefaultStaleSeriesConfig() StaleSeriesConfig {
	return StaleSeriesConfig{
		Enabled:         true,
		StaleTimeout:    15 * time.Minute,
		CleanupInterval: time.Minute,
		MaxStaleSeries:  100000,
		EvictionPolicy:  "lru",
	}
}

// NewStaleSeriesManager creates a stale series manager.
func NewStaleSeriesManager(config StaleSeriesConfig) *StaleSeriesManager {
	return &StaleSeriesManager{
		config:   config,
		lastSeen: make(map[string]time.Time),
		stopCh:   make(chan struct{}),
	}
}

// RecordAccess records that a series was written to.
func (m *StaleSeriesManager) RecordAccess(seriesKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSeen[seriesKey] = time.Now()
}

// GetStaleSeries returns series that haven't been seen since the timeout.
func (m *StaleSeriesManager) GetStaleSeries() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cutoff := time.Now().Add(-m.config.StaleTimeout)
	var stale []string
	for key, lastSeen := range m.lastSeen {
		if lastSeen.Before(cutoff) {
			stale = append(stale, key)
		}
	}
	return stale
}

// CleanupStale removes stale series entries.
func (m *StaleSeriesManager) CleanupStale() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-m.config.StaleTimeout)
	removed := 0
	for key, lastSeen := range m.lastSeen {
		if lastSeen.Before(cutoff) {
			delete(m.lastSeen, key)
			removed++
		}
	}
	return removed
}

// Start begins the cleanup goroutine.
func (m *StaleSeriesManager) Start() {
	if !m.config.Enabled {
		return
	}
	go m.cleanupLoop()
}

// Stop stops the cleanup goroutine.
func (m *StaleSeriesManager) Stop() {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
}

func (m *StaleSeriesManager) cleanupLoop() {
	ticker := time.NewTicker(m.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.CleanupStale()
		}
	}
}

// ActiveCount returns the number of tracked active series.
func (m *StaleSeriesManager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.lastSeen)
}

// RegisterRelabelHTTPHandlers registers HTTP endpoints for relabeling.
func (e *RelabelEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/relabel/rules", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(e.ListRules())
		case http.MethodPost:
			var rule RelabelRule
			if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := e.AddRule(rule); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "added"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

// RelabelWriteHook returns a pre-write hook function compatible with the
// DB write pipeline. It applies relabeling rules before points are written.
func (e *RelabelEngine) RelabelWriteHook() func(Point) (Point, error) {
	return func(p Point) (Point, error) {
		result, dropped := e.Apply(p)
		if dropped {
			return p, fmt.Errorf("point dropped by relabeling rule")
		}
		return result, nil
	}
}

// TenantCardinalityLimits provides per-tenant cardinality enforcement.
type TenantCardinalityLimits struct {
	limits map[string]int64 // tenantID -> max series
	counts map[string]int64 // tenantID -> current count
	mu     sync.RWMutex
}

// NewTenantCardinalityLimits creates per-tenant cardinality limits.
func NewTenantCardinalityLimits() *TenantCardinalityLimits {
	return &TenantCardinalityLimits{
		limits: make(map[string]int64),
		counts: make(map[string]int64),
	}
}

// SetLimit sets the cardinality limit for a tenant.
func (t *TenantCardinalityLimits) SetLimit(tenantID string, maxSeries int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.limits[tenantID] = maxSeries
}

// CheckAndTrack checks if a tenant can create a new series and tracks it.
func (t *TenantCardinalityLimits) CheckAndTrack(tenantID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	limit, hasLimit := t.limits[tenantID]
	if !hasLimit {
		return nil // No limit set
	}

	current := t.counts[tenantID]
	if current >= limit {
		return fmt.Errorf("tenant %q cardinality limit exceeded: %d/%d series",
			tenantID, current, limit)
	}

	t.counts[tenantID] = current + 1
	return nil
}

// GetUsage returns current cardinality usage for a tenant.
func (t *TenantCardinalityLimits) GetUsage(tenantID string) (current, limit int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.counts[tenantID], t.limits[tenantID]
}
