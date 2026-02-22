package chronicle

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"time"
)

// DataMaskingConfig configures the data masking engine.
type DataMaskingConfig struct {
	Enabled       bool   `json:"enabled"`
	DefaultPolicy string `json:"default_policy"` // none, redact, hash
	MaxRules      int    `json:"max_rules"`
}

// DefaultDataMaskingConfig returns sensible defaults.
func DefaultDataMaskingConfig() DataMaskingConfig {
	return DataMaskingConfig{
		Enabled:       true,
		DefaultPolicy: "none",
		MaxRules:      100,
	}
}

// MaskingRule defines how a specific tag should be masked.
type MaskingRule struct {
	ID            string    `json:"id"`
	MetricPattern string    `json:"metric_pattern"`
	TagKey        string    `json:"tag_key"`
	Action        string    `json:"action"` // redact, hash, truncate, none
	Role          string    `json:"role"`
	Priority      int       `json:"priority"`
	CreatedAt     time.Time `json:"created_at"`
}

// MaskingContext provides context for masking decisions.
type MaskingContext struct {
	UserRole  string `json:"user_role"`
	Purpose   string `json:"purpose"`
	RequestID string `json:"request_id"`
}

// MaskedResult reports the outcome of a masking operation.
type MaskedResult struct {
	OriginalPointCount int           `json:"original_point_count"`
	MaskedPointCount   int           `json:"masked_point_count"`
	RulesApplied       []string      `json:"rules_applied"`
	Duration           time.Duration `json:"duration"`
}

// DataMaskingStats holds engine statistics.
type DataMaskingStats struct {
	TotalApplied    int64            `json:"total_applied"`
	TotalMasked     int64            `json:"total_masked"`
	RuleCount       int64            `json:"rule_count"`
	AppliedByAction map[string]int64 `json:"applied_by_action"`
}

// DataMaskingEngine manages compliance data masking rules and application.
type DataMaskingEngine struct {
	db      *DB
	config  DataMaskingConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}

	rules []MaskingRule
	stats DataMaskingStats
}

// NewDataMaskingEngine creates a new data masking engine.
func NewDataMaskingEngine(db *DB, cfg DataMaskingConfig) *DataMaskingEngine {
	return &DataMaskingEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
		stats: DataMaskingStats{
			AppliedByAction: make(map[string]int64),
		},
	}
}

func (e *DataMaskingEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

func (e *DataMaskingEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// AddRule adds a masking rule.
func (e *DataMaskingEngine) AddRule(rule MaskingRule) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.rules) >= e.config.MaxRules {
		return fmt.Errorf("max rules limit (%d) reached", e.config.MaxRules)
	}

	for _, r := range e.rules {
		if r.ID == rule.ID {
			return fmt.Errorf("rule %s already exists", rule.ID)
		}
	}

	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = time.Now()
	}
	e.rules = append(e.rules, rule)
	e.stats.RuleCount = int64(len(e.rules))
	return nil
}

// RemoveRule removes a masking rule by ID.
func (e *DataMaskingEngine) RemoveRule(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, r := range e.rules {
		if r.ID == id {
			e.rules = append(e.rules[:i], e.rules[i+1:]...)
			e.stats.RuleCount = int64(len(e.rules))
			return nil
		}
	}
	return fmt.Errorf("rule %s not found", id)
}

// ListRules returns all masking rules.
func (e *DataMaskingEngine) ListRules() []MaskingRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]MaskingRule, len(e.rules))
	copy(result, e.rules)
	return result
}

// Apply applies masking rules to a set of points.
func (e *DataMaskingEngine) Apply(points []Point, ctx MaskingContext) ([]Point, *MaskedResult) {
	start := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()

	result := &MaskedResult{
		OriginalPointCount: len(points),
	}

	if len(e.rules) == 0 {
		out := make([]Point, len(points))
		copy(out, points)
		result.Duration = time.Since(start)
		return out, result
	}

	e.stats.TotalApplied++

	masked := make([]Point, len(points))
	appliedRules := make(map[string]bool)

	for i, p := range points {
		mp := Point{
			Metric:    p.Metric,
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Tags:      make(map[string]string, len(p.Tags)),
		}
		for k, v := range p.Tags {
			mp.Tags[k] = v
		}

		pointMasked := false
		for _, rule := range e.rules {
			// role-based filtering
			if rule.Role != "" && rule.Role != ctx.UserRole {
				continue
			}
			// metric pattern matching (simple prefix match)
			if rule.MetricPattern != "" && !matchesPattern(p.Metric, rule.MetricPattern) {
				continue
			}
			// apply masking to matching tag
			if _, ok := mp.Tags[rule.TagKey]; ok {
				switch rule.Action {
				case "redact":
					mp.Tags[rule.TagKey] = "***"
					pointMasked = true
				case "hash":
					h := fnv.New64a()
					h.Write([]byte(mp.Tags[rule.TagKey]))
					mp.Tags[rule.TagKey] = fmt.Sprintf("%x", h.Sum64())
					pointMasked = true
				case "truncate":
					v := mp.Tags[rule.TagKey]
					if len(v) > 3 {
						mp.Tags[rule.TagKey] = v[:3] + "..."
					}
					pointMasked = true
				}
				appliedRules[rule.ID] = true
				e.stats.AppliedByAction[rule.Action]++
			}
		}
		if pointMasked {
			result.MaskedPointCount++
			e.stats.TotalMasked++
		}
		masked[i] = mp
	}

	for id := range appliedRules {
		result.RulesApplied = append(result.RulesApplied, id)
	}

	result.Duration = time.Since(start)
	return masked, result
}

func matchesPattern(metric, pattern string) bool {
	if pattern == "*" {
		return true
	}
	// simple prefix matching with wildcard
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(metric) >= len(prefix) && metric[:len(prefix)] == prefix
	}
	return metric == pattern
}

// GetStats returns engine statistics.
func (e *DataMaskingEngine) GetStats() DataMaskingStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := e.stats
	// copy map
	s.AppliedByAction = make(map[string]int64, len(e.stats.AppliedByAction))
	for k, v := range e.stats.AppliedByAction {
		s.AppliedByAction[k] = v
	}
	return s
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *DataMaskingEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/masking/rules", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListRules())
	})
	mux.HandleFunc("/api/v1/masking/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
