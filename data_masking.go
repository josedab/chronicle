package chronicle

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// DataMaskingConfig configures the data masking engine.
type DataMaskingConfig struct {
	Enabled       bool   `json:"enabled"`
	DefaultPolicy string `json:"default_policy"` // none, redact, hash
	MaxRules      int    `json:"max_rules"`
	// HashSecret is the HMAC key used for the "hash" masking action.
	// When set, HMAC-SHA256 is used for cryptographically secure, deterministic
	// hashing. When empty, plain SHA-256 is used as a fallback.
	HashSecret string `json:"hash_secret,omitempty"`
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
					mp.Tags[rule.TagKey] = e.hashValue(mp.Tags[rule.TagKey])
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

// hashValue produces a deterministic, cryptographically secure hash of the
// input. Uses HMAC-SHA256 when HashSecret is configured, otherwise SHA-256.
func (e *DataMaskingEngine) hashValue(value string) string {
	if e.config.HashSecret != "" {
		mac := hmac.New(sha256.New, []byte(e.config.HashSecret))
		mac.Write([]byte(value))
		return hex.EncodeToString(mac.Sum(nil))
	}
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
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

// RegisterHTTPHandlers registers HTTP endpoints for masking rule management.
func (e *DataMaskingEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/masking/rules", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			if err := json.NewEncoder(w).Encode(e.ListRules()); err != nil {
				internalError(w, err, "encoding rules")
			}
		case http.MethodPost:
			var rule MaskingRule
			if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
				writeErrorf(w, http.StatusBadRequest, "invalid request body: %v", err)
				return
			}
			if rule.ID == "" {
				writeError(w, "rule id is required", http.StatusBadRequest)
				return
			}
			if rule.TagKey == "" {
				writeError(w, "tag_key is required", http.StatusBadRequest)
				return
			}
			if rule.Action == "" {
				writeError(w, "action is required", http.StatusBadRequest)
				return
			}
			switch rule.Action {
			case "redact", "hash", "truncate", "none":
				// valid
			default:
				writeErrorf(w, http.StatusBadRequest, "invalid action %q: must be redact, hash, truncate, or none", rule.Action)
				return
			}
			if err := e.AddRule(rule); err != nil {
				writeErrorf(w, http.StatusConflict, "%v", err)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(rule)
		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				writeError(w, "id query parameter is required", http.StatusBadRequest)
				return
			}
			if err := e.RemoveRule(id); err != nil {
				writeErrorf(w, http.StatusNotFound, "%v", err)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			writeErrorf(w, http.StatusMethodNotAllowed, "method %s not allowed", r.Method)
		}
	})
	mux.HandleFunc("/api/v1/masking/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(e.GetStats()); err != nil {
			internalError(w, err, "encoding stats")
		}
	})
}
