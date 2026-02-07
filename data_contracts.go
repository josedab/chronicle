package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DataContractConfig configures the declarative data contracts engine.
type DataContractConfig struct {
	Enabled              bool          `json:"enabled"`
	ValidateOnWrite      bool          `json:"validate_on_write"`
	AsyncValidation      bool          `json:"async_validation"`
	SamplingRate         float64       `json:"sampling_rate"`
	MaxViolationsPerMin  int           `json:"max_violations_per_min"`
	ProfilingEnabled     bool          `json:"profiling_enabled"`
	ProfilingWindow      time.Duration `json:"profiling_window"`
	AlertOnViolation     bool          `json:"alert_on_violation"`
	EnforcementMode      ContractEnforcementMode `json:"enforcement_mode"`
}

// ContractEnforcementMode controls how violations are handled.
type ContractEnforcementMode string

const (
	ContractEnforceLog    ContractEnforcementMode = "log"
	ContractEnforceReject ContractEnforcementMode = "reject"
	ContractEnforceAlert  ContractEnforcementMode = "alert"
)

// DefaultDataContractConfig returns sensible defaults.
func DefaultDataContractConfig() DataContractConfig {
	return DataContractConfig{
		Enabled:             true,
		ValidateOnWrite:     true,
		AsyncValidation:     false,
		SamplingRate:        1.0,
		MaxViolationsPerMin: 1000,
		ProfilingEnabled:    true,
		ProfilingWindow:     time.Hour,
		AlertOnViolation:    true,
		EnforcementMode:     ContractEnforceLog,
	}
}

// DataContract defines quality expectations for a metric.
type DataContract struct {
	ID              string                 `json:"id"`
	Name            string                 `json:"name"`
	Version         int                    `json:"version"`
	Description     string                 `json:"description"`
	MetricPattern   string                 `json:"metric_pattern"`
	Owner           string                 `json:"owner"`
	Rules           []ContractRule         `json:"rules"`
	SLOs            []ContractSLO          `json:"slos"`
	Tags            map[string]string      `json:"tags,omitempty"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
	Active          bool                   `json:"active"`
}

// ContractRule is a single validation rule within a contract.
type ContractRule struct {
	Name       string          `json:"name"`
	Type       ContractRuleType `json:"type"`
	Field      string          `json:"field"` // "value", "tags.<key>", "timestamp"
	Operator   string          `json:"operator"` // "gt", "lt", "gte", "lte", "eq", "ne", "in", "between", "regex"
	Threshold  float64         `json:"threshold,omitempty"`
	ThresholdHigh float64      `json:"threshold_high,omitempty"`
	StringValue string         `json:"string_value,omitempty"`
	StringList  []string       `json:"string_list,omitempty"`
	Severity   string          `json:"severity"` // "info", "warning", "error", "critical"
	Message    string          `json:"message,omitempty"`
}

// ContractRuleType identifies the type of contract rule.
type ContractRuleType string

const (
	ContractRuleRange       ContractRuleType = "range"
	ContractRuleNotNull     ContractRuleType = "not_null"
	ContractRuleFreshness   ContractRuleType = "freshness"
	ContractRuleCardinality ContractRuleType = "cardinality"
	ContractRuleSchema      ContractRuleType = "schema"
	ContractRuleCustom      ContractRuleType = "custom"
)

// ContractSLO defines a service-level objective for a metric.
type ContractSLO struct {
	Name        string        `json:"name"`
	Target      float64       `json:"target"` // e.g., 99.9 for 99.9%
	Window      time.Duration `json:"window"`
	Metric      string        `json:"metric_expression"`
	Description string        `json:"description"`
}

// ContractViolation records a contract rule violation.
type ContractViolation struct {
	ID           string        `json:"id"`
	ContractID   string        `json:"contract_id"`
	RuleName     string        `json:"rule_name"`
	Metric       string        `json:"metric"`
	ActualValue  float64       `json:"actual_value"`
	ExpectedOp   string        `json:"expected_operator"`
	Threshold    float64       `json:"threshold"`
	Severity     string        `json:"severity"`
	Message      string        `json:"message"`
	Timestamp    time.Time     `json:"timestamp"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// ContractProfile holds statistical profile data for a metric.
type ContractProfile struct {
	Metric        string    `json:"metric"`
	Count         int64     `json:"count"`
	Mean          float64   `json:"mean"`
	StdDev        float64   `json:"stddev"`
	Min           float64   `json:"min"`
	Max           float64   `json:"max"`
	P50           float64   `json:"p50"`
	P95           float64   `json:"p95"`
	P99           float64   `json:"p99"`
	NullRate      float64   `json:"null_rate"`
	Freshness     time.Duration `json:"freshness"`
	LastSeen      time.Time `json:"last_seen"`
	ProfiledAt    time.Time `json:"profiled_at"`
}

// ContractSLOStatus tracks SLO compliance.
type ContractSLOStatus struct {
	SLO           ContractSLO `json:"slo"`
	CurrentValue  float64     `json:"current_value"`
	IsCompliant   bool        `json:"is_compliant"`
	BudgetUsed    float64     `json:"budget_used"`
	BudgetRemaining float64   `json:"budget_remaining"`
	EvaluatedAt   time.Time   `json:"evaluated_at"`
}

// DataContractStats contains contract engine statistics.
type DataContractStats struct {
	TotalContracts      int   `json:"total_contracts"`
	ActiveContracts     int   `json:"active_contracts"`
	TotalRules          int   `json:"total_rules"`
	TotalViolations     int64 `json:"total_violations"`
	ViolationsLastHour  int64 `json:"violations_last_hour"`
	ValidationCount     int64 `json:"validation_count"`
	AvgValidationTimeNs int64 `json:"avg_validation_time_ns"`
	TotalSLOs           int   `json:"total_slos"`
	SLOCompliance       float64 `json:"slo_compliance_pct"`
}

// DataContractEngine manages declarative data quality contracts.
type DataContractEngine struct {
	db     *DB
	config DataContractConfig

	contracts  map[string]*DataContract
	violations []ContractViolation
	profiles   map[string]*ContractProfile

	validationCount   int64
	totalViolations   int64
	totalValidationNs int64
	violationCounter  int64

	mu sync.RWMutex
}

// NewDataContractEngine creates a new data contract engine.
func NewDataContractEngine(db *DB, cfg DataContractConfig) *DataContractEngine {
	return &DataContractEngine{
		db:         db,
		config:     cfg,
		contracts:  make(map[string]*DataContract),
		violations: make([]ContractViolation, 0),
		profiles:   make(map[string]*ContractProfile),
	}
}

// RegisterContract adds a new data contract.
func (dc *DataContractEngine) RegisterContract(contract DataContract) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if contract.ID == "" {
		return fmt.Errorf("data_contract: contract ID is required")
	}
	if contract.MetricPattern == "" {
		return fmt.Errorf("data_contract: metric pattern is required")
	}

	contract.CreatedAt = time.Now()
	contract.UpdatedAt = time.Now()
	contract.Active = true

	if existing, ok := dc.contracts[contract.ID]; ok {
		contract.Version = existing.Version + 1
	} else {
		contract.Version = 1
	}

	dc.contracts[contract.ID] = &contract
	return nil
}

// GetContract returns a contract by ID.
func (dc *DataContractEngine) GetContract(id string) *DataContract {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if c, ok := dc.contracts[id]; ok {
		cp := *c
		return &cp
	}
	return nil
}

// ListContracts returns all registered contracts.
func (dc *DataContractEngine) ListContracts() []DataContract {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	result := make([]DataContract, 0, len(dc.contracts))
	for _, c := range dc.contracts {
		result = append(result, *c)
	}
	return result
}

// DeactivateContract deactivates a contract.
func (dc *DataContractEngine) DeactivateContract(id string) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	c, ok := dc.contracts[id]
	if !ok {
		return fmt.Errorf("data_contract: contract %q not found", id)
	}
	c.Active = false
	c.UpdatedAt = time.Now()
	return nil
}

// ValidatePoint checks a point against all matching contracts.
func (dc *DataContractEngine) ValidatePoint(p Point) []ContractViolation {
	dc.mu.RLock()
	contracts := make([]*DataContract, 0)
	for _, c := range dc.contracts {
		if c.Active && dc.matchMetric(c.MetricPattern, p.Metric) {
			contracts = append(contracts, c)
		}
	}
	dc.mu.RUnlock()

	start := time.Now()
	var violations []ContractViolation

	for _, contract := range contracts {
		for _, rule := range contract.Rules {
			if v := dc.evaluateRule(contract, rule, p); v != nil {
				violations = append(violations, *v)
			}
		}
	}

	dc.mu.Lock()
	dc.validationCount++
	dc.totalValidationNs += int64(time.Since(start))
	if len(violations) > 0 {
		dc.totalViolations += int64(len(violations))
		dc.violations = append(dc.violations, violations...)
		// Keep last 10000 violations
		if len(dc.violations) > 10000 {
			dc.violations = dc.violations[len(dc.violations)-10000:]
		}
	}
	dc.mu.Unlock()

	// Update profile
	dc.updateProfile(p)

	return violations
}

func (dc *DataContractEngine) evaluateRule(contract *DataContract, rule ContractRule, p Point) *ContractViolation {
	var violated bool
	actualValue := p.Value

	switch rule.Operator {
	case "gt":
		violated = actualValue <= rule.Threshold
	case "gte":
		violated = actualValue < rule.Threshold
	case "lt":
		violated = actualValue >= rule.Threshold
	case "lte":
		violated = actualValue > rule.Threshold
	case "eq":
		violated = actualValue != rule.Threshold
	case "ne":
		violated = actualValue == rule.Threshold
	case "between":
		violated = actualValue < rule.Threshold || actualValue > rule.ThresholdHigh
	default:
		return nil
	}

	if !violated {
		return nil
	}

	dc.violationCounter++
	msg := rule.Message
	if msg == "" {
		msg = fmt.Sprintf("Rule %q violated: value %f %s %f", rule.Name, actualValue, rule.Operator, rule.Threshold)
	}

	return &ContractViolation{
		ID:          fmt.Sprintf("cv-%d", dc.violationCounter),
		ContractID:  contract.ID,
		RuleName:    rule.Name,
		Metric:      p.Metric,
		ActualValue: actualValue,
		ExpectedOp:  rule.Operator,
		Threshold:   rule.Threshold,
		Severity:    rule.Severity,
		Message:     msg,
		Timestamp:   time.Now(),
		Tags:        p.Tags,
	}
}

func (dc *DataContractEngine) matchMetric(pattern, metric string) bool {
	if pattern == "*" || pattern == "" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(metric, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == metric
}

func (dc *DataContractEngine) updateProfile(p Point) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	prof, ok := dc.profiles[p.Metric]
	if !ok {
		prof = &ContractProfile{
			Metric: p.Metric,
			Min:    p.Value,
			Max:    p.Value,
		}
		dc.profiles[p.Metric] = prof
	}

	prof.Count++
	prof.LastSeen = time.Now()
	prof.Freshness = 0

	// Running mean and variance (Welford's algorithm)
	delta := p.Value - prof.Mean
	prof.Mean += delta / float64(prof.Count)
	delta2 := p.Value - prof.Mean
	if prof.Count > 1 {
		prof.StdDev = math.Sqrt((prof.StdDev*prof.StdDev*float64(prof.Count-1) + delta*delta2) / float64(prof.Count))
	}

	if p.Value < prof.Min {
		prof.Min = p.Value
	}
	if p.Value > prof.Max {
		prof.Max = p.Value
	}
	prof.ProfiledAt = time.Now()
}

// GetProfile returns the statistical profile for a metric.
func (dc *DataContractEngine) GetProfile(metric string) *ContractProfile {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if p, ok := dc.profiles[metric]; ok {
		cp := *p
		return &cp
	}
	return nil
}

// ListProfiles returns all metric profiles.
func (dc *DataContractEngine) ListProfiles() []ContractProfile {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	result := make([]ContractProfile, 0, len(dc.profiles))
	for _, p := range dc.profiles {
		result = append(result, *p)
	}
	return result
}

// GetViolations returns recent violations.
func (dc *DataContractEngine) GetViolations(contractID string, limit int) []ContractViolation {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var result []ContractViolation
	for i := len(dc.violations) - 1; i >= 0 && len(result) < limit; i-- {
		v := dc.violations[i]
		if contractID == "" || v.ContractID == contractID {
			result = append(result, v)
		}
	}
	return result
}

// Stats returns contract engine statistics.
func (dc *DataContractEngine) Stats() DataContractStats {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	active := 0
	totalRules := 0
	totalSLOs := 0
	for _, c := range dc.contracts {
		if c.Active {
			active++
		}
		totalRules += len(c.Rules)
		totalSLOs += len(c.SLOs)
	}

	var avgNs int64
	if dc.validationCount > 0 {
		avgNs = dc.totalValidationNs / dc.validationCount
	}

	hourAgo := time.Now().Add(-time.Hour)
	var recentViolations int64
	for _, v := range dc.violations {
		if v.Timestamp.After(hourAgo) {
			recentViolations++
		}
	}

	return DataContractStats{
		TotalContracts:      len(dc.contracts),
		ActiveContracts:     active,
		TotalRules:          totalRules,
		TotalViolations:     dc.totalViolations,
		ViolationsLastHour:  recentViolations,
		ValidationCount:     dc.validationCount,
		AvgValidationTimeNs: avgNs,
		TotalSLOs:           totalSLOs,
	}
}

// RegisterHTTPHandlers registers data contract HTTP endpoints.
func (dc *DataContractEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/contracts", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(dc.ListContracts())
		case http.MethodPost:
			var contract DataContract
			if err := json.NewDecoder(r.Body).Decode(&contract); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if err := dc.RegisterContract(contract); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(dc.GetContract(contract.ID))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/contracts/violations", func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dc.GetViolations(contractID, 100))
	})
	mux.HandleFunc("/api/v1/contracts/profiles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dc.ListProfiles())
	})
	mux.HandleFunc("/api/v1/contracts/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dc.Stats())
	})
}
