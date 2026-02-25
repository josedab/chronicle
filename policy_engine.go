package chronicle

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PolicyEngine provides embedded policy enforcement for Chronicle
// Supports OPA/Rego-compatible policies for access control, data masking, and auditing

// PolicyType defines the type of policy
type PolicyType string

const (
	PolicyTypeAccess    PolicyType = "access"     // Controls read/write access
	PolicyTypeFilter    PolicyType = "filter"     // Filters query results
	PolicyTypeMask      PolicyType = "mask"       // Masks sensitive data
	PolicyTypeRateLimit PolicyType = "rate_limit" // Rate limiting
	PolicyTypeAudit     PolicyType = "audit"      // Audit logging
	PolicyTypeTransform PolicyType = "transform"  // Data transformation
)

// PolicyAction defines the action when policy matches
type PolicyAction string

const (
	PolicyActionAllow  PolicyAction = "allow"
	PolicyActionDeny   PolicyAction = "deny"
	PolicyActionMask   PolicyAction = "mask"
	PolicyActionFilter PolicyAction = "filter"
	PolicyActionLog    PolicyAction = "log"
)

// PolicyEngineConfig configures the policy engine
type PolicyEngineConfig struct {
	// Enable policy enforcement
	Enabled bool

	// Default action when no policy matches
	DefaultAction PolicyAction

	// Enable audit logging
	AuditEnabled bool

	// Audit log retention
	AuditRetention time.Duration

	// Cache policy evaluation results
	CacheEnabled bool
	CacheTTL     time.Duration

	// Rate limit configuration
	RateLimitEnabled bool
	RateLimitWindow  time.Duration

	// Enable async policy evaluation
	AsyncEvaluation bool
}

// DefaultPolicyEngineConfig returns default configuration
func DefaultPolicyEngineConfig() *PolicyEngineConfig {
	return &PolicyEngineConfig{
		Enabled:          true,
		DefaultAction:    PolicyActionAllow,
		AuditEnabled:     true,
		AuditRetention:   7 * 24 * time.Hour,
		CacheEnabled:     true,
		CacheTTL:         5 * time.Minute,
		RateLimitEnabled: true,
		RateLimitWindow:  time.Minute,
		AsyncEvaluation:  false,
	}
}

// Policy represents a security/access policy
type Policy struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Type        PolicyType        `json:"type"`
	Priority    int               `json:"priority"` // Lower = higher priority
	Enabled     bool              `json:"enabled"`
	Rules       []PolicyRule      `json:"rules"`
	Actions     []PolicyAction    `json:"actions"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// PolicyRule represents a single rule within a policy
type PolicyRule struct {
	ID         string         `json:"id"`
	Field      string         `json:"field"`    // Field to match (series, tags.*, user, etc.)
	Operator   string         `json:"operator"` // eq, ne, contains, regex, in, gt, lt
	Value      any            `json:"value"`
	Conditions map[string]any `json:"conditions"` // Additional conditions
}

// PolicyContext provides context for policy evaluation
type PolicyContext struct {
	User       string            `json:"user"`
	Roles      []string          `json:"roles"`
	Groups     []string          `json:"groups"`
	Operation  string            `json:"operation"`  // read, write, delete, admin
	Resource   string            `json:"resource"`   // series name or pattern
	Attributes map[string]string `json:"attributes"` // Additional attributes
	Timestamp  time.Time         `json:"timestamp"`
	SourceIP   string            `json:"source_ip"`
	RequestID  string            `json:"request_id"`
}

// PolicyResult represents the result of policy evaluation
type PolicyResult struct {
	Allowed   bool              `json:"allowed"`
	Action    PolicyAction      `json:"action"`
	PolicyID  string            `json:"policy_id"`
	Reason    string            `json:"reason"`
	Filters   []string          `json:"filters"`    // Additional filters to apply
	MaskRules []MaskRule        `json:"mask_rules"` // Data masking rules
	Metadata  map[string]string `json:"metadata"`
}

// MaskRule defines how to mask data
type MaskRule struct {
	Field      string            `json:"field"`
	Strategy   string            `json:"strategy"` // hash, redact, partial, encrypt, tokenize
	Parameters map[string]string `json:"parameters"`
}

// PolicyEngine manages and evaluates policies
type PolicyEngine struct {
	db     *DB
	config *PolicyEngineConfig

	policies map[string]*Policy
	policyMu sync.RWMutex

	cache      *policyCache
	rateLimits map[string]*rateLimitBucket
	rateMu     sync.RWMutex

	auditLog  []AuditEntry
	auditMu   sync.RWMutex
	auditChan chan AuditEntry

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	evaluations  int64
	cacheHits    int64
	denials      int64
	auditEntries int64
}

// AuditEntry represents an audit log entry
type AuditEntry struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	User      string            `json:"user"`
	Operation string            `json:"operation"`
	Resource  string            `json:"resource"`
	PolicyID  string            `json:"policy_id"`
	Action    PolicyAction      `json:"action"`
	Allowed   bool              `json:"allowed"`
	Reason    string            `json:"reason"`
	SourceIP  string            `json:"source_ip"`
	Duration  time.Duration     `json:"duration"`
	Metadata  map[string]string `json:"metadata"`
}

// NewPolicyEngine creates a new policy engine
func NewPolicyEngine(db *DB, config *PolicyEngineConfig) (*PolicyEngine, error) {
	if config == nil {
		config = DefaultPolicyEngineConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	pe := &PolicyEngine{
		db:         db,
		config:     config,
		policies:   make(map[string]*Policy),
		rateLimits: make(map[string]*rateLimitBucket),
		auditLog:   make([]AuditEntry, 0),
		auditChan:  make(chan AuditEntry, 1000),
		ctx:        ctx,
		cancel:     cancel,
	}

	if config.CacheEnabled {
		pe.cache = newPolicyCache(config.CacheTTL)
	}

	// Start background workers
	pe.wg.Add(1)
	go pe.auditWorker()

	return pe, nil
}

// RegisterPolicy adds a new policy
func (pe *PolicyEngine) RegisterPolicy(policy *Policy) error {
	if policy.ID == "" {
		return fmt.Errorf("policy ID required")
	}
	if policy.Name == "" {
		return fmt.Errorf("policy name required")
	}

	pe.policyMu.Lock()
	defer pe.policyMu.Unlock()

	policy.CreatedAt = time.Now()
	policy.UpdatedAt = time.Now()
	policy.Enabled = true

	pe.policies[policy.ID] = policy
	return nil
}

// UpdatePolicy updates an existing policy
func (pe *PolicyEngine) UpdatePolicy(policy *Policy) error {
	pe.policyMu.Lock()
	defer pe.policyMu.Unlock()

	if _, exists := pe.policies[policy.ID]; !exists {
		return fmt.Errorf("policy not found: %s", policy.ID)
	}

	policy.UpdatedAt = time.Now()
	pe.policies[policy.ID] = policy

	// Invalidate cache
	if pe.cache != nil {
		pe.cache.clear()
	}

	return nil
}

// DeletePolicy removes a policy
func (pe *PolicyEngine) DeletePolicy(policyID string) error {
	pe.policyMu.Lock()
	defer pe.policyMu.Unlock()

	if _, exists := pe.policies[policyID]; !exists {
		return fmt.Errorf("policy not found: %s", policyID)
	}

	delete(pe.policies, policyID)
	return nil
}

// GetPolicy returns a policy by ID
func (pe *PolicyEngine) GetPolicy(policyID string) (*Policy, error) {
	pe.policyMu.RLock()
	defer pe.policyMu.RUnlock()

	policy, exists := pe.policies[policyID]
	if !exists {
		return nil, fmt.Errorf("policy not found: %s", policyID)
	}
	return policy, nil
}

// ListPolicies returns all policies
func (pe *PolicyEngine) ListPolicies() []*Policy {
	pe.policyMu.RLock()
	defer pe.policyMu.RUnlock()

	policies := make([]*Policy, 0, len(pe.policies))
	for _, p := range pe.policies {
		policies = append(policies, p)
	}
	return policies
}

// Evaluate evaluates all policies for a given context
func (pe *PolicyEngine) Evaluate(ctx *PolicyContext) *PolicyResult {
	if !pe.config.Enabled {
		return &PolicyResult{Allowed: true, Action: PolicyActionAllow}
	}

	atomic.AddInt64(&pe.evaluations, 1)

	// Check cache
	if pe.cache != nil {
		cacheKey := pe.computeCacheKey(ctx)
		if cached, ok := pe.cache.get(cacheKey); ok {
			atomic.AddInt64(&pe.cacheHits, 1)
			return cached
		}
	}

	// Check rate limit
	if pe.config.RateLimitEnabled {
		if limited := pe.checkRateLimit(ctx.User); limited {
			atomic.AddInt64(&pe.denials, 1)
			result := &PolicyResult{
				Allowed: false,
				Action:  PolicyActionDeny,
				Reason:  "rate limit exceeded",
			}
			pe.recordAudit(ctx, result)
			return result
		}
	}

	// Get applicable policies sorted by priority
	policies := pe.getApplicablePolicies(ctx)

	// Evaluate policies in priority order
	result := &PolicyResult{
		Allowed:   true,
		Action:    pe.config.DefaultAction,
		MaskRules: make([]MaskRule, 0),
		Filters:   make([]string, 0),
	}

	for _, policy := range policies {
		policyResult := pe.evaluatePolicy(policy, ctx)
		if policyResult != nil {
			// Merge results
			if policyResult.Action == PolicyActionDeny {
				result.Allowed = false
				result.Action = PolicyActionDeny
				result.PolicyID = policy.ID
				result.Reason = policyResult.Reason
				break
			}
			if policyResult.Action == PolicyActionMask {
				result.Action = PolicyActionMask
				result.MaskRules = append(result.MaskRules, policyResult.MaskRules...)
			}
			if policyResult.Action == PolicyActionFilter {
				result.Filters = append(result.Filters, policyResult.Filters...)
			}
		}
	}

	// Apply default action if no explicit decision
	if result.Action == "" {
		result.Action = pe.config.DefaultAction
		result.Allowed = pe.config.DefaultAction == PolicyActionAllow
	}

	if !result.Allowed {
		atomic.AddInt64(&pe.denials, 1)
	}

	// Cache result
	if pe.cache != nil {
		cacheKey := pe.computeCacheKey(ctx)
		pe.cache.set(cacheKey, result)
	}

	// Record audit
	pe.recordAudit(ctx, result)

	return result
}

func (pe *PolicyEngine) getApplicablePolicies(ctx *PolicyContext) []*Policy {
	pe.policyMu.RLock()
	defer pe.policyMu.RUnlock()

	applicable := make([]*Policy, 0)
	for _, policy := range pe.policies {
		if !policy.Enabled {
			continue
		}
		applicable = append(applicable, policy)
	}

	// Sort by priority (lower = higher priority)
	for i := 0; i < len(applicable); i++ {
		for j := i + 1; j < len(applicable); j++ {
			if applicable[j].Priority < applicable[i].Priority {
				applicable[i], applicable[j] = applicable[j], applicable[i]
			}
		}
	}

	return applicable
}

func (pe *PolicyEngine) evaluatePolicy(policy *Policy, ctx *PolicyContext) *PolicyResult {
	// All rules must match for the policy to apply
	for _, rule := range policy.Rules {
		if !pe.evaluateRule(&rule, ctx) {
			return nil
		}
	}

	// Policy matches - determine action
	result := &PolicyResult{
		Allowed:   true,
		PolicyID:  policy.ID,
		MaskRules: make([]MaskRule, 0),
		Filters:   make([]string, 0),
	}

	for _, action := range policy.Actions {
		switch action {
		case PolicyActionDeny:
			result.Allowed = false
			result.Action = PolicyActionDeny
			result.Reason = fmt.Sprintf("denied by policy: %s", policy.Name)
			return result

		case PolicyActionMask:
			result.Action = PolicyActionMask
			// Extract mask rules from policy metadata
			if maskField, ok := policy.Metadata["mask_field"]; ok {
				result.MaskRules = append(result.MaskRules, MaskRule{
					Field:    maskField,
					Strategy: policy.Metadata["mask_strategy"],
				})
			}

		case PolicyActionFilter:
			result.Action = PolicyActionFilter
			if filter, ok := policy.Metadata["filter"]; ok {
				result.Filters = append(result.Filters, filter)
			}

		case PolicyActionLog:
			// Audit logging is automatic, but we note it
			if result.Metadata == nil {
				result.Metadata = make(map[string]string)
			}
			result.Metadata["logged"] = "true"
		}
	}

	return result
}

func (pe *PolicyEngine) evaluateRule(rule *PolicyRule, ctx *PolicyContext) bool {
	// Get the value to compare
	var fieldValue any
	switch rule.Field {
	case "user":
		fieldValue = ctx.User
	case "operation":
		fieldValue = ctx.Operation
	case "resource":
		fieldValue = ctx.Resource
	case "source_ip":
		fieldValue = ctx.SourceIP
	case "roles":
		fieldValue = ctx.Roles
	case "groups":
		fieldValue = ctx.Groups
	default:
		// Check attributes
		if strings.HasPrefix(rule.Field, "attributes.") {
			attrKey := strings.TrimPrefix(rule.Field, "attributes.")
			fieldValue = ctx.Attributes[attrKey]
		}
	}

	// Apply operator
	return pe.applyOperator(rule.Operator, fieldValue, rule.Value)
}

func (pe *PolicyEngine) applyOperator(op string, fieldValue, ruleValue any) bool {
	switch op {
	case "eq", "equals":
		return fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", ruleValue)

	case "ne", "not_equals":
		return fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", ruleValue)

	case "contains":
		return strings.Contains(fmt.Sprintf("%v", fieldValue), fmt.Sprintf("%v", ruleValue))

	case "not_contains":
		return !strings.Contains(fmt.Sprintf("%v", fieldValue), fmt.Sprintf("%v", ruleValue))

	case "starts_with":
		return strings.HasPrefix(fmt.Sprintf("%v", fieldValue), fmt.Sprintf("%v", ruleValue))

	case "ends_with":
		return strings.HasSuffix(fmt.Sprintf("%v", fieldValue), fmt.Sprintf("%v", ruleValue))

	case "regex", "matches":
		pattern, ok := ruleValue.(string)
		if !ok {
			return false
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false
		}
		return re.MatchString(fmt.Sprintf("%v", fieldValue))

	case "in":
		// Rule value should be a list
		list, ok := ruleValue.([]any)
		if !ok {
			if strList, ok := ruleValue.([]string); ok {
				for _, v := range strList {
					if fmt.Sprintf("%v", fieldValue) == v {
						return true
					}
				}
			}
			return false
		}
		for _, v := range list {
			if fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", v) {
				return true
			}
		}
		return false

	case "not_in":
		list, ok := ruleValue.([]any)
		if !ok {
			return true
		}
		for _, v := range list {
			if fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", v) {
				return false
			}
		}
		return true

	case "has_any":
		// Field value should be a list, check if it contains any of rule values
		fieldList, ok := fieldValue.([]string)
		if !ok {
			return false
		}
		ruleList, ok := ruleValue.([]any)
		if !ok {
			if strList, ok := ruleValue.([]string); ok {
				for _, fv := range fieldList {
					for _, rv := range strList {
						if fv == rv {
							return true
						}
					}
				}
			}
			return false
		}
		for _, fv := range fieldList {
			for _, rv := range ruleList {
				if fv == fmt.Sprintf("%v", rv) {
					return true
				}
			}
		}
		return false

	case "has_all":
		fieldList, ok := fieldValue.([]string)
		if !ok {
			return false
		}
		ruleList, ok := ruleValue.([]string)
		if !ok {
			return false
		}
		for _, rv := range ruleList {
			found := false
			for _, fv := range fieldList {
				if fv == rv {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true

	default:
		return false
	}
}
