package chronicle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Cache management, rate limiting, audit logging, data masking, and Rego policy parsing for the policy engine.

func (pe *PolicyEngine) computeCacheKey(ctx *PolicyContext) string {
	data := fmt.Sprintf("%s:%s:%s:%v:%v",
		ctx.User, ctx.Operation, ctx.Resource, ctx.Roles, ctx.Groups)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func (pe *PolicyEngine) checkRateLimit(user string) bool {
	pe.rateMu.Lock()
	defer pe.rateMu.Unlock()

	bucket, exists := pe.rateLimits[user]
	if !exists {
		bucket = &rateLimitBucket{
			tokens:     100,
			lastRefill: time.Now(),
		}
		pe.rateLimits[user] = bucket
	}

	// Refill tokens
	now := time.Now()
	elapsed := now.Sub(bucket.lastRefill)
	if elapsed >= pe.config.RateLimitWindow {
		bucket.tokens = 100
		bucket.lastRefill = now
	}

	// Check and consume token
	if bucket.tokens <= 0 {
		return true // Rate limited
	}

	bucket.tokens--
	return false
}

type rateLimitBucket struct {
	tokens     int
	lastRefill time.Time
}

func (pe *PolicyEngine) recordAudit(ctx *PolicyContext, result *PolicyResult) {
	if !pe.config.AuditEnabled {
		return
	}

	entry := AuditEntry{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Timestamp: time.Now(),
		User:      ctx.User,
		Operation: ctx.Operation,
		Resource:  ctx.Resource,
		PolicyID:  result.PolicyID,
		Action:    result.Action,
		Allowed:   result.Allowed,
		Reason:    result.Reason,
		SourceIP:  ctx.SourceIP,
		Metadata:  result.Metadata,
	}

	select {
	case pe.auditChan <- entry:
	default:
		// Channel full, drop entry
	}
}

func (pe *PolicyEngine) auditWorker() {
	defer pe.wg.Done()

	for {
		select {
		case <-pe.ctx.Done():
			// Drain remaining entries before exiting
			for {
				select {
				case entry := <-pe.auditChan:
					pe.auditMu.Lock()
					pe.auditLog = append(pe.auditLog, entry)
					atomic.AddInt64(&pe.auditEntries, 1)
					pe.auditMu.Unlock()
				default:
					return
				}
			}
		case entry := <-pe.auditChan:
			pe.auditMu.Lock()
			pe.auditLog = append(pe.auditLog, entry)
			atomic.AddInt64(&pe.auditEntries, 1)

			// Prune old entries
			if pe.config.AuditRetention > 0 {
				cutoff := time.Now().Add(-pe.config.AuditRetention)
				newLog := make([]AuditEntry, 0)
				for _, e := range pe.auditLog {
					if e.Timestamp.After(cutoff) {
						newLog = append(newLog, e)
					}
				}
				pe.auditLog = newLog
			}
			pe.auditMu.Unlock()
		}
	}
}

// GetAuditLog returns audit log entries
func (pe *PolicyEngine) GetAuditLog(limit int, offset int) []AuditEntry {
	pe.auditMu.RLock()
	defer pe.auditMu.RUnlock()

	if offset >= len(pe.auditLog) {
		return []AuditEntry{}
	}

	end := offset + limit
	if end > len(pe.auditLog) {
		end = len(pe.auditLog)
	}

	// Return newest first
	result := make([]AuditEntry, 0, end-offset)
	for i := len(pe.auditLog) - 1 - offset; i >= len(pe.auditLog)-end && i >= 0; i-- {
		result = append(result, pe.auditLog[i])
	}

	return result
}

// SearchAuditLog searches audit entries
func (pe *PolicyEngine) SearchAuditLog(user, operation, resource string, since time.Time) []AuditEntry {
	pe.auditMu.RLock()
	defer pe.auditMu.RUnlock()

	results := make([]AuditEntry, 0)
	for _, entry := range pe.auditLog {
		if !since.IsZero() && entry.Timestamp.Before(since) {
			continue
		}
		if user != "" && entry.User != user {
			continue
		}
		if operation != "" && entry.Operation != operation {
			continue
		}
		if resource != "" && !strings.Contains(entry.Resource, resource) {
			continue
		}
		results = append(results, entry)
	}
	return results
}

// MaskData applies masking rules to data
func (pe *PolicyEngine) MaskData(data any, rules []MaskRule) any {
	if len(rules) == 0 {
		return data
	}

	// Handle map data
	if m, ok := data.(map[string]any); ok {
		result := make(map[string]any)
		for k, v := range m {
			masked := false
			for _, rule := range rules {
				if rule.Field == k || rule.Field == "*" {
					result[k] = pe.applyMask(v, rule.Strategy, rule.Parameters)
					masked = true
					break
				}
			}
			if !masked {
				result[k] = v
			}
		}
		return result
	}

	return data
}

func (pe *PolicyEngine) applyMask(value any, strategy string, params map[string]string) any {
	str := fmt.Sprintf("%v", value)

	switch strategy {
	case "hash":
		hash := sha256.Sum256([]byte(str))
		return hex.EncodeToString(hash[:8])

	case "redact":
		return "***REDACTED***"

	case "partial":
		// Show first and last few characters
		if len(str) <= 4 {
			return "****"
		}
		return str[:2] + strings.Repeat("*", len(str)-4) + str[len(str)-2:]

	case "truncate":
		length := 4
		if l, ok := params["length"]; ok {
			fmt.Sscanf(l, "%d", &length)
		}
		if len(str) <= length {
			return str
		}
		return str[:length] + "..."

	case "null":
		return nil

	case "fixed":
		if fixed, ok := params["value"]; ok {
			return fixed
		}
		return "***"

	case "email":
		// Mask email preserving domain
		parts := strings.Split(str, "@")
		if len(parts) != 2 {
			return "***@***.***"
		}
		return string(parts[0][0]) + "***@" + parts[1]

	default:
		return "***"
	}
}

// FilterPoints applies policy filters to query results
func (pe *PolicyEngine) FilterPoints(points []Point, ctx *PolicyContext) []Point {
	result := pe.Evaluate(ctx)
	if !result.Allowed {
		return []Point{}
	}

	if len(result.Filters) == 0 && len(result.MaskRules) == 0 {
		return points
	}

	filtered := make([]Point, 0, len(points))
	for _, p := range points {
		// Apply filters
		include := true
		for _, filter := range result.Filters {
			if !pe.matchesFilter(p, filter) {
				include = false
				break
			}
		}

		if include {
			// Apply masking
			if len(result.MaskRules) > 0 {
				p = pe.maskPoint(p, result.MaskRules)
			}
			filtered = append(filtered, p)
		}
	}

	return filtered
}

func (pe *PolicyEngine) matchesFilter(p Point, filter string) bool {
	// Simple filter syntax: field:value or field:!value
	parts := strings.SplitN(filter, ":", 2)
	if len(parts) != 2 {
		return true
	}

	field := parts[0]
	value := parts[1]
	negate := strings.HasPrefix(value, "!")
	if negate {
		value = value[1:]
	}

	var fieldValue string
	switch field {
	case "series":
		fieldValue = p.Metric
	default:
		if v, ok := p.Tags[field]; ok {
			fieldValue = v
		}
	}

	matches := fieldValue == value || strings.Contains(fieldValue, value)
	if negate {
		return !matches
	}
	return matches
}

func (pe *PolicyEngine) maskPoint(p Point, rules []MaskRule) Point {
	masked := Point{
		Metric:    p.Metric,
		Timestamp: p.Timestamp,
		Value:     p.Value,
		Tags:      make(map[string]string),
	}

	for k, v := range p.Tags {
		maskedValue := v
		for _, rule := range rules {
			if rule.Field == k || rule.Field == "*" || strings.HasPrefix(rule.Field, "tags.") {
				maskedValue = fmt.Sprintf("%v", pe.applyMask(v, rule.Strategy, rule.Parameters))
				break
			}
		}
		masked.Tags[k] = maskedValue
	}

	// Mask value if specified
	for _, rule := range rules {
		if rule.Field == "value" {
			if maskedVal, ok := pe.applyMask(p.Value, rule.Strategy, rule.Parameters).(float64); ok {
				masked.Value = maskedVal
			}
		}
	}

	return masked
}

// Stats returns policy engine statistics
func (pe *PolicyEngine) Stats() PolicyEngineStats {
	return PolicyEngineStats{
		PolicyCount:  int64(len(pe.policies)),
		Evaluations:  atomic.LoadInt64(&pe.evaluations),
		CacheHits:    atomic.LoadInt64(&pe.cacheHits),
		Denials:      atomic.LoadInt64(&pe.denials),
		AuditEntries: atomic.LoadInt64(&pe.auditEntries),
	}
}

// PolicyEngineStats contains policy engine statistics
type PolicyEngineStats struct {
	PolicyCount  int64 `json:"policy_count"`
	Evaluations  int64 `json:"evaluations"`
	CacheHits    int64 `json:"cache_hits"`
	Denials      int64 `json:"denials"`
	AuditEntries int64 `json:"audit_entries"`
}

// Close shuts down the policy engine
func (pe *PolicyEngine) Close() error {
	pe.cancel()
	pe.wg.Wait()
	close(pe.auditChan)
	return nil
}

// Policy cache implementation
type policyCache struct {
	entries map[string]*policyCacheEntry
	ttl     time.Duration
	mu      sync.RWMutex
}

type policyCacheEntry struct {
	result    *PolicyResult
	expiresAt time.Time
}

func newPolicyCache(ttl time.Duration) *policyCache {
	return &policyCache{
		entries: make(map[string]*policyCacheEntry),
		ttl:     ttl,
	}
}

func (c *policyCache) get(key string) (*PolicyResult, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists {
		return nil, false
	}

	if time.Now().After(entry.expiresAt) {
		return nil, false
	}

	return entry.result, true
}

func (c *policyCache) set(key string, result *PolicyResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &policyCacheEntry{
		result:    result,
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *policyCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*policyCacheEntry)
}

// Rego compatibility layer - simplified policy language
type RegoPolicy struct {
	Package string     `json:"package"`
	Rules   []RegoRule `json:"rules"`
}

// RegoRule represents a single rule within a RegoPolicy, including its name, default value, and body expression.
type RegoRule struct {
	Name    string `json:"name"`
	Default bool   `json:"default"`
	Body    string `json:"body"`
}

// ParseRegoPolicy parses a simplified Rego-like policy
func ParseRegoPolicy(content string) (*Policy, error) {
	policy := &Policy{
		Rules:    make([]PolicyRule, 0),
		Actions:  make([]PolicyAction, 0),
		Metadata: make(map[string]string),
	}

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse package name
		if strings.HasPrefix(line, "package ") {
			policy.Name = strings.TrimPrefix(line, "package ")
			policy.ID = strings.ReplaceAll(policy.Name, ".", "_")
			continue
		}

		// Parse default action
		if strings.HasPrefix(line, "default ") {
			parts := strings.Split(line, "=")
			if len(parts) == 2 {
				value := strings.TrimSpace(parts[1])
				if value == "true" {
					policy.Actions = append(policy.Actions, PolicyActionAllow)
				} else {
					policy.Actions = append(policy.Actions, PolicyActionDeny)
				}
			}
			continue
		}

		// Parse allow/deny rules
		if strings.Contains(line, "allow {") || strings.Contains(line, "deny {") {
			// Extract conditions - simplified parsing
			isDeny := strings.Contains(line, "deny")
			if isDeny {
				policy.Actions = append(policy.Actions, PolicyActionDeny)
			} else {
				policy.Actions = append(policy.Actions, PolicyActionAllow)
			}
		}

		// Parse conditions like: input.user == "admin"
		if strings.Contains(line, "input.") {
			parts := strings.Split(line, "==")
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				field = strings.TrimPrefix(field, "input.")
				value := strings.TrimSpace(parts[1])
				value = strings.Trim(value, "\"'")

				policy.Rules = append(policy.Rules, PolicyRule{
					ID:       fmt.Sprintf("rule_%d", len(policy.Rules)),
					Field:    field,
					Operator: "eq",
					Value:    value,
				})
			}
		}
	}

	return policy, nil
}

// JSON serialization
func (p *Policy) MarshalJSON() ([]byte, error) {
	type Alias Policy
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(p),
	})
}
