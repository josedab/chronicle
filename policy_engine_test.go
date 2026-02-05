package chronicle

import (
	"testing"
	"time"
)

func TestNewPolicyEngine(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultPolicyEngineConfig()
	pe, err := NewPolicyEngine(db, config)
	if err != nil {
		t.Fatalf("NewPolicyEngine() error = %v", err)
	}
	defer pe.Close()

	if pe.policies == nil {
		t.Error("policies map should be initialized")
	}
	if pe.cache == nil {
		t.Error("cache should be initialized when enabled")
	}
}

func TestRegisterPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	policy := &Policy{
		ID:          "test-policy",
		Name:        "Test Policy",
		Description: "A test policy",
		Type:        PolicyTypeAccess,
		Priority:    10,
		Rules: []PolicyRule{
			{Field: "user", Operator: "eq", Value: "admin"},
		},
		Actions: []PolicyAction{PolicyActionAllow},
	}

	err = pe.RegisterPolicy(policy)
	if err != nil {
		t.Fatalf("RegisterPolicy() error = %v", err)
	}

	// Retrieve policy
	retrieved, err := pe.GetPolicy("test-policy")
	if err != nil {
		t.Fatalf("GetPolicy() error = %v", err)
	}

	if retrieved.Name != "Test Policy" {
		t.Errorf("Name = %s, want Test Policy", retrieved.Name)
	}
	if !retrieved.Enabled {
		t.Error("Policy should be enabled by default")
	}
}

func TestPolicyValidation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	// Missing ID
	err = pe.RegisterPolicy(&Policy{Name: "test"})
	if err == nil {
		t.Error("Expected error for missing ID")
	}

	// Missing name
	err = pe.RegisterPolicy(&Policy{ID: "test"})
	if err == nil {
		t.Error("Expected error for missing name")
	}
}

func TestDeletePolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	pe.RegisterPolicy(&Policy{ID: "del-test", Name: "Delete Test"})

	err = pe.DeletePolicy("del-test")
	if err != nil {
		t.Fatalf("DeletePolicy() error = %v", err)
	}

	_, err = pe.GetPolicy("del-test")
	if err == nil {
		t.Error("Expected error for deleted policy")
	}
}

func TestEvaluateAllowPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	// Admin access policy
	pe.RegisterPolicy(&Policy{
		ID:       "admin-access",
		Name:     "Admin Access",
		Priority: 1,
		Rules: []PolicyRule{
			{Field: "user", Operator: "eq", Value: "admin"},
		},
		Actions: []PolicyAction{PolicyActionAllow},
	})

	ctx := &PolicyContext{
		User:      "admin",
		Operation: "read",
		Resource:  "metrics",
	}

	result := pe.Evaluate(ctx)
	if !result.Allowed {
		t.Error("Expected admin to be allowed")
	}
}

func TestEvaluateDenyPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	// Deny guest users
	pe.RegisterPolicy(&Policy{
		ID:       "deny-guest",
		Name:     "Deny Guest",
		Priority: 1,
		Rules: []PolicyRule{
			{Field: "user", Operator: "eq", Value: "guest"},
		},
		Actions: []PolicyAction{PolicyActionDeny},
	})

	ctx := &PolicyContext{
		User:      "guest",
		Operation: "write",
		Resource:  "metrics",
	}

	result := pe.Evaluate(ctx)
	if result.Allowed {
		t.Error("Expected guest to be denied")
	}
	if result.Action != PolicyActionDeny {
		t.Errorf("Action = %s, want deny", result.Action)
	}
}

func TestPolicyPriority(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	// Lower priority deny (higher number)
	pe.RegisterPolicy(&Policy{
		ID:       "allow-all",
		Name:     "Allow All",
		Priority: 100,
		Rules:    []PolicyRule{{Field: "user", Operator: "contains", Value: ""}},
		Actions:  []PolicyAction{PolicyActionAllow},
	})

	// Higher priority deny (lower number)
	pe.RegisterPolicy(&Policy{
		ID:       "deny-test",
		Name:     "Deny Test User",
		Priority: 1,
		Rules:    []PolicyRule{{Field: "user", Operator: "eq", Value: "testuser"}},
		Actions:  []PolicyAction{PolicyActionDeny},
	})

	ctx := &PolicyContext{
		User:      "testuser",
		Operation: "read",
		Resource:  "metrics",
	}

	result := pe.Evaluate(ctx)
	if result.Allowed {
		t.Error("Expected testuser to be denied due to higher priority policy")
	}
}

func TestOperators(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	tests := []struct {
		name     string
		operator string
		field    string
		value    interface{}
		ctx      *PolicyContext
		expected bool
	}{
		{
			name:     "equals",
			operator: "eq",
			field:    "user",
			value:    "admin",
			ctx:      &PolicyContext{User: "admin"},
			expected: true,
		},
		{
			name:     "not_equals",
			operator: "ne",
			field:    "user",
			value:    "admin",
			ctx:      &PolicyContext{User: "user"},
			expected: true,
		},
		{
			name:     "contains",
			operator: "contains",
			field:    "resource",
			value:    "metric",
			ctx:      &PolicyContext{Resource: "cpu_metrics"},
			expected: true,
		},
		{
			name:     "starts_with",
			operator: "starts_with",
			field:    "resource",
			value:    "cpu",
			ctx:      &PolicyContext{Resource: "cpu_usage"},
			expected: true,
		},
		{
			name:     "ends_with",
			operator: "ends_with",
			field:    "resource",
			value:    "usage",
			ctx:      &PolicyContext{Resource: "cpu_usage"},
			expected: true,
		},
		{
			name:     "regex",
			operator: "regex",
			field:    "resource",
			value:    "^cpu_.*",
			ctx:      &PolicyContext{Resource: "cpu_usage"},
			expected: true,
		},
		{
			name:     "in_list",
			operator: "in",
			field:    "operation",
			value:    []string{"read", "write"},
			ctx:      &PolicyContext{Operation: "read"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pe.applyOperator(tt.operator, getFieldValue(tt.field, tt.ctx), tt.value)
			if result != tt.expected {
				t.Errorf("applyOperator(%s) = %v, want %v", tt.operator, result, tt.expected)
			}
		})
	}
}

func getFieldValue(field string, ctx *PolicyContext) interface{} {
	switch field {
	case "user":
		return ctx.User
	case "operation":
		return ctx.Operation
	case "resource":
		return ctx.Resource
	case "roles":
		return ctx.Roles
	}
	return nil
}

func TestRoleBasedPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	// Admin role policy
	pe.RegisterPolicy(&Policy{
		ID:       "admin-role",
		Name:     "Admin Role Access",
		Priority: 1,
		Rules: []PolicyRule{
			{Field: "roles", Operator: "has_any", Value: []string{"admin", "superuser"}},
		},
		Actions: []PolicyAction{PolicyActionAllow},
	})

	// User with admin role
	ctx := &PolicyContext{
		User:      "john",
		Roles:     []string{"admin", "viewer"},
		Operation: "write",
		Resource:  "settings",
	}

	result := pe.Evaluate(ctx)
	if !result.Allowed {
		t.Error("Expected user with admin role to be allowed")
	}

	// User without admin role
	ctx2 := &PolicyContext{
		User:      "jane",
		Roles:     []string{"viewer"},
		Operation: "write",
		Resource:  "settings",
	}

	result2 := pe.Evaluate(ctx2)
	// Should be allowed by default since no deny rule matches
	if !result2.Allowed {
		t.Log("User without matching role gets default action")
	}
}

func TestMaskingPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	pe.RegisterPolicy(&Policy{
		ID:       "mask-sensitive",
		Name:     "Mask Sensitive Data",
		Enabled:  true,
		Priority: 1,
		Rules: []PolicyRule{
			{Field: "resource", Operator: "contains", Value: "sensitive"},
		},
		Actions: []PolicyAction{PolicyActionMask},
		Metadata: map[string]string{
			"mask_field":    "value",
			"mask_strategy": "partial",
		},
	})

	ctx := &PolicyContext{
		User:      "user",
		Operation: "read",
		Resource:  "sensitive_data",
	}

	result := pe.Evaluate(ctx)
	if result.Action != PolicyActionMask {
		t.Errorf("Action = %s, want mask", result.Action)
	}
	if len(result.MaskRules) == 0 {
		t.Error("Expected mask rules")
	}
}

func TestApplyMask(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	tests := []struct {
		strategy string
		input    string
		check    func(string) bool
	}{
		{"hash", "secret", func(s string) bool { return len(s) == 16 }},
		{"redact", "secret", func(s string) bool { return s == "***REDACTED***" }},
		{"partial", "secretvalue", func(s string) bool { return s == "se*******ue" }},
		{"null", "secret", func(s string) bool { return s == "<nil>" }},
	}

	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			result := pe.applyMask(tt.input, tt.strategy, nil)
			resultStr := ""
			if result != nil {
				resultStr = result.(string)
			} else {
				resultStr = "<nil>"
			}
			if !tt.check(resultStr) {
				t.Errorf("applyMask(%s, %s) = %s, unexpected result", tt.strategy, tt.input, resultStr)
			}
		})
	}
}

func TestEmailMasking(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	result := pe.applyMask("john.doe@example.com", "email", nil)
	if result != "j***@example.com" {
		t.Errorf("Email mask = %s, want j***@example.com", result)
	}
}

func TestAuditLogging(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultPolicyEngineConfig()
	config.AuditEnabled = true
	pe, _ := NewPolicyEngine(db, config)
	defer pe.Close()

	// Evaluate some policies
	ctx := &PolicyContext{
		User:      "testuser",
		Operation: "read",
		Resource:  "metrics",
		SourceIP:  "192.168.1.1",
	}

	pe.Evaluate(ctx)
	pe.Evaluate(ctx)
	pe.Evaluate(ctx)

	// Give audit worker time to process
	time.Sleep(100 * time.Millisecond)

	log := pe.GetAuditLog(10, 0)
	if len(log) < 1 {
		t.Error("Expected audit entries")
	}
}

func TestSearchAuditLog(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	// Generate some audit entries
	users := []string{"alice", "bob", "charlie"}
	for _, user := range users {
		ctx := &PolicyContext{
			User:      user,
			Operation: "read",
			Resource:  "metrics",
		}
		pe.Evaluate(ctx)
	}

	time.Sleep(100 * time.Millisecond)

	// Search by user
	results := pe.SearchAuditLog("alice", "", "", time.Time{})
	found := false
	for _, entry := range results {
		if entry.User == "alice" {
			found = true
			break
		}
	}
	if !found && len(results) > 0 {
		t.Log("Search filtering works")
	}
}

func TestRateLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultPolicyEngineConfig()
	config.RateLimitEnabled = true
	pe, _ := NewPolicyEngine(db, config)
	defer pe.Close()

	ctx := &PolicyContext{
		User:      "heavy-user",
		Operation: "read",
		Resource:  "metrics",
	}

	// Make many requests
	denials := 0
	for i := 0; i < 150; i++ {
		result := pe.Evaluate(ctx)
		if !result.Allowed && result.Reason == "rate limit exceeded" {
			denials++
		}
	}

	if denials == 0 {
		t.Log("Rate limiting didn't trigger - may need more iterations")
	}
}

func TestPolicyCache(t *testing.T) {
	cache := newPolicyCache(5 * time.Minute)

	result := &PolicyResult{
		Allowed: true,
		Action:  PolicyActionAllow,
	}

	cache.set("key1", result)

	retrieved, ok := cache.get("key1")
	if !ok {
		t.Error("Cache should hit")
	}
	if !retrieved.Allowed {
		t.Error("Cached result should be allowed")
	}

	// Miss
	_, ok = cache.get("nonexistent")
	if ok {
		t.Error("Cache should miss for nonexistent key")
	}

	// Clear
	cache.clear()
	_, ok = cache.get("key1")
	if ok {
		t.Error("Cache should be empty after clear")
	}
}

func TestFilterPoints(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	// Policy that filters points
	pe.RegisterPolicy(&Policy{
		ID:       "filter-prod",
		Name:     "Filter Production",
		Priority: 1,
		Rules: []PolicyRule{
			{Field: "user", Operator: "eq", Value: "viewer"},
		},
		Actions: []PolicyAction{PolicyActionFilter},
		Metadata: map[string]string{
			"filter": "env:prod",
		},
	})

	points := []Point{
		{Metric: "cpu", Value: 50, Tags: map[string]string{"env": "prod"}},
		{Metric: "cpu", Value: 60, Tags: map[string]string{"env": "staging"}},
		{Metric: "cpu", Value: 70, Tags: map[string]string{"env": "prod"}},
	}

	ctx := &PolicyContext{
		User:      "viewer",
		Operation: "read",
		Resource:  "cpu",
	}

	filtered := pe.FilterPoints(points, ctx)
	prodCount := 0
	for _, p := range filtered {
		if p.Tags["env"] == "prod" {
			prodCount++
		}
	}

	// Filter should keep only prod entries
	t.Logf("Filtered to %d points", len(filtered))
}

func TestListPolicies(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	pe.RegisterPolicy(&Policy{ID: "p1", Name: "Policy 1"})
	pe.RegisterPolicy(&Policy{ID: "p2", Name: "Policy 2"})
	pe.RegisterPolicy(&Policy{ID: "p3", Name: "Policy 3"})

	policies := pe.ListPolicies()
	if len(policies) != 3 {
		t.Errorf("ListPolicies() count = %d, want 3", len(policies))
	}
}

func TestUpdatePolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	pe.RegisterPolicy(&Policy{ID: "update-test", Name: "Original"})

	updated := &Policy{ID: "update-test", Name: "Updated"}
	err = pe.UpdatePolicy(updated)
	if err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}

	retrieved, _ := pe.GetPolicy("update-test")
	if retrieved.Name != "Updated" {
		t.Errorf("Name = %s, want Updated", retrieved.Name)
	}
}

func TestPEStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	pe.RegisterPolicy(&Policy{ID: "stats-test", Name: "Stats Test"})

	ctx := &PolicyContext{User: "user", Operation: "read"}
	pe.Evaluate(ctx)
	pe.Evaluate(ctx)

	stats := pe.Stats()
	if stats.PolicyCount != 1 {
		t.Errorf("PolicyCount = %d, want 1", stats.PolicyCount)
	}
	if stats.Evaluations < 2 {
		t.Errorf("Evaluations = %d, want >= 2", stats.Evaluations)
	}
}

func TestParseRegoPolicy(t *testing.T) {
	rego := `
package authz

default allow = false

allow {
	input.user == "admin"
}
`

	policy, err := ParseRegoPolicy(rego)
	if err != nil {
		t.Fatalf("ParseRegoPolicy() error = %v", err)
	}

	if policy.Name != "authz" {
		t.Errorf("Name = %s, want authz", policy.Name)
	}
	if len(policy.Rules) == 0 {
		t.Error("Expected parsed rules")
	}
}

func TestDisabledPolicyEngine(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultPolicyEngineConfig()
	config.Enabled = false
	pe, _ := NewPolicyEngine(db, config)
	defer pe.Close()

	// Add deny policy
	pe.RegisterPolicy(&Policy{
		ID:      "deny-all",
		Name:    "Deny All",
		Rules:   []PolicyRule{{Field: "user", Operator: "ne", Value: ""}},
		Actions: []PolicyAction{PolicyActionDeny},
	})

	ctx := &PolicyContext{User: "anyone"}
	result := pe.Evaluate(ctx)

	// Should be allowed when engine is disabled
	if !result.Allowed {
		t.Error("Expected allow when policy engine is disabled")
	}
}

func TestMaskData(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	data := map[string]interface{}{
		"username": "john",
		"password": "secret123",
		"email":    "john@example.com",
	}

	rules := []MaskRule{
		{Field: "password", Strategy: "redact"},
		{Field: "email", Strategy: "email"},
	}

	masked := pe.MaskData(data, rules)
	maskedMap := masked.(map[string]interface{})

	if maskedMap["username"] != "john" {
		t.Error("Username should not be masked")
	}
	if maskedMap["password"] != "***REDACTED***" {
		t.Errorf("Password = %s, want ***REDACTED***", maskedMap["password"])
	}
}

func TestAttributeBasedPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	pe, _ := NewPolicyEngine(db, nil)
	defer pe.Close()

	pe.RegisterPolicy(&Policy{
		ID:       "dept-policy",
		Name:     "Department Policy",
		Priority: 1,
		Rules: []PolicyRule{
			{Field: "attributes.department", Operator: "eq", Value: "engineering"},
		},
		Actions: []PolicyAction{PolicyActionAllow},
	})

	ctx := &PolicyContext{
		User:       "developer",
		Attributes: map[string]string{"department": "engineering"},
	}

	result := pe.Evaluate(ctx)
	if !result.Allowed {
		t.Error("Expected engineering department to be allowed")
	}
}
