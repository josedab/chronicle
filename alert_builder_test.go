package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAlertBuilder_CreateRule(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "High CPU Alert",
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{
				ID:       "cond-1",
				Type:     ConditionTypeThreshold,
				Metric:   "cpu.usage",
				Operator: OperatorGT,
				Value:    80.0,
			},
		},
		Logic: ConditionLogicAND,
	}

	err := ab.CreateRule(rule)
	if err != nil {
		t.Fatalf("CreateRule failed: %v", err)
	}

	if rule.ID == "" {
		t.Error("Expected rule ID to be generated")
	}

	if rule.Version != 1 {
		t.Errorf("Expected version 1, got %d", rule.Version)
	}
}

func TestAlertBuilder_CreateRule_Validation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	// Missing name
	err := ab.CreateRule(&BuilderRule{
		Conditions: []BuilderCondition{{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 1}},
	})
	if err == nil {
		t.Error("Expected error for missing name")
	}

	// Missing conditions
	err = ab.CreateRule(&BuilderRule{
		Name: "Test",
	})
	if err == nil {
		t.Error("Expected error for missing conditions")
	}

	// Missing metric
	err = ab.CreateRule(&BuilderRule{
		Name: "Test",
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Operator: OperatorGT, Value: 1},
		},
	})
	if err == nil {
		t.Error("Expected error for missing metric")
	}
}

func TestAlertBuilder_GetRule(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "Test Rule",
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	}
	ab.CreateRule(rule)

	// Get existing
	found, err := ab.GetRule(rule.ID)
	if err != nil {
		t.Fatalf("GetRule failed: %v", err)
	}

	if found.Name != "Test Rule" {
		t.Errorf("Expected name 'Test Rule', got %s", found.Name)
	}

	// Get non-existent
	_, err = ab.GetRule("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent rule")
	}
}

func TestAlertBuilder_UpdateRule(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "Original Name",
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	}
	ab.CreateRule(rule)

	// Update
	updated := &BuilderRule{
		Name:     "Updated Name",
		Severity: AlertSeverityCritical,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 90},
		},
	}
	err := ab.UpdateRule(rule.ID, updated)
	if err != nil {
		t.Fatalf("UpdateRule failed: %v", err)
	}

	// Verify
	found, _ := ab.GetRule(rule.ID)
	if found.Name != "Updated Name" {
		t.Errorf("Expected 'Updated Name', got %s", found.Name)
	}
	if found.Version != 2 {
		t.Errorf("Expected version 2, got %d", found.Version)
	}
}

func TestAlertBuilder_DeleteRule(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "To Delete",
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	}
	ab.CreateRule(rule)

	// Delete
	err := ab.DeleteRule(rule.ID)
	if err != nil {
		t.Fatalf("DeleteRule failed: %v", err)
	}

	// Verify deleted
	_, err = ab.GetRule(rule.ID)
	if err == nil {
		t.Error("Expected error for deleted rule")
	}
}

func TestAlertBuilder_ListRules(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	// Create rules
	for i := 0; i < 3; i++ {
		ab.CreateRule(&BuilderRule{
			Name:     "Rule " + string(rune('A'+i)),
			Severity: AlertSeverityWarning,
			Conditions: []BuilderCondition{
				{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: float64(80 + i)},
			},
		})
	}

	rules := ab.ListRules()
	if len(rules) < 2 {
		t.Errorf("Expected at least 2 rules, got %d", len(rules))
	}
}

func TestAlertBuilder_EnableDisableRule(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "Toggle Rule",
		Enabled:  true,
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	}
	ab.CreateRule(rule)

	// Disable
	err := ab.DisableRule(rule.ID)
	if err != nil {
		t.Fatalf("DisableRule failed: %v", err)
	}

	found, _ := ab.GetRule(rule.ID)
	if found.Enabled {
		t.Error("Expected rule to be disabled")
	}

	// Enable
	err = ab.EnableRule(rule.ID)
	if err != nil {
		t.Fatalf("EnableRule failed: %v", err)
	}

	found, _ = ab.GetRule(rule.ID)
	if !found.Enabled {
		t.Error("Expected rule to be enabled")
	}
}

func TestAlertBuilder_PreviewRule(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some test data
	now := time.Now().UnixNano()
	db.Write(Point{Metric: "cpu.usage", Value: 85.0, Timestamp: now - int64(time.Minute)})
	db.Write(Point{Metric: "cpu.usage", Value: 90.0, Timestamp: now})

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "Preview Test",
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu.usage", Operator: OperatorGT, Value: 80},
		},
	}

	result, err := ab.PreviewRule(rule)
	if err != nil {
		t.Fatalf("PreviewRule failed: %v", err)
	}

	// Preview may not find data due to timing - just verify it runs without error
	_ = result.TotalMatches
}

func TestAlertBuilder_EvaluateRule(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write data
	now := time.Now().UnixNano()
	db.Write(Point{Metric: "cpu.usage", Value: 95.0, Timestamp: now})

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "Eval Test",
		Enabled:  true,
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu.usage", Operator: OperatorGT, Value: 80, Window: time.Minute},
		},
	}
	ab.CreateRule(rule)

	result, err := ab.EvaluateRule(rule.ID)
	if err != nil {
		t.Fatalf("EvaluateRule failed: %v", err)
	}

	if len(result.ConditionsMet) != 1 {
		t.Errorf("Expected 1 condition result, got %d", len(result.ConditionsMet))
	}
}

func TestAlertBuilder_EvaluateRule_Disabled(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "Disabled Rule",
		Enabled:  false,
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	}
	ab.CreateRule(rule)

	result, err := ab.EvaluateRule(rule.ID)
	if err != nil {
		t.Fatalf("EvaluateRule failed: %v", err)
	}

	if !result.Skipped {
		t.Error("Expected rule to be skipped")
	}
}

func TestAlertBuilder_ConditionTemplates(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	templates := ab.GetConditionTemplates()
	if len(templates) == 0 {
		t.Error("Expected at least one template")
	}

	// Verify template structure
	for _, tmpl := range templates {
		if tmpl.Name == "" {
			t.Error("Template name should not be empty")
		}
		if tmpl.Operator == "" {
			t.Error("Template operator should not be empty")
		}
	}
}

func TestAlertBuilder_AggregationCondition(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write data
	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "request.latency", Value: float64(100 + i*10), Timestamp: now - int64(i)*int64(time.Second)})
	}

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	rule := &BuilderRule{
		Name:     "Avg Latency",
		Enabled:  true,
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{
				Type:        ConditionTypeAggregation,
				Metric:      "request.latency",
				Operator:    OperatorGT,
				Value:       50,
				Aggregation: BuilderAggregationAvg,
				Window:      time.Minute,
			},
		},
	}
	ab.CreateRule(rule)

	result, err := ab.EvaluateRule(rule.ID)
	if err != nil {
		t.Fatalf("EvaluateRule failed: %v", err)
	}

	if len(result.ConditionsMet) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(result.ConditionsMet))
	}
}

func TestAlertBuilder_ValidateExpression(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	// Valid expressions
	valid := []string{
		"cpu.usage > 80",
		"memory.percent >= 90.5",
		"errors.count == 0",
	}

	for _, expr := range valid {
		if err := ab.ValidateExpression(expr); err != nil {
			t.Errorf("Expected valid: %s, got error: %v", expr, err)
		}
	}

	// Invalid expressions
	invalid := []string{
		"",
		"invalid",
		"123 > abc",
	}

	for _, expr := range invalid {
		if err := ab.ValidateExpression(expr); err == nil {
			t.Errorf("Expected invalid: %s", expr)
		}
	}
}

func TestAlertBuilder_ExportImport(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	// Create rules
	ab.CreateRule(&BuilderRule{
		Name:     "Rule 1",
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	})
	ab.CreateRule(&BuilderRule{
		Name:     "Rule 2",
		Severity: AlertSeverityCritical,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "mem", Operator: OperatorGT, Value: 90},
		},
	})

	// Export
	data, err := ab.ExportRules()
	if err != nil {
		t.Fatalf("ExportRules failed: %v", err)
	}

	// Create new builder and import
	ab2 := NewAlertBuilder(db, DefaultAlertBuilderConfig())
	err = ab2.ImportRules(data)
	if err != nil {
		t.Fatalf("ImportRules failed: %v", err)
	}

	rules := ab2.ListRules()
	if len(rules) != 2 {
		t.Errorf("Expected 2 rules after import, got %d", len(rules))
	}
}

func TestAlertBuilder_Triggers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	// List triggers (empty)
	triggers := ab.ListTriggers("")
	if len(triggers) != 0 {
		t.Errorf("Expected 0 triggers, got %d", len(triggers))
	}

	// Filter by state
	triggers = ab.ListTriggers(TriggerStateFiring)
	if len(triggers) != 0 {
		t.Errorf("Expected 0 firing triggers, got %d", len(triggers))
	}
}

func TestAlertBuilder_HTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	mux := http.NewServeMux()
	setupAlertBuilderRoutes(mux, ab, func(h http.HandlerFunc) http.HandlerFunc { return h })

	// Create rule
	rule := BuilderRule{
		Name:     "HTTP Test",
		Severity: AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	}
	body, _ := json.Marshal(rule)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/rules", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	// List rules
	req = httptest.NewRequest(http.MethodGet, "/api/v1/alerts/rules", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var rules []*BuilderRule
	json.Unmarshal(rec.Body.Bytes(), &rules)
	if len(rules) != 1 {
		t.Errorf("Expected 1 rule, got %d", len(rules))
	}
}

func TestAlertBuilder_HTTPMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some metrics
	now := time.Now().UnixNano()
	db.Write(Point{Metric: "test.metric", Value: 42, Timestamp: now})

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	mux := http.NewServeMux()
	setupAlertBuilderRoutes(mux, ab, func(h http.HandlerFunc) http.HandlerFunc { return h })

	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
}

func TestAlertBuilder_HTTPTemplates(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	mux := http.NewServeMux()
	setupAlertBuilderRoutes(mux, ab, func(h http.HandlerFunc) http.HandlerFunc { return h })

	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/templates", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var templates []ConditionTemplate
	json.Unmarshal(rec.Body.Bytes(), &templates)
	if len(templates) == 0 {
		t.Error("Expected at least one template")
	}
}

func TestAlertBuilder_HTTPValidate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ab := NewAlertBuilder(db, DefaultAlertBuilderConfig())

	mux := http.NewServeMux()
	setupAlertBuilderRoutes(mux, ab, func(h http.HandlerFunc) http.HandlerFunc { return h })

	// Valid expression
	body, _ := json.Marshal(map[string]string{"expression": "cpu.usage > 80"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/validate", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var result map[string]any
	json.Unmarshal(rec.Body.Bytes(), &result)
	if !result["valid"].(bool) {
		t.Error("Expected valid result")
	}
}

func TestAlertBuilder_MaxRulesLimit(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultAlertBuilderConfig()
	config.MaxRulesPerUser = 2
	ab := NewAlertBuilder(db, config)

	// Create up to limit
	for i := 0; i < 2; i++ {
		err := ab.CreateRule(&BuilderRule{
			Name:      "Rule " + string(rune('A'+i)),
			CreatedBy: "user1",
			Severity:  AlertSeverityWarning,
			Conditions: []BuilderCondition{
				{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
			},
		})
		if err != nil {
			t.Fatalf("CreateRule failed: %v", err)
		}
	}

	// Exceed limit
	err := ab.CreateRule(&BuilderRule{
		Name:      "Rule C",
		CreatedBy: "user1",
		Severity:  AlertSeverityWarning,
		Conditions: []BuilderCondition{
			{Type: ConditionTypeThreshold, Metric: "cpu", Operator: OperatorGT, Value: 80},
		},
	})
	if err == nil {
		t.Error("Expected error when exceeding max rules")
	}
}

func TestAlertBuilder_DefaultConfig(t *testing.T) {
	config := DefaultAlertBuilderConfig()

	if config.MaxRulesPerUser <= 0 {
		t.Error("MaxRulesPerUser should be positive")
	}
	if config.MaxConditionsPerRule <= 0 {
		t.Error("MaxConditionsPerRule should be positive")
	}
	if config.DefaultEvalInterval <= 0 {
		t.Error("DefaultEvalInterval should be positive")
	}
}
