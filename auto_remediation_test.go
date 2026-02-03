package chronicle

import (
	"testing"
	"time"
)

func TestAutoRemediationConfig(t *testing.T) {
	cfg := DefaultAutoRemediationConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.MaxActionsPerHour != 20 {
		t.Errorf("expected MaxActionsPerHour 20, got %d", cfg.MaxActionsPerHour)
	}
	if cfg.CircuitBreakerThreshold != 3 {
		t.Errorf("expected CircuitBreakerThreshold 3, got %d", cfg.CircuitBreakerThreshold)
	}
	if !cfg.EnableMLRecommendations {
		t.Error("expected EnableMLRecommendations to be true")
	}
}

func TestAutoRemediationEngine_RegisterAction(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	action := &RemediationAction{
		ID:          "action1",
		Name:        "Send Alert",
		Type:        RemediationActionAlert,
		Priority:    1,
		RiskLevel:   1,
		Parameters: map[string]interface{}{
			"severity": "warning",
		},
	}

	err := engine.RegisterAction(action)
	if err != nil {
		t.Fatalf("failed to register action: %v", err)
	}

	retrieved, err := engine.GetAction("action1")
	if err != nil {
		t.Fatalf("failed to get action: %v", err)
	}
	if retrieved.Name != "Send Alert" {
		t.Errorf("expected name 'Send Alert', got '%s'", retrieved.Name)
	}
}

func TestAutoRemediationEngine_RegisterRule(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	rule := &AutoRemediationRule{
		ID:          "rule1",
		Name:        "High CPU Alert",
		AnomalyTypes: []string{"spike", "trend"},
		MetricPattern: "cpu_usage",
		Actions:     []string{"action1"},
	}

	err := engine.RegisterRule(rule)
	if err != nil {
		t.Fatalf("failed to register rule: %v", err)
	}

	retrieved, err := engine.GetRule("rule1")
	if err != nil {
		t.Fatalf("failed to get rule: %v", err)
	}
	if retrieved.Name != "High CPU Alert" {
		t.Errorf("expected name 'High CPU Alert', got '%s'", retrieved.Name)
	}
}

func TestAutoRemediationEngine_ListActions(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	_ = engine.RegisterAction(&RemediationAction{ID: "a1", Name: "Action 1", Type: RemediationActionAlert})
	_ = engine.RegisterAction(&RemediationAction{ID: "a2", Name: "Action 2", Type: RemediationActionWebhook})
	_ = engine.RegisterAction(&RemediationAction{ID: "a3", Name: "Action 3", Type: RemediationActionNotify})

	actions := engine.ListActions()
	if len(actions) != 3 {
		t.Errorf("expected 3 actions, got %d", len(actions))
	}
}

func TestAutoRemediationEngine_ListRules(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	_ = engine.RegisterRule(&AutoRemediationRule{ID: "r1", Name: "Rule 1"})
	_ = engine.RegisterRule(&AutoRemediationRule{ID: "r2", Name: "Rule 2"})

	rules := engine.ListRules()
	if len(rules) != 2 {
		t.Errorf("expected 2 rules, got %d", len(rules))
	}
}

func TestAutoRemediationEngine_EvaluateAnomaly(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	// Register action
	_ = engine.RegisterAction(&RemediationAction{
		ID:        "alert1",
		Name:      "Alert Action",
		Type:      RemediationActionAlert,
		RiskLevel: 1,
		Parameters: map[string]interface{}{
			"severity": "warning",
		},
	})

	// Register rule
	_ = engine.RegisterRule(&AutoRemediationRule{
		ID:           "rule1",
		Name:         "CPU Spike Rule",
		AnomalyTypes: []string{"spike"},
		MetricPattern: "cpu",
		Actions:      []string{"alert1"},
	})

	// Create anomaly
	anomaly := &DetectedAnomaly{
		ID:        "anomaly1",
		Type:      "spike",
		Metric:    "cpu",
		Score:     0.9,
		Value:     95.0,
		Expected:  50.0,
		Timestamp: time.Now(),
	}

	executions, err := engine.EvaluateAnomaly(anomaly)
	if err != nil {
		t.Fatalf("failed to evaluate anomaly: %v", err)
	}

	if len(executions) != 1 {
		t.Errorf("expected 1 execution, got %d", len(executions))
	}
}

func TestAutoRemediationEngine_RateLimiting(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultAutoRemediationConfig()
	config.MaxActionsPerHour = 2

	engine := NewAutoRemediationEngine(db, config)
	defer func() { _ = engine.Close() }()

	// Register action and rule
	_ = engine.RegisterAction(&RemediationAction{
		ID:        "alert1",
		Name:      "Alert",
		Type:      RemediationActionAlert,
		RiskLevel: 1,
	})
	_ = engine.RegisterRule(&AutoRemediationRule{
		ID:           "rule1",
		Name:         "Rule",
		AnomalyTypes: []string{"spike"},
		Actions:      []string{"alert1"},
	})

	anomaly := &DetectedAnomaly{
		ID:     "anomaly1",
		Type:   "spike",
		Metric: "cpu",
	}

	// First two should succeed
	_, err := engine.EvaluateAnomaly(anomaly)
	if err != nil {
		t.Errorf("first evaluation should succeed: %v", err)
	}

	_, err = engine.EvaluateAnomaly(anomaly)
	if err != nil {
		t.Errorf("second evaluation should succeed: %v", err)
	}

	// Third should be rate limited
	_, err = engine.EvaluateAnomaly(anomaly)
	if err == nil {
		t.Error("expected rate limit error")
	}
}

func TestAutoRemediationEngine_ApprovalWorkflow(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultAutoRemediationConfig()
	config.RequireApproval = true

	engine := NewAutoRemediationEngine(db, config)
	defer func() { _ = engine.Close() }()

	// Register high-risk action
	_ = engine.RegisterAction(&RemediationAction{
		ID:        "scale1",
		Name:      "Scale Down",
		Type:      RemediationActionScale,
		RiskLevel: 4, // High risk - requires approval
	})
	_ = engine.RegisterRule(&AutoRemediationRule{
		ID:           "rule1",
		Name:         "Scale Rule",
		AnomalyTypes: []string{"spike"},
		Actions:      []string{"scale1"},
	})

	anomaly := &DetectedAnomaly{
		ID:     "anomaly1",
		Type:   "spike",
		Metric: "cpu",
	}

	executions, _ := engine.EvaluateAnomaly(anomaly)
	if len(executions) == 0 {
		t.Fatal("expected execution to be created")
	}

	// Should be pending
	pending := engine.GetPendingExecutions()
	if len(pending) != 1 {
		t.Errorf("expected 1 pending execution, got %d", len(pending))
	}

	// Approve
	err := engine.ApproveExecution(executions[0].ID, "admin")
	if err != nil {
		t.Fatalf("failed to approve execution: %v", err)
	}

	// Should no longer be pending
	pending = engine.GetPendingExecutions()
	if len(pending) != 0 {
		t.Errorf("expected 0 pending executions, got %d", len(pending))
	}
}

func TestAutoRemediationEngine_RejectExecution(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultAutoRemediationConfig()
	config.RequireApproval = true

	engine := NewAutoRemediationEngine(db, config)
	defer func() { _ = engine.Close() }()

	_ = engine.RegisterAction(&RemediationAction{
		ID:        "action1",
		Name:      "Risky Action",
		Type:      RemediationActionScale,
		RiskLevel: 5,
	})
	_ = engine.RegisterRule(&AutoRemediationRule{
		ID:           "rule1",
		Name:         "Rule",
		AnomalyTypes: []string{"spike"},
		Actions:      []string{"action1"},
	})

	anomaly := &DetectedAnomaly{ID: "a1", Type: "spike", Metric: "cpu"}
	executions, _ := engine.EvaluateAnomaly(anomaly)

	err := engine.RejectExecution(executions[0].ID, "admin", "not appropriate")
	if err != nil {
		t.Fatalf("failed to reject: %v", err)
	}

	// Check it's in executions with skipped status
	allExecs := engine.GetExecutions(10)
	found := false
	for _, e := range allExecs {
		if e.ID == executions[0].ID && e.Status == ExecutionSkipped {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected rejected execution to be in history with skipped status")
	}
}

func TestAutoRemediationEngine_GetRecommendations(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	// Add historical data
	_ = engine.RegisterAction(&RemediationAction{
		ID:   "scale_up",
		Name: "Scale Up",
		Type: RemediationActionScale,
	})

	// Simulate successful historical incident
	engine.mlModel.mu.Lock()
	engine.mlModel.actionEffectiveness["scale_up"] = 0.85
	engine.mlModel.patternIndex["spike:cpu"] = []string{"scale_up"}
	engine.mlModel.historicalIncidents = append(engine.mlModel.historicalIncidents, HistoricalIncident{
		ID:          "h1",
		AnomalyType: "spike",
		MetricPattern: "cpu",
		ActionTaken: "scale_up",
		Success:     true,
		Severity:    0.8,
	})
	engine.mlModel.mu.Unlock()

	anomaly := &DetectedAnomaly{
		ID:     "a1",
		Type:   "spike",
		Metric: "cpu",
		Score:  0.8,
	}

	recs := engine.GetRecommendations(anomaly)
	if len(recs) == 0 {
		t.Error("expected at least one recommendation")
	}

	if recs[0].Confidence != 0.85 {
		t.Errorf("expected confidence 0.85, got %f", recs[0].Confidence)
	}
}

func TestAutoRemediationEngine_Stats(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	_ = engine.RegisterAction(&RemediationAction{ID: "a1", Name: "A1", Type: RemediationActionAlert})
	_ = engine.RegisterRule(&AutoRemediationRule{ID: "r1", Name: "R1"})

	stats := engine.Stats()
	if stats.TotalActions != 1 {
		t.Errorf("expected TotalActions 1, got %d", stats.TotalActions)
	}
	if stats.TotalRules != 1 {
		t.Errorf("expected TotalRules 1, got %d", stats.TotalRules)
	}
}

func TestAutoRemediationEngine_AuditLog(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewAutoRemediationEngine(db, DefaultAutoRemediationConfig())
	defer func() { _ = engine.Close() }()

	// Register action - this creates an audit entry
	_ = engine.RegisterAction(&RemediationAction{ID: "a1", Name: "A1", Type: RemediationActionAlert})

	auditLog := engine.GetAuditLog(10)
	if len(auditLog) == 0 {
		t.Error("expected audit log entries")
	}

	if auditLog[0].EventType != "action_registered" {
		t.Errorf("expected event type 'action_registered', got '%s'", auditLog[0].EventType)
	}
}

func TestRemediationAction(t *testing.T) {
	action := &RemediationAction{
		ID:          "test-action",
		Name:        "Test Alert",
		Description: "Send test alert",
		Type:        RemediationActionAlert,
		Priority:    2,
		RiskLevel:   1,
		Conditions: []RemediationCondition{
			{
				Type:     "metric_value",
				Field:    "cpu_usage",
				Operator: "gt",
				Value:    80.0,
			},
		},
		Parameters: map[string]interface{}{
			"severity": "critical",
			"channel":  "slack",
		},
		MaxExecutions:  10,
		CooldownPeriod: time.Minute,
	}

	if action.ID != "test-action" {
		t.Errorf("expected ID 'test-action', got '%s'", action.ID)
	}
	if action.Type != RemediationActionAlert {
		t.Errorf("expected type Alert, got %s", action.Type)
	}
	if len(action.Conditions) != 1 {
		t.Errorf("expected 1 condition, got %d", len(action.Conditions))
	}
}

func TestAutoRemediationRule(t *testing.T) {
	rule := &AutoRemediationRule{
		ID:           "test-rule",
		Name:         "CPU Alert Rule",
		Description:  "Alert on high CPU",
		AnomalyTypes: []string{"spike", "trend_up"},
		MetricPattern: "cpu_*",
		TagFilters:   map[string]string{"env": "production"},
		Actions:      []string{"alert1", "scale1"},
		EscalationPolicy: &EscalationPolicy{
			Levels: []EscalationLevel{
				{
					Delay:     5 * time.Minute,
					ActionIDs: []string{"notify_oncall"},
				},
			},
			MaxEscalations: 3,
		},
	}

	if rule.Name != "CPU Alert Rule" {
		t.Errorf("expected name 'CPU Alert Rule', got '%s'", rule.Name)
	}
	if len(rule.AnomalyTypes) != 2 {
		t.Errorf("expected 2 anomaly types, got %d", len(rule.AnomalyTypes))
	}
	if rule.EscalationPolicy == nil {
		t.Error("expected escalation policy")
	}
}

func TestRemediationCondition(t *testing.T) {
	condition := RemediationCondition{
		Type:     "threshold",
		Field:    "response_time",
		Operator: "gt",
		Value:    1000.0,
		Duration: 5 * time.Minute,
		Severity: "critical",
	}

	if condition.Type != "threshold" {
		t.Errorf("expected type 'threshold', got '%s'", condition.Type)
	}
	if condition.Duration != 5*time.Minute {
		t.Errorf("expected duration 5m, got %v", condition.Duration)
	}
}

func TestDetectedAnomaly(t *testing.T) {
	anomaly := &DetectedAnomaly{
		ID:        "anomaly-123",
		Type:      "spike",
		Metric:    "cpu_usage",
		Tags:      map[string]string{"host": "server1"},
		Score:     0.95,
		Value:     98.5,
		Expected:  45.0,
		Timestamp: time.Now(),
		Duration:  10 * time.Minute,
	}

	if anomaly.Score != 0.95 {
		t.Errorf("expected score 0.95, got %f", anomaly.Score)
	}
	if anomaly.Value != 98.5 {
		t.Errorf("expected value 98.5, got %f", anomaly.Value)
	}
}

func TestExecutionStatus(t *testing.T) {
	statuses := []ExecutionStatus{
		ExecutionPending,
		ExecutionApproved,
		ExecutionRunning,
		ExecutionCompleted,
		ExecutionFailed,
		ExecutionRolledBack,
		ExecutionSkipped,
	}

	expected := []string{
		"pending", "approved", "running", "completed", "failed", "rolled_back", "skipped",
	}

	for i, status := range statuses {
		if string(status) != expected[i] {
			t.Errorf("expected status '%s', got '%s'", expected[i], status)
		}
	}
}

func TestRemediationActionTypes(t *testing.T) {
	types := []RemediationActionType{
		RemediationActionWebhook,
		RemediationActionScale,
		RemediationActionAlert,
		RemediationActionThreshold,
		RemediationActionQuery,
		RemediationActionScript,
		RemediationActionNotify,
		RemediationActionRestartProbe,
		RemediationActionIsolate,
		RemediationActionRollback,
	}

	if len(types) != 10 {
		t.Errorf("expected 10 action types, got %d", len(types))
	}
}

func TestRemediationExecution(t *testing.T) {
	exec := &RemediationExecution{
		ID:        "exec-1",
		RuleID:    "rule-1",
		ActionID:  "action-1",
		AnomalyID: "anomaly-1",
		Status:    ExecutionCompleted,
		StartTime: time.Now().Add(-time.Minute),
		EndTime:   time.Now(),
		Duration:  time.Minute,
		Input:     map[string]interface{}{"key": "value"},
		Output:    map[string]interface{}{"result": "success"},
	}

	if exec.Status != ExecutionCompleted {
		t.Errorf("expected status completed, got %s", exec.Status)
	}
	if exec.Duration != time.Minute {
		t.Errorf("expected duration 1m, got %v", exec.Duration)
	}
}

func TestHistoricalIncident(t *testing.T) {
	incident := HistoricalIncident{
		ID:            "inc-1",
		AnomalyType:   "spike",
		MetricPattern: "cpu_usage",
		Severity:      0.8,
		ActionTaken:   "scale_up",
		Success:       true,
		ResolutionTime: 5 * time.Minute,
		Context:       map[string]interface{}{"env": "prod"},
		Timestamp:     time.Now(),
	}

	if !incident.Success {
		t.Error("expected success to be true")
	}
	if incident.ResolutionTime != 5*time.Minute {
		t.Errorf("expected resolution time 5m, got %v", incident.ResolutionTime)
	}
}

func TestRemediationRecommendation(t *testing.T) {
	rec := &RemediationRecommendation{
		ID:           "rec-1",
		AnomalyID:    "anomaly-1",
		ActionType:   RemediationActionScale,
		Confidence:   0.85,
		Reasoning:    "Similar past incidents resolved with scaling",
		Parameters:   map[string]interface{}{"direction": "up"},
		PredictedImpact: 0.9,
		SimilarIncidents: []string{"inc-1", "inc-2"},
		CreatedAt:    time.Now(),
	}

	if rec.Confidence != 0.85 {
		t.Errorf("expected confidence 0.85, got %f", rec.Confidence)
	}
	if len(rec.SimilarIncidents) != 2 {
		t.Errorf("expected 2 similar incidents, got %d", len(rec.SimilarIncidents))
	}
}
