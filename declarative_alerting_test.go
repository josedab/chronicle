package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDefaultDeclarativeAlertingConfig(t *testing.T) {
	cfg := DefaultDeclarativeAlertingConfig()
	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.MaxAlertRules != 1000 {
		t.Errorf("expected MaxAlertRules=1000, got %d", cfg.MaxAlertRules)
	}
	if cfg.ReloadInterval != 30*time.Second {
		t.Errorf("expected ReloadInterval=30s, got %v", cfg.ReloadInterval)
	}
	if cfg.ValidationStrict {
		t.Error("expected ValidationStrict to be false")
	}
	if cfg.DryRunMode {
		t.Error("expected DryRunMode to be false")
	}
}

func newTestDeclarativeEngine(t *testing.T) (*DeclarativeAlertingEngine, *DB) {
	t.Helper()
	db := setupTestDB(t)
	cfg := DefaultDeclarativeAlertingConfig()
	cfg.DryRunMode = true
	engine := NewDeclarativeAlertingEngine(db, cfg)
	return engine, db
}

func testAlertDefinition() AlertDefinition {
	return AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata: AlertMetadata{
			Name:      "high-cpu",
			Namespace: "production",
			Labels:    map[string]string{"team": "platform"},
		},
		Spec: AlertSpec{
			Metric: "cpu.usage",
			Conditions: []AlertConditionSpec{
				{Type: "threshold", Operator: "gt", Value: 90.0},
			},
			Severity: "critical",
			Tags:     map[string]string{"host": "web-1"},
		},
	}
}

func TestDeclarativeAlertingEngine_LoadDefinition(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	if err := engine.LoadDefinition(def); err != nil {
		t.Fatalf("LoadDefinition failed: %v", err)
	}

	la := engine.GetAlert("high-cpu")
	if la == nil {
		t.Fatal("expected alert to be loaded")
	}
	if la.State != "inactive" {
		t.Errorf("expected state=inactive, got %s", la.State)
	}
	if la.Definition.Metadata.Name != "high-cpu" {
		t.Errorf("expected name=high-cpu, got %s", la.Definition.Metadata.Name)
	}
}

func TestDeclarativeAlertingEngine_LoadDefinition_Validation(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	// Missing name
	def := AlertDefinition{
		Spec: AlertSpec{
			Metric:     "cpu",
			Conditions: []AlertConditionSpec{{Type: "threshold", Operator: "gt", Value: 1}},
		},
	}
	if err := engine.LoadDefinition(def); err == nil {
		t.Error("expected error for missing name")
	}

	// Missing metric
	def = AlertDefinition{
		Metadata: AlertMetadata{Name: "test"},
		Spec: AlertSpec{
			Conditions: []AlertConditionSpec{{Type: "threshold", Operator: "gt", Value: 1}},
		},
	}
	if err := engine.LoadDefinition(def); err == nil {
		t.Error("expected error for missing metric")
	}

	// No conditions
	def = AlertDefinition{
		Metadata: AlertMetadata{Name: "test"},
		Spec:     AlertSpec{Metric: "cpu"},
	}
	if err := engine.LoadDefinition(def); err == nil {
		t.Error("expected error for no conditions")
	}
}

func TestDeclarativeAlertingEngine_ValidateDefinition(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	// Valid definition
	def := testAlertDefinition()
	vr := engine.ValidateDefinition(def)
	if !vr.Valid {
		t.Errorf("expected valid, got errors: %v", vr.Errors)
	}

	// Invalid severity
	def.Spec.Severity = "bogus"
	vr = engine.ValidateDefinition(def)
	if vr.Valid {
		t.Error("expected invalid for bad severity")
	}

	// Invalid logic
	def = testAlertDefinition()
	def.Spec.Logic = "xor"
	vr = engine.ValidateDefinition(def)
	if vr.Valid {
		t.Error("expected invalid for bad logic")
	}

	// Invalid condition type
	def = testAlertDefinition()
	def.Spec.Conditions = []AlertConditionSpec{{Type: "unknown", Operator: "gt", Value: 1}}
	vr = engine.ValidateDefinition(def)
	if vr.Valid {
		t.Error("expected invalid for bad condition type")
	}

	// Invalid operator
	def = testAlertDefinition()
	def.Spec.Conditions = []AlertConditionSpec{{Type: "threshold", Operator: "xyz", Value: 1}}
	vr = engine.ValidateDefinition(def)
	if vr.Valid {
		t.Error("expected invalid for bad operator")
	}

	// Invalid window
	def = testAlertDefinition()
	def.Spec.Conditions = []AlertConditionSpec{{Type: "threshold", Operator: "gt", Value: 1, Window: "bad"}}
	vr = engine.ValidateDefinition(def)
	if vr.Valid {
		t.Error("expected invalid for bad window")
	}

	// Warnings for missing apiVersion/kind
	def = testAlertDefinition()
	def.APIVersion = ""
	def.Kind = ""
	vr = engine.ValidateDefinition(def)
	if !vr.Valid {
		t.Error("expected valid even with warnings")
	}
	if len(vr.Warnings) != 2 {
		t.Errorf("expected 2 warnings, got %d", len(vr.Warnings))
	}
}

func TestDeclarativeAlertingEngine_StrictValidation(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultDeclarativeAlertingConfig()
	cfg.ValidationStrict = true
	engine := NewDeclarativeAlertingEngine(db, cfg)

	def := testAlertDefinition()
	def.APIVersion = ""
	def.Kind = ""
	vr := engine.ValidateDefinition(def)
	if vr.Valid {
		t.Error("expected invalid in strict mode with missing apiVersion/kind")
	}
}

func TestDeclarativeAlertingEngine_LoadFromYAML(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	yamlData := []byte(`
apiVersion: v1
kind: AlertRule
metadata:
  name: memory-alert
  labels:
    team: backend
spec:
  metric: memory.usage
  conditions:
    - type: threshold
      operator: gt
      value: 80
  severity: warning
`)

	if err := engine.LoadFromYAML(yamlData); err != nil {
		t.Fatalf("LoadFromYAML failed: %v", err)
	}

	la := engine.GetAlert("memory-alert")
	if la == nil {
		t.Fatal("expected memory-alert to be loaded")
	}
	if la.Definition.Spec.Metric != "memory.usage" {
		t.Errorf("expected metric=memory.usage, got %s", la.Definition.Spec.Metric)
	}
	if la.Definition.Spec.Severity != "warning" {
		t.Errorf("expected severity=warning, got %s", la.Definition.Spec.Severity)
	}
}

func TestDeclarativeAlertingEngine_LoadFromYAML_Invalid(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	if err := engine.LoadFromYAML([]byte(`{{{`)); err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestDeclarativeAlertingEngine_UnloadAlert(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	if err := engine.UnloadAlert("high-cpu"); err != nil {
		t.Fatalf("UnloadAlert failed: %v", err)
	}
	if la := engine.GetAlert("high-cpu"); la != nil {
		t.Error("expected alert to be removed")
	}

	// Unload non-existent
	if err := engine.UnloadAlert("nope"); err == nil {
		t.Error("expected error for non-existent alert")
	}
}

func TestDeclarativeAlertingEngine_EvaluateAlert_Threshold(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	// Write data above threshold
	now := time.Now()
	db.Write(Point{Metric: "cpu.usage", Tags: map[string]string{"host": "web-1"}, Value: 95.0, Timestamp: now.UnixNano()})
	_ = db.Flush()

	firing, err := engine.EvaluateAlert("high-cpu")
	if err != nil {
		t.Fatalf("EvaluateAlert failed: %v", err)
	}
	if !firing {
		t.Error("expected alert to be firing (value 95 > 90)")
	}

	la := engine.GetAlert("high-cpu")
	if la.State != "firing" {
		t.Errorf("expected state=firing, got %s", la.State)
	}
	if la.FireCount != 1 {
		t.Errorf("expected FireCount=1, got %d", la.FireCount)
	}
}

func TestDeclarativeAlertingEngine_EvaluateAlert_BelowThreshold(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	now := time.Now()
	db.Write(Point{Metric: "cpu.usage", Tags: map[string]string{"host": "web-1"}, Value: 50.0, Timestamp: now.UnixNano()})
	_ = db.Flush()

	firing, err := engine.EvaluateAlert("high-cpu")
	if err != nil {
		t.Fatalf("EvaluateAlert failed: %v", err)
	}
	if firing {
		t.Error("expected alert NOT to be firing (value 50 < 90)")
	}
}

func TestDeclarativeAlertingEngine_EvaluateAlert_NotFound(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)
	_, err := engine.EvaluateAlert("nope")
	if err == nil {
		t.Error("expected error for non-existent alert")
	}
}

func TestDeclarativeAlertingEngine_LogicAND(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata:   AlertMetadata{Name: "and-test"},
		Spec: AlertSpec{
			Metric: "cpu.usage",
			Conditions: []AlertConditionSpec{
				{Type: "threshold", Operator: "gt", Value: 80},
				{Type: "threshold", Operator: "lt", Value: 100},
			},
			Logic:    "and",
			Severity: "warning",
		},
	}
	engine.LoadDefinition(def)

	now := time.Now()
	db.Write(Point{Metric: "cpu.usage", Value: 90, Timestamp: now.UnixNano()})
	_ = db.Flush()

	firing, _ := engine.EvaluateAlert("and-test")
	if !firing {
		t.Error("expected AND to fire (90 > 80 AND 90 < 100)")
	}
}

func TestDeclarativeAlertingEngine_LogicOR(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata:   AlertMetadata{Name: "or-test"},
		Spec: AlertSpec{
			Metric: "cpu.usage",
			Conditions: []AlertConditionSpec{
				{Type: "threshold", Operator: "gt", Value: 95},
				{Type: "threshold", Operator: "lt", Value: 10},
			},
			Logic:    "or",
			Severity: "warning",
		},
	}
	engine.LoadDefinition(def)

	now := time.Now()
	db.Write(Point{Metric: "cpu.usage", Value: 5, Timestamp: now.UnixNano()})
	_ = db.Flush()

	firing, _ := engine.EvaluateAlert("or-test")
	if !firing {
		t.Error("expected OR to fire (5 < 10)")
	}
}

func TestDeclarativeAlertingEngine_AlertLifecycle(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata:   AlertMetadata{Name: "lifecycle"},
		Spec: AlertSpec{
			Metric:     "temp",
			Conditions: []AlertConditionSpec{{Type: "threshold", Operator: "gt", Value: 50}},
			For:        "5m",
			Severity:   "warning",
		},
	}
	engine.LoadDefinition(def)

	la := engine.GetAlert("lifecycle")
	if la.State != "inactive" {
		t.Errorf("expected initial state=inactive, got %s", la.State)
	}

	// Write high value → pending (because of For duration)
	now := time.Now()
	db.Write(Point{Metric: "temp", Value: 60, Timestamp: now.UnixNano()})
	_ = db.Flush()
	engine.EvaluateAlert("lifecycle")

	la = engine.GetAlert("lifecycle")
	if la.State != "pending" {
		t.Errorf("expected state=pending, got %s", la.State)
	}

	// Evaluate again while pending → firing
	engine.EvaluateAlert("lifecycle")
	la = engine.GetAlert("lifecycle")
	if la.State != "firing" {
		t.Errorf("expected state=firing, got %s", la.State)
	}

	// Write low value → resolved
	db.Write(Point{Metric: "temp", Value: 30, Timestamp: time.Now().UnixNano()})
	_ = db.Flush()
	engine.EvaluateAlert("lifecycle")

	la = engine.GetAlert("lifecycle")
	if la.State != "resolved" {
		t.Errorf("expected state=resolved, got %s", la.State)
	}

	// Evaluate again while resolved and still low → inactive
	engine.EvaluateAlert("lifecycle")
	la = engine.GetAlert("lifecycle")
	if la.State != "inactive" {
		t.Errorf("expected state=inactive, got %s", la.State)
	}
}

func TestDeclarativeAlertingEngine_EvaluateAll(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def1 := testAlertDefinition()
	engine.LoadDefinition(def1)

	def2 := AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata:   AlertMetadata{Name: "low-disk"},
		Spec: AlertSpec{
			Metric:     "disk.free",
			Conditions: []AlertConditionSpec{{Type: "threshold", Operator: "lt", Value: 10}},
			Severity:   "critical",
		},
	}
	engine.LoadDefinition(def2)

	now := time.Now()
	db.Write(Point{Metric: "cpu.usage", Tags: map[string]string{"host": "web-1"}, Value: 95, Timestamp: now.UnixNano()})
	_ = db.Flush()
	db.Write(Point{Metric: "disk.free", Value: 5, Timestamp: now.UnixNano()})
	_ = db.Flush()

	results := engine.EvaluateAll()
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestDeclarativeAlertingEngine_RunTest(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	tc := AlertTestCase{
		Name:      "cpu-spike-test",
		AlertName: "high-cpu",
		InputPoints: []TestPoint{
			{Value: 95, OffsetSec: -10},
		},
		ExpectFiring: true,
	}

	result := engine.RunTest(tc)
	if !result.Passed {
		t.Errorf("expected test to pass, error: %s", result.Error)
	}
	if !result.Actual {
		t.Error("expected actual=true")
	}
}

func TestDeclarativeAlertingEngine_RunTest_NotFiring(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	tc := AlertTestCase{
		Name:      "cpu-normal-test",
		AlertName: "high-cpu",
		InputPoints: []TestPoint{
			{Value: 50, OffsetSec: -10},
		},
		ExpectFiring: false,
	}

	result := engine.RunTest(tc)
	if !result.Passed {
		t.Errorf("expected test to pass, error: %s", result.Error)
	}
}

func TestDeclarativeAlertingEngine_RunTest_AlertNotFound(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	tc := AlertTestCase{
		Name:      "missing-test",
		AlertName: "nope",
	}
	result := engine.RunTest(tc)
	if result.Error == "" {
		t.Error("expected error for missing alert")
	}
}

func TestDeclarativeAlertingEngine_RunTests(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	tests := []AlertTestCase{
		{Name: "test-1", AlertName: "high-cpu", InputPoints: []TestPoint{{Value: 95}}, ExpectFiring: true},
		{Name: "test-2", AlertName: "high-cpu", InputPoints: []TestPoint{{Value: 50}}, ExpectFiring: false},
	}

	results := engine.RunTests(tests)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if !r.Passed {
			t.Errorf("test %s failed: %s", r.TestName, r.Error)
		}
	}
}

func TestDeclarativeAlertingEngine_ListTestResults(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)
	engine.RunTest(AlertTestCase{Name: "t1", AlertName: "high-cpu", InputPoints: []TestPoint{{Value: 95}}, ExpectFiring: true})

	tr := engine.ListTestResults()
	if len(tr["high-cpu"]) != 1 {
		t.Errorf("expected 1 test result for high-cpu, got %d", len(tr["high-cpu"]))
	}
}

func TestDeclarativeAlertingEngine_ListAlerts(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	engine.LoadDefinition(testAlertDefinition())
	alerts := engine.ListAlerts()
	if len(alerts) != 1 {
		t.Errorf("expected 1 alert, got %d", len(alerts))
	}
}

func TestDeclarativeAlertingEngine_ExportAll(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	engine.LoadDefinition(testAlertDefinition())
	defs := engine.ExportAll()
	if len(defs) != 1 {
		t.Fatalf("expected 1 definition, got %d", len(defs))
	}
	if defs[0].Metadata.Name != "high-cpu" {
		t.Errorf("expected name=high-cpu, got %s", defs[0].Metadata.Name)
	}
}

func TestDeclarativeAlertingEngine_Stats(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	now := time.Now()
	db.Write(Point{Metric: "cpu.usage", Tags: map[string]string{"host": "web-1"}, Value: 95, Timestamp: now.UnixNano()})
	_ = db.Flush()
	engine.EvaluateAlert("high-cpu")

	stats := engine.Stats()
	if stats.TotalDefinitions != 1 {
		t.Errorf("expected TotalDefinitions=1, got %d", stats.TotalDefinitions)
	}
	if stats.TotalEvaluations != 1 {
		t.Errorf("expected TotalEvaluations=1, got %d", stats.TotalEvaluations)
	}
	if stats.TotalFirings != 1 {
		t.Errorf("expected TotalFirings=1, got %d", stats.TotalFirings)
	}
}

func TestDeclarativeAlertingEngine_MaxAlertRules(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultDeclarativeAlertingConfig()
	cfg.MaxAlertRules = 1
	engine := NewDeclarativeAlertingEngine(db, cfg)

	engine.LoadDefinition(testAlertDefinition())

	def2 := AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata:   AlertMetadata{Name: "second"},
		Spec: AlertSpec{
			Metric:     "mem",
			Conditions: []AlertConditionSpec{{Type: "threshold", Operator: "gt", Value: 1}},
			Severity:   "info",
		},
	}
	if err := engine.LoadDefinition(def2); err == nil {
		t.Error("expected error when exceeding max rules")
	}
}

func TestDeclarativeAlertingEngine_StartStop(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	engine.Start()
	// Starting twice should be safe
	engine.Start()

	time.Sleep(50 * time.Millisecond)
	engine.Stop()
	// Stopping twice should be safe
	engine.Stop()
}

func TestDeclarativeAlertingEngine_Routing(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	def.Spec.Routing = []AlertRouting{
		{Type: "log", Target: "stdout"},
	}
	// DryRunMode is true, so route won't actually fire
	engine.config.DryRunMode = false
	engine.LoadDefinition(def)

	now := time.Now()
	db.Write(Point{Metric: "cpu.usage", Tags: map[string]string{"host": "web-1"}, Value: 95, Timestamp: now.UnixNano()})
	_ = db.Flush()
	engine.EvaluateAlert("high-cpu")

	la := engine.GetAlert("high-cpu")
	if la.State != "firing" {
		t.Errorf("expected state=firing, got %s", la.State)
	}
}

func TestDeclarativeAlertingEngine_Reload(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	if err := engine.Reload(); err != nil {
		t.Fatalf("Reload failed: %v", err)
	}
	stats := engine.Stats()
	if stats.LastReload.IsZero() {
		t.Error("expected LastReload to be set")
	}
}

func TestCompareValue(t *testing.T) {
	tests := []struct {
		value    float64
		op       string
		thresh   float64
		expected bool
	}{
		{10, "gt", 5, true},
		{3, "gt", 5, false},
		{3, "lt", 5, true},
		{10, "lt", 5, false},
		{5, "eq", 5, true},
		{5, "eq", 6, false},
		{5, "ne", 6, true},
		{5, "ne", 5, false},
		{5, "gte", 5, true},
		{6, "gte", 5, true},
		{4, "gte", 5, false},
		{5, "lte", 5, true},
		{4, "lte", 5, true},
		{6, "lte", 5, false},
		{5, "unknown", 5, false},
	}
	for _, tt := range tests {
		result := compareValue(tt.value, tt.op, tt.thresh)
		if result != tt.expected {
			t.Errorf("compareValue(%v, %q, %v) = %v, want %v", tt.value, tt.op, tt.thresh, result, tt.expected)
		}
	}
}

// HTTP handler tests

func TestDeclarativeAlertingEngine_HTTPListDefinitions(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)
	engine.LoadDefinition(testAlertDefinition())

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerting/definitions", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestDeclarativeAlertingEngine_HTTPLoadDefinition(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	def := testAlertDefinition()
	body, _ := json.Marshal(def)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerting/definitions", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeclarativeAlertingEngine_HTTPDeleteDefinition(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)
	engine.LoadDefinition(testAlertDefinition())

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/alerting/definitions?name=high-cpu", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeclarativeAlertingEngine_HTTPValidate(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	def := testAlertDefinition()
	body, _ := json.Marshal(def)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerting/validate", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var result AlertValidationResult
	json.NewDecoder(w.Body).Decode(&result)
	if !result.Valid {
		t.Errorf("expected valid, got errors: %v", result.Errors)
	}
}

func TestDeclarativeAlertingEngine_HTTPEvaluate(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerting/evaluate", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestDeclarativeAlertingEngine_HTTPStats(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerting/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestDeclarativeAlertingEngine_HTTPReload(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerting/reload", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestDeclarativeAlertingEngine_HTTPTest(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)
	engine.LoadDefinition(testAlertDefinition())

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	tests := []AlertTestCase{
		{Name: "t1", AlertName: "high-cpu", InputPoints: []TestPoint{{Value: 95}}, ExpectFiring: true},
	}
	body, _ := json.Marshal(tests)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerting/test", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeclarativeAlertingEngine_HTTPMethodNotAllowed(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/alerting/validate"},
		{http.MethodGet, "/api/v1/alerting/evaluate"},
		{http.MethodGet, "/api/v1/alerting/test"},
		{http.MethodGet, "/api/v1/alerting/reload"},
		{http.MethodPost, "/api/v1/alerting/stats"},
	}

	for _, ep := range endpoints {
		req := httptest.NewRequest(ep.method, ep.path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: expected 405, got %d", ep.method, ep.path, w.Code)
		}
	}
}

func TestDeclarativeAlertingEngine_AbsentCondition(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata:   AlertMetadata{Name: "absent-test"},
		Spec: AlertSpec{
			Metric:     "ghost.metric",
			Conditions: []AlertConditionSpec{{Type: "absent", Operator: "eq", Value: 0, Window: "5m"}},
			Severity:   "warning",
		},
	}
	engine.LoadDefinition(def)

	// No data written, so absent condition should fire
	firing, err := engine.EvaluateAlert("absent-test")
	if err != nil {
		t.Fatalf("EvaluateAlert failed: %v", err)
	}
	if !firing {
		t.Error("expected absent alert to fire when no data exists")
	}
}

func TestDeclarativeAlertingEngine_RateCondition(t *testing.T) {
	engine, db := newTestDeclarativeEngine(t)

	def := AlertDefinition{
		APIVersion: "v1",
		Kind:       "AlertRule",
		Metadata:   AlertMetadata{Name: "rate-test"},
		Spec: AlertSpec{
			Metric:     "requests.rate",
			Conditions: []AlertConditionSpec{{Type: "rate", Operator: "gt", Value: 50, Window: "10m"}},
			Severity:   "warning",
		},
	}
	engine.LoadDefinition(def)

	now := time.Now()
	// Write two points with increasing values within window
	db.Write(Point{Metric: "requests.rate", Value: 100, Timestamp: now.Add(-2 * time.Second).UnixNano()})
	_ = db.Flush()
	db.Write(Point{Metric: "requests.rate", Value: 200, Timestamp: now.Add(-1 * time.Second).UnixNano()})
	_ = db.Flush()

	firing, err := engine.EvaluateAlert("rate-test")
	if err != nil {
		t.Fatalf("EvaluateAlert failed: %v", err)
	}
	// rate = 200 - 100 = 100 > 50
	if !firing {
		t.Error("expected rate alert to fire (rate=100 > 50)")
	}
}

func TestDeclarativeAlertingEngine_TestStatsTracking(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)
	engine.LoadDefinition(testAlertDefinition())

	engine.RunTest(AlertTestCase{Name: "pass", AlertName: "high-cpu", InputPoints: []TestPoint{{Value: 95}}, ExpectFiring: true})
	engine.RunTest(AlertTestCase{Name: "fail", AlertName: "high-cpu", InputPoints: []TestPoint{{Value: 95}}, ExpectFiring: false})

	stats := engine.Stats()
	if stats.TestsRun != 2 {
		t.Errorf("expected TestsRun=2, got %d", stats.TestsRun)
	}
	if stats.TestsPassed != 1 {
		t.Errorf("expected TestsPassed=1, got %d", stats.TestsPassed)
	}
	if stats.TestsFailed != 1 {
		t.Errorf("expected TestsFailed=1, got %d", stats.TestsFailed)
	}
}

func TestDeclarativeAlertingEngine_OverwriteExisting(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	engine.LoadDefinition(def)

	// Overwrite with updated severity
	def.Spec.Severity = "warning"
	if err := engine.LoadDefinition(def); err != nil {
		t.Fatalf("overwrite failed: %v", err)
	}

	la := engine.GetAlert("high-cpu")
	if la.Definition.Spec.Severity != "warning" {
		t.Errorf("expected severity=warning after overwrite, got %s", la.Definition.Spec.Severity)
	}
}

func TestDeclarativeAlertingEngine_ForDuration(t *testing.T) {
	engine, _ := newTestDeclarativeEngine(t)

	def := testAlertDefinition()
	def.Spec.For = "invalid-duration"
	vr := engine.ValidateDefinition(def)
	if vr.Valid {
		t.Error("expected invalid for bad For duration")
	}
}
