package chronicle

import (
	"testing"
	"time"
)

func TestSLOEngine_AddSLO(t *testing.T) {
	db := setupTestDB(t)

	engine := NewSLOEngine(db, DefaultSLOEngineConfig())

	slo := SLODefinition{
		Name:    "api_availability",
		SLIType: SLIAvailability,
		Target:  0.999,
		Metric:  "http_requests_total",
		Window:  30 * 24 * time.Hour,
	}

	if err := engine.AddSLO(slo); err != nil {
		t.Fatalf("AddSLO failed: %v", err)
	}

	got, ok := engine.GetSLO("api_availability")
	if !ok {
		t.Fatal("GetSLO returned false")
	}
	if got.Target != 0.999 {
		t.Errorf("expected target 0.999, got %f", got.Target)
	}
}

func TestSLOEngine_Validation(t *testing.T) {
	db := setupTestDB(t)

	engine := NewSLOEngine(db, DefaultSLOEngineConfig())

	// Missing name
	err := engine.AddSLO(SLODefinition{Metric: "m", Target: 0.99})
	if err == nil {
		t.Error("expected error for missing name")
	}

	// Invalid target
	err = engine.AddSLO(SLODefinition{Name: "test", Metric: "m", Target: 1.5})
	if err == nil {
		t.Error("expected error for invalid target")
	}
}

func TestSLOEngine_ErrorBudget(t *testing.T) {
	db := setupTestDB(t)

	engine := NewSLOEngine(db, DefaultSLOEngineConfig())
	engine.AddSLO(SLODefinition{
		Name:    "test_slo",
		SLIType: SLIAvailability,
		Target:  0.999,
		Metric:  "requests",
		Window:  24 * time.Hour,
	})

	// Record 1000 events, 999 good, 1 bad → 99.9% = exactly at target
	for i := 0; i < 999; i++ {
		engine.RecordEvent("test_slo", true)
	}
	engine.RecordEvent("test_slo", false)

	status, ok := engine.GetStatus("test_slo")
	if !ok {
		t.Fatal("GetStatus failed")
	}

	if status.TotalEvents != 1000 {
		t.Errorf("expected 1000 total events, got %d", status.TotalEvents)
	}
	if status.GoodEvents != 999 {
		t.Errorf("expected 999 good events, got %d", status.GoodEvents)
	}
	if status.Current != 0.999 {
		t.Errorf("expected current SLI 0.999, got %f", status.Current)
	}
	// Error budget should be fully consumed
	if status.BudgetRemaining < -0.1 || status.BudgetRemaining > 0.1 {
		t.Errorf("expected ~0%% budget remaining, got %f%%", status.BudgetRemaining)
	}
}

func TestSLOEngine_ListStatuses(t *testing.T) {
	db := setupTestDB(t)

	engine := NewSLOEngine(db, DefaultSLOEngineConfig())
	engine.AddSLO(SLODefinition{Name: "slo1", SLIType: SLIAvailability, Target: 0.99, Metric: "m1"})
	engine.AddSLO(SLODefinition{Name: "slo2", SLIType: SLILatency, Target: 0.95, Metric: "m2"})

	statuses := engine.ListStatuses()
	if len(statuses) != 2 {
		t.Errorf("expected 2 statuses, got %d", len(statuses))
	}
}

func TestSLOEngine_RemoveSLO(t *testing.T) {
	db := setupTestDB(t)

	engine := NewSLOEngine(db, DefaultSLOEngineConfig())
	engine.AddSLO(SLODefinition{Name: "to_remove", Target: 0.99, Metric: "m"})

	if err := engine.RemoveSLO("to_remove"); err != nil {
		t.Fatalf("RemoveSLO failed: %v", err)
	}

	if _, ok := engine.GetSLO("to_remove"); ok {
		t.Error("SLO should have been removed")
	}
}

func TestBurnRateEngine_DefaultAlerts(t *testing.T) {
	db := setupTestDB(t)

	sloEngine := NewSLOEngine(db, DefaultSLOEngineConfig())
	sloEngine.AddSLO(SLODefinition{Name: "api", Target: 0.999, Metric: "requests", SLIType: SLIAvailability})

	burnEngine := NewBurnRateEngine(sloEngine)
	if err := burnEngine.AddDefaultAlerts("api"); err != nil {
		t.Fatalf("AddDefaultAlerts failed: %v", err)
	}

	alerts := burnEngine.GetAlerts()
	if len(alerts) != 4 {
		t.Errorf("expected 4 default alerts, got %d", len(alerts))
	}
}

func TestBurnRateEngine_Evaluate(t *testing.T) {
	db := setupTestDB(t)

	sloEngine := NewSLOEngine(db, DefaultSLOEngineConfig())
	sloEngine.AddSLO(SLODefinition{Name: "test", Target: 0.999, Metric: "m", SLIType: SLIAvailability})

	// Record high error rate: 50% → burn rate = 0.5 / 0.001 = 500x
	for i := 0; i < 50; i++ {
		sloEngine.RecordEvent("test", true)
	}
	for i := 0; i < 50; i++ {
		sloEngine.RecordEvent("test", false)
	}

	burnEngine := NewBurnRateEngine(sloEngine)
	burnEngine.AddDefaultAlerts("test")
	burnEngine.Evaluate()

	firing := burnEngine.GetFiringAlerts()
	if len(firing) != 4 {
		t.Errorf("expected all 4 alerts firing, got %d", len(firing))
	}
}

func TestBurnRateEngine_CalculateBurnRate(t *testing.T) {
	db := setupTestDB(t)

	sloEngine := NewSLOEngine(db, DefaultSLOEngineConfig())
	sloEngine.AddSLO(SLODefinition{Name: "test", Target: 0.99, Metric: "m", SLIType: SLIAvailability})

	// 95% good → 5% error → error budget 1% → burn rate = 5
	for i := 0; i < 95; i++ {
		sloEngine.RecordEvent("test", true)
	}
	for i := 0; i < 5; i++ {
		sloEngine.RecordEvent("test", false)
	}

	burnEngine := NewBurnRateEngine(sloEngine)
	rate, err := burnEngine.CalculateBurnRate("test")
	if err != nil {
		t.Fatalf("CalculateBurnRate failed: %v", err)
	}
	if rate != 5.0 {
		t.Errorf("expected burn rate 5.0, got %f", rate)
	}
}

func TestSLOEngine_CreateRecordingRules(t *testing.T) {
	db := setupTestDB(t)

	sloEngine := NewSLOEngine(db, DefaultSLOEngineConfig())
	sloEngine.AddSLO(SLODefinition{
		Name:    "api_avail",
		Target:  0.999,
		Metric:  "http_requests_total",
		SLIType: SLIAvailability,
	})

	rre := NewRecordingRulesEngine(db)
	if err := sloEngine.CreateRecordingRules(rre); err != nil {
		t.Fatalf("CreateRecordingRules failed: %v", err)
	}

	rules := rre.ListRules()
	if len(rules) == 0 {
		t.Error("expected at least one recording rule created")
	}

	found := false
	for _, r := range rules {
		if r.Labels["slo"] == "api_avail" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected recording rule with slo=api_avail label")
	}
}

func TestSLOEngine_CreateBurnRateAlerts(t *testing.T) {
	db := setupTestDB(t)

	sloEngine := NewSLOEngine(db, DefaultSLOEngineConfig())
	sloEngine.AddSLO(SLODefinition{
		Name:    "api_avail",
		Target:  0.999,
		Metric:  "http_requests_total",
		SLIType: SLIAvailability,
	})

	am := NewAlertManager(db)
	if err := sloEngine.CreateBurnRateAlerts(am); err != nil {
		t.Fatalf("CreateBurnRateAlerts failed: %v", err)
	}

	rules := am.ListRules()
	if len(rules) < 2 {
		t.Errorf("expected at least 2 alert rules (page + ticket), got %d", len(rules))
	}
}
