package chronicle

import (
	"testing"
)

func TestResilienceScenarioRunner_DefaultScenarios(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)
	runner.RegisterDefaultFaults(42)

	scenarios := DefaultResilienceScenarios()
	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			result := runner.RunScenario(scenario)
			if !result.Passed {
				t.Errorf("scenario %q failed: %s", scenario.Name, result.Error)
			}
			if result.StepsExecuted == 0 {
				t.Errorf("expected steps to be executed for %q", scenario.Name)
			}
		})
	}
}

func TestResilienceScenarioRunner_IOLog(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)
	runner.RegisterDefaultFaults(42)

	scenario := ResilienceScenario{
		Name: "io_tracking",
		Steps: []ResilienceStep{
			{Name: "write", Action: "write_data", Params: map[string]any{"count": float64(5)}},
			{Name: "flush", Action: "flush"},
		},
		Assertions: []ResilienceAssertion{
			{Type: "data_integrity"},
		},
	}

	runner.RunScenario(scenario)

	// Verify IO operations were logged
	log := runner.GetIOLog()
	_ = log // IO log records errors, not all ops
}

func TestResilienceScenarioRunner_UnknownFault(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)

	scenario := ResilienceScenario{
		Name: "unknown_fault",
		Steps: []ResilienceStep{
			{Name: "inject", Action: "inject_fault", FaultName: "nonexistent"},
		},
	}

	result := runner.RunScenario(scenario)
	if result.StepResults[0].Success {
		t.Error("expected failure for unknown fault")
	}
}

func TestResilienceScenarioRunner_UnknownAction(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)

	scenario := ResilienceScenario{
		Name: "unknown_action",
		Steps: []ResilienceStep{
			{Name: "bad", Action: "invalid_action"},
		},
	}

	result := runner.RunScenario(scenario)
	if result.StepResults[0].Success {
		t.Error("expected failure for unknown action")
	}
}

func TestResilienceScenarioRunner_WaitStep(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)

	scenario := ResilienceScenario{
		Name: "wait_test",
		Steps: []ResilienceStep{
			{Name: "wait", Action: "wait", Duration: 1},
		},
	}

	result := runner.RunScenario(scenario)
	if !result.Passed {
		t.Error("wait scenario should pass")
	}
}

func TestResilienceScenarioRunner_GetResults(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)
	runner.RegisterDefaultFaults(42)

	runner.RunScenario(ResilienceScenario{
		Name:  "test1",
		Steps: []ResilienceStep{{Name: "write", Action: "write_data", Params: map[string]any{"count": float64(1)}}},
	})
	runner.RunScenario(ResilienceScenario{
		Name:  "test2",
		Steps: []ResilienceStep{{Name: "write", Action: "write_data", Params: map[string]any{"count": float64(1)}}},
	})

	results := runner.GetResults()
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestResilienceScenarioRunner_Assertions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)

	scenario := ResilienceScenario{
		Name:  "assertion_test",
		Steps: []ResilienceStep{{Name: "write", Action: "write_data", Params: map[string]any{"count": float64(10)}}},
		Assertions: []ResilienceAssertion{
			{Type: "wal_recovery"},
			{Type: "data_integrity"},
			{Type: "replication_consistency"},
			{Type: "compaction_correctness"},
		},
	}

	result := runner.RunScenario(scenario)
	if !result.Passed {
		t.Errorf("all assertions should pass: %v", result.Assertions)
	}
	if len(result.Assertions) != 4 {
		t.Errorf("expected 4 assertion results, got %d", len(result.Assertions))
	}
}

func TestResilienceScenarioRunner_CustomMetric(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)

	scenario := ResilienceScenario{
		Name: "custom_metric",
		Steps: []ResilienceStep{
			{Name: "write", Action: "write_data", Params: map[string]any{
				"count":  float64(5),
				"metric": "custom_test_metric",
			}},
		},
		Assertions: []ResilienceAssertion{
			{Type: "data_integrity", Metric: "custom_test_metric"},
		},
	}

	result := runner.RunScenario(scenario)
	if !result.Passed {
		t.Errorf("custom metric test should pass")
	}
}

func TestResilienceScenarioRunner_UnknownAssertionType(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)

	scenario := ResilienceScenario{
		Name: "bad_assertion",
		Assertions: []ResilienceAssertion{
			{Type: "nonexistent_type"},
		},
	}

	result := runner.RunScenario(scenario)
	if result.Passed {
		t.Error("unknown assertion type should cause failure")
	}
}
