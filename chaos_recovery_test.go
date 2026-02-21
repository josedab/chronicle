package chronicle

import (
	"testing"
)

func TestChaosRecoveryEngine(t *testing.T) {
	db := setupTestDB(t)

	t.Run("default scenarios loaded", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		scenarios := e.ListScenarios()
		if len(scenarios) < 5 {
			t.Errorf("expected at least 5 default scenarios, got %d", len(scenarios))
		}
	})

	t.Run("run single scenario", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		result, err := e.RunScenario("clock-skew")
		if err != nil {
			t.Fatal(err)
		}
		if !result.Passed {
			t.Errorf("clock-skew scenario should pass, error: %s", result.ErrorMessage)
		}
		if result.DataIntegrity != true {
			t.Error("expected data integrity after recovery")
		}
		if result.RecoveryTime <= 0 {
			t.Error("expected positive recovery time")
		}
	})

	t.Run("run corruption scenario", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		result, err := e.RunScenario("data-corruption")
		if err != nil {
			t.Fatal(err)
		}
		if !result.Passed {
			t.Errorf("corruption scenario should recover, error: %s", result.ErrorMessage)
		}
	})

	t.Run("run io pressure scenario", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		result, err := e.RunScenario("io-pressure")
		if err != nil {
			t.Fatal(err)
		}
		if !result.Passed {
			t.Errorf("io-pressure should pass, error: %s", result.ErrorMessage)
		}
		if result.WritesBefore != 100 {
			t.Errorf("expected 100 baseline writes, got %d", result.WritesBefore)
		}
	})

	t.Run("run all scenarios", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		results := e.RunAll()
		if len(results) < 5 {
			t.Errorf("expected at least 5 results, got %d", len(results))
		}
		passed := 0
		for _, r := range results {
			if r.Passed {
				passed++
			}
		}
		if passed == 0 {
			t.Error("expected at least some scenarios to pass")
		}
	})

	t.Run("scenario not found", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		_, err := e.RunScenario("nonexistent")
		if err == nil {
			t.Error("expected error for unknown scenario")
		}
	})

	t.Run("add custom scenario", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		err := e.AddScenario(RecoveryScenario{
			ID:          "custom-test",
			Name:        "Custom Test",
			Type:        "restart",
			Description: "Custom chaos scenario",
			Severity:    "low",
		})
		if err != nil {
			t.Fatal(err)
		}
		result, _ := e.RunScenario("custom-test")
		if !result.Passed {
			t.Error("custom scenario should pass")
		}
	})

	t.Run("empty scenario ID rejected", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		if e.AddScenario(RecoveryScenario{}) == nil {
			t.Error("expected error for empty ID")
		}
	})

	t.Run("stats tracking", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		e.RunScenario("clock-skew")
		e.RunScenario("rapid-restart")
		stats := e.GetStats()
		if stats.TestsExecuted != 2 {
			t.Errorf("expected 2 tests, got %d", stats.TestsExecuted)
		}
		if stats.ScenariosRegistered < 5 {
			t.Error("expected registered scenarios")
		}
	})

	t.Run("results history", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		e.RunScenario("clock-skew")
		results := e.Results()
		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewChaosRecoveryEngine(db, DefaultChaosRecoveryConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
