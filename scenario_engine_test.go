package chronicle

import (
	"testing"
)

func TestScenarioEngine_CreateScenario(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{
		{Metric: "cpu", Value: 50.0},
		{Metric: "cpu", Value: 60.0},
		{Metric: "mem", Value: 70.0},
	})

	engine := NewScenarioEngine(bs)

	config := ScenarioConfig{
		Name:       "scale_test",
		BaseBranch: "main",
		Transformations: []Transformation{
			{Type: "scale", Metric: "cpu", Factor: 2.0},
		},
	}

	if err := engine.CreateScenario(config); err != nil {
		t.Fatalf("CreateScenario failed: %v", err)
	}

	scenarios := engine.ListScenarios()
	if len(scenarios) != 1 {
		t.Errorf("expected 1 scenario, got %d", len(scenarios))
	}
}

func TestScenarioEngine_CompareScenario(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{
		{Metric: "cpu", Value: 50.0},
		{Metric: "cpu", Value: 60.0},
	})

	engine := NewScenarioEngine(bs)
	engine.CreateScenario(ScenarioConfig{
		Name:       "double_cpu",
		BaseBranch: "main",
		Transformations: []Transformation{
			{Type: "scale", Metric: "cpu", Factor: 2.0},
		},
	})

	result, err := engine.CompareScenario("double_cpu")
	if err != nil {
		t.Fatalf("CompareScenario failed: %v", err)
	}

	if result.ScenarioName != "double_cpu" {
		t.Errorf("expected scenario name 'double_cpu', got %q", result.ScenarioName)
	}
}

func TestScenarioEngine_DeleteScenario(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")

	engine := NewScenarioEngine(bs)
	engine.CreateScenario(ScenarioConfig{Name: "temp", BaseBranch: "main"})

	if err := engine.DeleteScenario("temp"); err != nil {
		t.Fatalf("DeleteScenario failed: %v", err)
	}

	if err := engine.DeleteScenario("nonexistent"); err == nil {
		t.Error("expected error deleting nonexistent scenario")
	}
}

func TestScenarioEngine_Validation(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	engine := NewScenarioEngine(bs)

	// Missing name
	err := engine.CreateScenario(ScenarioConfig{})
	if err == nil {
		t.Error("expected error for missing name")
	}

	// Duplicate
	bs.CreateBranch("main", "")
	engine.CreateScenario(ScenarioConfig{Name: "test", BaseBranch: "main"})
	err = engine.CreateScenario(ScenarioConfig{Name: "test", BaseBranch: "main"})
	if err == nil {
		t.Error("expected error for duplicate scenario")
	}
}

func TestScenarioEngine_ShiftTransformation(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{
		{Metric: "latency", Value: 100.0},
		{Metric: "latency", Value: 200.0},
	})

	engine := NewScenarioEngine(bs)
	engine.CreateScenario(ScenarioConfig{
		Name:       "shift_test",
		BaseBranch: "main",
		Transformations: []Transformation{
			{Type: "shift", Metric: "latency", Factor: 50.0},
		},
	})

	result, err := engine.CompareScenario("shift_test")
	if err != nil {
		t.Fatalf("CompareScenario failed: %v", err)
	}

	if len(result.Comparisons) == 0 {
		t.Error("expected at least one comparison")
	}
}

func TestScenarioEngine_CompareNonexistent(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	engine := NewScenarioEngine(bs)

	_, err := engine.CompareScenario("nonexistent")
	if err == nil {
		t.Error("expected error comparing nonexistent scenario")
	}
}
