package chronicle

import (
	"testing"
	"time"
)

func TestDiskFullFault(t *testing.T) {
	fault := &DiskFullFault{}

	if fault.IsActive() {
		t.Error("should not be active initially")
	}

	fault.Inject()
	if !fault.IsActive() {
		t.Error("should be active after inject")
	}
	if err := fault.Check(); err == nil {
		t.Error("expected error from disk full check")
	}

	fault.Remove()
	if fault.IsActive() {
		t.Error("should not be active after remove")
	}
	if err := fault.Check(); err != nil {
		t.Errorf("unexpected error after remove: %v", err)
	}
}

func TestSlowIOFault(t *testing.T) {
	fault := NewSlowIOFault(1 * time.Millisecond)

	fault.Inject()
	start := time.Now()
	fault.MaybeDelay()
	elapsed := time.Since(start)

	if elapsed < time.Millisecond {
		t.Error("expected delay from slow IO fault")
	}

	fault.Remove()
}

func TestCorruptWriteFault(t *testing.T) {
	fault := NewCorruptWriteFault(1.0, 42) // 100% corruption

	fault.Inject()
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	corrupted := fault.CorruptBytes(data)

	if string(corrupted) == string(data) {
		t.Error("data should be corrupted with 100% probability")
	}

	fault.Remove()
}

func TestOOMFault(t *testing.T) {
	fault := &OOMFault{}

	fault.Inject()
	if err := fault.CheckAllocation(2 * 1024 * 1024); err == nil {
		t.Error("expected OOM error for large allocation")
	}
	if err := fault.CheckAllocation(512); err != nil {
		t.Errorf("small allocation should succeed: %v", err)
	}

	fault.Remove()
}

func TestClockSkewFault(t *testing.T) {
	fault := NewClockSkewFault(time.Hour)

	realNow := time.Now()
	fault.Inject()
	skewed := fault.Now()

	if skewed.Before(realNow.Add(50 * time.Minute)) {
		t.Error("skewed time should be approximately 1 hour ahead")
	}

	fault.Remove()
	normal := fault.Now()
	if normal.After(realNow.Add(10 * time.Minute)) {
		t.Error("normal time should not be skewed")
	}
}

func TestRandomFaultInjector(t *testing.T) {
	faults := []ChaosFaultInjector{
		&DiskFullFault{},
		&OOMFault{},
	}

	injector := NewRandomFaultInjector(42, faults)

	fault, err := injector.InjectRandom()
	if err != nil {
		t.Fatalf("InjectRandom failed: %v", err)
	}
	if !fault.IsActive() {
		t.Error("injected fault should be active")
	}

	injector.RemoveAll()
	for _, f := range faults {
		if f.IsActive() {
			t.Errorf("fault %s should be inactive after RemoveAll", f.Name())
		}
	}
}

func TestRandomFaultInjector_Reproducibility(t *testing.T) {
	faults1 := []ChaosFaultInjector{&DiskFullFault{}, &OOMFault{}}
	faults2 := []ChaosFaultInjector{&DiskFullFault{}, &OOMFault{}}

	inj1 := NewRandomFaultInjector(42, faults1)
	inj2 := NewRandomFaultInjector(42, faults2)

	f1, _ := inj1.InjectRandom()
	inj1.RemoveAll()
	f2, _ := inj2.InjectRandom()
	inj2.RemoveAll()

	if f1.Name() != f2.Name() {
		t.Error("same seed should produce same fault sequence")
	}
}

func TestChaosScenarioRunner(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)
	runner.RegisterDefaultFaults(42)

	scenario := ResilienceScenario{
		Name: "basic_test",
		Steps: []ResilienceStep{
			{Name: "write_baseline", Action: "write_data", Params: map[string]any{"count": float64(10)}},
			{Name: "flush", Action: "flush"},
		},
		Assertions: []ResilienceAssertion{
			{Type: "data_integrity", Metric: "chaos_test_metric"},
		},
	}

	result := runner.RunScenario(scenario)

	if !result.Passed {
		t.Errorf("scenario should pass: %s", result.Error)
	}
	if result.StepsExecuted != 2 {
		t.Errorf("expected 2 steps executed, got %d", result.StepsExecuted)
	}
}

func TestChaosScenarioRunner_WithFaults(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	runner := NewResilienceScenarioRunner(db)
	runner.RegisterDefaultFaults(42)

	scenario := ResilienceScenario{
		Name: "fault_injection_test",
		Steps: []ResilienceStep{
			{Name: "write_baseline", Action: "write_data", Params: map[string]any{"count": float64(10)}},
			{Name: "inject_slow_io", Action: "inject_fault", FaultName: "slow_io"},
			{Name: "write_during_fault", Action: "write_data", Params: map[string]any{"count": float64(5)}},
			{Name: "remove_slow_io", Action: "remove_fault", FaultName: "slow_io"},
			{Name: "flush", Action: "flush"},
		},
		Assertions: []ResilienceAssertion{
			{Type: "wal_recovery"},
			{Type: "data_integrity"},
		},
	}

	result := runner.RunScenario(scenario)

	if !result.Passed {
		t.Errorf("scenario should pass: %s", result.Error)
	}
}

func TestDefaultResilienceScenarios(t *testing.T) {
	scenarios := DefaultResilienceScenarios()
	if len(scenarios) < 3 {
		t.Errorf("expected at least 3 default scenarios, got %d", len(scenarios))
	}
}
