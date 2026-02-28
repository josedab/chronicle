package raft

import (
	"context"
	"testing"
	"time"
)

func TestSimClock_Advance(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewSimClock(start)

	clock.Advance(5 * time.Second)
	now := clock.Now()
	expected := start.Add(5 * time.Second)
	if !now.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, now)
	}
}

func TestSimClock_After(t *testing.T) {
	clock := NewSimClock(time.Now())
	ch := clock.After(1 * time.Second)

	// Should not fire yet
	select {
	case <-ch:
		t.Fatal("timer fired too early")
	default:
	}

	clock.Advance(2 * time.Second)

	select {
	case <-ch:
		// OK
	default:
		t.Fatal("timer should have fired")
	}
}

func TestSimRand_Deterministic(t *testing.T) {
	r1 := NewSimRand(42)
	r2 := NewSimRand(42)

	for i := 0; i < 100; i++ {
		a := r1.Intn(1000)
		b := r2.Intn(1000)
		if a != b {
			t.Fatalf("iteration %d: values differ: %d != %d", i, a, b)
		}
	}
}

func TestFailureLibrary(t *testing.T) {
	lib := NewFailureLibrary()
	scenarios := lib.GetScenarios()
	if len(scenarios) < 5 {
		t.Fatalf("expected at least 5 scenarios, got %d", len(scenarios))
	}

	lib.AddScenario(FailureScenario{
		Type:     FailureByzantine,
		Duration: 10 * time.Second,
	})
	if len(lib.GetScenarios()) != len(scenarios)+1 {
		t.Fatal("scenario not added")
	}
}

func TestLinearizabilityChecker_Valid(t *testing.T) {
	checker := NewLinearizabilityChecker()
	now := time.Now()

	checker.RecordOperation(Operation{
		OpType:    "write",
		Key:       "x",
		Value:     "1",
		StartTime: now,
		EndTime:   now.Add(1 * time.Millisecond),
	})
	checker.RecordOperation(Operation{
		OpType:    "read",
		Key:       "x",
		Value:     "1",
		StartTime: now.Add(2 * time.Millisecond),
		EndTime:   now.Add(3 * time.Millisecond),
	})

	result := checker.Check()
	if !result.Linearizable {
		t.Fatalf("expected linearizable, violations: %v", result.Violations)
	}
	if result.TotalOps != 2 {
		t.Fatalf("expected 2 ops, got %d", result.TotalOps)
	}
}

func TestLinearizabilityChecker_Empty(t *testing.T) {
	checker := NewLinearizabilityChecker()
	result := checker.Check()
	if !result.Linearizable {
		t.Fatal("empty ops should be linearizable")
	}
}

func TestSimulator_Run(t *testing.T) {
	config := DefaultSimConfig()
	config.SimulatedHours = 1
	config.StepSize = 10 * time.Millisecond // fast steps for test

	sim := NewSimulator(config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := sim.Run(ctx)
	if result.SimulatedDuration != 1*time.Hour {
		t.Fatalf("expected 1h simulated, got %v", result.SimulatedDuration)
	}
	if result.TotalSteps == 0 {
		t.Fatal("expected non-zero steps")
	}
	if !result.Linearizable {
		t.Fatalf("expected linearizable, violations: %v", result.LinearizeResult.Violations)
	}
}

func TestSimulator_WithFailures(t *testing.T) {
	config := DefaultSimConfig()
	config.SimulatedHours = 1
	config.StepSize = 50 * time.Millisecond
	config.Failures = []FailureScenario{
		{
			Type:       FailureNetworkPartition,
			TargetNode: "sim-node-0",
			StartTime:  10 * time.Minute,
			Duration:   5 * time.Minute,
		},
		{
			Type:       FailureNodeCrash,
			TargetNode: "sim-node-1",
			StartTime:  20 * time.Minute,
			Duration:   2 * time.Minute,
		},
	}

	sim := NewSimulator(config)
	ctx := context.Background()
	result := sim.Run(ctx)

	if result.FailuresInjected == 0 {
		t.Fatal("expected failures to be injected")
	}
	if len(result.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestFailureType_String(t *testing.T) {
	tests := []struct {
		ft   FailureType
		want string
	}{
		{FailureNetworkPartition, "network_partition"},
		{FailureClockSkew, "clock_skew"},
		{FailureDiskFailure, "disk_failure"},
		{FailureNodeCrash, "node_crash"},
		{FailureSlowDisk, "slow_disk"},
		{FailureByzantine, "byzantine"},
	}
	for _, tt := range tests {
		if got := tt.ft.String(); got != tt.want {
			t.Errorf("FailureType(%d).String() = %q, want %q", tt.ft, got, tt.want)
		}
	}
}
