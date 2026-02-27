package chaos

import (
	"testing"
	"time"
)

func TestDefaultChaosConfig(t *testing.T) {
	cfg := DefaultChaosConfig()
	if !cfg.Enabled {
		t.Error("expected Enabled")
	}
	if cfg.MaxConcurrentFaults != 5 {
		t.Errorf("expected 5, got %d", cfg.MaxConcurrentFaults)
	}
	if !cfg.SafetyChecks {
		t.Error("expected SafetyChecks")
	}
}

func TestFaultInjector_InjectAndRemove(t *testing.T) {
	fi := NewFaultInjector(DefaultChaosConfig())

	fault := &FaultConfig{
		Type:     FaultNetworkDelay,
		Duration: time.Minute,
		Target:   "test",
	}

	af, err := fi.InjectFault(fault)
	if err != nil {
		t.Fatalf("InjectFault: %v", err)
	}
	if af.ID == "" {
		t.Error("expected non-empty fault ID")
	}

	if err := fi.RemoveFault(af.ID); err != nil {
		t.Fatalf("RemoveFault: %v", err)
	}
}

func TestFaultInjector_Disabled(t *testing.T) {
	cfg := DefaultChaosConfig()
	cfg.Enabled = false
	fi := NewFaultInjector(cfg)

	_, err := fi.InjectFault(&FaultConfig{Type: FaultDiskFail, Duration: time.Second})
	if err == nil {
		t.Error("expected error when injector is disabled")
	}
}

func TestFaultInjector_MaxConcurrentFaults(t *testing.T) {
	cfg := DefaultChaosConfig()
	cfg.MaxConcurrentFaults = 1
	fi := NewFaultInjector(cfg)

	_, err := fi.InjectFault(&FaultConfig{Type: FaultDiskSlow, Duration: time.Minute})
	if err != nil {
		t.Fatalf("first inject: %v", err)
	}

	_, err = fi.InjectFault(&FaultConfig{Type: FaultNetworkDelay, Duration: time.Minute})
	if err == nil {
		t.Error("expected error when max concurrent faults reached")
	}
}

func TestDiskFullFault(t *testing.T) {
	f := &DiskFullFault{}
	if f.Name() != "disk_full" {
		t.Errorf("expected disk_full, got %s", f.Name())
	}
	if f.IsActive() {
		t.Error("expected not active initially")
	}
	if err := f.Check(); err != nil {
		t.Errorf("expected no error when inactive: %v", err)
	}

	f.Inject()
	if !f.IsActive() {
		t.Error("expected active after Inject")
	}
	if err := f.Check(); err == nil {
		t.Error("expected error when active")
	}

	f.Remove()
	if f.IsActive() {
		t.Error("expected not active after Remove")
	}
}
