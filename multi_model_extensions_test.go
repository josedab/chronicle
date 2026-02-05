package chronicle

import (
	"testing"
	"time"
)

func TestSLOTracker_Register(t *testing.T) {
	tracker := NewSLOTracker()
	err := tracker.Register(SLOConfig{
		Name:         "availability",
		Target:       0.999,
		Window:       24 * time.Hour,
		BadThreshold: 500,
		Operator:     SLOOperatorGreaterThanOrEqual,
	})
	if err != nil {
		t.Fatalf("register error: %v", err)
	}
}

func TestSLOTracker_RegisterValidation(t *testing.T) {
	tracker := NewSLOTracker()
	err := tracker.Register(SLOConfig{})
	if err == nil {
		t.Error("expected error for empty name")
	}
	err = tracker.Register(SLOConfig{Name: "x", Target: 1.5})
	if err == nil {
		t.Error("expected error for invalid target")
	}
}

func TestSLOTracker_RecordAndStatus(t *testing.T) {
	tracker := NewSLOTracker()
	tracker.Register(SLOConfig{
		Name:         "latency",
		Target:       0.95,
		Window:       1 * time.Hour,
		BadThreshold: 200,
		Operator:     SLOOperatorGreaterThan,
	})

	// 90 good events (value <= 200) + 10 bad events (value > 200)
	for i := 0; i < 90; i++ {
		tracker.RecordEvent("latency", 100)
	}
	for i := 0; i < 10; i++ {
		tracker.RecordEvent("latency", 300)
	}

	status, err := tracker.Status("latency")
	if err != nil {
		t.Fatal(err)
	}

	if status.TotalEvents != 100 {
		t.Errorf("total = %d, want 100", status.TotalEvents)
	}
	if status.BadEvents != 10 {
		t.Errorf("bad = %d, want 10", status.BadEvents)
	}
	if status.Current != 0.9 {
		t.Errorf("current = %.4f, want 0.9", status.Current)
	}
	if !status.IsViolated {
		t.Error("expected violation (90% < 95% target)")
	}
}

func TestSLOTracker_AllStatuses(t *testing.T) {
	tracker := NewSLOTracker()
	tracker.Register(SLOConfig{Name: "a", Target: 0.99, Window: time.Hour})
	tracker.Register(SLOConfig{Name: "b", Target: 0.95, Window: time.Hour})

	statuses := tracker.AllStatuses()
	if len(statuses) != 2 {
		t.Errorf("statuses = %d, want 2", len(statuses))
	}
}

func TestSLOTracker_NotFound(t *testing.T) {
	tracker := NewSLOTracker()
	_, err := tracker.Status("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent SLO")
	}
}

func TestSLOOperator(t *testing.T) {
	tests := []struct {
		op       SLOOperator
		value    float64
		thresh   float64
		expected bool
	}{
		{SLOOperatorLessThan, 5, 10, true},
		{SLOOperatorLessThan, 10, 5, false},
		{SLOOperatorGreaterThan, 10, 5, true},
		{SLOOperatorGreaterThan, 5, 10, false},
		{SLOOperatorLessThanOrEqual, 5, 5, true},
		{SLOOperatorGreaterThanOrEqual, 5, 5, true},
	}

	for _, tt := range tests {
		if got := tt.op.evaluate(tt.value, tt.thresh); got != tt.expected {
			t.Errorf("%s.evaluate(%.0f, %.0f) = %v, want %v", tt.op, tt.value, tt.thresh, got, tt.expected)
		}
	}
}

func TestCrossSignalAlertEngine_Basic(t *testing.T) {
	engine := NewCrossSignalAlertEngine()
	engine.AddAlert(CrossSignalAlert{
		Name:     "high-error-rate",
		Signals:  []SignalType{SignalMetric, SignalLog},
		Severity: AlertSeverityCritical,
	})

	if engine.AlertCount() != 1 {
		t.Errorf("alert count = %d, want 1", engine.AlertCount())
	}

	var fired bool
	engine.OnFiring(func(f AlertFiring) {
		fired = true
	})

	engine.Fire("high-error-rate", "error rate above 5%")

	if !fired {
		t.Error("expected callback to fire")
	}

	firings := engine.Firings()
	if len(firings) != 1 {
		t.Fatalf("firings = %d, want 1", len(firings))
	}
	if firings[0].Alert != "high-error-rate" {
		t.Errorf("alert = %q, want high-error-rate", firings[0].Alert)
	}
	if firings[0].Severity != AlertSeverityCritical {
		t.Error("expected critical severity")
	}
}

func TestCrossSignalAlertEngine_NoAlerts(t *testing.T) {
	engine := NewCrossSignalAlertEngine()
	firings := engine.Firings()
	if len(firings) != 0 {
		t.Error("expected no firings")
	}
}
