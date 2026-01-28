package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestRecordingRulesEngine_AddRule(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	rule := RecordingRule{
		Name:         "test_rule",
		Query:        "SELECT mean(value) FROM cpu",
		TargetMetric: "cpu_mean",
		Interval:     time.Minute,
		Enabled:      true,
	}

	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	// Retrieve rule
	retrieved, ok := engine.GetRule("test_rule")
	if !ok {
		t.Fatal("rule not found")
	}

	if retrieved.Name != rule.Name {
		t.Errorf("name mismatch: got %s, want %s", retrieved.Name, rule.Name)
	}
	if retrieved.Query != rule.Query {
		t.Errorf("query mismatch")
	}
}

func TestRecordingRulesEngine_AddRule_Validation(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	tests := []struct {
		name    string
		rule    RecordingRule
		wantErr bool
	}{
		{
			name:    "missing name",
			rule:    RecordingRule{Query: "SELECT * FROM cpu", TargetMetric: "target"},
			wantErr: true,
		},
		{
			name:    "missing query",
			rule:    RecordingRule{Name: "test", TargetMetric: "target"},
			wantErr: true,
		},
		{
			name:    "missing target",
			rule:    RecordingRule{Name: "test", Query: "SELECT * FROM cpu"},
			wantErr: true,
		},
		{
			name:    "invalid query",
			rule:    RecordingRule{Name: "test", Query: "INVALID QUERY", TargetMetric: "target"},
			wantErr: true,
		},
		{
			name:    "valid rule",
			rule:    RecordingRule{Name: "test", Query: "SELECT mean(value) FROM cpu", TargetMetric: "target"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := engine.AddRule(tt.rule)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddRule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRecordingRulesEngine_RemoveRule(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	engine.AddRule(RecordingRule{
		Name:         "test_rule",
		Query:        "SELECT mean(value) FROM cpu",
		TargetMetric: "target",
	})

	engine.RemoveRule("test_rule")

	_, ok := engine.GetRule("test_rule")
	if ok {
		t.Error("rule should have been removed")
	}
}

func TestRecordingRulesEngine_ListRules(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	for i := 0; i < 3; i++ {
		engine.AddRule(RecordingRule{
			Name:         "rule_" + string(rune('a'+i)),
			Query:        "SELECT mean(value) FROM cpu",
			TargetMetric: "target_" + string(rune('a'+i)),
		})
	}

	rules := engine.ListRules()
	if len(rules) != 3 {
		t.Errorf("expected 3 rules, got %d", len(rules))
	}
}

func TestRecordingRulesEngine_EnableDisable(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	engine.AddRule(RecordingRule{
		Name:         "test_rule",
		Query:        "SELECT mean(value) FROM cpu",
		TargetMetric: "target",
		Enabled:      true,
	})

	// Disable
	if err := engine.Disable("test_rule"); err != nil {
		t.Fatalf("Disable failed: %v", err)
	}

	rule, _ := engine.GetRule("test_rule")
	if rule.Enabled {
		t.Error("rule should be disabled")
	}

	// Enable
	if err := engine.Enable("test_rule"); err != nil {
		t.Fatalf("Enable failed: %v", err)
	}

	rule, _ = engine.GetRule("test_rule")
	if !rule.Enabled {
		t.Error("rule should be enabled")
	}
}

func TestRecordingRulesEngine_EnableDisable_NotFound(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	if err := engine.Enable("nonexistent"); err == nil {
		t.Error("expected error for nonexistent rule")
	}

	if err := engine.Disable("nonexistent"); err == nil {
		t.Error("expected error for nonexistent rule")
	}
}

func TestRecordingRulesEngine_EvaluateNow_NotFound(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	if err := engine.EvaluateNow(context.Background(), "nonexistent"); err == nil {
		t.Error("expected error for nonexistent rule")
	}
}

func TestRecordingRulesEngine_Stats(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	engine.AddRule(RecordingRule{
		Name:         "test_rule",
		Query:        "SELECT mean(value) FROM cpu",
		TargetMetric: "target",
		Enabled:      true,
	})

	stats := engine.Stats()
	if len(stats) != 1 {
		t.Errorf("expected 1 stat, got %d", len(stats))
	}

	if stats[0].Name != "test_rule" {
		t.Error("wrong rule name in stats")
	}
}

func TestRecordingRulesEngine_StartStop(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine.Start(ctx)

	// Should not panic when called multiple times
	engine.Start(ctx)

	// Stop
	engine.Stop()
}

func TestDefaultRecordingRulesConfig(t *testing.T) {
	config := DefaultRecordingRulesConfig()

	if !config.Enabled {
		t.Error("default should be enabled")
	}
	if config.DefaultInterval != time.Minute {
		t.Error("default interval should be 1 minute")
	}
	if config.ConcurrencyLimit != 4 {
		t.Error("default concurrency should be 4")
	}
}

func TestRecordingRule_DefaultInterval(t *testing.T) {
	engine := NewRecordingRulesEngine(nil)

	rule := RecordingRule{
		Name:         "test",
		Query:        "SELECT mean(value) FROM cpu",
		TargetMetric: "target",
		Interval:     0, // Should default to 1 minute
	}

	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	retrieved, _ := engine.GetRule("test")
	if retrieved.Interval != time.Minute {
		t.Errorf("interval should default to 1 minute, got %v", retrieved.Interval)
	}
}
