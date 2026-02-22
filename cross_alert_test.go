package chronicle

import (
	"testing"
)

func TestCrossAlertConfig(t *testing.T) {
	cfg := DefaultCrossAlertConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.MaxRules != 100 {
		t.Errorf("expected 100 max rules, got %d", cfg.MaxRules)
	}
}

func TestCrossAlertAddAndListRules(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewCrossAlertEngine(db, DefaultCrossAlertConfig())

	t.Run("add rule", func(t *testing.T) {
		err := e.AddRule(CrossAlertRule{
			Name:       "high_cpu_and_memory",
			Expression: "cpu > 90 AND memory > 80",
			Conditions: []CrossAlertCondition{
				{Metric: "cpu", Operator: "gt", Threshold: 90},
				{Metric: "memory", Operator: "gt", Threshold: 80},
			},
			Operator: "and",
		})
		if err != nil {
			t.Fatal(err)
		}
		rules := e.ListRules()
		if len(rules) != 1 {
			t.Fatalf("expected 1 rule, got %d", len(rules))
		}
		if rules[0].Name != "high_cpu_and_memory" {
			t.Errorf("expected rule name high_cpu_and_memory, got %s", rules[0].Name)
		}
		if rules[0].State != "inactive" {
			t.Errorf("expected inactive state, got %s", rules[0].State)
		}
	})

	t.Run("remove rule", func(t *testing.T) {
		removed := e.RemoveRule("high_cpu_and_memory")
		if !removed {
			t.Error("expected rule to be removed")
		}
		rules := e.ListRules()
		if len(rules) != 0 {
			t.Errorf("expected 0 rules, got %d", len(rules))
		}
	})

	t.Run("remove nonexistent", func(t *testing.T) {
		removed := e.RemoveRule("nonexistent")
		if removed {
			t.Error("expected false for nonexistent rule")
		}
	})
}

func TestCrossAlertEvaluate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewCrossAlertEngine(db, DefaultCrossAlertConfig())

	_ = e.AddRule(CrossAlertRule{
		Name:       "high_load",
		Expression: "cpu > 90 AND memory > 80",
		Conditions: []CrossAlertCondition{
			{Metric: "cpu", Operator: "gt", Threshold: 90},
			{Metric: "memory", Operator: "gt", Threshold: 80},
		},
		Operator: "and",
	})

	_ = e.AddRule(CrossAlertRule{
		Name:       "any_issue",
		Expression: "disk > 95 OR latency > 500",
		Conditions: []CrossAlertCondition{
			{Metric: "disk", Operator: "gt", Threshold: 95},
			{Metric: "latency", Operator: "gt", Threshold: 500},
		},
		Operator: "or",
	})

	t.Run("all conditions match AND rule", func(t *testing.T) {
		fired := e.Evaluate(map[string]float64{
			"cpu":    95,
			"memory": 85,
		})
		found := false
		for _, f := range fired {
			if f.RuleName == "high_load" {
				found = true
				if f.ConditionsMatched != 2 {
					t.Errorf("expected 2 conditions matched, got %d", f.ConditionsMatched)
				}
			}
		}
		if !found {
			t.Error("expected high_load to fire")
		}
	})

	t.Run("partial match does not fire AND rule", func(t *testing.T) {
		fired := e.Evaluate(map[string]float64{
			"cpu":    95,
			"memory": 50,
		})
		for _, f := range fired {
			if f.RuleName == "high_load" {
				t.Error("expected high_load not to fire with partial match")
			}
		}
	})

	t.Run("one condition fires OR rule", func(t *testing.T) {
		fired := e.Evaluate(map[string]float64{
			"disk":    96,
			"latency": 100,
		})
		found := false
		for _, f := range fired {
			if f.RuleName == "any_issue" {
				found = true
			}
		}
		if !found {
			t.Error("expected any_issue to fire")
		}
	})

	t.Run("no conditions match", func(t *testing.T) {
		fired := e.Evaluate(map[string]float64{
			"cpu":     10,
			"memory":  20,
			"disk":    30,
			"latency": 50,
		})
		if len(fired) != 0 {
			t.Errorf("expected 0 fired alerts, got %d", len(fired))
		}
	})

	t.Run("stats updated", func(t *testing.T) {
		stats := e.GetStats()
		if stats.TotalEvaluations == 0 {
			t.Error("expected evaluations > 0")
		}
		if stats.TotalFired == 0 {
			t.Error("expected fired > 0")
		}
	})
}

func TestCrossAlertStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewCrossAlertEngine(db, DefaultCrossAlertConfig())
	e.Start()
	e.Start() // idempotent
	e.Stop()
}

func TestCrossAlertMaxRules(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultCrossAlertConfig()
	cfg.MaxRules = 2
	e := NewCrossAlertEngine(db, cfg)

	_ = e.AddRule(CrossAlertRule{Name: "r1", Conditions: []CrossAlertCondition{{Metric: "a", Operator: "gt", Threshold: 1}}})
	_ = e.AddRule(CrossAlertRule{Name: "r2", Conditions: []CrossAlertCondition{{Metric: "b", Operator: "gt", Threshold: 1}}})
	err := e.AddRule(CrossAlertRule{Name: "r3", Conditions: []CrossAlertCondition{{Metric: "c", Operator: "gt", Threshold: 1}}})
	if err == nil {
		t.Error("expected error when exceeding max rules")
	}
}
