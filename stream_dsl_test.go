package chronicle

import (
	"sync"
	"testing"
)

func TestStreamRuleBuilder_InsertAlert(t *testing.T) {
	var fired bool
	rule := OnInsert().
		Named("high_cpu").
		IntoMetric("cpu.usage").
		WhereValueGT(90).
		EmitAlert("CPU above 90%").
		Build()

	sp := NewStreamProcessor()
	sp.AddRule(rule)
	sp.OnAlert(func(name, msg string, p *Point) {
		fired = true
		if name != "high_cpu" {
			t.Errorf("name = %q", name)
		}
	})

	// Should not fire
	sp.Process(StreamTriggerInsert, &Point{Metric: "cpu.usage", Value: 50})
	if fired {
		t.Error("should not fire for 50")
	}

	// Should fire
	sp.Process(StreamTriggerInsert, &Point{Metric: "cpu.usage", Value: 95})
	if !fired {
		t.Error("should fire for 95")
	}
}

func TestStreamRuleBuilder_MetricFilter(t *testing.T) {
	var count int
	rule := OnAny().
		Named("mem_rule").
		IntoMetric("memory.used").
		EmitMetric("memory.alerts").
		Build()

	sp := NewStreamProcessor()
	sp.AddRule(rule)
	sp.OnMetric(func(metric string, p *Point) {
		count++
		if metric != "memory.alerts" {
			t.Errorf("metric = %q", metric)
		}
	})

	sp.Process(StreamTriggerInsert, &Point{Metric: "cpu.usage", Value: 1})
	sp.Process(StreamTriggerInsert, &Point{Metric: "memory.used", Value: 1})

	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestStreamRuleBuilder_Callback(t *testing.T) {
	var captured float64
	rule := OnInsert().
		Named("capture").
		Do(func(p *Point) { captured = p.Value }).
		Build()

	sp := NewStreamProcessor()
	sp.AddRule(rule)
	sp.Process(StreamTriggerInsert, &Point{Value: 42})

	if captured != 42 {
		t.Errorf("captured = %f, want 42", captured)
	}
}

func TestStreamRuleBuilder_Log(t *testing.T) {
	var logMsg string
	rule := OnDelete().
		Named("log_delete").
		EmitLog("point deleted").
		Build()

	sp := NewStreamProcessor()
	sp.AddRule(rule)
	sp.OnLog(func(msg string, p *Point) { logMsg = msg })

	sp.Process(StreamTriggerInsert, &Point{Value: 1})
	if logMsg != "" {
		t.Error("should not log on insert")
	}

	sp.Process(StreamTriggerDelete, &Point{Value: 1})
	if logMsg != "point deleted" {
		t.Errorf("logMsg = %q", logMsg)
	}
}

func TestStreamRuleBuilder_TagFilter(t *testing.T) {
	var fired bool
	rule := OnInsert().
		Named("prod_only").
		WhereTagEquals("env", "prod").
		EmitAlert("prod event").
		Build()

	sp := NewStreamProcessor()
	sp.AddRule(rule)
	sp.OnAlert(func(_, _ string, _ *Point) { fired = true })

	sp.Process(StreamTriggerInsert, &Point{Tags: map[string]string{"env": "dev"}})
	if fired {
		t.Error("should not fire for dev")
	}

	sp.Process(StreamTriggerInsert, &Point{Tags: map[string]string{"env": "prod"}})
	if !fired {
		t.Error("should fire for prod")
	}
}

func TestStreamProcessor_Stats(t *testing.T) {
	sp := NewStreamProcessor()
	sp.AddRule(OnInsert().Named("r1").EmitAlert("a").Build())
	sp.AddRule(OnUpdate().Named("r2").EmitAlert("b").Build())

	sp.Process(StreamTriggerInsert, &Point{Value: 1})
	sp.Process(StreamTriggerInsert, &Point{Value: 2})

	stats := sp.Stats()
	if stats.PointsProcessed != 2 {
		t.Errorf("processed = %d, want 2", stats.PointsProcessed)
	}
	if stats.RulesFired != 2 {
		t.Errorf("fired = %d, want 2", stats.RulesFired)
	}
}

func TestStreamProcessor_RuleCount(t *testing.T) {
	sp := NewStreamProcessor()
	if sp.RuleCount() != 0 {
		t.Error("expected 0")
	}
	sp.AddRule(OnInsert().Build())
	sp.AddRule(OnUpdate().Build())
	if sp.RuleCount() != 2 {
		t.Errorf("got %d, want 2", sp.RuleCount())
	}
}

func TestStreamProcessor_Concurrent(t *testing.T) {
	sp := NewStreamProcessor()
	sp.AddRule(OnAny().Named("any").EmitLog("ok").Build())

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(v float64) {
			defer wg.Done()
			sp.Process(StreamTriggerInsert, &Point{Value: v})
		}(float64(i))
	}
	wg.Wait()

	stats := sp.Stats()
	if stats.PointsProcessed != 100 {
		t.Errorf("processed = %d, want 100", stats.PointsProcessed)
	}
}

func TestParseStreamRule_Full(t *testing.T) {
	rule, err := ParseStreamRule("ON INSERT INTO cpu.usage WHERE value > 90 EMIT ALERT CPU too high")
	if err != nil {
		t.Fatal(err)
	}
	if rule.Trigger != StreamTriggerInsert {
		t.Error("wrong trigger")
	}
	if rule.MetricFilter != "cpu.usage" {
		t.Errorf("metric = %q", rule.MetricFilter)
	}
	if rule.Action != StreamActionEmitAlert {
		t.Errorf("action = %v", rule.Action)
	}
	if rule.Message != "CPU too high" {
		t.Errorf("message = %q", rule.Message)
	}
	// Predicate should match
	if !rule.Predicate(&Point{Value: 95}) {
		t.Error("predicate should match 95")
	}
	if rule.Predicate(&Point{Value: 50}) {
		t.Error("predicate should not match 50")
	}
}

func TestParseStreamRule_MetricEmit(t *testing.T) {
	rule, err := ParseStreamRule("ON ANY EMIT METRIC derived.total")
	if err != nil {
		t.Fatal(err)
	}
	if rule.Trigger != StreamTriggerAny {
		t.Error("wrong trigger")
	}
	if rule.Action != StreamActionEmitMetric {
		t.Error("wrong action")
	}
	if rule.TargetMetric != "derived.total" {
		t.Errorf("target = %q", rule.TargetMetric)
	}
}

func TestParseStreamRule_Operators(t *testing.T) {
	tests := []struct {
		dsl   string
		value float64
		match bool
	}{
		{"ON INSERT WHERE value < 10 EMIT LOG low", 5, true},
		{"ON INSERT WHERE value < 10 EMIT LOG low", 15, false},
		{"ON INSERT WHERE value >= 100 EMIT LOG high", 100, true},
		{"ON INSERT WHERE value <= 50 EMIT LOG mid", 50, true},
		{"ON INSERT WHERE value == 42 EMIT LOG exact", 42, true},
		{"ON INSERT WHERE value = 42 EMIT LOG exact", 42, true},
	}
	for _, tt := range tests {
		rule, err := ParseStreamRule(tt.dsl)
		if err != nil {
			t.Fatalf("%s: %v", tt.dsl, err)
		}
		got := rule.Predicate(&Point{Value: tt.value})
		if got != tt.match {
			t.Errorf("%s with %f: got %v, want %v", tt.dsl, tt.value, got, tt.match)
		}
	}
}

func TestParseStreamRule_Errors(t *testing.T) {
	bad := []string{
		"",
		"ON",
		"FOO INSERT",
		"ON UNKNOWN",
		"ON INSERT INTO",
		"ON INSERT WHERE value",
		"ON INSERT WHERE value > abc",
		"ON INSERT WHERE bad > 10",
		"ON INSERT WHERE value ? 10",
		"ON INSERT EMIT",
		"ON INSERT EMIT UNKNOWN",
	}
	for _, dsl := range bad {
		_, err := ParseStreamRule(dsl)
		if err == nil {
			t.Errorf("expected error for %q", dsl)
		}
	}
}

func TestStreamTriggerOp_String(t *testing.T) {
	if StreamTriggerInsert.String() != "INSERT" {
		t.Error("wrong string")
	}
	if StreamTriggerUpdate.String() != "UPDATE" {
		t.Error("wrong string")
	}
	if StreamTriggerDelete.String() != "DELETE" {
		t.Error("wrong string")
	}
	if StreamTriggerAny.String() != "ANY" {
		t.Error("wrong string")
	}
}

func TestStreamAction_String(t *testing.T) {
	if StreamActionEmitMetric.String() != "EMIT_METRIC" {
		t.Error("wrong string")
	}
	if StreamActionEmitAlert.String() != "EMIT_ALERT" {
		t.Error("wrong string")
	}
	if StreamActionLog.String() != "LOG" {
		t.Error("wrong string")
	}
	if StreamActionCallback.String() != "CALLBACK" {
		t.Error("wrong string")
	}
}
