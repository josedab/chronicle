package chronicle

import (
	"strings"
	"testing"
)

func TestQueryPlanVizEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultQueryPlanVizConfig()
	engine := NewQueryPlanVizEngine(db, cfg)
	engine.Start()
	defer engine.Stop()

	t.Run("ExplainSimpleQuery", func(t *testing.T) {
		q := &Query{
			Metric: "cpu.usage",
			Start:  1000,
			End:    2000,
		}
		plan := engine.Explain(q)
		if plan == nil {
			t.Fatal("expected non-nil plan")
		}
		if plan.RootNode.Operation != "Scan" {
			t.Errorf("expected root operation Scan, got %s", plan.RootNode.Operation)
		}
		if plan.TotalCost == 0 {
			t.Error("expected non-zero total cost")
		}
		if plan.EstimatedRows == 0 {
			t.Error("expected non-zero estimated rows")
		}
	})

	t.Run("ExplainWithFilter", func(t *testing.T) {
		q := &Query{
			Metric: "cpu.usage",
			Tags:   map[string]string{"host": "server1"},
			Start:  1000,
			End:    2000,
		}
		plan := engine.Explain(q)
		if plan.RootNode.Operation != "Filter" {
			t.Errorf("expected root operation Filter, got %s", plan.RootNode.Operation)
		}
		if len(plan.RootNode.Children) != 1 {
			t.Errorf("expected 1 child, got %d", len(plan.RootNode.Children))
		}
		if plan.RootNode.Children[0].Operation != "Scan" {
			t.Errorf("expected child operation Scan, got %s", plan.RootNode.Children[0].Operation)
		}
	})

	t.Run("ExplainWithAggregation", func(t *testing.T) {
		q := &Query{
			Metric:      "cpu.usage",
			Start:       1000,
			End:         2000,
			Aggregation: &Aggregation{Function: AggMean},
		}
		plan := engine.Explain(q)
		if plan.RootNode.Operation != "Aggregate" {
			t.Errorf("expected root operation Aggregate, got %s", plan.RootNode.Operation)
		}
	})

	t.Run("ExplainWithLimit", func(t *testing.T) {
		q := &Query{
			Metric: "cpu.usage",
			Start:  1000,
			End:    2000,
			Limit:  10,
		}
		plan := engine.Explain(q)
		if plan.RootNode.Operation != "Limit" {
			t.Errorf("expected root operation Limit, got %s", plan.RootNode.Operation)
		}
	})

	t.Run("TextOutput", func(t *testing.T) {
		q := &Query{
			Metric: "mem.used",
			Tags:   map[string]string{"host": "a"},
			Start:  1000,
			End:    2000,
		}
		plan := engine.Explain(q)
		text := engine.ExplainToText(plan)
		if !strings.Contains(text, "Query Plan:") {
			t.Error("expected text to contain 'Query Plan:'")
		}
		if !strings.Contains(text, "Filter") {
			t.Error("expected text to contain 'Filter'")
		}
		if !strings.Contains(text, "Scan") {
			t.Error("expected text to contain 'Scan'")
		}
	})

	t.Run("DOTOutput", func(t *testing.T) {
		q := &Query{
			Metric: "disk.io",
			Start:  1000,
			End:    2000,
		}
		plan := engine.Explain(q)
		dot := engine.ExplainToDOT(plan)
		if !strings.Contains(dot, "digraph QueryPlan") {
			t.Error("expected DOT output to contain 'digraph QueryPlan'")
		}
		if !strings.Contains(dot, "Scan") {
			t.Error("expected DOT output to contain 'Scan'")
		}
	})

	t.Run("Stats", func(t *testing.T) {
		stats := engine.GetStats()
		if stats.TotalExplains == 0 {
			t.Error("expected non-zero total explains")
		}
		if stats.AvgPlanDepth == 0 {
			t.Error("expected non-zero avg plan depth")
		}
	})
}
