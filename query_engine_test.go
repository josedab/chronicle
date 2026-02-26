package chronicle

import (
	"testing"
	"time"
)

func TestQueryEngine(t *testing.T) {
	db := setupTestDB(t)

	t.Run("new_query_engine", func(t *testing.T) {
		qe := NewQueryEngine(db)
		if qe == nil {
			t.Fatal("expected non-nil QueryEngine")
		}
	})

	t.Run("validate_nil_query", func(t *testing.T) {
		qe := NewQueryEngine(db)
		if err := qe.ValidateQuery(nil); err == nil {
			t.Error("expected error for nil query")
		}
	})

	t.Run("validate_empty_metric", func(t *testing.T) {
		qe := NewQueryEngine(db)
		if err := qe.ValidateQuery(&Query{}); err == nil {
			t.Error("expected error for empty metric")
		}
	})

	t.Run("validate_valid_query", func(t *testing.T) {
		qe := NewQueryEngine(db)
		q := &Query{Metric: "cpu.usage", Start: 1, End: 100}
		if err := qe.ValidateQuery(q); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("validate_start_after_end", func(t *testing.T) {
		qe := NewQueryEngine(db)
		q := &Query{Metric: "cpu.usage", Start: 200, End: 100}
		if err := qe.ValidateQuery(q); err == nil {
			t.Error("expected error when start > end")
		}
	})

	t.Run("estimate_cost_empty_db", func(t *testing.T) {
		qe := NewQueryEngine(db)
		q := &Query{Metric: "cpu.usage", Start: 0, End: time.Now().UnixNano()}
		cost := qe.EstimateQueryCost(q)
		if cost.EstimatedPartitions < 0 {
			t.Error("expected non-negative partitions")
		}
		if cost.EstimatedSeries < 0 {
			t.Error("expected non-negative series")
		}
	})

	t.Run("execute_on_empty_db", func(t *testing.T) {
		qe := NewQueryEngine(db)
		q := &Query{Metric: "nonexistent", Start: 1, End: time.Now().UnixNano()}
		result, err := qe.Execute(q)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if len(result.Points) != 0 {
			t.Errorf("expected 0 points, got %d", len(result.Points))
		}
	})
}
