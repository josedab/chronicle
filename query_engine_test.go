package chronicle

import (
	"context"
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

	t.Run("validate_negative_limit", func(t *testing.T) {
		qe := NewQueryEngine(db)
		q := &Query{Metric: "cpu.usage", Limit: -1}
		if err := qe.ValidateQuery(q); err == nil {
			t.Error("expected error for negative limit")
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

// writeTestData writes n points for the given metric into db and flushes.
func writeTestData(t *testing.T, db *DB, metric string, n int) {
	t.Helper()
	baseTime := time.Now().Add(-time.Duration(n) * time.Second).UnixNano()
	for i := 0; i < n; i++ {
		if err := db.Write(Point{
			Metric:    metric,
			Value:     float64(i + 1),
			Timestamp: baseTime + int64(i)*int64(time.Second),
			Tags:      map[string]string{"host": "h1"},
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestQueryEngine_ExecuteWithData(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.cpu", 50)

	qe := NewQueryEngine(db)

	t.Run("execute_returns_points", func(t *testing.T) {
		result, err := qe.Execute(&Query{
			Metric: "qe.cpu",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if len(result.Points) != 50 {
			t.Errorf("expected 50 points, got %d", len(result.Points))
		}
	})

	t.Run("execute_invalid_query", func(t *testing.T) {
		_, err := qe.Execute(&Query{})
		if err == nil {
			t.Error("expected validation error")
		}
	})
}

func TestQueryEngine_ExecuteContext(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.mem", 20)

	qe := NewQueryEngine(db)

	t.Run("execute_context_returns_data", func(t *testing.T) {
		ctx := context.Background()
		result, err := qe.ExecuteContext(ctx, &Query{
			Metric: "qe.mem",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err != nil {
			t.Fatalf("ExecuteContext: %v", err)
		}
		if len(result.Points) != 20 {
			t.Errorf("expected 20 points, got %d", len(result.Points))
		}
	})

	t.Run("execute_context_canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately
		_, err := qe.ExecuteContext(ctx, &Query{
			Metric: "qe.mem",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err == nil {
			t.Error("expected error for canceled context")
		}
	})
}

func TestQueryEngine_EstimateCostWithData(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.disk", 100)

	qe := NewQueryEngine(db)
	cost := qe.EstimateQueryCost(&Query{
		Metric: "qe.disk",
		Start:  0,
		End:    time.Now().Add(time.Hour).UnixNano(),
	})
	if cost.EstimatedPartitions == 0 {
		t.Error("expected non-zero partitions with data")
	}
}

func TestQueryEngine_ExecuteVectorized(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.vec", 30)

	qe := NewQueryEngine(db)
	ctx := context.Background()

	t.Run("no_aggregation_uses_row_path", func(t *testing.T) {
		result, path, err := qe.ExecuteVectorized(ctx, &Query{
			Metric: "qe.vec",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err != nil {
			t.Fatalf("ExecuteVectorized: %v", err)
		}
		if path != PathRowOriented {
			t.Errorf("expected row-oriented path, got %v", path)
		}
		if len(result.Points) != 30 {
			t.Errorf("expected 30 points, got %d", len(result.Points))
		}
	})

	t.Run("small_agg_uses_row_path", func(t *testing.T) {
		result, path, err := qe.ExecuteVectorized(ctx, &Query{
			Metric:      "qe.vec",
			Start:       0,
			End:         time.Now().Add(time.Hour).UnixNano(),
			Aggregation: &Aggregation{Function: AggSum, Window: time.Hour},
		})
		if err != nil {
			t.Fatalf("ExecuteVectorized: %v", err)
		}
		// Small data → row-oriented path
		if path != PathRowOriented {
			t.Errorf("expected row-oriented for small data, got %v", path)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
	})

	t.Run("invalid_query_rejected", func(t *testing.T) {
		_, _, err := qe.ExecuteVectorized(ctx, &Query{})
		if err == nil {
			t.Error("expected validation error")
		}
	})
}

func TestQueryEngine_ScanWithPredicate(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.pred", 20)

	qe := NewQueryEngine(db)
	ctx := context.Background()
	endTime := time.Now().Add(time.Hour).UnixNano()

	t.Run("nil_predicate", func(t *testing.T) {
		result, err := qe.ScanWithPredicate(ctx, &Query{
			Metric: "qe.pred", Start: 0, End: endTime,
		}, nil)
		if err != nil {
			t.Fatalf("ScanWithPredicate: %v", err)
		}
		if len(result.Points) != 20 {
			t.Errorf("expected 20 points, got %d", len(result.Points))
		}
	})

	t.Run("min_value_filter", func(t *testing.T) {
		minVal := 10.0
		result, err := qe.ScanWithPredicate(ctx, &Query{
			Metric: "qe.pred", Start: 0, End: endTime,
		}, &PredicateFilter{MinValue: &minVal})
		if err != nil {
			t.Fatalf("ScanWithPredicate: %v", err)
		}
		for _, p := range result.Points {
			if p.Value < minVal {
				t.Errorf("point value %f below min %f", p.Value, minVal)
			}
		}
	})

	t.Run("max_value_filter", func(t *testing.T) {
		maxVal := 5.0
		result, err := qe.ScanWithPredicate(ctx, &Query{
			Metric: "qe.pred", Start: 0, End: endTime,
		}, &PredicateFilter{MaxValue: &maxVal})
		if err != nil {
			t.Fatalf("ScanWithPredicate: %v", err)
		}
		for _, p := range result.Points {
			if p.Value > maxVal {
				t.Errorf("point value %f above max %f", p.Value, maxVal)
			}
		}
	})

	t.Run("tag_match_predicate", func(t *testing.T) {
		result, err := qe.ScanWithPredicate(ctx, &Query{
			Metric: "qe.pred", Start: 0, End: endTime,
		}, &PredicateFilter{TagMatch: map[string]string{"host": "h1"}})
		if err != nil {
			t.Fatalf("ScanWithPredicate: %v", err)
		}
		if len(result.Points) != 20 {
			t.Errorf("expected 20 points with host=h1, got %d", len(result.Points))
		}
	})

	t.Run("invalid_query_rejected", func(t *testing.T) {
		_, err := qe.ScanWithPredicate(ctx, &Query{}, nil)
		if err == nil {
			t.Error("expected validation error")
		}
	})
}

func TestQueryEngine_ExecuteAdaptive(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.adapt", 20)

	qe := NewQueryEngine(db)
	ctx := context.Background()
	endTime := time.Now().Add(time.Hour).UnixNano()

	t.Run("no_aggregation_uses_row", func(t *testing.T) {
		result, path, err := qe.ExecuteAdaptive(ctx, &Query{
			Metric: "qe.adapt", Start: 0, End: endTime,
		})
		if err != nil {
			t.Fatalf("ExecuteAdaptive: %v", err)
		}
		if path != PathRowOriented {
			t.Errorf("expected row path, got %v", path)
		}
		if len(result.Points) != 20 {
			t.Errorf("expected 20 points, got %d", len(result.Points))
		}
	})

	t.Run("with_aggregation", func(t *testing.T) {
		result, _, err := qe.ExecuteAdaptive(ctx, &Query{
			Metric:      "qe.adapt",
			Start:       0,
			End:         endTime,
			Aggregation: &Aggregation{Function: AggSum, Window: time.Hour},
		})
		if err != nil {
			t.Fatalf("ExecuteAdaptive: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
	})

	t.Run("invalid_query_rejected", func(t *testing.T) {
		_, _, err := qe.ExecuteAdaptive(ctx, &Query{})
		if err == nil {
			t.Error("expected validation error")
		}
	})
}

func TestQueryEngine_WithCBO(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.cbo", 10)

	cbo := NewCostBasedOptimizer(db, DefaultCostBasedOptimizerConfig())
	qe := NewQueryEngineWithCBO(db, cbo)

	if qe.cbo == nil {
		t.Fatal("expected CBO to be set")
	}

	ctx := context.Background()
	result, _, err := qe.ExecuteAdaptive(ctx, &Query{
		Metric:      "qe.cbo",
		Start:       0,
		End:         time.Now().Add(time.Hour).UnixNano(),
		Aggregation: &Aggregation{Function: AggMean, Window: time.Hour},
	})
	if err != nil {
		t.Fatalf("ExecuteAdaptive with CBO: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestQueryEngine_WithConfig(t *testing.T) {
	db := setupTestDB(t)
	writeTestData(t, db, "qe.cfg", 10)

	t.Run("ExecuteWithTimeoutConfig", func(t *testing.T) {
		qe := NewQueryEngineWithConfig(db, QueryEngineConfig{
			MaxQueryDuration: 5 * time.Second,
		})
		result, err := qe.Execute(&Query{
			Metric: "qe.cfg",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err != nil {
			t.Fatalf("Execute with timeout config: %v", err)
		}
		if len(result.Points) != 10 {
			t.Errorf("expected 10 points, got %d", len(result.Points))
		}
	})

	t.Run("ExecuteContextWithTimeoutConfig", func(t *testing.T) {
		qe := NewQueryEngineWithConfig(db, QueryEngineConfig{
			MaxQueryDuration: 5 * time.Second,
		})
		ctx := context.Background()
		result, err := qe.ExecuteContext(ctx, &Query{
			Metric: "qe.cfg",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err != nil {
			t.Fatalf("ExecuteContext with timeout config: %v", err)
		}
		if len(result.Points) != 10 {
			t.Errorf("expected 10 points, got %d", len(result.Points))
		}
	})

	t.Run("ExecuteContextRespectsExistingDeadline", func(t *testing.T) {
		qe := NewQueryEngineWithConfig(db, QueryEngineConfig{
			MaxQueryDuration: 10 * time.Second,
		})
		// Set a tighter deadline than MaxQueryDuration
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := qe.ExecuteContext(ctx, &Query{
			Metric: "qe.cfg",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err != nil {
			t.Fatalf("ExecuteContext with existing deadline: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
	})

	t.Run("NoTimeoutWhenZero", func(t *testing.T) {
		qe := NewQueryEngineWithConfig(db, QueryEngineConfig{
			MaxQueryDuration: 0, // no timeout
		})
		result, err := qe.Execute(&Query{
			Metric: "qe.cfg",
			Start:  0,
			End:    time.Now().Add(time.Hour).UnixNano(),
		})
		if err != nil {
			t.Fatalf("Execute without timeout: %v", err)
		}
		if len(result.Points) != 10 {
			t.Errorf("expected 10 points, got %d", len(result.Points))
		}
	})
}
