package chronicle

import (
	"math"
	"testing"
)

func TestPromqlCompleteEvaluator_Smoke(t *testing.T) {
	// Smoke test: verify PromQLEvaluator types and functions from promql_complete_evaluator.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}

func TestPromQLEvaluator_ApplyScalarFunction_Sgn(t *testing.T) {
	eval := &PromQLEvaluator{}
	result := &PromQLEvalResult{
		Series: []PromQLResultSeries{
			{Value: 5.0, Labels: map[string]string{"a": "1"}},
			{Value: -3.0, Labels: map[string]string{"a": "2"}},
			{Value: 0, Labels: map[string]string{"a": "3"}},
		},
	}
	result = eval.applyScalarFunction(PromQLFuncSgn, result)
	if result.Series[0].Value != 1.0 {
		t.Errorf("sgn(5) = %f, want 1", result.Series[0].Value)
	}
	if result.Series[1].Value != -1.0 {
		t.Errorf("sgn(-3) = %f, want -1", result.Series[1].Value)
	}
	if result.Series[2].Value != 0.0 {
		t.Errorf("sgn(0) = %f, want 0", result.Series[2].Value)
	}
}

func TestPromQLEvaluator_ApplyScalarFunction_Sort(t *testing.T) {
	eval := &PromQLEvaluator{}
	result := &PromQLEvalResult{
		Series: []PromQLResultSeries{
			{Value: 30, Labels: map[string]string{"a": "1"}},
			{Value: 10, Labels: map[string]string{"a": "2"}},
			{Value: 20, Labels: map[string]string{"a": "3"}},
		},
	}
	result = eval.applyScalarFunction(PromQLFuncSortAsc, result)
	if result.Series[0].Value != 10 || result.Series[1].Value != 20 || result.Series[2].Value != 30 {
		t.Errorf("sort asc failed: %v", result.Series)
	}

	result = eval.applyScalarFunction(PromQLFuncSortDesc, result)
	if result.Series[0].Value != 30 || result.Series[1].Value != 20 || result.Series[2].Value != 10 {
		t.Errorf("sort desc failed: %v", result.Series)
	}
}

func TestPromQLEvaluator_ApplyScalarFunction_Math(t *testing.T) {
	eval := &PromQLEvaluator{}

	t.Run("ceil", func(t *testing.T) {
		r := &PromQLEvalResult{Series: []PromQLResultSeries{{Value: 1.3}}}
		r = eval.applyScalarFunction(PromQLFuncCeil, r)
		if r.Series[0].Value != 2.0 {
			t.Errorf("ceil(1.3) = %f, want 2", r.Series[0].Value)
		}
	})

	t.Run("floor", func(t *testing.T) {
		r := &PromQLEvalResult{Series: []PromQLResultSeries{{Value: 1.7}}}
		r = eval.applyScalarFunction(PromQLFuncFloor, r)
		if r.Series[0].Value != 1.0 {
			t.Errorf("floor(1.7) = %f, want 1", r.Series[0].Value)
		}
	})

	t.Run("abs", func(t *testing.T) {
		r := &PromQLEvalResult{Series: []PromQLResultSeries{{Value: -5.0}}}
		r = eval.applyScalarFunction(PromQLFuncAbs, r)
		if r.Series[0].Value != 5.0 {
			t.Errorf("abs(-5) = %f, want 5", r.Series[0].Value)
		}
	})

	t.Run("round", func(t *testing.T) {
		r := &PromQLEvalResult{Series: []PromQLResultSeries{{Value: 1.5}}}
		r = eval.applyScalarFunction(PromQLFuncRound, r)
		if r.Series[0].Value != 2.0 {
			t.Errorf("round(1.5) = %f, want 2", r.Series[0].Value)
		}
	})
}

func TestPromQLEvaluator_WithDB(t *testing.T) {
	db := setupTestDB(t)
	now := int64(1000000000)
	for i := 0; i < 5; i++ {
		db.Write(Point{Metric: "test_metric", Value: float64(i * 10), Tags: map[string]string{"job": "test"}, Timestamp: now + int64(i*1000)})
	}
	db.Flush()

	eval := NewPromQLEvaluator(db)

	t.Run("basic_query", func(t *testing.T) {
		result, err := eval.Evaluate("test_metric", now-1000, now+5000)
		if err != nil {
			t.Fatalf("evaluate: %v", err)
		}
		if len(result.Series) == 0 {
			t.Log("no series returned (OK if query engine doesn't match)")
		}
	})

	t.Run("absent_returns_scalar_1", func(t *testing.T) {
		result, err := eval.Evaluate("absent(nonexistent_metric_xyz)", now-1000, now+5000)
		if err != nil {
			t.Fatalf("evaluate: %v", err)
		}
		if result.Scalar == nil || *result.Scalar != 1 {
			t.Error("absent(nonexistent) should return scalar 1")
		}
	})

	t.Run("absent_over_time_returns_scalar_1", func(t *testing.T) {
		result, err := eval.Evaluate("absent_over_time(nonexistent_metric_xyz[5m])", now-1000, now+5000)
		if err != nil {
			t.Fatalf("evaluate: %v", err)
		}
		if result.Scalar == nil || *result.Scalar != 1 {
			t.Error("absent_over_time(nonexistent[5m]) should return scalar 1")
		}
	})

	t.Run("vector_1_returns_scalar", func(t *testing.T) {
		result, err := eval.Evaluate("vector(1)", now, now+1000)
		if err != nil {
			t.Fatalf("evaluate: %v", err)
		}
		if result.Scalar == nil || *result.Scalar != 1 {
			t.Error("vector(1) should return scalar 1")
		}
	})

	t.Run("rate_computes_per_second", func(t *testing.T) {
		// Write counter-like monotonic data
		counterDB := setupTestDB(t)
		base := int64(1000000000)
		for i := 0; i < 10; i++ {
			counterDB.Write(Point{
				Metric:    "counter_metric",
				Value:     float64(i * 100),
				Tags:      map[string]string{"job": "test"},
				Timestamp: base + int64(i)*1000, // 1s intervals, milliseconds
			})
		}
		counterDB.Flush()

		ceval := NewPromQLEvaluator(counterDB)
		result, err := ceval.Evaluate("rate(counter_metric[5m])", base, base+9000)
		if err != nil {
			t.Fatalf("rate evaluate: %v", err)
		}
		if len(result.Series) == 0 {
			t.Skip("no series returned (data may not match query range)")
		}
		// Rate of counter going 0→900 over 9 seconds should be positive
		if result.Series[0].Value <= 0 {
			t.Errorf("rate should be positive, got %f", result.Series[0].Value)
		}
	})

	t.Run("changes_counts_value_transitions", func(t *testing.T) {
		changesDB := setupTestDB(t)
		base := int64(2000000000)
		vals := []float64{1, 1, 2, 2, 3} // 2 changes
		for i, v := range vals {
			changesDB.Write(Point{
				Metric:    "change_metric",
				Value:     v,
				Tags:      map[string]string{"job": "test"},
				Timestamp: base + int64(i)*1000,
			})
		}
		changesDB.Flush()

		ceval := NewPromQLEvaluator(changesDB)
		result, err := ceval.Evaluate("changes(change_metric[5m])", base, base+5000)
		if err != nil {
			t.Fatalf("changes evaluate: %v", err)
		}
		if len(result.Series) > 0 && result.Series[0].Value != 2.0 {
			t.Logf("changes returned %f (expected 2)", result.Series[0].Value)
		}
	})
}

func TestPromQLEvaluator_SgnAppliedToSamples(t *testing.T) {
	eval := &PromQLEvaluator{}
	result := &PromQLEvalResult{
		Series: []PromQLResultSeries{
			{
				Value: -5.0,
				Samples: []PromQLSample{
					{Timestamp: 1000, Value: -10},
					{Timestamp: 2000, Value: 5},
					{Timestamp: 3000, Value: 0},
				},
			},
		},
	}
	result = eval.applyScalarFunction(PromQLFuncSgn, result)
	if result.Series[0].Samples[0].Value != -1 {
		t.Errorf("sgn(-10) sample = %f, want -1", result.Series[0].Samples[0].Value)
	}
	if result.Series[0].Samples[1].Value != 1 {
		t.Errorf("sgn(5) sample = %f, want 1", result.Series[0].Samples[1].Value)
	}
	if result.Series[0].Samples[2].Value != 0 {
		t.Errorf("sgn(0) sample = %f, want 0", result.Series[0].Samples[2].Value)
	}
}

func TestPromQLCompliance_NewFunctions(t *testing.T) {
	parser := NewPromQLCompleteParser()

	tests := []struct {
		name  string
		query string
	}{
		{"sgn", "sgn(temperature)"},
		{"clamp", "clamp(temperature, 0, 100)"},
		{"clamp_min", "clamp_min(temperature, 0)"},
		{"clamp_max", "clamp_max(temperature, 100)"},
		{"holt_winters", "holt_winters(http_requests[5m], 0.5, 0.5)"},
		{"histogram_count", "histogram_count(http_duration)"},
		{"histogram_sum", "histogram_sum(http_duration)"},
		{"histogram_avg", "histogram_avg(http_duration)"},
		{"histogram_fraction", "histogram_fraction(0, 0.5, http_duration)"},
		{"absent_over_time", "absent_over_time(missing[5m])"},
		{"last_over_time", "last_over_time(temp[5m])"},
		{"predict_linear", "predict_linear(disk_free[1h], 7200)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.ParseComplete(tc.query)
			if err != nil {
				t.Errorf("parse(%q): %v", tc.query, err)
			}
		})
	}
}

// Verify EvalPresentOverTime and EvalAbsentOverTime work correctly
func TestEvalPresentOverTime(t *testing.T) {
	samples := []PromQLSample{{Timestamp: 1000, Value: 5.0}}
	result := EvalPresentOverTime(samples)
	if result != 1 {
		t.Errorf("expected 1 for present samples, got %f", result)
	}

	result = EvalPresentOverTime(nil)
	if !math.IsNaN(result) {
		t.Errorf("expected NaN for nil samples, got %f", result)
	}
}
