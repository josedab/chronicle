package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestPromQLCompleteParser(t *testing.T) {
	parser := NewPromQLCompleteParser()

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"simple metric", "http_requests_total", false},
		{"metric with labels", `http_requests_total{method="GET"}`, false},
		{"absent", "absent(up)", false},
		{"histogram_quantile", `histogram_quantile(0.95, http_request_duration_bucket)`, false},
		{"label_replace", `label_replace(up, "host", "$1", "instance", "(.*):.*")`, false},
		{"label_join", `label_join(up, "combined", "-", "job", "instance")`, false},
		{"vector", "vector(1)", false},
		{"scalar", "scalar(up)", false},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.ParseComplete(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseComplete(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
			}
		})
	}
}

func TestPromQLHistogramQuantile(t *testing.T) {
	buckets := []PromQLHistogramBucket{
		{UpperBound: 0.005, Count: 10},
		{UpperBound: 0.01, Count: 20},
		{UpperBound: 0.025, Count: 50},
		{UpperBound: 0.05, Count: 80},
		{UpperBound: 0.1, Count: 90},
		{UpperBound: 0.5, Count: 95},
		{UpperBound: 1.0, Count: 98},
		{UpperBound: 5.0, Count: 100},
	}

	q50 := PromQLHistogramQuantile(0.5, buckets)
	if q50 <= 0 || math.IsNaN(q50) {
		t.Errorf("p50 should be positive, got %f", q50)
	}

	q95 := PromQLHistogramQuantile(0.95, buckets)
	if q95 <= q50 {
		t.Errorf("p95 (%f) should be > p50 (%f)", q95, q50)
	}

	// Edge cases
	if !math.IsNaN(PromQLHistogramQuantile(0.5, nil)) {
		t.Error("expected NaN for empty buckets")
	}
	if !math.IsNaN(PromQLHistogramQuantile(1.5, buckets)) {
		t.Error("expected NaN for q > 1")
	}
}

func TestPromQLLabelReplace(t *testing.T) {
	labels := map[string]string{
		"instance": "localhost:9090",
		"job":      "prometheus",
	}

	result, err := PromQLLabelReplace(labels, "host", "$1", "instance", "(.*):(.*)")
	if err != nil {
		t.Fatalf("label_replace failed: %v", err)
	}
	if result["host"] == "" {
		t.Error("expected host label to be set")
	}
}

func TestPromQLLabelJoin(t *testing.T) {
	labels := map[string]string{
		"job":      "api",
		"instance": "server-01",
	}

	result := PromQLLabelJoin(labels, "combined", "-", "job", "instance")
	if result["combined"] != "api-server-01" {
		t.Errorf("expected 'api-server-01', got %q", result["combined"])
	}
}

func TestPromQLMathApply(t *testing.T) {
	values := []float64{-3.7, 2.1, 0, -1.5, 4.9}

	abs := PromQLMathApply("abs", values)
	if abs[0] != 3.7 {
		t.Errorf("abs(-3.7) = %f, want 3.7", abs[0])
	}

	ceil := PromQLMathApply("ceil", values)
	if ceil[1] != 3 {
		t.Errorf("ceil(2.1) = %f, want 3", ceil[1])
	}

	floor := PromQLMathApply("floor", values)
	if floor[1] != 2 {
		t.Errorf("floor(2.1) = %f, want 2", floor[1])
	}
}

func TestPromQLComplianceSuite(t *testing.T) {
	suite := NewPromQLComplianceSuite()
	results := suite.RunAll()

	if len(results) == 0 {
		t.Fatal("expected compliance test results")
	}

	passRate := suite.PassRate()
	if passRate < 0.8 {
		t.Errorf("expected pass rate >= 80%%, got %.0f%%", passRate*100)
		for _, r := range results {
			if !r.Passed {
				t.Logf("FAILED: %s (%s): %s", r.Name, r.Query, r.Error)
			}
		}
	}

	summary := suite.Summary()
	if summary["total_tests"].(int) != len(results) {
		t.Errorf("summary total mismatch")
	}
}

func TestSplitPromQLArgs(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{`0.95, rate(http_requests[5m])`, 2},
		{`up, "host", "$1", "instance", "(.*):.*"`, 5},
		{`up, "combined", "-", "job", "instance"`, 5},
		{`simple`, 1},
	}
	for _, tt := range tests {
		parts := splitPromQLArgs(tt.input)
		if len(parts) != tt.want {
			t.Errorf("splitPromQLArgs(%q) = %d parts, want %d", tt.input, len(parts), tt.want)
		}
	}
}

func TestPromQLBinaryExprParsing(t *testing.T) {
	parser := NewPromQLCompleteParser()
	tests := []struct {
		expr string
		op   PromQLBinaryOp
	}{
		{"cpu_usage + memory_usage", PromQLBinAdd},
		{"requests_total - errors_total", PromQLBinSub},
		{"rate_a * rate_b", PromQLBinMul},
		{"bytes_total / ops_total", PromQLBinDiv},
		{"latency > threshold", PromQLBinGreaterThan},
		{"metric_a == metric_b", PromQLBinEqual},
		{"active and healthy", PromQLBinAnd},
		{"up or down", PromQLBinOr},
		{"alerts unless silenced", PromQLBinUnless},
	}
	for _, tt := range tests {
		q, err := parser.ParseComplete(tt.expr)
		if err != nil {
			t.Errorf("ParseComplete(%q) error: %v", tt.expr, err)
			continue
		}
		if q.BinaryExpr == nil {
			t.Errorf("ParseComplete(%q): expected BinaryExpr, got nil", tt.expr)
			continue
		}
		if q.BinaryExpr.Op != tt.op {
			t.Errorf("ParseComplete(%q): op = %v, want %v", tt.expr, q.BinaryExpr.Op, tt.op)
		}
	}
}

func TestApplyBinaryOp(t *testing.T) {
	left := []float64{10, 20, 30}
	right := []float64{1, 2, 3}

	result := ApplyBinaryOp(PromQLBinAdd, left, right)
	if len(result) != 3 || result[0] != 11 || result[1] != 22 || result[2] != 33 {
		t.Errorf("Add: got %v", result)
	}

	result = ApplyBinaryOp(PromQLBinDiv, left, right)
	if len(result) != 3 || result[0] != 10 || result[1] != 10 || result[2] != 10 {
		t.Errorf("Div: got %v", result)
	}

	result = ApplyBinaryOp(PromQLBinGreaterThan, left, right)
	if len(result) != 3 || result[0] != 10 || result[1] != 20 || result[2] != 30 {
		t.Errorf("GreaterThan: got %v", result)
	}

	// Division by zero
	result = ApplyBinaryOp(PromQLBinDiv, []float64{1}, []float64{0})
	if len(result) != 1 || !math.IsNaN(result[0]) {
		t.Errorf("DivByZero: got %v, want NaN", result)
	}
}

func TestPromQLRegexMatcherConversion(t *testing.T) {
	parser := &PromQLParser{}
	q, err := parser.Parse(`http_requests{method=~"GET|POST"}`)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	cq := q.ToChronicleQuery(0, 1000)
	if len(cq.TagFilters) != 1 {
		t.Fatalf("expected 1 TagFilter, got %d", len(cq.TagFilters))
	}
	if cq.TagFilters[0].Op != TagOpRegex {
		t.Errorf("expected TagOpRegex, got %v", cq.TagFilters[0].Op)
	}
	if cq.TagFilters[0].Values[0] != "GET|POST" {
		t.Errorf("expected regex value GET|POST, got %s", cq.TagFilters[0].Values[0])
	}
}

func TestPromQLNotRegexMatcherConversion(t *testing.T) {
	parser := &PromQLParser{}
	q, err := parser.Parse(`http_requests{status!~"5.."}`)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	cq := q.ToChronicleQuery(0, 1000)
	if len(cq.TagFilters) != 1 {
		t.Fatalf("expected 1 TagFilter, got %d", len(cq.TagFilters))
	}
	if cq.TagFilters[0].Op != TagOpNotRegex {
		t.Errorf("expected TagOpNotRegex, got %v", cq.TagFilters[0].Op)
	}
}

func TestPromQLEvaluatorEndToEnd(t *testing.T) {
	db := setupTestDB(t)

	// Write real time-series data
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Second).UnixNano()
		_ = db.Write(Point{Metric: "http_requests_total", Value: float64(i * 10), Timestamp: ts, Tags: map[string]string{"method": "GET", "status": "200"}})
		_ = db.Write(Point{Metric: "http_requests_total", Value: float64(i * 5), Timestamp: ts, Tags: map[string]string{"method": "POST", "status": "200"}})
		_ = db.Write(Point{Metric: "temperature", Value: 20.0 + float64(i)*0.5, Timestamp: ts, Tags: map[string]string{"sensor": "a"}})
	}
	db.Flush()

	eval := NewPromQLEvaluator(db)
	start := baseTime.UnixNano()
	end := baseTime.Add(20 * time.Second).UnixNano()

	t.Run("SimpleMetric", func(t *testing.T) {
		result, err := eval.Evaluate("http_requests_total", start, end)
		if err != nil {
			t.Fatalf("Evaluate: %v", err)
		}
		if len(result.Series) == 0 {
			t.Fatal("expected non-empty result")
		}
	})

	t.Run("AbsentReturnsEmptyWhenPresent", func(t *testing.T) {
		result, err := eval.Evaluate("absent(http_requests_total)", start, end)
		if err != nil {
			t.Fatalf("Evaluate: %v", err)
		}
		if result.Scalar != nil {
			t.Error("absent should return empty when series exist")
		}
	})

	t.Run("AbsentReturns1WhenMissing", func(t *testing.T) {
		result, err := eval.Evaluate("absent(nonexistent_metric)", start, end)
		if err != nil {
			t.Fatalf("Evaluate: %v", err)
		}
		if result.Scalar == nil || *result.Scalar != 1.0 {
			t.Errorf("absent(nonexistent) should return scalar 1, got %v", result.Scalar)
		}
	})

	t.Run("GroupReturns1PerGroup", func(t *testing.T) {
		result, err := eval.Evaluate("group(http_requests_total)", start, end)
		if err != nil {
			t.Fatalf("Evaluate: %v", err)
		}
		for _, s := range result.Series {
			if s.Value != 1.0 {
				t.Errorf("group should return 1, got %f", s.Value)
			}
		}
	})

	t.Run("StdvarComputes", func(t *testing.T) {
		result, err := eval.Evaluate("stdvar(temperature)", start, end)
		if err != nil {
			t.Fatalf("Evaluate: %v", err)
		}
		if len(result.Series) == 0 {
			t.Fatal("expected stdvar result")
		}
		if result.Series[0].Value <= 0 {
			t.Errorf("stdvar should be positive for varied data, got %f", result.Series[0].Value)
		}
	})

	t.Run("RangeFunctionEvaluation", func(t *testing.T) {
		// Test that rate/increase functions parse and evaluate
		samples := []PromQLSample{
			{Timestamp: 1000, Value: 100},
			{Timestamp: 2000, Value: 200},
			{Timestamp: 3000, Value: 350},
		}
		rate := EvalRangeFunction(RangeFuncRate, samples, 3000)
		if math.IsNaN(rate) || rate <= 0 {
			t.Errorf("rate should be positive, got %f", rate)
		}

		increase := EvalRangeFunction(RangeFuncIncrease, samples, 3000)
		if math.IsNaN(increase) || increase <= 0 {
			t.Errorf("increase should be positive, got %f", increase)
		}
	})

	t.Run("QuantileOverTime", func(t *testing.T) {
		samples := []PromQLSample{
			{Timestamp: 1000, Value: 1}, {Timestamp: 2000, Value: 2},
			{Timestamp: 3000, Value: 3}, {Timestamp: 4000, Value: 4},
			{Timestamp: 5000, Value: 5},
		}
		median := EvalRangeFunctionWithParam(RangeFuncQuantileOverTime, samples, 5000, 0.5)
		if math.Abs(median-3.0) > 0.01 {
			t.Errorf("p50 should be ~3, got %f", median)
		}
	})

	t.Run("CompliancePassRate", func(t *testing.T) {
		suite := NewPromQLComplianceSuite()
		passRate := suite.PassRate()
		suite.RunAll()
		passRate = suite.PassRate()
		if passRate < 0.90 {
			t.Errorf("compliance pass rate %.2f%% < 90%%", passRate*100)
		}
		t.Logf("PromQL compliance: %.1f%% (%d tests)", passRate*100, len(suite.RunAll()))
	})
}
