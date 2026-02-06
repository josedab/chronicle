package chronicle

import (
	"math"
	"testing"
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
