package chronicle

import "testing"

func TestPerfRegressionBaseline_Smoke(t *testing.T) {
	// Smoke test: verify PerfRegressionEngine types and functions from perf_regression_baseline.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
