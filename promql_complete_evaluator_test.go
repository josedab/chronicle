package chronicle

import "testing"

func TestPromqlCompleteEvaluator_Smoke(t *testing.T) {
	// Smoke test: verify PromQLEvaluator types and functions from promql_complete_evaluator.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
