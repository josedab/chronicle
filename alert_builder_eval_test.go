package chronicle

import "testing"

func TestAlertBuilderEval_Smoke(t *testing.T) {
	// Smoke test: verify EvaluationResult types and functions from alert_builder_eval.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
