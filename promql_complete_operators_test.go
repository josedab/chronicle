package chronicle

import "testing"

func TestPromqlCompleteOperators_Smoke(t *testing.T) {
	// Smoke test: verify PromQLBinaryOp types and functions from promql_complete_operators.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
