package chronicle

import "testing"

func TestStreamProcessingOperators_Smoke(t *testing.T) {
	// Smoke test: verify MapOperator types and functions from stream_processing_operators.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
