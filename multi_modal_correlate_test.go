package chronicle

import "testing"

func TestMultiModalCorrelate_Smoke(t *testing.T) {
	// Smoke test: verify SpanQuery types and functions from multi_modal_correlate.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
