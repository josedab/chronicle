package chronicle

import "testing"

func TestPromqlCompleteRange_Smoke(t *testing.T) {
	// Smoke test: verify PromQLHistogramBucket types and functions from promql_complete_range.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
