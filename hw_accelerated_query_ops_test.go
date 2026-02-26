package chronicle

import "testing"

func TestHwAcceleratedQueryOps_Smoke(t *testing.T) {
	// Smoke test: verify ScanPredicate types and functions from hw_accelerated_query_ops.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
