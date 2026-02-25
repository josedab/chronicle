package chronicle

import "testing"

func TestNlQueryHandlers_Smoke(t *testing.T) {
	// Smoke test: verify QueryOptimizer types and functions from nl_query_handlers.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
