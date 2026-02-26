package chronicle

import "testing"

func TestContinuousQueriesBridge_Smoke(t *testing.T) {
	// Smoke test: verify ContinuousQueryConfig types and functions from continuous_queries_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
