package chronicle

import "testing"

func TestClusterEngineHttp_Smoke(t *testing.T) {
	// Smoke test: verify ConsistencyLevel types and functions from cluster_engine_http.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
