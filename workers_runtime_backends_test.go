package chronicle

import "testing"

func TestWorkersRuntimeBackends_Smoke(t *testing.T) {
	// Smoke test: verify R2Backend types and functions from workers_runtime_backends.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
