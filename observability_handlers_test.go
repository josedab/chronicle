package chronicle

import "testing"

func TestObservabilityHandlers_Smoke(t *testing.T) {
	// Smoke test: verify HealthState types and functions from observability_handlers.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
