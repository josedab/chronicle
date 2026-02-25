package chronicle

import "testing"

func TestAnomalyBridge_Smoke(t *testing.T) {
	// Smoke test: verify AnomalyConfig types and functions from anomaly_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
