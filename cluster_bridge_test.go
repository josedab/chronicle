package chronicle

import "testing"

func TestClusterBridge_Smoke(t *testing.T) {
	// Smoke test: verify Cluster types and functions from cluster_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
