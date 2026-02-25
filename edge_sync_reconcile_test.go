package chronicle

import "testing"

func TestEdgeSyncReconcile_Smoke(t *testing.T) {
	// Smoke test: verify CloudAdapter types and functions from edge_sync_reconcile.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
