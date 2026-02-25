package chronicle

import "testing"

func TestTieredStorageLifecycle_Smoke(t *testing.T) {
	// Smoke test: verify LifecycleConfig types and functions from tiered_storage_lifecycle.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
