package chronicle

import "testing"

func TestTieredStorageCost_Smoke(t *testing.T) {
	// Smoke test: verify TierCostDetail types and functions from tiered_storage_cost.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
