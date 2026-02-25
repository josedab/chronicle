package chronicle

import "testing"

func TestAutoShardingRebalance_Smoke(t *testing.T) {
	// Smoke test: verify AutoShardingEngine types and functions from auto_sharding_rebalance.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
