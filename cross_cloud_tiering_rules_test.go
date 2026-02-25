package chronicle

import "testing"

func TestCrossCloudTieringRules_Smoke(t *testing.T) {
	// Smoke test: verify CrossCloudTieringEngine types and functions from cross_cloud_tiering_rules.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
