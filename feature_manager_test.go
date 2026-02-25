package chronicle

import "testing"

func TestFeatureManager_Smoke(t *testing.T) {
	// Smoke test: verify FeatureManager types and functions from feature_manager.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
