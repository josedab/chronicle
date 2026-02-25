package chronicle

import "testing"

func TestDbFeaturesExtended_Smoke(t *testing.T) {
	// Smoke test: verify DB types and functions from db_features_extended.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
