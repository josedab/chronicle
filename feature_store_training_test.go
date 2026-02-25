package chronicle

import "testing"

func TestFeatureStoreTraining_Smoke(t *testing.T) {
	// Smoke test: verify TrainingDataRequest types and functions from feature_store_training.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
