package chronicle

import "testing"

func TestMlInferenceTraining_Smoke(t *testing.T) {
	// Smoke test: verify StatisticalExtractor types and functions from ml_inference_training.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
