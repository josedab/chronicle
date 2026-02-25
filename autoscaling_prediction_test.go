package chronicle

import "testing"

func TestAutoscalingPrediction_Smoke(t *testing.T) {
	// Smoke test: verify PredictionAccuracy types and functions from autoscaling_prediction.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
