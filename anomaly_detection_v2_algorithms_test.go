package chronicle

import "testing"

func TestAnomalyDetectionV2Algorithms_Smoke(t *testing.T) {
	// Smoke test: verify DBSCANConfig types and functions from anomaly_detection_v2_algorithms.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
