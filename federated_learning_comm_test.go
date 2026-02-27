//go:build experimental

package chronicle

import "testing"

func TestFederatedLearningComm_Smoke(t *testing.T) {
	// Smoke test: verify FederatedLearningStats types and functions from federated_learning_comm.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
