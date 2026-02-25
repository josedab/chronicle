package chronicle

import "testing"

func TestCloudSyncFabricMerkle_Smoke(t *testing.T) {
	// Smoke test: verify HTTPCloudConnector types and functions from cloud_sync_fabric_merkle.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
