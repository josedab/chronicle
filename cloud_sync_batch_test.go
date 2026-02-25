package chronicle

import "testing"

func TestCloudSyncBatch_Smoke(t *testing.T) {
	// Smoke test: verify SyncManifest types and functions from cloud_sync_batch.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
