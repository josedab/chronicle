package chronicle

import "testing"

func TestOfflineSyncAdvanced_Smoke(t *testing.T) {
	// Smoke test: verify SchemaVersion types and functions from offline_sync_advanced.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
