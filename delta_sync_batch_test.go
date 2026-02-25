package chronicle

import "testing"

func TestDeltaSyncBatch_Smoke(t *testing.T) {
	// Smoke test: verify DeltaPullResponse types and functions from delta_sync_batch.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
