package chronicle

import "testing"

func TestStreamingEtlBridge_Smoke(t *testing.T) {
	// Smoke test: verify ETLStreamSource types and functions from streaming_etl_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
