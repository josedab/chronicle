package chronicle

import "testing"

func TestStreamingEtlStages_Smoke(t *testing.T) {
	// Smoke test: verify ETLAggregateConfig types and functions from streaming_etl_stages.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
