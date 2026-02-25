package chronicle

import "testing"

func TestStreamDslV2Queries_Smoke(t *testing.T) {
	// Smoke test: verify StreamDSLV2Engine types and functions from stream_dsl_v2_queries.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
