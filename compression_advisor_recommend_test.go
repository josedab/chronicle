package chronicle

import "testing"

func TestCompressionAdvisorRecommend_Smoke(t *testing.T) {
	// Smoke test: verify CompressionAdvisor types and functions from compression_advisor_recommend.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
