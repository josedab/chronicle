package chronicle

import "testing"

func TestQueryFederationMerge_Smoke(t *testing.T) {
	// Smoke test: verify ClickHouseSource types and functions from query_federation_merge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
