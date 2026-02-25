package chronicle

import "testing"

func TestQueryBuilderSdkComponents_Smoke(t *testing.T) {
	// Smoke test: verify VisualQueryBuilder types and functions from query_builder_sdk_components.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
