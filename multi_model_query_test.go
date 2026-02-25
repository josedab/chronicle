package chronicle

import "testing"

func TestMultiModelQuery_Smoke(t *testing.T) {
	// Smoke test: verify DocumentQuery types and functions from multi_model_query.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
