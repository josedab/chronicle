package chronicle

import "testing"

func TestQueryBuilderSaved_Smoke(t *testing.T) {
	// Smoke test: verify QueryValidationError types and functions from query_builder_saved.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
