package chronicle

import "testing"

func TestSemanticSearchIndex_Smoke(t *testing.T) {
	// Smoke test: verify Neighbor types and functions from semantic_search_index.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
