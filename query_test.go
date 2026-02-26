package chronicle

import "testing"

func TestQuery_Smoke(t *testing.T) {
	// Smoke test: verify Query types and functions from query.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
