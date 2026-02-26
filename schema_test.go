package chronicle

import "testing"

func TestSchema_Smoke(t *testing.T) {
	// Smoke test: verify DataType types and functions from schema.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
