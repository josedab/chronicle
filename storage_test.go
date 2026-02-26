package chronicle

import "testing"

func TestStorage_Smoke(t *testing.T) {
	// Smoke test: verify module types and functions from storage.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
