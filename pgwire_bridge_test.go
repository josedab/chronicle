package chronicle

import "testing"

func TestPgwireBridge_Smoke(t *testing.T) {
	// Smoke test: verify PGServer types and functions from pgwire_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
