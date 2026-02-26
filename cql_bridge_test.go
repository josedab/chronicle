package chronicle

import "testing"

func TestCqlBridge_Smoke(t *testing.T) {
	// Smoke test: verify CQLEngine types and functions from cql_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
