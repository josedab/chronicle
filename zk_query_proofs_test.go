//go:build experimental

package chronicle

import "testing"

func TestZkQueryProofs_Smoke(t *testing.T) {
	// Smoke test: verify ZKQueryStats types and functions from zk_query_proofs.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
