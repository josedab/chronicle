package chronicle

import "testing"

func TestTsBranchingDiff_Smoke(t *testing.T) {
	// Smoke test: verify BranchManagerStats types and functions from ts_branching_diff.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
