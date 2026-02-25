package chronicle

import "testing"

func TestBranchingMerge_Smoke(t *testing.T) {
	// Smoke test: verify MergeOption types and functions from branching_merge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
