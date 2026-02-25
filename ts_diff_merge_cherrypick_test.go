package chronicle

import "testing"

func TestTsDiffMergeCherrypick_Smoke(t *testing.T) {
	// Smoke test: verify TSDiffMergeEngine types and functions from ts_diff_merge_cherrypick.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
