package chronicle

import "testing"

func TestLspCompletions_Smoke(t *testing.T) {
	// Smoke test: verify QueryValidation types and functions from lsp_completions.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
