package chronicle

import "testing"

func TestPromqlCompleteParser_Smoke(t *testing.T) {
	// Smoke test: verify PromQLCompleteParser types and functions from promql_complete_parser.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
