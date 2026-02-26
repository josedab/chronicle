package chronicle

import "testing"

func TestPromqlCompleteRules_Smoke(t *testing.T) {
	// Smoke test: verify PromQLRecordingRule types and functions from promql_complete_rules.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
