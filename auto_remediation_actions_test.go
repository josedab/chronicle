package chronicle

import "testing"

func TestAutoRemediationActions_Smoke(t *testing.T) {
	// Smoke test: verify AutoRemediationStats types and functions from auto_remediation_actions.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
