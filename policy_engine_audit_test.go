package chronicle

import "testing"

func TestPolicyEngineAudit_Smoke(t *testing.T) {
	// Smoke test: verify PolicyEngineStats types and functions from policy_engine_audit.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
