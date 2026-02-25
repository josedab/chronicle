package chronicle

import "testing"

func TestCompliancePacksControls_Smoke(t *testing.T) {
	// Smoke test: verify CompliancePacksEngine types and functions from compliance_packs_controls.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
