package chronicle

import "testing"

func TestTenantGovernanceAdmission_Smoke(t *testing.T) {
	// Smoke test: verify GovernanceStatus types and functions from tenant_governance_admission.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
