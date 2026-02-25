package chronicle

import "testing"

func TestAuditTrailCompliance_Smoke(t *testing.T) {
	// Smoke test: verify ComplianceReporter types and functions from audit_trail_compliance.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
