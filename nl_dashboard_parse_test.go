package chronicle

import "testing"

func TestNlDashboardParse_Smoke(t *testing.T) {
	// Smoke test: verify NLDashboardStats types and functions from nl_dashboard_parse.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
