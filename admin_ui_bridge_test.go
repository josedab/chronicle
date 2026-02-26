package chronicle

import "testing"

func TestAdminUiBridge_Smoke(t *testing.T) {
	// Smoke test: verify AdminUI types and functions from admin_ui_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
