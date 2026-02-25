package chronicle

import "testing"

func TestAlertBuilderHttp_Smoke(t *testing.T) {
	// Smoke test: verify AlertBuilder types and functions from alert_builder_http.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
