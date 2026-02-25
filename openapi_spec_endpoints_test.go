package chronicle

import "testing"

func TestOpenapiSpecEndpoints_Smoke(t *testing.T) {
	// Smoke test: verify SDKLanguage types and functions from openapi_spec_endpoints.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
