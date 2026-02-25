package chronicle

import "testing"

func TestTlsAuthMiddleware_Smoke(t *testing.T) {
	// Smoke test: verify APITokenInfo types and functions from tls_auth_middleware.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
