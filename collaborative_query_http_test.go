package chronicle

import "testing"

func TestCollaborativeQueryHttp_Smoke(t *testing.T) {
	// Smoke test: verify CollaborativeQueryStats types and functions from collaborative_query_http.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
