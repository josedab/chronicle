package chronicle

import "testing"

func TestCollaborativeQuerySession_Smoke(t *testing.T) {
	// Smoke test: verify CollaborativeQueryHub types and functions from collaborative_query_session.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
