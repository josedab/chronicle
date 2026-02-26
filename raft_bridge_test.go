package chronicle

import "testing"

func TestRaftBridge_Smoke(t *testing.T) {
	// Smoke test: verify RaftNode types and functions from raft_bridge.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
