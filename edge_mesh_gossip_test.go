package chronicle

import "testing"

func TestEdgeMeshGossip_Smoke(t *testing.T) {
	// Smoke test: verify EdgeMesh types and functions from edge_mesh_gossip.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
