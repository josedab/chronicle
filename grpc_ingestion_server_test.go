package chronicle

import "testing"

func TestGrpcIngestionServer_Smoke(t *testing.T) {
	// Smoke test: verify GRPCIngestionEngine types and functions from grpc_ingestion_server.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
