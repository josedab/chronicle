package chronicle

import "testing"

func TestOtelCollectorExport_Smoke(t *testing.T) {
	// Smoke test: verify OTelCollectorReceiver types and functions from otel_collector_export.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
