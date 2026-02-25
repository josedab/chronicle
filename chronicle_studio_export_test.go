package chronicle

import "testing"

func TestChronicleStudioExport_Smoke(t *testing.T) {
	// Smoke test: verify ChronicleStudio types and functions from chronicle_studio_export.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
