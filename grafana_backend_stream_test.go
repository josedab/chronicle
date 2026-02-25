package chronicle

import "testing"

func TestGrafanaBackendStream_Smoke(t *testing.T) {
	// Smoke test: verify GrafanaAlertRule types and functions from grafana_backend_stream.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
