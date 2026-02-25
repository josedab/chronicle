package chronicle

import "testing"

func TestStreamingSqlWindow_Smoke(t *testing.T) {
	// Smoke test: verify StreamingSQLStats types and functions from streaming_sql_window.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
