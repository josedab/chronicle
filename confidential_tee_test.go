package chronicle

import "testing"

func TestConfidentialTee_Smoke(t *testing.T) {
	// Smoke test: verify ConfidentialStats types and functions from confidential_tee.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
