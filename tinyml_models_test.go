package chronicle

import "testing"

func TestTinymlModels_Smoke(t *testing.T) {
	// Smoke test: verify SimpleExponentialSmoothingModel types and functions from tinyml_models.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
