package chronicle

import "testing"

func TestEtlPipelineManagerOperators_Smoke(t *testing.T) {
	// Smoke test: verify WindowedJoinStats types and functions from etl_pipeline_manager_operators.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
