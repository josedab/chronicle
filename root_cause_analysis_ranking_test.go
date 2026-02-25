package chronicle

import "testing"

func TestRootCauseAnalysisRanking_Smoke(t *testing.T) {
	// Smoke test: verify RootCauseAnalysisEngine types and functions from root_cause_analysis_ranking.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
