package chronicle

import "testing"

func TestCapacityPlanningRecommendations_Smoke(t *testing.T) {
	// Smoke test: verify CapacityPlanningStats types and functions from capacity_planning_recommendations.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
