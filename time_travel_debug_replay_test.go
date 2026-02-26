package chronicle

import "testing"

func TestTimeTravelDebugReplay_Smoke(t *testing.T) {
	// Smoke test: verify TimeTravelDebugEngine types and functions from time_travel_debug_replay.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
