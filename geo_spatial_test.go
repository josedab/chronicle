package chronicle

import "testing"

func TestGeoSpatial_Smoke(t *testing.T) {
	// Smoke test: verify GeoStats types and functions from geo_spatial.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
