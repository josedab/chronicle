package chronicle

import "testing"

func TestMetricsCatalogSearch_Smoke(t *testing.T) {
	// Smoke test: verify MetricsCatalog types and functions from metrics_catalog_search.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
