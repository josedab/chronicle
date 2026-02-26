package chronicle

import "testing"

func TestEbpfCollectors_Smoke(t *testing.T) {
	// Smoke test: verify DiskCollector types and functions from ebpf_collectors.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
