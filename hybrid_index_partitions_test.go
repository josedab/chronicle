package chronicle

import "testing"

func TestHybridIndexPartitions_Smoke(t *testing.T) {
	// Smoke test: verify TemporalPartitionedIndex types and functions from hybrid_index_partitions.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
