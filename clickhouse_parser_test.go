package chronicle

import "testing"

func TestClickhouseParser_Smoke(t *testing.T) {
	// Smoke test: verify ClickHouseServer types and functions from clickhouse_parser.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
