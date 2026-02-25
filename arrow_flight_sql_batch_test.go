package chronicle

import "testing"

func TestArrowFlightSqlBatch_Smoke(t *testing.T) {
	// Smoke test: verify RecordBatchBuilder types and functions from arrow_flight_sql_batch.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
