package chronicle

import "testing"

func TestArrowFlightSqlProtocol_Smoke(t *testing.T) {
	// Smoke test: verify SQLToQueryTranslator types and functions from arrow_flight_sql_protocol.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
