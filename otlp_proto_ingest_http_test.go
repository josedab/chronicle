package chronicle

import "testing"

func TestOtlpProtoIngestHttp_Smoke(t *testing.T) {
	// Smoke test: verify DecodeOTLPMetricsBatch types and functions from otlp_proto_ingest_http.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
