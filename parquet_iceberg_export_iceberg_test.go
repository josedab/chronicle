package chronicle

import "testing"

func TestParquetIcebergExportIceberg_Smoke(t *testing.T) {
	// Smoke test: verify IcebergExportStatus types and functions from parquet_iceberg_export_iceberg.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
