// Package chronicle provides an embedded time-series database optimized for
// constrained and edge environments.
//
// Chronicle offers compressed columnar storage, SQL-like queries, automatic
// retention, and downsampling in a single-file format.
//
// # Basic Usage
//
// Open a database with default configuration:
//
//	db, err := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
// Write data points:
//
//	err := db.Write(chronicle.Point{
//	    Metric:    "temperature",
//	    Tags:      map[string]string{"room": "kitchen"},
//	    Value:     21.5,
//	    Timestamp: time.Now().UnixNano(),
//	})
//
// Query data:
//
//	result, err := db.Execute(&chronicle.Query{
//	    Metric: "temperature",
//	    Start:  time.Now().Add(-time.Hour).UnixNano(),
//	    End:    time.Now().UnixNano(),
//	})
//
// # Features
//
// Core Storage:
//   - Single-file storage with append-only partitions
//   - Gorilla float compression and delta timestamp encoding
//   - Dictionary tag compression and per-column encoding
//   - WAL-based crash recovery with rotation
//   - Pluggable storage backends (file, memory, S3, tiered)
//
// Query & Analytics:
//   - SQL-like query parser
//   - PromQL subset support for Prometheus compatibility
//   - Time-series forecasting (Holt-Winters, anomaly detection)
//   - Recording rules for pre-computed queries
//   - Native histograms with exponential bucketing
//   - Downsampling with configurable rules
//
// Integrations:
//   - Optional HTTP API with Influx line protocol
//   - Prometheus remote write ingestion
//   - OpenTelemetry OTLP receiver
//   - GraphQL API with playground
//   - WebAssembly compilation for browser/edge
//   - Grafana data source plugin
//
// Enterprise Features:
//   - Encryption at rest (AES-256-GCM)
//   - Schema registry for metric validation
//   - Multi-tenancy with namespace isolation
//   - Alerting engine with webhook notifications
//   - Streaming API for real-time subscriptions
//   - Query federation across instances
//
// # Configuration
//
// Use [Config] to customize behavior:
//
//	cfg := chronicle.Config{
//	    Path: "data.db",
//	    Storage: chronicle.StorageConfig{
//	        MaxMemory:         64 * 1024 * 1024,
//	        PartitionDuration: time.Hour,
//	    },
//	    Retention: chronicle.RetentionConfig{
//	        RetentionDuration: 7 * 24 * time.Hour,
//	    },
//	}
//
// Or use [DefaultConfig] for sensible defaults.
package chronicle
