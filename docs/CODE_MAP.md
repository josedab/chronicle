# Code Map

Chronicle has a flat package structure with ~292 non-test Go files in the root package. This document groups them by domain so contributors can mentally navigate the codebase.

> **Note**: See [PACKAGES.md](PACKAGES.md) for the restructuring plan to extract these into sub-packages.

## Core Database (15 files)

The essential database lifecycle, types, and write path.

| File | Purpose |
|------|---------|
| `db_core.go` | DB struct, `Open()`, `Close()`, lifecycle management |
| `db_write.go` | `Write()`, `WriteBatch()`, `WriteContext()` |
| `db_features.go` | Feature initialization and lazy loading |
| `db_metrics.go` | Internal metrics collection |
| `db_open_helpers.go` | Database open/create helpers |
| `db_recovery.go` | Crash recovery logic |
| `db_retention.go` | Data retention enforcement |
| `db_schema.go` | Schema management |
| `point.go` | `Point` type (core data model) |
| `point_validator.go` | Point validation rules |
| `config.go` | `Config`, `Validate()`, `DefaultConfig()` |
| `config_builder.go` | Fluent config builder |
| `config_hot_reload.go` | Runtime config reload |
| `errors.go` | Error types and sentinels |
| `doc.go` | Package documentation |

## Query Engine (20 files)

Query parsing, planning, optimization, and execution.

| File | Purpose |
|------|---------|
| `query.go` | `Query` struct, `Result` struct, `Execute()` |
| `query_builder.go` | Fluent query builder |
| `query_builder_sdk.go` | SDK query builder extensions |
| `query_engine.go` | Query execution engine |
| `query_planner.go` | Query plan generation |
| `query_optimizer.go` | Query plan optimization |
| `query_compiler.go` | Query compilation |
| `query_cache.go` | Query result caching |
| `query_cost.go` | Query cost estimation |
| `query_profiler.go` | Query performance profiling |
| `query_plan_viz.go` | Query plan visualization |
| `query_middleware.go` | Query middleware pipeline |
| `query_federation.go` | Cross-instance query federation |
| `query_console.go` | Interactive query console |
| `query_assistant.go` | AI-powered query assistance |
| `cost_based_optimizer.go` | Cost-based query optimization |
| `collaborative_query.go` | Multi-user collaborative queries |
| `distributed_query.go` | Distributed query execution |
| `distributed_query_federation.go` | Distributed federation layer |
| `zero_copy_query.go` | Zero-copy query paths |

## Query Languages (8 files)

Parsers and evaluators for different query languages.

| File | Purpose |
|------|---------|
| `parser.go` | Core query parser |
| `promql.go` | PromQL parser and evaluator |
| `promql_complete.go` | PromQL auto-completion |
| `sql_pipelines.go` | SQL pipeline queries |
| `streaming_sql.go` | Streaming SQL engine |
| `streaming_sql_v2.go` | Streaming SQL v2 |
| `nl_query.go` | Natural language query interface |
| `cql_bridge.go` | CQL (Chronicle Query Language) bridge |

## Storage Engine (12 files)

Low-level storage, partitioning, and indexing.

| File | Purpose |
|------|---------|
| `storage.go` | Storage layer abstraction |
| `storage_engine.go` | Storage engine implementation |
| `storage_stats.go` | Storage statistics |
| `storage_backend_interface.go` | `StorageBackend` interface |
| `storage_backend_file.go` | File-based storage backend |
| `storage_backend_memory.go` | In-memory storage backend |
| `storage_backend_s3.go` | S3 storage backend |
| `storage_backend_tiered.go` | Tiered storage backend |
| `partition_core.go` | Partition data structure |
| `partition_query.go` | Partition-level query execution |
| `partition_codec.go` | Partition serialization |
| `partition_pruner.go` | Partition pruning for queries |

## Indexing & Data Structures (8 files)

Core data structures for indexing and data management.

| File | Purpose |
|------|---------|
| `btree.go` | B-tree partition index |
| `index.go` | Index management |
| `hybrid_index.go` | Hybrid index (multiple strategies) |
| `tag_index.go` | Tag-based secondary index |
| `tags.go` | Tag operations |
| `columns.go` | Column store operations |
| `histogram.go` | Histogram data structure |
| `cardinality.go` | Cardinality estimation |

## WAL & Buffer (4 files)

Write-ahead log and write buffering.

| File | Purpose |
|------|---------|
| `wal.go` | Write-ahead log (crash recovery) |
| `wal_snapshot.go` | WAL snapshots |
| `buffer.go` | Write buffer (batching) |
| `write_hooks.go` | Pre/post write hooks |

## Compression & Encoding (8 files)

Data compression and encoding strategies.

| File | Purpose |
|------|---------|
| `gorilla.go` | Gorilla compression (float encoding) |
| `delta.go` | Delta encoding |
| `dictionary.go` | Dictionary encoding |
| `codec.go` | Codec registry |
| `column_codec.go` | Column-level codecs |
| `adaptive_compression.go` | Adaptive compression v1 |
| `adaptive_compression_v2.go` | Adaptive compression v2 |
| `adaptive_compression_v3.go` | Adaptive compression v3 |

## HTTP API (8 files)

HTTP server and route handlers.

| File | Purpose |
|------|---------|
| `http_server.go` | HTTP server setup and lifecycle |
| `http_core.go` | Core HTTP handler logic |
| `http_helpers.go` | HTTP utility functions |
| `http_routes_admin.go` | Admin API routes |
| `http_routes_feature.go` | Feature management routes |
| `http_routes_nextgen.go` | Next-gen API routes |
| `http_routes_prom.go` | Prometheus-compatible routes |
| `http_routes_write.go` | Write/ingest API routes |

## Observability & Monitoring (14 files)

Metrics, tracing, alerting, and monitoring.

| File | Purpose |
|------|---------|
| `observability.go` | Observability framework |
| `otel.go` | OpenTelemetry integration |
| `otel_collector.go` | OTel collector |
| `otel_distribution.go` | OTel distribution |
| `otlp_proto_ingest.go` | OTLP protobuf ingestion |
| `tracing.go` | Distributed tracing |
| `self_instrumentation.go` | Self-instrumentation |
| `alerting.go` | Alerting engine |
| `alert_builder.go` | Alert rule builder |
| `declarative_alerting.go` | Declarative alert definitions |
| `cross_alert.go` | Cross-metric alerting |
| `metric_correlation.go` | Metric correlation analysis |
| `metric_lifecycle.go` | Metric lifecycle management |
| `metric_metadata_store.go` | Metric metadata storage |

## Streaming & Real-time (10 files)

Stream processing, CDC, and real-time pipelines.

| File | Purpose |
|------|---------|
| `streaming.go` | Core streaming engine |
| `streaming_etl.go` | Streaming ETL |
| `streaming_etl_bridge.go` | ETL bridge to internal package |
| `stream_processing.go` | Stream processing framework |
| `stream_dsl.go` | Stream processing DSL |
| `stream_dsl_v2.go` | Stream DSL v2 |
| `stream_replay.go` | Stream replay |
| `cdc.go` | Change data capture |
| `continuous.go` | Continuous queries |
| `continuous_agg.go` | Continuous aggregations |

## Replication & Clustering (10 files)

Distributed operation, replication, and clustering.

| File | Purpose |
|------|---------|
| `replication.go` | Core replication |
| `multi_region_replication.go` | Multi-region replication |
| `cluster_engine.go` | Cluster engine |
| `cluster_bridge.go` | Cluster bridge to internal |
| `embedded_cluster.go` | Embedded cluster mode |
| `raft_bridge.go` | Raft consensus bridge |
| `federation.go` | Federation layer |
| `auto_sharding.go` | Automatic sharding |
| `connection_pool.go` | Connection pool management |
| `autoscaling.go` | Auto-scaling logic |

## Kubernetes & Cloud (12 files)

Kubernetes operator, cloud sync, and edge deployment.

| File | Purpose |
|------|---------|
| `k8s_operator.go` | Kubernetes operator |
| `k8s_operator_crd.go` | Custom Resource Definitions |
| `k8s_reconciler.go` | K8s reconciliation loop |
| `k8s_sidecar.go` | K8s sidecar collector |
| `cloud_sync.go` | Cloud synchronization |
| `cloud_sync_fabric.go` | Cloud sync fabric |
| `cloud_relay.go` | Cloud relay |
| `cloud_saas.go` | SaaS deployment |
| `edge_sync.go` | Edge synchronization |
| `edge_mesh.go` | Edge mesh networking |
| `edge_cloud_fabric.go` | Edge-cloud fabric |
| `cross_cloud_tiering.go` | Cross-cloud data tiering |

## ML & AI (12 files)

Machine learning, forecasting, and AI features.

| File | Purpose |
|------|---------|
| `forecast.go` | Time-series forecasting |
| `forecast_v2.go` | Forecasting v2 |
| `auto_ml.go` | Automated ML pipelines |
| `ml_inference.go` | ML model inference |
| `federated_learning.go` | Federated learning |
| `federated_ml_training.go` | Federated ML training |
| `foundation_model.go` | Foundation model integration |
| `anomaly_detection_v2.go` | Anomaly detection |
| `anomaly_correlation.go` | Anomaly correlation |
| `anomaly_explainability.go` | Anomaly explainability |
| `anomaly_pipeline.go` | Anomaly detection pipeline |
| `tinyml.go` | TinyML for edge devices |

## Data Management (14 files)

Data quality, lineage, retention, and lifecycle.

| File | Purpose |
|------|---------|
| `data_quality.go` | Data quality checks |
| `data_lineage.go` | Data lineage tracking |
| `data_contracts.go` | Data contracts |
| `data_masking.go` | Data masking/PII |
| `data_mesh.go` | Data mesh patterns |
| `data_rehydration.go` | Data rehydration from cold storage |
| `data_store.go` | Abstract data store |
| `schema.go` | Schema management |
| `schema_evolution.go` | Schema evolution |
| `schema_inference.go` | Schema inference |
| `schema_registry.go` | Schema registry |
| `schema_designer.go` | Schema designer tool |
| `smart_retention.go` | Smart retention policies |
| `retention_optimizer.go` | Retention optimization |

## Backup & Recovery (5 files)

Backup, restore, and point-in-time recovery.

| File | Purpose |
|------|---------|
| `backup.go` | Backup engine |
| `hot_backup.go` | Hot backup (online) |
| `incremental_backup.go` | Incremental backups |
| `backup_pitr.go` | Point-in-time recovery |
| `chaos_recovery.go` | Chaos recovery testing |

## Export & Integration (16 files)

External system integrations and data export.

| File | Purpose |
|------|---------|
| `export.go` | Data export framework |
| `parquet.go` | Parquet format support |
| `parquet_bridge.go` | Parquet bridge |
| `parquet_iceberg_export.go` | Parquet/Iceberg export |
| `arrow_flight.go` | Apache Arrow Flight |
| `arrow_flight_sql.go` | Arrow Flight SQL |
| `clickhouse.go` | ClickHouse compatibility |
| `duckdb_backend.go` | DuckDB backend |
| `sqlite_backend.go` | SQLite backend |
| `grafana_plugin.go` | Grafana plugin |
| `grafana_backend.go` | Grafana backend datasource |
| `prometheus_dropin.go` | Prometheus drop-in replacement |
| `prom_scraper.go` | Prometheus scraper |
| `graphql.go` | GraphQL API |
| `grpc_ingestion.go` | gRPC ingestion |
| `wire_protocol.go` | Wire protocol (PostgreSQL-compatible) |

## Feature System (5 files)

Feature flags, registry, and management.

| File | Purpose |
|------|---------|
| `feature_manager.go` | Lazy feature initialization |
| `feature_registry.go` | Plugin-style feature management |
| `feature_registry_bridge.go` | Feature registry bridge |
| `feature_flags.go` | Feature flag system |
| `feature_store.go` | Feature store for ML |

## API Surface (5 files)

API stability, aliases, and documentation.

| File | Purpose |
|------|---------|
| `api_stability.go` | API stability annotations |
| `api_aliases.go` | Type aliases for API compatibility |
| `deprecation.go` | Deprecation tracking |
| `openapi_spec.go` | OpenAPI specification |
| `engine.go` | Engine interface |

## SDKs & Developer Tools (14 files)

SDKs, developer tools, and interactive environments.

| File | Purpose |
|------|---------|
| `metrics_sdk.go` | Metrics SDK |
| `metrics_catalog.go` | Metrics catalog |
| `universal_sdk.go` | Universal SDK |
| `sdk_generator.go` | SDK code generator |
| `mobile_sdk.go` | Mobile SDK |
| `iot_device_sdk.go` | IoT device SDK |
| `plugin_sdk.go` | Plugin SDK |
| `plugin_lifecycle.go` | Plugin lifecycle management |
| `lsp.go` | Language Server Protocol |
| `lsp_enhanced.go` | Enhanced LSP features |
| `jupyter_kernel.go` | Jupyter kernel |
| `notebooks.go` | Notebook integration |
| `chronicle_studio.go` | Chronicle Studio IDE |
| `studio_enhanced.go` | Enhanced studio features |

## Operations & DevOps (18 files)

Operations, compliance, security, and infrastructure.

| File | Purpose |
|------|---------|
| `compliance_engine.go` | Compliance engine |
| `compliance_automation.go` | Compliance automation |
| `compliance_packs.go` | Compliance rule packs |
| `regulatory_compliance.go` | Regulatory compliance |
| `audit_log.go` | Audit logging |
| `audit_trail.go` | Audit trail |
| `blockchain_audit.go` | Blockchain audit trail |
| `encryption.go` | Encryption at rest |
| `tls_auth.go` | TLS authentication |
| `confidential.go` | Confidential computing |
| `zk_query.go` | Zero-knowledge queries |
| `privacy_federation.go` | Privacy-preserving federation |
| `policy_engine.go` | Policy engine |
| `terraform_provider.go` | Terraform provider |
| `gitops.go` | GitOps integration |
| `saas_control_plane.go` | SaaS control plane |
| `saas_fleet.go` | SaaS fleet management |
| `fleet_manager.go` | Fleet management |

## Miscellaneous (18 files)

Remaining utilities, experimental features, and niche integrations.

| File | Purpose |
|------|---------|
| `util.go` | General utilities |
| `retry.go` | Retry logic |
| `profile.go` | Profiling support |
| `health_check.go` | Health check endpoints |
| `rate_controller.go` | Rate limiting/control |
| `production_hardening.go` | Production hardening checks |
| `auto_remediation.go` | Auto-remediation |
| `root_cause_analysis.go` | Root cause analysis |
| `capacity_planning.go` | Capacity planning |
| `predictive_autoscaling.go` | Predictive autoscaling |
| `autoscale.go` | Autoscale helpers |
| `perf_regression.go` | Performance regression detection |
| `bench_runner.go` | Benchmark runner |
| `benchmark_suite.go` | Benchmark suite |
| `compression_advisor.go` | Compression advisor |
| `compression_plugin.go` | Compression plugin interface |
| `connector_hub.go` | Connector hub |
| `marketplace.go` | Plugin marketplace |

## Experimental / Advanced (16 files)

Advanced and experimental features.

| File | Purpose |
|------|---------|
| `multi_model.go` | Multi-model database |
| `multi_model_extensions.go` | Multi-model extensions |
| `multi_model_graph.go` | Graph data model |
| `multi_model_store.go` | Multi-model store |
| `multi_modal.go` | Multi-modal data |
| `vector.go` | Vector/embedding support |
| `semantic_search.go` | Semantic search |
| `ts_rag.go` | Time-series RAG |
| `ts_rag_llm.go` | RAG + LLM integration |
| `ts_branching.go` | Time-series branching |
| `ts_diff.go` | Time-series diff |
| `ts_diff_merge.go` | Time-series diff/merge |
| `timetravel.go` | Time travel queries |
| `time_travel_debug.go` | Time travel debugging |
| `chaos.go` | Chaos engineering |
| `ebpf.go` | eBPF integration |

## Runtime & Platform (16 files)

Runtime environments, WASM, and platform integrations.

| File | Purpose |
|------|---------|
| `wasm_runtime.go` | WASM runtime |
| `wasm_udf.go` | WASM user-defined functions |
| `wasm_playground.go` | WASM playground |
| `wasm_console.go` | WASM console |
| `deno_runtime.go` | Deno runtime |
| `workers_runtime.go` | Cloudflare Workers runtime |
| `cffi.go` | C FFI bindings |
| `pgwire_bridge.go` | PostgreSQL wire protocol bridge |
| `nl_dashboard.go` | Natural language dashboard |
| `embeddable_dashboard.go` | Embeddable dashboard |
| `recording_rules.go` | Recording rules |
| `materialized_views.go` | Materialized views |
| `materialized_views_v2.go` | Materialized views v2 |
| `downsample.go` | Downsampling |
| `exemplar.go` | Exemplar support |
| `pipeline_dsl.go` | Pipeline DSL |

## Bridge Files (9 files)

Thin wrappers connecting `internal/` packages to the root API.

| File | Purpose |
|------|---------|
| `admin_ui_bridge.go` | Admin UI bridge |
| `anomaly_bridge.go` | Anomaly detection bridge |
| `cluster_bridge.go` | Cluster bridge |
| `continuous_queries_bridge.go` | Continuous queries bridge |
| `cql_bridge.go` | CQL bridge |
| `feature_registry_bridge.go` | Feature registry bridge |
| `parquet_bridge.go` | Parquet bridge |
| `pgwire_bridge.go` | PostgreSQL wire protocol bridge |
| `streaming_etl_bridge.go` | Streaming ETL bridge |

## Internal Packages

Already extracted to `internal/`:

| Package | Purpose |
|---------|---------|
| `internal/raft/` | Raft consensus |
| `internal/adminui/` | Admin UI components |
| `internal/cql/` | CQL query engine |
| `internal/encoding/` | Compression codecs |
| `internal/cluster/` | Cluster coordination |
| `internal/anomaly/` | Anomaly detection |
| `internal/cep/` | Complex event processing |
| `internal/edgemesh/` | Edge mesh networking |
| `internal/continuousquery/` | Continuous query engine |

## Other Directories

| Directory | Purpose |
|-----------|---------|
| `cmd/chronicle/` | CLI tool |
| `examples/` | Example applications (http-server, iot-collector, etc.) |
| `contrib/` | Community contributions |
| `ffi/` | Foreign function interface |
| `k8s/` | Kubernetes manifests |
| `grafana-plugin/` | Grafana datasource plugin |
| `wasm/` | WASM build output |
| `scripts/` | Build and development scripts |
| `docs/` | Documentation |
| `website/` | Docusaurus documentation site |
