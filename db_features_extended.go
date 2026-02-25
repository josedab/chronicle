// db_features_extended.go contains extended db features functionality.
package chronicle

// MultiRegionReplication returns the multi-region replication engine.
func (db *DB) MultiRegionReplication() *MultiRegionReplicationEngine {
	if db.features == nil {
		return nil
	}
	return db.features.MultiRegionReplication()
}

// UniversalSDK returns the universal SDK generator engine.
func (db *DB) UniversalSDK() *UniversalSDKEngine {
	if db.features == nil {
		return nil
	}
	return db.features.UniversalSDK()
}

// StudioEnhanced returns the enhanced Chronicle Studio IDE engine.
func (db *DB) StudioEnhanced() *StudioEnhancedEngine {
	if db.features == nil {
		return nil
	}
	return db.features.StudioEnhanced()
}

// SchemaInference returns the smart schema inference engine.
func (db *DB) SchemaInference() *SchemaInferenceEngine {
	if db.features == nil {
		return nil
	}
	return db.features.SchemaInference()
}

// CloudSaaS returns the Chronicle Cloud SaaS engine.
func (db *DB) CloudSaaS() *CloudSaaSEngine {
	if db.features == nil {
		return nil
	}
	return db.features.CloudSaaS()
}

// StreamDSLV2 returns the advanced stream processing DSL engine.
func (db *DB) StreamDSLV2() *StreamDSLV2Engine {
	if db.features == nil {
		return nil
	}
	return db.features.StreamDSLV2()
}

// AnomalyExplainability returns the AI-powered anomaly explainability engine.
func (db *DB) AnomalyExplainability() *AnomalyExplainabilityEngine {
	if db.features == nil {
		return nil
	}
	return db.features.AnomalyExplainability()
}

// HWAcceleratedQuery returns the hardware-accelerated query engine.
func (db *DB) HWAcceleratedQuery() *HWAcceleratedQueryEngine {
	if db.features == nil {
		return nil
	}
	return db.features.HWAcceleratedQuery()
}

// Marketplace returns the plugin marketplace engine.
func (db *DB) Marketplace() *MarketplaceEngine {
	if db.features == nil {
		return nil
	}
	return db.features.Marketplace()
}

// RegulatoryCompliance returns the regulatory compliance automation engine.
func (db *DB) RegulatoryCompliance() *RegulatoryComplianceEngine {
	if db.features == nil {
		return nil
	}
	return db.features.RegulatoryCompliance()
}

// GRPCIngestion returns the gRPC ingestion engine.
func (db *DB) GRPCIngestion() *GRPCIngestionEngine {
	if db.features == nil {
		return nil
	}
	return db.features.GRPCIngestion()
}

// ClusterEngine returns the embedded cluster engine.
func (db *DB) ClusterEngine() *EmbeddedClusterEngine {
	if db.features == nil {
		return nil
	}
	return db.features.ClusterEngine()
}

// AnomalyV2 returns the adaptive anomaly detection v2 engine.
func (db *DB) AnomalyV2() *AnomalyDetectionV2Engine {
	if db.features == nil {
		return nil
	}
	return db.features.AnomalyV2()
}

// DuckDBBackend returns the DuckDB query backend engine.
func (db *DB) DuckDBBackend() *DuckDBBackendEngine {
	if db.features == nil {
		return nil
	}
	return db.features.DuckDBBackend()
}

// WASMUDF returns the WASM UDF extension engine.
func (db *DB) WASMUDF() *WASMUDFEngine {
	if db.features == nil {
		return nil
	}
	return db.features.WASMUDF()
}

// PromDropIn returns the Prometheus drop-in compatibility engine.
func (db *DB) PromDropIn() *PrometheusDropInEngine {
	if db.features == nil {
		return nil
	}
	return db.features.PromDropIn()
}

// SchemaEvolution returns the automated schema evolution engine.
func (db *DB) SchemaEvolution() *SchemaEvolutionEngine {
	if db.features == nil {
		return nil
	}
	return db.features.SchemaEvolution()
}

// EdgeCloudFabric returns the edge-to-cloud data fabric engine.
func (db *DB) EdgeCloudFabric() *EdgeCloudFabricEngine {
	if db.features == nil {
		return nil
	}
	return db.features.EdgeCloudFabric()
}

// QueryProfiler returns the interactive query profiler engine.
func (db *DB) QueryProfiler() *QueryProfilerEngine {
	if db.features == nil {
		return nil
	}
	return db.features.QueryProfiler()
}

// MetricsSDK returns the embeddable metrics SDK engine.
func (db *DB) MetricsSDK() *MetricsSDKEngine {
	if db.features == nil {
		return nil
	}
	return db.features.MetricsSDK()
}

// DistributedQuery returns the distributed query coordinator.
func (db *DB) DistributedQuery() *DistributedQueryCoordinator {
	if db.features == nil {
		return nil
	}
	return db.features.DistributedQuery()
}

// ContinuousAgg returns the continuous aggregation engine.
func (db *DB) ContinuousAgg() *ContinuousAggEngine {
	if db.features == nil {
		return nil
	}
	return db.features.ContinuousAgg()
}

// DataLineage returns the data lineage tracker.
func (db *DB) DataLineage() *DataLineageEngine {
	if db.features == nil {
		return nil
	}
	return db.features.DataLineage()
}

// SmartCompaction returns the smart compaction engine.
func (db *DB) SmartCompaction() *SmartCompactionEngine {
	if db.features == nil {
		return nil
	}
	return db.features.SmartCompaction()
}

// MetricCorrelation returns the metric correlation engine.
func (db *DB) MetricCorrelation() *MetricCorrelationEngine {
	if db.features == nil {
		return nil
	}
	return db.features.MetricCorrelation()
}

// AdaptiveSampling returns the adaptive sampling engine.
func (db *DB) AdaptiveSampling() *AdaptiveSamplingEngine {
	if db.features == nil {
		return nil
	}
	return db.features.AdaptiveSampling()
}

// TSDiff returns the time-series diff engine.
func (db *DB) TSDiff() *TSDiffEngine {
	if db.features == nil {
		return nil
	}
	return db.features.TSDiff()
}

// DataQuality returns the data quality monitor.
func (db *DB) DataQuality() *DataQualityEngine {
	if db.features == nil {
		return nil
	}
	return db.features.DataQuality()
}

// QueryCost returns the query cost estimator.
func (db *DB) QueryCost() *QueryCostEstimator {
	if db.features == nil {
		return nil
	}
	return db.features.QueryCost()
}

// StreamReplay returns the stream replay engine.
func (db *DB) StreamReplay() *StreamReplayEngine {
	if db.features == nil {
		return nil
	}
	return db.features.StreamReplay()
}

// TagIndex returns the tag inverted index.
func (db *DB) TagIndex() *TagInvertedIndex {
	if db.features == nil {
		return nil
	}
	return db.features.TagIndex()
}

// WritePipeline returns the write pipeline hooks engine.
func (db *DB) WritePipeline() *WritePipelineEngine {
	if db.features == nil {
		return nil
	}
	return db.features.WritePipeline()
}

// MetricLifecycle returns the metric lifecycle manager.
func (db *DB) MetricLifecycle() *MetricLifecycleManager {
	if db.features == nil {
		return nil
	}
	return db.features.MetricLifecycle()
}

// RateController returns the ingestion rate controller.
func (db *DB) RateController() *RateControllerEngine {
	if db.features == nil {
		return nil
	}
	return db.features.RateController()
}

// HotBackup returns the hot backup manager.
func (db *DB) HotBackup() *HotBackupEngine {
	if db.features == nil {
		return nil
	}
	return db.features.HotBackup()
}

// CrossAlert returns the cross-metric alert engine.
func (db *DB) CrossAlert() *CrossAlertEngine {
	if db.features == nil {
		return nil
	}
	return db.features.CrossAlert()
}

// ResultCache returns the query result cache.
func (db *DB) ResultCache() *ResultCacheEngine {
	if db.features == nil {
		return nil
	}
	return db.features.ResultCache()
}

// WebhookSystem returns the webhook notification engine.
func (db *DB) WebhookSystem() *WebhookEngine {
	if db.features == nil {
		return nil
	}
	return db.features.WebhookSystem()
}

// RetentionOptimizer returns the retention policy optimizer.
func (db *DB) RetentionOptimizer() *RetentionOptimizerEngine {
	if db.features == nil {
		return nil
	}
	return db.features.RetentionOptimizer()
}

// BenchRunner returns the built-in benchmark runner.
func (db *DB) BenchRunner() *BenchRunnerEngine {
	if db.features == nil {
		return nil
	}
	return db.features.BenchRunner()
}

// MetricMetadataStore returns the metric metadata store.
func (db *DB) MetricMetadataStore() *MetricMetadataStoreEngine {
	if db.features == nil {
		return nil
	}
	return db.features.MetricMetadataStore()
}

// PointValidator returns the point validator engine.
func (db *DB) PointValidator() *PointValidatorEngine {
	if db.features == nil {
		return nil
	}
	return db.features.PointValidator()
}

// ConfigReload returns the config hot reload engine.
func (db *DB) ConfigReload() *ConfigReloadEngine {
	if db.features == nil {
		return nil
	}
	return db.features.ConfigReload()
}

// HealthCheck returns the health check system.
func (db *DB) HealthCheck() *HealthCheckEngine {
	if db.features == nil {
		return nil
	}
	return db.features.HealthCheck()
}

// AuditLog returns the audit log engine.
func (db *DB) AuditLog() *AuditLogEngine {
	if db.features == nil {
		return nil
	}
	return db.features.AuditLog()
}

// SeriesDedup returns the series deduplication engine.
func (db *DB) SeriesDedup() *SeriesDedupEngine {
	if db.features == nil {
		return nil
	}
	return db.features.SeriesDedup()
}

// PartitionPruner returns the partition pruner engine.
func (db *DB) PartitionPruner() *PartitionPrunerEngine {
	if db.features == nil {
		return nil
	}
	return db.features.PartitionPruner()
}

// QueryMiddleware returns the query middleware pipeline.
func (db *DB) QueryMiddleware() *QueryMiddlewareEngine {
	if db.features == nil {
		return nil
	}
	return db.features.QueryMiddleware()
}

// ConnectionPool returns the connection pool manager.
func (db *DB) ConnectionPool() *ConnectionPoolEngine {
	if db.features == nil {
		return nil
	}
	return db.features.ConnectionPool()
}

// StorageStatsCollector returns the storage statistics collector.
func (db *DB) StorageStatsCollector() *StorageStatsEngine {
	if db.features == nil {
		return nil
	}
	return db.features.StorageStatsCollector()
}

// WireProtocol returns the wire protocol server.
func (db *DB) WireProtocol() *WireProtocolEngine {
	if db.features == nil {
		return nil
	}
	return db.features.WireProtocol()
}

// WALSnapshot returns the WAL snapshot compaction engine.
func (db *DB) WALSnapshot() *WALSnapshotEngine {
	if db.features == nil {
		return nil
	}
	return db.features.WALSnapshot()
}

// PromScraper returns the embedded Prometheus scraper.
func (db *DB) PromScraper() *PromScraperEngine {
	if db.features == nil {
		return nil
	}
	return db.features.PromScraper()
}

// ForecastV2 returns the Prophet-style forecasting engine.
func (db *DB) ForecastV2() *ForecastV2Engine {
	if db.features == nil {
		return nil
	}
	return db.features.ForecastV2()
}

// TenantIsolation returns the multi-tenant isolation engine.
func (db *DB) TenantIsolation() *TenantIsolationEngine {
	if db.features == nil {
		return nil
	}
	return db.features.TenantIsolation()
}

// IncrementalBackup returns the incremental backup engine.
func (db *DB) IncrementalBackup() *IncrementalBackupEngine {
	if db.features == nil {
		return nil
	}
	return db.features.IncrementalBackup()
}

// OTLPProto returns the OTLP proto ingestion engine.
func (db *DB) OTLPProto() *OTLPProtoEngine {
	if db.features == nil {
		return nil
	}
	return db.features.OTLPProto()
}

// QueryPlanViz returns the query plan visualization engine.
func (db *DB) QueryPlanViz() *QueryPlanVizEngine {
	if db.features == nil {
		return nil
	}
	return db.features.QueryPlanViz()
}

// DataRehydration returns the data rehydration pipeline.
func (db *DB) DataRehydration() *DataRehydrationEngine {
	if db.features == nil {
		return nil
	}
	return db.features.DataRehydration()
}

// DataMasking returns the compliance data masking engine.
func (db *DB) DataMasking() *DataMaskingEngine {
	if db.features == nil {
		return nil
	}
	return db.features.DataMasking()
}

// MigrationTool returns the data migration engine.
func (db *DB) MigrationTool() *ImportEngine {
	if db.features == nil {
		return nil
	}
	return db.features.MigrationTool()
}

// ChaosRecovery returns the chaos recovery validation engine.
func (db *DB) ChaosRecovery() *ChaosRecoveryEngine {
	if db.features == nil {
		return nil
	}
	return db.features.ChaosRecovery()
}

// FeatureFlags returns the feature flag engine.
func (db *DB) FeatureFlags() *FeatureFlagEngine {
	if db.features == nil {
		return nil
	}
	return db.features.FeatureFlags()
}

// SelfInstrumentation returns the self-instrumentation engine.
func (db *DB) SelfInstrumentation() *SelfInstrumentationEngine {
	if db.features == nil {
		return nil
	}
	return db.features.SelfInstrumentation()
}

// Deprecation returns the deprecation engine.
func (db *DB) Deprecation() *DeprecationEngine {
	if db.features == nil {
		return nil
	}
	return db.features.Deprecation()
}
