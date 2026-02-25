// feature_manager_accessors_extended.go contains extended feature manager accessors functionality.
package chronicle

func (fm *FeatureManager) TrackCardinality(p Point) error {
	if fm.cardinalityTracker == nil {
		return nil
	}
	return fm.cardinalityTracker.TrackPoint(p)
}

// AddAlertRule adds an alerting rule.
func (fm *FeatureManager) AddAlertRule(rule AlertRule) error {
	if fm.alertManager == nil {
		return nil
	}
	return fm.alertManager.AddRule(rule)
}

// RemoveAlertRule removes an alerting rule.
func (fm *FeatureManager) RemoveAlertRule(name string) {
	if fm.alertManager == nil {
		return
	}
	fm.alertManager.RemoveRule(name)
}

// GetAlerts returns all active alerts.
func (fm *FeatureManager) GetAlerts() []*Alert {
	if fm.alertManager == nil {
		return nil
	}
	return fm.alertManager.ListAlerts()
}

// GetAlertRules returns all alert rules.
func (fm *FeatureManager) GetAlertRules() []*AlertRule {
	if fm.alertManager == nil {
		return nil
	}
	return fm.alertManager.ListRules()
}

// GRPCIngestion returns the gRPC ingestion engine.
func (fm *FeatureManager) GRPCIngestion() *GRPCIngestionEngine {
	fm.grpcIngestionOnce.Do(func() {
		fm.grpcIngestion = NewGRPCIngestionEngine(fm.db, DefaultGRPCIngestionConfig())
	})
	return fm.grpcIngestion
}

// ClusterEngine returns the embedded cluster engine.
func (fm *FeatureManager) ClusterEngine() *EmbeddedClusterEngine {
	fm.clusterEngineOnce.Do(func() {
		fm.clusterEngine = NewEmbeddedClusterEngine(fm.db, DefaultGossipClusterConfig())
	})
	return fm.clusterEngine
}

// AnomalyV2 returns the adaptive anomaly detection v2 engine.
func (fm *FeatureManager) AnomalyV2() *AnomalyDetectionV2Engine {
	fm.anomalyV2Once.Do(func() {
		fm.anomalyV2 = NewAnomalyDetectionV2Engine(fm.db, DefaultAnomalyDetectionV2Config())
	})
	return fm.anomalyV2
}

// DuckDBBackend returns the DuckDB query backend engine.
func (fm *FeatureManager) DuckDBBackend() *DuckDBBackendEngine {
	fm.duckdbBackendOnce.Do(func() {
		fm.duckdbBackend = NewDuckDBBackendEngine(fm.db, DefaultDuckDBBackendConfig())
	})
	return fm.duckdbBackend
}

// WASMUDF returns the WASM UDF extension engine.
func (fm *FeatureManager) WASMUDF() *WASMUDFEngine {
	fm.wasmUDFOnce.Do(func() {
		fm.wasmUDF = NewWASMUDFEngine(fm.db, DefaultWASMUDFConfig())
	})
	return fm.wasmUDF
}

// PromDropIn returns the Prometheus drop-in compatibility engine.
func (fm *FeatureManager) PromDropIn() *PrometheusDropInEngine {
	fm.promDropInOnce.Do(func() {
		fm.promDropIn = NewPrometheusDropInEngine(fm.db, DefaultPrometheusDropInConfig())
	})
	return fm.promDropIn
}

// SchemaEvolution returns the automated schema evolution engine.
func (fm *FeatureManager) SchemaEvolution() *SchemaEvolutionEngine {
	fm.schemaEvolutionOnce.Do(func() {
		fm.schemaEvolution = NewSchemaEvolutionEngine(fm.db, DefaultSchemaEvolutionConfig())
	})
	return fm.schemaEvolution
}

// EdgeCloudFabric returns the edge-to-cloud data fabric engine.
func (fm *FeatureManager) EdgeCloudFabric() *EdgeCloudFabricEngine {
	fm.edgeCloudFabricOnce.Do(func() {
		fm.edgeCloudFabric = NewEdgeCloudFabricEngine(fm.db, DefaultEdgeCloudFabricConfig())
	})
	return fm.edgeCloudFabric
}

// QueryProfiler returns the interactive query profiler engine.
func (fm *FeatureManager) QueryProfiler() *QueryProfilerEngine {
	fm.queryProfilerOnce.Do(func() {
		fm.queryProfiler = NewQueryProfilerEngine(fm.db, DefaultQueryProfilerConfig())
	})
	return fm.queryProfiler
}

// MetricsSDK returns the embeddable metrics SDK engine.
func (fm *FeatureManager) MetricsSDK() *MetricsSDKEngine {
	fm.metricsSDKOnce.Do(func() {
		fm.metricsSDK = NewMetricsSDKEngine(fm.db, DefaultMetricsSDKConfig())
	})
	return fm.metricsSDK
}

// DistributedQuery returns the distributed query coordinator.
func (fm *FeatureManager) DistributedQuery() *DistributedQueryCoordinator {
	fm.distributedQueryOnce.Do(func() {
		fm.distributedQuery = NewDistributedQueryCoordinator(fm.db, DefaultDistributedQueryConfig())
	})
	return fm.distributedQuery
}

// ContinuousAgg returns the continuous aggregation engine.
func (fm *FeatureManager) ContinuousAgg() *ContinuousAggEngine {
	fm.continuousAggOnce.Do(func() {
		fm.continuousAgg = NewContinuousAggEngine(fm.db, DefaultContinuousAggConfig())
	})
	return fm.continuousAgg
}

// DataLineage returns the data lineage tracker.
func (fm *FeatureManager) DataLineage() *DataLineageEngine {
	fm.dataLineageOnce.Do(func() {
		fm.dataLineage = NewDataLineageEngine(fm.db, DefaultDataLineageConfig())
	})
	return fm.dataLineage
}

// SmartCompaction returns the smart compaction engine.
func (fm *FeatureManager) SmartCompaction() *SmartCompactionEngine {
	fm.smartCompactionOnce.Do(func() {
		fm.smartCompaction = NewSmartCompactionEngine(fm.db, DefaultSmartCompactionConfig())
	})
	return fm.smartCompaction
}

// MetricCorrelation returns the metric correlation engine.
func (fm *FeatureManager) MetricCorrelation() *MetricCorrelationEngine {
	fm.metricCorrelationOnce.Do(func() {
		fm.metricCorrelation = NewMetricCorrelationEngine(fm.db, DefaultMetricCorrelationConfig())
	})
	return fm.metricCorrelation
}

// AdaptiveSampling returns the adaptive sampling engine.
func (fm *FeatureManager) AdaptiveSampling() *AdaptiveSamplingEngine {
	fm.adaptiveSamplingOnce.Do(func() {
		fm.adaptiveSampling = NewAdaptiveSamplingEngine(fm.db, DefaultAdaptiveSamplingConfig())
	})
	return fm.adaptiveSampling
}

// TSDiff returns the time-series diff engine.
func (fm *FeatureManager) TSDiff() *TSDiffEngine {
	fm.tsDiffOnce.Do(func() {
		fm.tsDiff = NewTSDiffEngine(fm.db, DefaultTSDiffConfig())
	})
	return fm.tsDiff
}

// DataQuality returns the data quality monitor.
func (fm *FeatureManager) DataQuality() *DataQualityEngine {
	fm.dataQualityOnce.Do(func() {
		fm.dataQuality = NewDataQualityEngine(fm.db, DefaultDataQualityConfig())
	})
	return fm.dataQuality
}

// QueryCost returns the query cost estimator.
func (fm *FeatureManager) QueryCost() *QueryCostEstimator {
	fm.queryCostOnce.Do(func() {
		fm.queryCost = NewQueryCostEstimator(fm.db, DefaultQueryCostConfig())
	})
	return fm.queryCost
}

// StreamReplay returns the stream replay engine.
func (fm *FeatureManager) StreamReplay() *StreamReplayEngine {
	fm.streamReplayOnce.Do(func() {
		fm.streamReplay = NewStreamReplayEngine(fm.db, DefaultStreamReplayConfig())
	})
	return fm.streamReplay
}

// TagIndex returns the tag inverted index.
func (fm *FeatureManager) TagIndex() *TagInvertedIndex {
	fm.tagIndexOnce.Do(func() {
		fm.tagIndex = NewTagInvertedIndex(fm.db, DefaultTagIndexConfig())
	})
	return fm.tagIndex
}

// WritePipeline returns the write pipeline hooks engine.
func (fm *FeatureManager) WritePipeline() *WritePipelineEngine {
	fm.writePipelineOnce.Do(func() {
		fm.writePipeline = NewWritePipelineEngine(fm.db, DefaultWritePipelineConfig())
	})
	return fm.writePipeline
}

// MetricLifecycle returns the metric lifecycle manager.
func (fm *FeatureManager) MetricLifecycle() *MetricLifecycleManager {
	fm.metricLifecycleOnce.Do(func() {
		fm.metricLifecycle = NewMetricLifecycleManager(fm.db, DefaultMetricLifecycleConfig())
	})
	return fm.metricLifecycle
}

// RateController returns the ingestion rate controller.
func (fm *FeatureManager) RateController() *RateControllerEngine {
	fm.rateControllerOnce.Do(func() {
		fm.rateController = NewRateControllerEngine(fm.db, DefaultRateControllerConfig())
	})
	return fm.rateController
}

// HotBackup returns the hot backup manager.
func (fm *FeatureManager) HotBackup() *HotBackupEngine {
	fm.hotBackupOnce.Do(func() {
		fm.hotBackup = NewHotBackupEngine(fm.db, DefaultHotBackupConfig())
	})
	return fm.hotBackup
}

// CrossAlert returns the cross-metric alert engine.
func (fm *FeatureManager) CrossAlert() *CrossAlertEngine {
	fm.crossAlertOnce.Do(func() {
		fm.crossAlert = NewCrossAlertEngine(fm.db, DefaultCrossAlertConfig())
	})
	return fm.crossAlert
}

// ResultCache returns the query result cache.
func (fm *FeatureManager) ResultCache() *ResultCacheEngine {
	fm.resultCacheOnce.Do(func() {
		fm.resultCache = NewResultCacheEngine(fm.db, DefaultResultCacheConfig())
	})
	return fm.resultCache
}

// WebhookSystem returns the webhook notification engine.
func (fm *FeatureManager) WebhookSystem() *WebhookEngine {
	fm.webhookSystemOnce.Do(func() {
		fm.webhookSystem = NewWebhookEngine(fm.db, DefaultWebhookConfig())
	})
	return fm.webhookSystem
}

// RetentionOptimizer returns the retention policy optimizer.
func (fm *FeatureManager) RetentionOptimizer() *RetentionOptimizerEngine {
	fm.retentionOptimizerOnce.Do(func() {
		fm.retentionOptimizer = NewRetentionOptimizerEngine(fm.db, DefaultRetentionOptimizerConfig())
	})
	return fm.retentionOptimizer
}

// BenchRunner returns the built-in benchmark runner.
func (fm *FeatureManager) BenchRunner() *BenchRunnerEngine {
	fm.benchRunnerOnce.Do(func() {
		fm.benchRunner = NewBenchRunnerEngine(fm.db, DefaultBenchRunnerConfig())
	})
	return fm.benchRunner
}

// MetricMetadataStore returns the metric metadata store.
func (fm *FeatureManager) MetricMetadataStore() *MetricMetadataStoreEngine {
	fm.metricMetadataStoreOnce.Do(func() {
		fm.metricMetadataStore = NewMetricMetadataStoreEngine(fm.db, DefaultMetricMetadataStoreConfig())
	})
	return fm.metricMetadataStore
}

// PointValidator returns the point validator engine.
func (fm *FeatureManager) PointValidator() *PointValidatorEngine {
	fm.pointValidatorOnce.Do(func() {
		fm.pointValidator = NewPointValidatorEngine(fm.db, DefaultPointValidatorConfig())
	})
	return fm.pointValidator
}

// ConfigReload returns the config hot reload engine.
func (fm *FeatureManager) ConfigReload() *ConfigReloadEngine {
	fm.configReloadOnce.Do(func() {
		fm.configReload = NewConfigReloadEngine(fm.db, DefaultConfigReloadConfig())
	})
	return fm.configReload
}

// HealthCheck returns the health check system.
func (fm *FeatureManager) HealthCheck() *HealthCheckEngine {
	fm.healthCheckOnce.Do(func() {
		fm.healthCheck = NewHealthCheckEngine(fm.db, DefaultHealthCheckConfig())
	})
	return fm.healthCheck
}

// AuditLog returns the audit log engine.
func (fm *FeatureManager) AuditLog() *AuditLogEngine {
	fm.auditLogOnce.Do(func() {
		fm.auditLog = NewAuditLogEngine(fm.db, DefaultAuditLogConfig())
	})
	return fm.auditLog
}

// SeriesDedup returns the series deduplication engine.
func (fm *FeatureManager) SeriesDedup() *SeriesDedupEngine {
	fm.seriesDedupOnce.Do(func() {
		fm.seriesDedup = NewSeriesDedupEngine(fm.db, DefaultSeriesDedupConfig())
	})
	return fm.seriesDedup
}

// PartitionPruner returns the partition pruner engine.
func (fm *FeatureManager) PartitionPruner() *PartitionPrunerEngine {
	fm.partitionPrunerOnce.Do(func() {
		fm.partitionPruner = NewPartitionPrunerEngine(fm.db, DefaultPartitionPrunerConfig())
	})
	return fm.partitionPruner
}

// QueryMiddleware returns the query middleware pipeline.
func (fm *FeatureManager) QueryMiddleware() *QueryMiddlewareEngine {
	fm.queryMiddlewareOnce.Do(func() {
		fm.queryMiddleware = NewQueryMiddlewareEngine(fm.db, DefaultQueryMiddlewareConfig())
	})
	return fm.queryMiddleware
}

// ConnectionPool returns the connection pool manager.
func (fm *FeatureManager) ConnectionPool() *ConnectionPoolEngine {
	fm.connectionPoolOnce.Do(func() {
		fm.connectionPool = NewConnectionPoolEngine(fm.db, DefaultConnectionPoolConfig())
	})
	return fm.connectionPool
}

// StorageStatsCollector returns the storage statistics collector.
func (fm *FeatureManager) StorageStatsCollector() *StorageStatsEngine {
	fm.storageStatsCollectorOnce.Do(func() {
		fm.storageStatsCollector = NewStorageStatsEngine(fm.db, DefaultStorageStatsConfig())
	})
	return fm.storageStatsCollector
}

// WireProtocol returns the wire protocol server.
func (fm *FeatureManager) WireProtocol() *WireProtocolEngine {
	fm.wireProtocolOnce.Do(func() {
		fm.wireProtocol = NewWireProtocolEngine(fm.db, DefaultWireProtocolConfig())
	})
	return fm.wireProtocol
}

// WALSnapshot returns the WAL snapshot compaction engine.
func (fm *FeatureManager) WALSnapshot() *WALSnapshotEngine {
	fm.walSnapshotOnce.Do(func() {
		fm.walSnapshot = NewWALSnapshotEngine(fm.db, DefaultWALSnapshotConfig())
	})
	return fm.walSnapshot
}

// PromScraper returns the embedded Prometheus scraper.
func (fm *FeatureManager) PromScraper() *PromScraperEngine {
	fm.promScraperOnce.Do(func() {
		fm.promScraper = NewPromScraperEngine(fm.db, DefaultPromScraperConfig())
	})
	return fm.promScraper
}

// ForecastV2 returns the Prophet-style forecasting engine.
func (fm *FeatureManager) ForecastV2() *ForecastV2Engine {
	fm.forecastV2Once.Do(func() {
		fm.forecastV2 = NewForecastV2Engine(fm.db, DefaultForecastV2Config())
	})
	return fm.forecastV2
}

// TenantIsolation returns the multi-tenant isolation engine.
func (fm *FeatureManager) TenantIsolation() *TenantIsolationEngine {
	fm.tenantIsolationOnce.Do(func() {
		fm.tenantIsolation = NewTenantIsolationEngine(fm.db, DefaultTenantIsolationConfig())
	})
	return fm.tenantIsolation
}

// IncrementalBackup returns the incremental backup engine.
func (fm *FeatureManager) IncrementalBackup() *IncrementalBackupEngine {
	fm.incrementalBackupOnce.Do(func() {
		fm.incrementalBackup = NewIncrementalBackupEngine(fm.db, DefaultIncrementalBackupConfig())
	})
	return fm.incrementalBackup
}

// OTLPProto returns the OTLP proto ingestion engine.
func (fm *FeatureManager) OTLPProto() *OTLPProtoEngine {
	fm.otlpProtoOnce.Do(func() {
		fm.otlpProto = NewOTLPProtoEngine(fm.db, DefaultOTLPProtoConfig())
	})
	return fm.otlpProto
}

// QueryPlanViz returns the query plan visualization engine.
func (fm *FeatureManager) QueryPlanViz() *QueryPlanVizEngine {
	fm.queryPlanVizOnce.Do(func() {
		fm.queryPlanViz = NewQueryPlanVizEngine(fm.db, DefaultQueryPlanVizConfig())
	})
	return fm.queryPlanViz
}

// DataRehydration returns the data rehydration pipeline.
func (fm *FeatureManager) DataRehydration() *DataRehydrationEngine {
	fm.dataRehydrationOnce.Do(func() {
		fm.dataRehydration = NewDataRehydrationEngine(fm.db, DefaultDataRehydrationConfig())
	})
	return fm.dataRehydration
}

// DataMasking returns the compliance data masking engine.
func (fm *FeatureManager) DataMasking() *DataMaskingEngine {
	fm.dataMaskingOnce.Do(func() {
		fm.dataMasking = NewDataMaskingEngine(fm.db, DefaultDataMaskingConfig())
	})
	return fm.dataMasking
}

// MigrationTool returns the data migration engine.
func (fm *FeatureManager) MigrationTool() *ImportEngine {
	fm.migrationToolOnce.Do(func() {
		fm.migrationTool = NewImportEngine(fm.db, DefaultImportConfig())
	})
	return fm.migrationTool
}

// ChaosRecovery returns the chaos recovery validation engine.
func (fm *FeatureManager) ChaosRecovery() *ChaosRecoveryEngine {
	fm.chaosRecoveryOnce.Do(func() {
		fm.chaosRecovery = NewChaosRecoveryEngine(fm.db, DefaultChaosRecoveryConfig())
	})
	return fm.chaosRecovery
}

// FeatureFlags returns the feature flag engine.
func (fm *FeatureManager) FeatureFlags() *FeatureFlagEngine {
	fm.featureFlagsOnce.Do(func() {
		fm.featureFlags = NewFeatureFlagEngine(fm.db, DefaultFeatureFlagConfig())
	})
	return fm.featureFlags
}

// SelfInstrumentation returns the self-instrumentation engine.
func (fm *FeatureManager) SelfInstrumentation() *SelfInstrumentationEngine {
	fm.selfInstrumentationOnce.Do(func() {
		fm.selfInstrumentation = NewSelfInstrumentationEngine(fm.db, DefaultSelfInstrumentationConfig())
	})
	return fm.selfInstrumentation
}

// Deprecation returns the deprecation engine.
func (fm *FeatureManager) Deprecation() *DeprecationEngine {
	fm.deprecationOnce.Do(func() {
		fm.deprecation = NewDeprecationEngine(fm.db, DefaultDeprecationConfig())
	})
	return fm.deprecation
}
