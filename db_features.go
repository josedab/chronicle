package chronicle

import (
	"fmt"
)

// Registry returns the plugin-style feature registry.
// New features should register here instead of being added to FeatureManager.
func (db *DB) Registry() *FeatureRegistry {
	return db.registry
}

// CQLEngine returns the CQL query engine.
func (db *DB) CQLEngine() *CQLEngine {
	if db.features == nil {
		return nil
	}
	return db.features.CQLEngine()
}

// Observability returns the observability suite.
func (db *DB) Observability() *ObservabilitySuite {
	if db.features == nil {
		return nil
	}
	return db.features.Observability()
}

// MaterializedViews returns the V1 materialized view engine.
//
// Deprecated: Use [DB.MaterializedViewsV2] instead.
func (db *DB) MaterializedViews() *MaterializedViewEngine {
	if db.features == nil {
		return nil
	}
	return db.features.MaterializedViews()
}

// ExemplarStore returns the exemplar store for direct access.
// Deprecated: Use db.Features().ExemplarStore() instead for cleaner architecture.
func (db *DB) ExemplarStore() *ExemplarStore {
	return db.exemplarStore
}

// HistogramStore returns the histogram store for direct access.
// Deprecated: Use db.Features().HistogramStore() instead for cleaner architecture.
func (db *DB) HistogramStore() *HistogramStore {
	return db.histogramStore
}

// CardinalityTracker returns the cardinality tracker for direct access.
// Deprecated: Use db.Features().CardinalityTracker() instead for cleaner architecture.
func (db *DB) CardinalityTracker() *CardinalityTracker {
	return db.cardinalityTracker
}

// CardinalityStats returns current cardinality statistics.
func (db *DB) CardinalityStats() CardinalityStats {
	if db.cardinalityTracker == nil {
		return CardinalityStats{}
	}
	return db.cardinalityTracker.Stats()
}

// AlertManager returns the alert manager for direct access.
// Deprecated: Use db.Features().AlertManager() instead for cleaner architecture.
func (db *DB) AlertManager() *AlertManager {
	return db.alertManager
}

// Features returns the feature manager for accessing optional features.
// This is the preferred way to access features like alerting, histograms, etc.
func (db *DB) Features() *FeatureManager {
	return db.features
}

// AnomalyPipeline returns the streaming anomaly detection pipeline.
func (db *DB) AnomalyPipeline() *AnomalyPipeline {
	if db.features == nil {
		return nil
	}
	return db.features.AnomalyPipeline()
}

// AnomalyCorrelation returns the anomaly correlation engine.
func (db *DB) AnomalyCorrelation() *AnomalyCorrelationEngine {
	if db.features == nil {
		return nil
	}
	return db.features.AnomalyCorrelation()
}

// QueryPlanner returns the adaptive query planner.
func (db *DB) QueryPlanner() *QueryPlanner {
	if db.features == nil {
		return nil
	}
	return db.features.QueryPlanner()
}

// ConnectorHub returns the connector hub.
func (db *DB) ConnectorHub() *ConnectorHub {
	if db.features == nil {
		return nil
	}
	return db.features.ConnectorHub()
}

// NotebookEngine returns the notebook engine.
func (db *DB) NotebookEngine() *NotebookEngine {
	if db.features == nil {
		return nil
	}
	return db.features.NotebookEngine()
}

// WriteHistogram writes a histogram point to the database.
func (db *DB) WriteHistogram(p HistogramPoint) error {
	if db.histogramStore == nil {
		return fmt.Errorf("histogram store not initialized")
	}
	return db.histogramStore.Write(p)
}

// WriteExemplar writes a point with an exemplar to the database.
func (db *DB) WriteExemplar(p ExemplarPoint) error {
	if db.exemplarStore == nil {
		return fmt.Errorf("exemplar store not initialized")
	}
	return db.exemplarStore.Write(p)
}

// QueryCompiler returns the unified query compiler.
func (db *DB) QueryCompiler() *QueryCompiler {
	if db.features == nil {
		return nil
	}
	return db.features.QueryCompiler()
}

// TSRAG returns the time-series RAG engine.
func (db *DB) TSRAG() *TSRAGEngine {
	if db.features == nil {
		return nil
	}
	return db.features.TSRAG()
}

// PluginRegistry returns the plugin registry.
func (db *DB) PluginRegistry() *PluginRegistry {
	if db.features == nil {
		return nil
	}
	return db.features.PluginRegistry()
}

// MaterializedViewsV2 returns the v2 materialized view engine.
func (db *DB) MaterializedViewsV2() *MaterializedViewV2Engine {
	if db.features == nil {
		return nil
	}
	return db.features.MaterializedViewsV2()
}

// MultiModelGraph returns the multi-model graph+document store.
func (db *DB) MultiModelGraph() *MultiModelGraphStore {
	if db.features == nil {
		return nil
	}
	return db.features.MultiModelGraph()
}

// FleetManager returns the fleet manager.
func (db *DB) FleetManager() *SaaSFleetManager {
	if db.features == nil {
		return nil
	}
	return db.features.FleetManager()
}

// HardeningSuite returns the production hardening suite.
func (db *DB) HardeningSuite() *HardeningSuite {
	if db.features == nil {
		return nil
	}
	return db.features.HardeningSuite()
}

// OTelDistro returns the OpenTelemetry distribution.
func (db *DB) OTelDistro() *OTelDistro {
	if db.features == nil {
		return nil
	}
	return db.features.OTelDistro()
}

// EmbeddedCluster returns the embedded cluster manager.
func (db *DB) EmbeddedCluster() *EmbeddedCluster {
	if db.features == nil {
		return nil
	}
	return db.features.EmbeddedCluster()
}

// SmartRetention returns the smart retention engine.
func (db *DB) SmartRetention() *SmartRetentionEngine {
	if db.features == nil {
		return nil
	}
	return db.features.SmartRetention()
}

// Dashboard returns the embeddable dashboard.
func (db *DB) Dashboard() *EmbeddableDashboard {
	if db.features == nil {
		return nil
	}
	return db.features.Dashboard()
}

// LSPEnhanced returns the enhanced LSP server.
func (db *DB) LSPEnhanced() *LSPEnhancedServer {
	if db.features == nil {
		return nil
	}
	return db.features.LSPEnhanced()
}

// ETLManager returns the ETL pipeline manager.
func (db *DB) ETLManager() *ETLPipelineManager {
	if db.features == nil {
		return nil
	}
	return db.features.ETLManager()
}

// CloudSyncFabric returns the multi-cloud sync fabric.
func (db *DB) CloudSyncFabric() *CloudSyncFabric {
	if db.features == nil {
		return nil
	}
	return db.features.CloudSyncFabric()
}

// DataMesh returns the data mesh federation engine.
func (db *DB) DataMesh() *DataMesh {
	if db.features == nil {
		return nil
	}
	return db.features.DataMesh()
}

// FoundationModel returns the time-series foundation model engine.
func (db *DB) FoundationModel() *FoundationModel {
	if db.features == nil {
		return nil
	}
	return db.features.FoundationModel()
}

// DataContracts returns the data contracts engine.
func (db *DB) DataContracts() *DataContractEngine {
	if db.features == nil {
		return nil
	}
	return db.features.DataContracts()
}

// QueryCache returns the query result cache.
func (db *DB) QueryCache() *QueryCache {
	if db.features == nil {
		return nil
	}
	return db.features.QueryCache()
}

// SQLPipelines returns the SQL pipeline engine.
func (db *DB) SQLPipelines() *SQLPipelineEngine {
	if db.features == nil {
		return nil
	}
	return db.features.SQLPipelines()
}

// MultiModelStore returns the multi-model store.
func (db *DB) MultiModelStore() *IntegratedMultiModelStore {
	if db.features == nil {
		return nil
	}
	return db.features.MultiModelStore()
}

// AdaptiveOptimizer returns the adaptive query optimizer.
func (db *DB) AdaptiveOptimizer() *AdaptiveOptimizer {
	if db.features == nil {
		return nil
	}
	return db.features.AdaptiveOptimizer()
}

// ComplianceAutomation returns the compliance automation suite.
func (db *DB) ComplianceAutomation() *ComplianceAutomation {
	if db.features == nil {
		return nil
	}
	return db.features.ComplianceAutomation()
}

// SchemaDesigner returns the visual schema designer.
func (db *DB) SchemaDesigner() *SchemaDesigner {
	if db.features == nil {
		return nil
	}
	return db.features.SchemaDesigner()
}

// MobileSDK returns the mobile SDK framework.
func (db *DB) MobileSDK() *MobileSDK {
	if db.features == nil {
		return nil
	}
	return db.features.MobileSDK()
}

// StreamProcessing returns the stream processing engine.
func (db *DB) StreamProcessing() *StreamProcessingEngine {
	if db.features == nil {
		return nil
	}
	return db.features.StreamProcessing()
}

// TimeTravelDebug returns the time-travel debug engine.
func (db *DB) TimeTravelDebug() *TimeTravelDebugEngine {
	if db.features == nil {
		return nil
	}
	return db.features.TimeTravelDebug()
}

// AutoSharding returns the auto-sharding engine.
func (db *DB) AutoSharding() *AutoShardingEngine {
	if db.features == nil {
		return nil
	}
	return db.features.AutoSharding()
}

// RootCauseAnalysis returns the root cause analysis engine.
func (db *DB) RootCauseAnalysis() *RootCauseAnalysisEngine {
	if db.features == nil {
		return nil
	}
	return db.features.RootCauseAnalysis()
}

// CrossCloudTiering returns the cross-cloud tiering engine.
func (db *DB) CrossCloudTiering() *CrossCloudTieringEngine {
	if db.features == nil {
		return nil
	}
	return db.features.CrossCloudTiering()
}

// DeclarativeAlerting returns the declarative alerting engine.
func (db *DB) DeclarativeAlerting() *DeclarativeAlertingEngine {
	if db.features == nil {
		return nil
	}
	return db.features.DeclarativeAlerting()
}

// MetricsCatalog returns the metrics catalog.
func (db *DB) MetricsCatalog() *MetricsCatalog {
	if db.features == nil {
		return nil
	}
	return db.features.MetricsCatalog()
}

// CompressionAdvisor returns the compression advisor.
func (db *DB) CompressionAdvisor() *CompressionAdvisor {
	if db.features == nil {
		return nil
	}
	return db.features.CompressionAdvisor()
}

// TSDiffMerge returns the time-series diff and merge engine.
func (db *DB) TSDiffMerge() *TSDiffMergeEngine {
	if db.features == nil {
		return nil
	}
	return db.features.TSDiffMerge()
}

// CompliancePacks returns the compliance packs engine.
func (db *DB) CompliancePacks() *CompliancePacksEngine {
	if db.features == nil {
		return nil
	}
	return db.features.CompliancePacks()
}

// BlockchainAudit returns the blockchain audit trail.
func (db *DB) BlockchainAudit() *BlockchainAuditTrail {
	if db.features == nil {
		return nil
	}
	return db.features.BlockchainAudit()
}

// ChronicleStudio returns the Chronicle Studio IDE engine.
func (db *DB) ChronicleStudio() *ChronicleStudio {
	if db.features == nil {
		return nil
	}
	return db.features.ChronicleStudio()
}

// IoTDeviceSDK returns the IoT device SDK manager.
func (db *DB) IoTDeviceSDK() *IoTDeviceSDK {
	if db.features == nil {
		return nil
	}
	return db.features.IoTDeviceSDK()
}

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
	if db.features == nil { return nil }
	return db.features.DistributedQuery()
}

// ContinuousAgg returns the continuous aggregation engine.
func (db *DB) ContinuousAgg() *ContinuousAggEngine {
	if db.features == nil { return nil }
	return db.features.ContinuousAgg()
}

// DataLineage returns the data lineage tracker.
func (db *DB) DataLineage() *DataLineageEngine {
	if db.features == nil { return nil }
	return db.features.DataLineage()
}

// SmartCompaction returns the smart compaction engine.
func (db *DB) SmartCompaction() *SmartCompactionEngine {
	if db.features == nil { return nil }
	return db.features.SmartCompaction()
}

// MetricCorrelation returns the metric correlation engine.
func (db *DB) MetricCorrelation() *MetricCorrelationEngine {
	if db.features == nil { return nil }
	return db.features.MetricCorrelation()
}

// AdaptiveSampling returns the adaptive sampling engine.
func (db *DB) AdaptiveSampling() *AdaptiveSamplingEngine {
	if db.features == nil { return nil }
	return db.features.AdaptiveSampling()
}

// TSDiff returns the time-series diff engine.
func (db *DB) TSDiff() *TSDiffEngine {
	if db.features == nil { return nil }
	return db.features.TSDiff()
}

// DataQuality returns the data quality monitor.
func (db *DB) DataQuality() *DataQualityEngine {
	if db.features == nil { return nil }
	return db.features.DataQuality()
}

// QueryCost returns the query cost estimator.
func (db *DB) QueryCost() *QueryCostEstimator {
	if db.features == nil { return nil }
	return db.features.QueryCost()
}

// StreamReplay returns the stream replay engine.
func (db *DB) StreamReplay() *StreamReplayEngine {
	if db.features == nil { return nil }
	return db.features.StreamReplay()
}

// TagIndex returns the tag inverted index.
func (db *DB) TagIndex() *TagInvertedIndex {
	if db.features == nil { return nil }
	return db.features.TagIndex()
}

// WritePipeline returns the write pipeline hooks engine.
func (db *DB) WritePipeline() *WritePipelineEngine {
	if db.features == nil { return nil }
	return db.features.WritePipeline()
}

// MetricLifecycle returns the metric lifecycle manager.
func (db *DB) MetricLifecycle() *MetricLifecycleManager {
	if db.features == nil { return nil }
	return db.features.MetricLifecycle()
}

// RateController returns the ingestion rate controller.
func (db *DB) RateController() *RateControllerEngine {
	if db.features == nil { return nil }
	return db.features.RateController()
}

// HotBackup returns the hot backup manager.
func (db *DB) HotBackup() *HotBackupEngine {
	if db.features == nil { return nil }
	return db.features.HotBackup()
}

// CrossAlert returns the cross-metric alert engine.
func (db *DB) CrossAlert() *CrossAlertEngine {
	if db.features == nil { return nil }
	return db.features.CrossAlert()
}

// ResultCache returns the query result cache.
func (db *DB) ResultCache() *ResultCacheEngine {
	if db.features == nil { return nil }
	return db.features.ResultCache()
}

// WebhookSystem returns the webhook notification engine.
func (db *DB) WebhookSystem() *WebhookEngine {
	if db.features == nil { return nil }
	return db.features.WebhookSystem()
}

// RetentionOptimizer returns the retention policy optimizer.
func (db *DB) RetentionOptimizer() *RetentionOptimizerEngine {
	if db.features == nil { return nil }
	return db.features.RetentionOptimizer()
}

// BenchRunner returns the built-in benchmark runner.
func (db *DB) BenchRunner() *BenchRunnerEngine {
	if db.features == nil { return nil }
	return db.features.BenchRunner()
}

// MetricMetadataStore returns the metric metadata store.
func (db *DB) MetricMetadataStore() *MetricMetadataStoreEngine {
	if db.features == nil { return nil }
	return db.features.MetricMetadataStore()
}

// PointValidator returns the point validator engine.
func (db *DB) PointValidator() *PointValidatorEngine {
	if db.features == nil { return nil }
	return db.features.PointValidator()
}

// ConfigReload returns the config hot reload engine.
func (db *DB) ConfigReload() *ConfigReloadEngine {
	if db.features == nil { return nil }
	return db.features.ConfigReload()
}

// HealthCheck returns the health check system.
func (db *DB) HealthCheck() *HealthCheckEngine {
	if db.features == nil { return nil }
	return db.features.HealthCheck()
}

// AuditLog returns the audit log engine.
func (db *DB) AuditLog() *AuditLogEngine {
	if db.features == nil { return nil }
	return db.features.AuditLog()
}

// SeriesDedup returns the series deduplication engine.
func (db *DB) SeriesDedup() *SeriesDedupEngine {
	if db.features == nil { return nil }
	return db.features.SeriesDedup()
}

// PartitionPruner returns the partition pruner engine.
func (db *DB) PartitionPruner() *PartitionPrunerEngine {
	if db.features == nil { return nil }
	return db.features.PartitionPruner()
}

// QueryMiddleware returns the query middleware pipeline.
func (db *DB) QueryMiddleware() *QueryMiddlewareEngine {
	if db.features == nil { return nil }
	return db.features.QueryMiddleware()
}

// ConnectionPool returns the connection pool manager.
func (db *DB) ConnectionPool() *ConnectionPoolEngine {
	if db.features == nil { return nil }
	return db.features.ConnectionPool()
}

// StorageStatsCollector returns the storage statistics collector.
func (db *DB) StorageStatsCollector() *StorageStatsEngine {
	if db.features == nil { return nil }
	return db.features.StorageStatsCollector()
}

// WireProtocol returns the wire protocol server.
func (db *DB) WireProtocol() *WireProtocolEngine {
	if db.features == nil { return nil }
	return db.features.WireProtocol()
}

// WALSnapshot returns the WAL snapshot compaction engine.
func (db *DB) WALSnapshot() *WALSnapshotEngine {
	if db.features == nil { return nil }
	return db.features.WALSnapshot()
}

// PromScraper returns the embedded Prometheus scraper.
func (db *DB) PromScraper() *PromScraperEngine {
	if db.features == nil { return nil }
	return db.features.PromScraper()
}

// ForecastV2 returns the Prophet-style forecasting engine.
func (db *DB) ForecastV2() *ForecastV2Engine {
	if db.features == nil { return nil }
	return db.features.ForecastV2()
}

// TenantIsolation returns the multi-tenant isolation engine.
func (db *DB) TenantIsolation() *TenantIsolationEngine {
	if db.features == nil { return nil }
	return db.features.TenantIsolation()
}

// IncrementalBackup returns the incremental backup engine.
func (db *DB) IncrementalBackup() *IncrementalBackupEngine {
	if db.features == nil { return nil }
	return db.features.IncrementalBackup()
}

// OTLPProto returns the OTLP proto ingestion engine.
func (db *DB) OTLPProto() *OTLPProtoEngine {
	if db.features == nil { return nil }
	return db.features.OTLPProto()
}

// QueryPlanViz returns the query plan visualization engine.
func (db *DB) QueryPlanViz() *QueryPlanVizEngine {
	if db.features == nil { return nil }
	return db.features.QueryPlanViz()
}

// DataRehydration returns the data rehydration pipeline.
func (db *DB) DataRehydration() *DataRehydrationEngine {
	if db.features == nil { return nil }
	return db.features.DataRehydration()
}

// DataMasking returns the compliance data masking engine.
func (db *DB) DataMasking() *DataMaskingEngine {
	if db.features == nil { return nil }
	return db.features.DataMasking()
}

// MigrationTool returns the data migration engine.
func (db *DB) MigrationTool() *ImportEngine {
	if db.features == nil { return nil }
	return db.features.MigrationTool()
}

// ChaosRecovery returns the chaos recovery validation engine.
func (db *DB) ChaosRecovery() *ChaosRecoveryEngine {
	if db.features == nil { return nil }
	return db.features.ChaosRecovery()
}

// FeatureFlags returns the feature flag engine.
func (db *DB) FeatureFlags() *FeatureFlagEngine {
	if db.features == nil { return nil }
	return db.features.FeatureFlags()
}

// SelfInstrumentation returns the self-instrumentation engine.
func (db *DB) SelfInstrumentation() *SelfInstrumentationEngine {
	if db.features == nil { return nil }
	return db.features.SelfInstrumentation()
}

// Deprecation returns the deprecation engine.
func (db *DB) Deprecation() *DeprecationEngine {
	if db.features == nil { return nil }
	return db.features.Deprecation()
}
