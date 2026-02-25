package chronicle

// feature_manager_accessors.go contains all FeatureManager accessor methods.
// Each method lazily initializes its corresponding feature on first access.
// See feature_manager.go for the core struct, config, and lifecycle methods.

// ExemplarStore returns the exemplar store.
func (fm *FeatureManager) ExemplarStore() *ExemplarStore {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.exemplarStore
}

// HistogramStore returns the histogram store.
func (fm *FeatureManager) HistogramStore() *HistogramStore {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.histogramStore
}

// CardinalityTracker returns the cardinality tracker.
func (fm *FeatureManager) CardinalityTracker() *CardinalityTracker {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.cardinalityTracker
}

// AlertManager returns the alert manager.
func (fm *FeatureManager) AlertManager() *AlertManager {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.alertManager
}

// SchemaRegistry returns the schema registry.
func (fm *FeatureManager) SchemaRegistry() *SchemaRegistry {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.schemaRegistry
}

// CQLEngine returns the CQL query engine.
// Observability returns the observability suite.
// MaterializedViews returns the V1 materialized view engine.
//
// Deprecated: Use [FeatureManager.MaterializedViewsV2] instead.
// ChaosInjector returns the fault injector.
// OfflineSync returns the offline sync manager.
// AnomalyPipeline returns the streaming anomaly detection pipeline.
// AnomalyCorrelation returns the anomaly correlation engine.
// CloudRelay returns the cloud relay agent.
// Playground returns the query playground.
// QueryPlanner returns the adaptive query planner.
// ConnectorHub returns the connector hub.
// Autoscaler returns the predictive autoscaler.
// NotebookEngine returns the notebook engine.
// SaaSControlPlane returns the SaaS control plane.
// GitOpsEngine returns the GitOps engine.
// FederatedMLTrainer returns the federated ML trainer.
// EdgeMesh returns the edge mesh network manager.
func (fm *FeatureManager) EdgeMesh() *EdgeMesh {
	fm.edgeMeshOnce.Do(func() {
		fm.edgeMesh, _ = NewEdgeMesh(fm.db, DefaultEdgeMeshConfig()) //nolint:errcheck // EdgeMesh init is best-effort
	})
	return fm.edgeMesh
}

// QueryCompiler returns the unified query compiler.
// EdgePlatform returns the edge platform manager.
// TSRAG returns the time-series RAG engine.
// PluginRegistry returns the plugin registry.
// PluginMarketplace returns the plugin marketplace.
func (fm *FeatureManager) PluginMarketplace() *PluginMarketplace {
	fm.pluginMarketplaceOnce.Do(func() {
		fm.pluginMarketplace = NewPluginMarketplace(DefaultPluginSDKConfig().MarketplaceURL, fm.PluginRegistry())
	})
	return fm.pluginMarketplace
}

// MaterializedViewsV2 returns the v2 materialized view engine.
func (fm *FeatureManager) MaterializedViewsV2() *MaterializedViewV2Engine {
	fm.matViewV2Once.Do(func() {
		fm.matViewV2 = NewMaterializedViewV2Engine(fm.db, DefaultMaterializedViewV2Config())
	})
	return fm.matViewV2
}

// ClusterReconciler returns the K8s cluster reconciler.
func (fm *FeatureManager) ClusterReconciler() *ClusterReconciler {
	fm.clusterReconcilerOnce.Do(func() {
		fm.clusterReconciler = NewClusterReconciler()
	})
	return fm.clusterReconciler
}

// AdaptiveCompressorV3 returns the ML-driven adaptive compressor.
func (fm *FeatureManager) AdaptiveCompressorV3() *AdaptiveCompressorV3 {
	fm.adaptiveV3Once.Do(func() {
		fm.adaptiveV3 = NewAdaptiveCompressorV3(DefaultAdaptiveCompressionV3Config())
	})
	return fm.adaptiveV3
}

// MultiModelGraph returns the multi-model graph+document store.
func (fm *FeatureManager) MultiModelGraph() *MultiModelGraphStore {
	fm.multiModelGraphOnce.Do(func() {
		fm.multiModelGraph = NewMultiModelGraphStore(fm.db)
	})
	return fm.multiModelGraph
}

// FleetManager returns the fleet manager.
func (fm *FeatureManager) FleetManager() *SaaSFleetManager {
	fm.fleetManagerOnce.Do(func() {
		fm.fleetManager = NewSaaSFleetManager(DefaultSaaSFleetConfig())
	})
	return fm.fleetManager
}

// HardeningSuite returns the production hardening suite.
func (fm *FeatureManager) HardeningSuite() *HardeningSuite {
	fm.hardeningSuiteOnce.Do(func() {
		fm.hardeningSuite = NewHardeningSuite(fm.db, DefaultHardeningConfig())
	})
	return fm.hardeningSuite
}

// OTelDistro returns the OpenTelemetry distribution.
func (fm *FeatureManager) OTelDistro() *OTelDistro {
	fm.otelDistroOnce.Do(func() {
		fm.otelDistro = NewOTelDistro(fm.db, DefaultOTelDistroConfig())
	})
	return fm.otelDistro
}

// EmbeddedCluster returns the embedded cluster manager.
func (fm *FeatureManager) EmbeddedCluster() *EmbeddedCluster {
	fm.embeddedClusterOnce.Do(func() {
		fm.embeddedCluster = NewEmbeddedCluster(fm.db, DefaultEmbeddedClusterConfig())
	})
	return fm.embeddedCluster
}

// SmartRetention returns the smart retention engine.
func (fm *FeatureManager) SmartRetention() *SmartRetentionEngine {
	fm.smartRetentionOnce.Do(func() {
		fm.smartRetention = NewSmartRetentionEngine(fm.db, DefaultSmartRetentionConfig())
	})
	return fm.smartRetention
}

// Dashboard returns the embeddable dashboard.
func (fm *FeatureManager) Dashboard() *EmbeddableDashboard {
	fm.dashboardOnce.Do(func() {
		fm.dashboard = NewEmbeddableDashboard(fm.db, DefaultDashboardConfig())
	})
	return fm.dashboard
}

// LSPEnhanced returns the enhanced LSP server.
func (fm *FeatureManager) LSPEnhanced() *LSPEnhancedServer {
	fm.lspEnhancedOnce.Do(func() {
		fm.lspEnhanced = NewLSPEnhancedServer(fm.db, DefaultLSPEnhancedConfig())
	})
	return fm.lspEnhanced
}

// ETLManager returns the ETL pipeline manager.
func (fm *FeatureManager) ETLManager() *ETLPipelineManager {
	fm.etlManagerOnce.Do(func() {
		fm.etlManager = NewETLPipelineManager(fm.db, DefaultETLPipelineManagerConfig())
	})
	return fm.etlManager
}

// CloudSyncFabric returns the multi-cloud sync fabric.
func (fm *FeatureManager) CloudSyncFabric() *CloudSyncFabric {
	fm.cloudSyncFabricOnce.Do(func() {
		fm.cloudSyncFabric = NewCloudSyncFabric(fm.db, DefaultCloudSyncFabricConfig())
	})
	return fm.cloudSyncFabric
}

// DataMesh returns the data mesh federation engine.
func (fm *FeatureManager) DataMesh() *DataMesh {
	fm.dataMeshOnce.Do(func() {
		fm.dataMesh = NewDataMesh(fm.db, DefaultDataMeshConfig())
	})
	return fm.dataMesh
}

// FoundationModel returns the time-series foundation model engine.
func (fm *FeatureManager) FoundationModel() *FoundationModel {
	fm.foundationModelOnce.Do(func() {
		fm.foundationModel = NewFoundationModel(fm.db, DefaultFoundationModelConfig())
	})
	return fm.foundationModel
}

// DataContracts returns the data contracts engine.
func (fm *FeatureManager) DataContracts() *DataContractEngine {
	fm.dataContractsOnce.Do(func() {
		fm.dataContracts = NewDataContractEngine(fm.db, DefaultDataContractConfig())
	})
	return fm.dataContracts
}

// QueryCache returns the query result cache.
func (fm *FeatureManager) QueryCache() *QueryCache {
	fm.queryCacheOnce.Do(func() {
		fm.queryCache = NewQueryCache(fm.db, DefaultQueryCacheConfig())
	})
	return fm.queryCache
}

// SQLPipelines returns the SQL pipeline engine.
func (fm *FeatureManager) SQLPipelines() *SQLPipelineEngine {
	fm.sqlPipelinesOnce.Do(func() {
		fm.sqlPipelines = NewSQLPipelineEngine(fm.db, DefaultSQLPipelineConfig())
	})
	return fm.sqlPipelines
}

// MultiModelStore returns the multi-model store.
func (fm *FeatureManager) MultiModelStore() *IntegratedMultiModelStore {
	fm.multiModelStoreOnce.Do(func() {
		fm.multiModelStore = NewIntegratedMultiModelStore(fm.db, DefaultIntegratedMultiModelStoreConfig())
	})
	return fm.multiModelStore
}

// AdaptiveOptimizer returns the adaptive query optimizer.
func (fm *FeatureManager) AdaptiveOptimizer() *AdaptiveOptimizer {
	fm.adaptiveOptimizerOnce.Do(func() {
		fm.adaptiveOptimizer = NewAdaptiveOptimizer(fm.db, DefaultAdaptiveOptimizerConfig())
	})
	return fm.adaptiveOptimizer
}

// ComplianceAutomation returns the compliance automation suite.
func (fm *FeatureManager) ComplianceAutomation() *ComplianceAutomation {
	fm.complianceAutomationOnce.Do(func() {
		fm.complianceAutomation = NewComplianceAutomation(fm.db, DefaultComplianceAutomationConfig())
	})
	return fm.complianceAutomation
}

// SchemaDesigner returns the visual schema designer.
func (fm *FeatureManager) SchemaDesigner() *SchemaDesigner {
	fm.schemaDesignerOnce.Do(func() {
		fm.schemaDesigner = NewSchemaDesigner(fm.db, DefaultSchemaDesignerConfig())
	})
	return fm.schemaDesigner
}

// MobileSDK returns the mobile SDK framework.
func (fm *FeatureManager) MobileSDK() *MobileSDK {
	fm.mobileSDKOnce.Do(func() {
		fm.mobileSDK = NewMobileSDK(fm.db, DefaultMobileSDKConfig())
	})
	return fm.mobileSDK
}

// StreamProcessing returns the stream processing engine.
func (fm *FeatureManager) StreamProcessing() *StreamProcessingEngine {
	fm.streamProcessingOnce.Do(func() {
		fm.streamProcessing = NewStreamProcessingEngine(fm.db, DefaultStreamProcessingConfig())
	})
	return fm.streamProcessing
}

// TimeTravelDebug returns the time-travel debug engine.
func (fm *FeatureManager) TimeTravelDebug() *TimeTravelDebugEngine {
	fm.timeTravelDebugOnce.Do(func() {
		fm.timeTravelDebug = NewTimeTravelDebugEngine(fm.db, DefaultTimeTravelDebugConfig())
	})
	return fm.timeTravelDebug
}

// AutoSharding returns the auto-sharding engine.
func (fm *FeatureManager) AutoSharding() *AutoShardingEngine {
	fm.autoShardingOnce.Do(func() {
		fm.autoSharding = NewAutoShardingEngine(fm.db, DefaultAutoShardingConfig())
	})
	return fm.autoSharding
}

// RootCauseAnalysis returns the root cause analysis engine.
func (fm *FeatureManager) RootCauseAnalysis() *RootCauseAnalysisEngine {
	fm.rootCauseAnalysisOnce.Do(func() {
		fm.rootCauseAnalysis = NewRootCauseAnalysisEngine(fm.db, DefaultRootCauseAnalysisConfig())
	})
	return fm.rootCauseAnalysis
}

// CrossCloudTiering returns the cross-cloud tiering engine.
func (fm *FeatureManager) CrossCloudTiering() *CrossCloudTieringEngine {
	fm.crossCloudTieringOnce.Do(func() {
		fm.crossCloudTiering = NewCrossCloudTieringEngine(fm.db, DefaultCrossCloudTieringConfig())
	})
	return fm.crossCloudTiering
}

// DeclarativeAlerting returns the declarative alerting engine.
func (fm *FeatureManager) DeclarativeAlerting() *DeclarativeAlertingEngine {
	fm.declarativeAlertingOnce.Do(func() {
		fm.declarativeAlerting = NewDeclarativeAlertingEngine(fm.db, DefaultDeclarativeAlertingConfig())
	})
	return fm.declarativeAlerting
}

// MetricsCatalog returns the metrics catalog.
func (fm *FeatureManager) MetricsCatalog() *MetricsCatalog {
	fm.metricsCatalogOnce.Do(func() {
		fm.metricsCatalog = NewMetricsCatalog(fm.db, DefaultMetricsCatalogConfig())
	})
	return fm.metricsCatalog
}

// CompressionAdvisor returns the compression advisor.
func (fm *FeatureManager) CompressionAdvisor() *CompressionAdvisor {
	fm.compressionAdvisorOnce.Do(func() {
		fm.compressionAdvisor = NewCompressionAdvisor(fm.db, DefaultCompressionAdvisorConfig())
	})
	return fm.compressionAdvisor
}

// TSDiffMerge returns the time-series diff and merge engine.
func (fm *FeatureManager) TSDiffMerge() *TSDiffMergeEngine {
	fm.tsDiffMergeOnce.Do(func() {
		fm.tsDiffMerge = NewTSDiffMergeEngine(fm.db, DefaultTSDiffMergeConfig())
	})
	return fm.tsDiffMerge
}

// CompliancePacks returns the compliance packs engine.
func (fm *FeatureManager) CompliancePacks() *CompliancePacksEngine {
	fm.compliancePacksOnce.Do(func() {
		fm.compliancePacks = NewCompliancePacksEngine(fm.db, DefaultCompliancePacksConfig())
	})
	return fm.compliancePacks
}

// BlockchainAudit returns the blockchain audit trail.
func (fm *FeatureManager) BlockchainAudit() *BlockchainAuditTrail {
	fm.blockchainAuditOnce.Do(func() {
		fm.blockchainAudit = NewBlockchainAuditTrail(fm.db, DefaultBlockchainAuditConfig())
	})
	return fm.blockchainAudit
}

// ChronicleStudio returns the Chronicle Studio IDE engine.
func (fm *FeatureManager) ChronicleStudio() *ChronicleStudio {
	fm.chronicleStudioOnce.Do(func() {
		fm.chronicleStudio = NewChronicleStudio(fm.db, DefaultChronicleStudioConfig())
	})
	return fm.chronicleStudio
}

// IoTDeviceSDK returns the IoT device SDK manager.
func (fm *FeatureManager) IoTDeviceSDK() *IoTDeviceSDK {
	fm.iotDeviceSDKOnce.Do(func() {
		fm.iotDeviceSDK = NewIoTDeviceSDK(fm.db, DefaultIoTDeviceSDKConfig())
	})
	return fm.iotDeviceSDK
}

// MultiRegionReplication returns the multi-region replication engine.
func (fm *FeatureManager) MultiRegionReplication() *MultiRegionReplicationEngine {
	fm.multiRegionReplicationOnce.Do(func() {
		fm.multiRegionReplication = NewMultiRegionReplicationEngine(fm.db, DefaultMultiRegionReplicationConfig())
	})
	return fm.multiRegionReplication
}

// UniversalSDK returns the universal SDK generator engine.
func (fm *FeatureManager) UniversalSDK() *UniversalSDKEngine {
	fm.universalSDKOnce.Do(func() {
		fm.universalSDK = NewUniversalSDKEngine(fm.db, DefaultUniversalSDKConfig())
	})
	return fm.universalSDK
}

// StudioEnhanced returns the enhanced Chronicle Studio IDE engine.
func (fm *FeatureManager) StudioEnhanced() *StudioEnhancedEngine {
	fm.studioEnhancedOnce.Do(func() {
		fm.studioEnhanced = NewStudioEnhancedEngine(fm.db, DefaultStudioEnhancedConfig())
	})
	return fm.studioEnhanced
}

// SchemaInference returns the smart schema inference engine.
func (fm *FeatureManager) SchemaInference() *SchemaInferenceEngine {
	fm.schemaInferenceOnce.Do(func() {
		fm.schemaInference = NewSchemaInferenceEngine(fm.db, DefaultSchemaInferenceConfig())
	})
	return fm.schemaInference
}

// CloudSaaS returns the Chronicle Cloud SaaS engine.
func (fm *FeatureManager) CloudSaaS() *CloudSaaSEngine {
	fm.cloudSaaSOnce.Do(func() {
		fm.cloudSaaS = NewCloudSaaSEngine(fm.db, DefaultCloudSaaSConfig())
	})
	return fm.cloudSaaS
}

// StreamDSLV2 returns the advanced stream processing DSL engine.
func (fm *FeatureManager) StreamDSLV2() *StreamDSLV2Engine {
	fm.streamDSLV2Once.Do(func() {
		fm.streamDSLV2 = NewStreamDSLV2Engine(fm.db, DefaultStreamDSLV2Config())
	})
	return fm.streamDSLV2
}

// AnomalyExplainability returns the AI-powered anomaly explainability engine.
func (fm *FeatureManager) AnomalyExplainability() *AnomalyExplainabilityEngine {
	fm.anomalyExplainabilityOnce.Do(func() {
		fm.anomalyExplainability = NewAnomalyExplainabilityEngine(fm.db, DefaultAnomalyExplainabilityConfig())
	})
	return fm.anomalyExplainability
}

// HWAcceleratedQuery returns the hardware-accelerated query engine.
func (fm *FeatureManager) HWAcceleratedQuery() *HWAcceleratedQueryEngine {
	fm.hwAcceleratedQueryOnce.Do(func() {
		fm.hwAcceleratedQuery = NewHWAcceleratedQueryEngine(fm.db, DefaultHWAcceleratedQueryConfig())
	})
	return fm.hwAcceleratedQuery
}

// Marketplace returns the plugin marketplace engine.
func (fm *FeatureManager) Marketplace() *MarketplaceEngine {
	fm.marketplaceOnce.Do(func() {
		fm.marketplace = NewMarketplaceEngine(fm.db, DefaultMarketplaceConfig())
	})
	return fm.marketplace
}

// RegulatoryCompliance returns the regulatory compliance automation engine.
func (fm *FeatureManager) RegulatoryCompliance() *RegulatoryComplianceEngine {
	fm.regulatoryComplianceOnce.Do(func() {
		fm.regulatoryCompliance = NewRegulatoryComplianceEngine(fm.db, DefaultRegulatoryComplianceConfig())
	})
	return fm.regulatoryCompliance
}

// ValidatePoint validates a point against registered schemas.
func (fm *FeatureManager) ValidatePoint(p Point) error {
	if fm.schemaRegistry == nil {
		return nil
	}
	return fm.schemaRegistry.Validate(p)
}

// WriteExemplar writes an exemplar point.
func (fm *FeatureManager) WriteExemplar(p ExemplarPoint) error {
	if fm.exemplarStore == nil {
		return nil
	}
	return fm.exemplarStore.Write(p)
}

// WriteHistogram writes a histogram point.
func (fm *FeatureManager) WriteHistogram(p HistogramPoint) error {
	if fm.histogramStore == nil {
		return nil
	}
	return fm.histogramStore.Write(p)
}

// TrackCardinality tracks a point's cardinality.
