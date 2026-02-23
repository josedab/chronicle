package chronicle

// feature_manager.go registers all optional features using lazy initialization.
//
// Each feature follows the pattern:
//   1. Struct field + sync.Once in FeatureManager
//   2. Accessor method that initializes on first call
//   3. DB accessor in db_features.go that delegates here
//   4. HTTP route registration in http_routes_nextgen.go
//   5. API stability entry in api_stability.go
//
// To add a new feature, add its definition to scripts/gen_feature_accessors.go
// and run: go generate ./...
// See docs/MODULE_SPLIT.md for the planned modularization.

//go:generate go run scripts/gen_feature_accessors.go -out feature_manager_gen.go

import (
	"log"
	"sync"
)

// safeInit runs fn and recovers from panics, logging the error.
// This prevents a single feature initialization failure from crashing the process.
func safeInit(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("chronicle: feature %q initialization panicked: %v", name, r)
		}
	}()
	fn()
}

// FeatureManager manages optional database features like exemplars,
// histograms, cardinality tracking, and alerting.
type FeatureManager struct {
	db *DB

	exemplarStore      *ExemplarStore
	histogramStore     *HistogramStore
	cardinalityTracker *CardinalityTracker
	alertManager       *AlertManager
	schemaRegistry     *SchemaRegistry

	// Next-gen features
	cqlEngine         *CQLEngine
	cqlEngineOnce        sync.Once
	observability     *ObservabilitySuite
	observabilityOnce                 sync.Once
	materializedViews *MaterializedViewEngine
	materializedViewsOnce                     sync.Once
	chaosInjector     *FaultInjector
	chaosInjectorOnce            sync.Once
	offlineSync       *OfflineSyncManager
	offlineSyncOnce                 sync.Once

	// Next-gen v2 features
	anomalyPipeline    *AnomalyPipeline
	anomalyPipelineOnce              sync.Once
	anomalyCorrelation *AnomalyCorrelationEngine
	anomalyCorrelationOnce                       sync.Once
	cloudRelay         *CloudRelay
	cloudRelayOnce         sync.Once
	playground         *Playground
	playgroundOnce         sync.Once
	queryPlanner       *QueryPlanner
	queryPlannerOnce           sync.Once
	connectorHub       *ConnectorHub
	connectorHubOnce           sync.Once
	autoscaler         *PredictiveAutoscaler
	autoscalerOnce                   sync.Once
	notebookEngine     *NotebookEngine
	notebookEngineOnce             sync.Once
	saasControlPlane   *SaaSControlPlane
	saasControlPlaneOnce               sync.Once
	gitopsEngine       *GitOpsEngine
	gitopsEngineOnce           sync.Once
	federatedML        *FederatedMLTrainer
	federatedMLOnce                 sync.Once

	// Next-gen v4 features
	hardeningSuite  *HardeningSuite
	hardeningSuiteOnce             sync.Once
	otelDistro      *OTelDistro
	otelDistroOnce         sync.Once
	embeddedCluster *EmbeddedCluster
	embeddedClusterOnce              sync.Once
	smartRetention  *SmartRetentionEngine
	smartRetentionOnce                   sync.Once
	dashboard       *EmbeddableDashboard
	dashboardOnce                  sync.Once
	lspEnhanced     *LSPEnhancedServer
	lspEnhancedOnce                sync.Once
	etlManager      *ETLPipelineManager
	etlManagerOnce                 sync.Once
	cloudSyncFabric *CloudSyncFabric
	cloudSyncFabricOnce              sync.Once

	// Next-gen v3 features
	edgeMesh          *EdgeMesh
	edgeMeshOnce       sync.Once
	queryCompiler     *QueryCompiler
	queryCompilerOnce            sync.Once
	edgePlatform      *EdgePlatformManager
	edgePlatformOnce                  sync.Once
	tsRAG             *TSRAGEngine
	tsRAGOnce          sync.Once
	pluginRegistry    *PluginRegistry
	pluginRegistryOnce             sync.Once
	pluginMarketplace *PluginMarketplace
	pluginMarketplaceOnce                sync.Once
	matViewV2         *MaterializedViewV2Engine
	matViewV2Once                       sync.Once
	clusterReconciler *ClusterReconciler
	clusterReconcilerOnce                sync.Once
	adaptiveV3        *AdaptiveCompressorV3
	adaptiveV3Once                   sync.Once
	multiModelGraph   *MultiModelGraphStore
	multiModelGraphOnce                   sync.Once
	fleetManager      *SaaSFleetManager
	fleetManagerOnce               sync.Once

	// Next-gen v5 features
	dataMesh             *DataMesh
	dataMeshOnce       sync.Once
	foundationModel      *FoundationModel
	foundationModelOnce              sync.Once
	dataContracts        *DataContractEngine
	dataContractsOnce                 sync.Once
	queryCache           *QueryCache
	queryCacheOnce         sync.Once
	sqlPipelines         *SQLPipelineEngine
	sqlPipelinesOnce                sync.Once
	multiModelStore      *IntegratedMultiModelStore
	multiModelStoreOnce                        sync.Once
	adaptiveOptimizer    *AdaptiveOptimizer
	adaptiveOptimizerOnce                sync.Once
	complianceAutomation *ComplianceAutomation
	complianceAutomationOnce                   sync.Once
	schemaDesigner       *SchemaDesigner
	schemaDesignerOnce             sync.Once
	mobileSDK            *MobileSDK
	mobileSDKOnce        sync.Once

	// Next-gen v6 features
	streamProcessing    *StreamProcessingEngine
	streamProcessingOnce                     sync.Once
	timeTravelDebug     *TimeTravelDebugEngine
	timeTravelDebugOnce                    sync.Once
	autoSharding        *AutoShardingEngine
	autoShardingOnce                 sync.Once
	rootCauseAnalysis   *RootCauseAnalysisEngine
	rootCauseAnalysisOnce                      sync.Once
	crossCloudTiering   *CrossCloudTieringEngine
	crossCloudTieringOnce                      sync.Once
	declarativeAlerting *DeclarativeAlertingEngine
	declarativeAlertingOnce                        sync.Once
	metricsCatalog      *MetricsCatalog
	metricsCatalogOnce             sync.Once
	compressionAdvisor  *CompressionAdvisor
	compressionAdvisorOnce                 sync.Once
	tsDiffMerge         *TSDiffMergeEngine
	tsDiffMergeOnce                sync.Once
	compliancePacks     *CompliancePacksEngine
	compliancePacksOnce                    sync.Once

	// Next-gen v7 features
	blockchainAudit *BlockchainAuditTrail
	blockchainAuditOnce                   sync.Once
	chronicleStudio *ChronicleStudio
	chronicleStudioOnce              sync.Once
	iotDeviceSDK    *IoTDeviceSDK
	iotDeviceSDKOnce           sync.Once

	// Next-gen v8 features
	multiRegionReplication *MultiRegionReplicationEngine
	multiRegionReplicationOnce                           sync.Once
	universalSDK           *UniversalSDKEngine
	universalSDKOnce                 sync.Once
	studioEnhanced         *StudioEnhancedEngine
	studioEnhancedOnce                   sync.Once
	schemaInference        *SchemaInferenceEngine
	schemaInferenceOnce                    sync.Once
	cloudSaaS              *CloudSaaSEngine
	cloudSaaSOnce              sync.Once
	streamDSLV2            *StreamDSLV2Engine
	streamDSLV2Once                sync.Once
	anomalyExplainability  *AnomalyExplainabilityEngine
	anomalyExplainabilityOnce                          sync.Once
	hwAcceleratedQuery     *HWAcceleratedQueryEngine
	hwAcceleratedQueryOnce                       sync.Once
	marketplace            *MarketplaceEngine
	marketplaceOnce                sync.Once
	regulatoryCompliance   *RegulatoryComplianceEngine
	regulatoryComplianceOnce                         sync.Once

	// Next-gen v9 features
	grpcIngestion        *GRPCIngestionEngine
	grpcIngestionOnce                      sync.Once
	clusterEngine        *EmbeddedClusterEngine
	clusterEngineOnce                       sync.Once
	anomalyV2            *AnomalyDetectionV2Engine
	anomalyV2Once                            sync.Once
	duckdbBackend        *DuckDBBackendEngine
	duckdbBackendOnce                      sync.Once
	wasmUDF              *WASMUDFEngine
	wasmUDFOnce           sync.Once
	promDropIn           *PrometheusDropInEngine
	promDropInOnce                        sync.Once
	schemaEvolution      *SchemaEvolutionEngine
	schemaEvolutionOnce                     sync.Once
	edgeCloudFabric      *EdgeCloudFabricEngine
	edgeCloudFabricOnce                     sync.Once
	queryProfiler        *QueryProfilerEngine
	queryProfilerOnce                   sync.Once
	metricsSDK           *MetricsSDKEngine
	metricsSDKOnce                  sync.Once

	// Next-gen v10 features
	distributedQuery   *DistributedQueryCoordinator
	distributedQueryOnce                          sync.Once
	continuousAgg      *ContinuousAggEngine
	continuousAggOnce                   sync.Once
	dataLineage        *DataLineageEngine
	dataLineageOnce                 sync.Once
	smartCompaction    *SmartCompactionEngine
	smartCompactionOnce                      sync.Once
	metricCorrelation  *MetricCorrelationEngine
	metricCorrelationOnce                       sync.Once
	adaptiveSampling   *AdaptiveSamplingEngine
	adaptiveSamplingOnce                      sync.Once
	tsDiff             *TSDiffEngine
	tsDiffOnce           sync.Once
	dataQuality        *DataQualityEngine
	dataQualityOnce                 sync.Once
	queryCost          *QueryCostEstimator
	queryCostOnce                 sync.Once
	streamReplay       *StreamReplayEngine
	streamReplayOnce                  sync.Once

	// Next-gen v11 features
	tagIndex           *TagInvertedIndex
	tagIndexOnce                    sync.Once
	writePipeline      *WritePipelineEngine
	writePipelineOnce                     sync.Once
	metricLifecycle    *MetricLifecycleManager
	metricLifecycleOnce                       sync.Once
	rateController     *RateControllerEngine
	rateControllerOnce                      sync.Once
	hotBackup          *HotBackupEngine
	hotBackupOnce                  sync.Once
	crossAlert         *CrossAlertEngine
	crossAlertOnce                  sync.Once
	resultCache        *ResultCacheEngine
	resultCacheOnce                   sync.Once
	webhookSystem      *WebhookEngine
	webhookSystemOnce                sync.Once
	retentionOptimizer *RetentionOptimizerEngine
	retentionOptimizerOnce                       sync.Once
	benchRunner        *BenchRunnerEngine
	benchRunnerOnce                  sync.Once

	// Next-gen v12 features
	metricMetadataStore  *MetricMetadataStoreEngine
	metricMetadataStoreOnce                        sync.Once
	pointValidator       *PointValidatorEngine
	pointValidatorOnce                      sync.Once
	configReload         *ConfigReloadEngine
	configReloadOnce                   sync.Once
	healthCheck          *HealthCheckEngine
	healthCheckOnce                  sync.Once
	auditLog             *AuditLogEngine
	auditLogOnce                 sync.Once
	seriesDedup          *SeriesDedupEngine
	seriesDedupOnce                   sync.Once
	partitionPruner      *PartitionPrunerEngine
	partitionPrunerOnce                       sync.Once
	queryMiddleware      *QueryMiddlewareEngine
	queryMiddlewareOnce                       sync.Once
	connectionPool       *ConnectionPoolEngine
	connectionPoolOnce                      sync.Once
	storageStatsCollector *StorageStatsEngine
	storageStatsCollectorOnce                    sync.Once

	// Next-gen v13 features
	wireProtocol          *WireProtocolEngine
	wireProtocolOnce                       sync.Once
	walSnapshot           *WALSnapshotEngine
	walSnapshotOnce                     sync.Once
	promScraper           *PromScraperEngine
	promScraperOnce                    sync.Once
	forecastV2            *ForecastV2Engine
	forecastV2Once                  sync.Once
	tenantIsolation       *TenantIsolationEngine
	tenantIsolationOnce                        sync.Once
	incrementalBackup     *IncrementalBackupEngine
	incrementalBackupOnce                          sync.Once
	otlpProto             *OTLPProtoEngine
	otlpProtoOnce                   sync.Once
	queryPlanViz          *QueryPlanVizEngine
	queryPlanVizOnce                      sync.Once
	dataRehydration       *DataRehydrationEngine
	dataRehydrationOnce                        sync.Once
	dataMasking           *DataMaskingEngine
	dataMaskingOnce                     sync.Once

	// Next-gen v14 features
	migrationTool         *ImportEngine
	migrationToolOnce                    sync.Once
	chaosRecovery         *ChaosRecoveryEngine
	chaosRecoveryOnce                       sync.Once

	// Next-gen v15 features
	featureFlags          *FeatureFlagEngine
	featureFlagsOnce                     sync.Once
	selfInstrumentation   *SelfInstrumentationEngine
	selfInstrumentationOnce                            sync.Once
	deprecation           *DeprecationEngine
	deprecationOnce                     sync.Once

	// Stored configs for lazy initialization
	cqlConfig               CQLConfig
	observabilityConfig      ObservabilitySuiteConfig
	materializedViewsConfig  MaterializedViewConfig

	mu sync.RWMutex
}

// FeatureManagerConfig holds configuration for feature management.
type FeatureManagerConfig struct {
	ExemplarConfig    ExemplarConfig
	CardinalityConfig CardinalityConfig
	StrictSchema      bool
	Schemas           []MetricSchema
	CQL               CQLConfig
	Observability     ObservabilitySuiteConfig
	MaterializedViews MaterializedViewConfig
}

// DefaultFeatureManagerConfig returns sensible defaults for feature management.
func DefaultFeatureManagerConfig() FeatureManagerConfig {
	return FeatureManagerConfig{
		ExemplarConfig:    DefaultExemplarConfig(),
		CardinalityConfig: DefaultCardinalityConfig(),
		StrictSchema:      false,
		CQL:               DefaultCQLConfig(),
		Observability:     DefaultObservabilitySuiteConfig(),
		MaterializedViews: DefaultMaterializedViewConfig(),
	}
}

// NewFeatureManager creates a new feature manager.
func NewFeatureManager(db *DB, cfg FeatureManagerConfig) (*FeatureManager, error) {
	fm := &FeatureManager{
		db: db,
	}

	// Initialize schema registry
	fm.schemaRegistry = NewSchemaRegistry(cfg.StrictSchema)
	for _, schema := range cfg.Schemas {
		if err := fm.schemaRegistry.Register(schema); err != nil {
			return nil, err
		}
	}

	// Initialize feature stores
	fm.exemplarStore = NewExemplarStore(db, cfg.ExemplarConfig)
	fm.histogramStore = NewHistogramStore(db)
	fm.cardinalityTracker = NewCardinalityTracker(db, cfg.CardinalityConfig)
	fm.alertManager = NewAlertManager(db)

	// Store configs for lazy initialization
	fm.cqlConfig = cfg.CQL
	fm.observabilityConfig = cfg.Observability
	fm.materializedViewsConfig = cfg.MaterializedViews

	return fm, nil
}

// Start starts all background feature processes.
func (fm *FeatureManager) Start() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.alertManager != nil {
		fm.alertManager.Start()
	}
	if fm.observability != nil {
		fm.observability.Start()
	}
	if fm.materializedViews != nil {
		fm.materializedViews.Start()
	}
	if fm.offlineSync != nil {
		fm.offlineSync.Start()
	}
	if fm.matViewV2 != nil {
		fm.matViewV2.Start()
	}
	if fm.fleetManager != nil {
		fm.fleetManager.Start()
	}
}

// Stop stops all background feature processes.
func (fm *FeatureManager) Stop() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.alertManager != nil {
		fm.alertManager.Stop()
	}
	if fm.observability != nil {
		fm.observability.Stop()
	}
	if fm.materializedViews != nil {
		fm.materializedViews.Stop()
	}
	if fm.offlineSync != nil {
		fm.offlineSync.Stop()
	}
	if fm.matViewV2 != nil {
		fm.matViewV2.Stop()
	}
	if fm.fleetManager != nil {
		fm.fleetManager.Stop()
	}
	if fm.dataMesh != nil {
		fm.dataMesh.Stop()
	}
	if fm.sqlPipelines != nil {
		fm.sqlPipelines.Stop()
	}
}

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
