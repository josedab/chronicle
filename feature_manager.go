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
	cqlEngine             *CQLEngine
	cqlEngineOnce         sync.Once
	observability         *ObservabilitySuite
	observabilityOnce     sync.Once
	materializedViews     *MaterializedViewEngine
	materializedViewsOnce sync.Once
	chaosInjector         *FaultInjector
	chaosInjectorOnce     sync.Once
	offlineSync           *OfflineSyncManager
	offlineSyncOnce       sync.Once

	// Next-gen v2 features
	anomalyPipeline        *AnomalyPipeline
	anomalyPipelineOnce    sync.Once
	anomalyCorrelation     *AnomalyCorrelationEngine
	anomalyCorrelationOnce sync.Once
	cloudRelay             *CloudRelay
	cloudRelayOnce         sync.Once
	playground             *Playground
	playgroundOnce         sync.Once
	queryPlanner           *QueryPlanner
	queryPlannerOnce       sync.Once
	connectorHub           *ConnectorHub
	connectorHubOnce       sync.Once
	autoscaler             *PredictiveAutoscaler
	autoscalerOnce         sync.Once
	notebookEngine         *NotebookEngine
	notebookEngineOnce     sync.Once
	saasControlPlane       *SaaSControlPlane
	saasControlPlaneOnce   sync.Once
	gitopsEngine           *GitOpsEngine
	gitopsEngineOnce       sync.Once
	federatedML            *FederatedMLTrainer
	federatedMLOnce        sync.Once

	// Next-gen v4 features
	hardeningSuite      *HardeningSuite
	hardeningSuiteOnce  sync.Once
	otelDistro          *OTelDistro
	otelDistroOnce      sync.Once
	embeddedCluster     *EmbeddedCluster
	embeddedClusterOnce sync.Once
	smartRetention      *SmartRetentionEngine
	smartRetentionOnce  sync.Once
	dashboard           *EmbeddableDashboard
	dashboardOnce       sync.Once
	lspEnhanced         *LSPEnhancedServer
	lspEnhancedOnce     sync.Once
	etlManager          *ETLPipelineManager
	etlManagerOnce      sync.Once
	cloudSyncFabric     *CloudSyncFabric
	cloudSyncFabricOnce sync.Once

	// Next-gen v3 features
	edgeMesh              *EdgeMesh
	edgeMeshOnce          sync.Once
	queryCompiler         *QueryCompiler
	queryCompilerOnce     sync.Once
	edgePlatform          *EdgePlatformManager
	edgePlatformOnce      sync.Once
	tsRAG                 *TSRAGEngine
	tsRAGOnce             sync.Once
	pluginRegistry        *PluginRegistry
	pluginRegistryOnce    sync.Once
	pluginMarketplace     *PluginMarketplace
	pluginMarketplaceOnce sync.Once
	matViewV2             *MaterializedViewV2Engine
	matViewV2Once         sync.Once
	clusterReconciler     *ClusterReconciler
	clusterReconcilerOnce sync.Once
	adaptiveV3            *AdaptiveCompressorV3
	adaptiveV3Once        sync.Once
	multiModelGraph       *MultiModelGraphStore
	multiModelGraphOnce   sync.Once
	fleetManager          *SaaSFleetManager
	fleetManagerOnce      sync.Once

	// Next-gen v5 features
	dataMesh                 *DataMesh
	dataMeshOnce             sync.Once
	foundationModel          *FoundationModel
	foundationModelOnce      sync.Once
	dataContracts            *DataContractEngine
	dataContractsOnce        sync.Once
	queryCache               *QueryCache
	queryCacheOnce           sync.Once
	sqlPipelines             *SQLPipelineEngine
	sqlPipelinesOnce         sync.Once
	multiModelStore          *IntegratedMultiModelStore
	multiModelStoreOnce      sync.Once
	adaptiveOptimizer        *AdaptiveOptimizer
	adaptiveOptimizerOnce    sync.Once
	complianceAutomation     *ComplianceAutomation
	complianceAutomationOnce sync.Once
	schemaDesigner           *SchemaDesigner
	schemaDesignerOnce       sync.Once
	mobileSDK                *MobileSDK
	mobileSDKOnce            sync.Once

	// Next-gen v6 features
	streamProcessing        *StreamProcessingEngine
	streamProcessingOnce    sync.Once
	timeTravelDebug         *TimeTravelDebugEngine
	timeTravelDebugOnce     sync.Once
	autoSharding            *AutoShardingEngine
	autoShardingOnce        sync.Once
	rootCauseAnalysis       *RootCauseAnalysisEngine
	rootCauseAnalysisOnce   sync.Once
	crossCloudTiering       *CrossCloudTieringEngine
	crossCloudTieringOnce   sync.Once
	declarativeAlerting     *DeclarativeAlertingEngine
	declarativeAlertingOnce sync.Once
	metricsCatalog          *MetricsCatalog
	metricsCatalogOnce      sync.Once
	compressionAdvisor      *CompressionAdvisor
	compressionAdvisorOnce  sync.Once
	tsDiffMerge             *TSDiffMergeEngine
	tsDiffMergeOnce         sync.Once
	compliancePacks         *CompliancePacksEngine
	compliancePacksOnce     sync.Once

	// Next-gen v7 features
	blockchainAudit     *BlockchainAuditTrail
	blockchainAuditOnce sync.Once
	chronicleStudio     *ChronicleStudio
	chronicleStudioOnce sync.Once
	iotDeviceSDK        *IoTDeviceSDK
	iotDeviceSDKOnce    sync.Once

	// Next-gen v8 features
	multiRegionReplication     *MultiRegionReplicationEngine
	multiRegionReplicationOnce sync.Once
	universalSDK               *UniversalSDKEngine
	universalSDKOnce           sync.Once
	studioEnhanced             *StudioEnhancedEngine
	studioEnhancedOnce         sync.Once
	schemaInference            *SchemaInferenceEngine
	schemaInferenceOnce        sync.Once
	cloudSaaS                  *CloudSaaSEngine
	cloudSaaSOnce              sync.Once
	streamDSLV2                *StreamDSLV2Engine
	streamDSLV2Once            sync.Once
	anomalyExplainability      *AnomalyExplainabilityEngine
	anomalyExplainabilityOnce  sync.Once
	hwAcceleratedQuery         *HWAcceleratedQueryEngine
	hwAcceleratedQueryOnce     sync.Once
	marketplace                *MarketplaceEngine
	marketplaceOnce            sync.Once
	regulatoryCompliance       *RegulatoryComplianceEngine
	regulatoryComplianceOnce   sync.Once

	// Next-gen v9 features
	grpcIngestion       *GRPCIngestionEngine
	grpcIngestionOnce   sync.Once
	clusterEngine       *EmbeddedClusterEngine
	clusterEngineOnce   sync.Once
	anomalyV2           *AnomalyDetectionV2Engine
	anomalyV2Once       sync.Once
	duckdbBackend       *DuckDBBackendEngine
	duckdbBackendOnce   sync.Once
	wasmUDF             *WASMUDFEngine
	wasmUDFOnce         sync.Once
	promDropIn          *PrometheusDropInEngine
	promDropInOnce      sync.Once
	schemaEvolution     *SchemaEvolutionEngine
	schemaEvolutionOnce sync.Once
	edgeCloudFabric     *EdgeCloudFabricEngine
	edgeCloudFabricOnce sync.Once
	queryProfiler       *QueryProfilerEngine
	queryProfilerOnce   sync.Once
	metricsSDK          *MetricsSDKEngine
	metricsSDKOnce      sync.Once

	// Next-gen v10 features
	distributedQuery      *DistributedQueryCoordinator
	distributedQueryOnce  sync.Once
	continuousAgg         *ContinuousAggEngine
	continuousAggOnce     sync.Once
	dataLineage           *DataLineageEngine
	dataLineageOnce       sync.Once
	smartCompaction       *SmartCompactionEngine
	smartCompactionOnce   sync.Once
	metricCorrelation     *MetricCorrelationEngine
	metricCorrelationOnce sync.Once
	adaptiveSampling      *AdaptiveSamplingEngine
	adaptiveSamplingOnce  sync.Once
	tsDiff                *TSDiffEngine
	tsDiffOnce            sync.Once
	dataQuality           *DataQualityEngine
	dataQualityOnce       sync.Once
	queryCost             *QueryCostEstimator
	queryCostOnce         sync.Once
	streamReplay          *StreamReplayEngine
	streamReplayOnce      sync.Once

	// Next-gen v11 features
	tagIndex               *TagInvertedIndex
	tagIndexOnce           sync.Once
	writePipeline          *WritePipelineEngine
	writePipelineOnce      sync.Once
	metricLifecycle        *MetricLifecycleManager
	metricLifecycleOnce    sync.Once
	rateController         *RateControllerEngine
	rateControllerOnce     sync.Once
	hotBackup              *HotBackupEngine
	hotBackupOnce          sync.Once
	crossAlert             *CrossAlertEngine
	crossAlertOnce         sync.Once
	resultCache            *ResultCacheEngine
	resultCacheOnce        sync.Once
	webhookSystem          *WebhookEngine
	webhookSystemOnce      sync.Once
	retentionOptimizer     *RetentionOptimizerEngine
	retentionOptimizerOnce sync.Once
	benchRunner            *BenchRunnerEngine
	benchRunnerOnce        sync.Once

	// Next-gen v12 features
	metricMetadataStore       *MetricMetadataStoreEngine
	metricMetadataStoreOnce   sync.Once
	pointValidator            *PointValidatorEngine
	pointValidatorOnce        sync.Once
	configReload              *ConfigReloadEngine
	configReloadOnce          sync.Once
	healthCheck               *HealthCheckEngine
	healthCheckOnce           sync.Once
	auditLog                  *AuditLogEngine
	auditLogOnce              sync.Once
	seriesDedup               *SeriesDedupEngine
	seriesDedupOnce           sync.Once
	partitionPruner           *PartitionPrunerEngine
	partitionPrunerOnce       sync.Once
	queryMiddleware           *QueryMiddlewareEngine
	queryMiddlewareOnce       sync.Once
	connectionPool            *ConnectionPoolEngine
	connectionPoolOnce        sync.Once
	storageStatsCollector     *StorageStatsEngine
	storageStatsCollectorOnce sync.Once

	// Next-gen v13 features
	wireProtocol          *WireProtocolEngine
	wireProtocolOnce      sync.Once
	walSnapshot           *WALSnapshotEngine
	walSnapshotOnce       sync.Once
	promScraper           *PromScraperEngine
	promScraperOnce       sync.Once
	forecastV2            *ForecastV2Engine
	forecastV2Once        sync.Once
	tenantIsolation       *TenantIsolationEngine
	tenantIsolationOnce   sync.Once
	incrementalBackup     *IncrementalBackupEngine
	incrementalBackupOnce sync.Once
	otlpProto             *OTLPProtoEngine
	otlpProtoOnce         sync.Once
	queryPlanViz          *QueryPlanVizEngine
	queryPlanVizOnce      sync.Once
	dataRehydration       *DataRehydrationEngine
	dataRehydrationOnce   sync.Once
	dataMasking           *DataMaskingEngine
	dataMaskingOnce       sync.Once

	// Next-gen v14 features
	migrationTool     *ImportEngine
	migrationToolOnce sync.Once
	chaosRecovery     *ChaosRecoveryEngine
	chaosRecoveryOnce sync.Once

	// Next-gen v15 features
	featureFlags            *FeatureFlagEngine
	featureFlagsOnce        sync.Once
	selfInstrumentation     *SelfInstrumentationEngine
	selfInstrumentationOnce sync.Once
	deprecation             *DeprecationEngine
	deprecationOnce         sync.Once

	// Stored configs for lazy initialization
	cqlConfig               CQLConfig
	observabilityConfig     ObservabilitySuiteConfig
	materializedViewsConfig MaterializedViewConfig

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
