package chronicle

import (
	"sync"
)

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
	observability     *ObservabilitySuite
	materializedViews *MaterializedViewEngine
	chaosInjector     *FaultInjector
	offlineSync       *OfflineSyncManager

	// Next-gen v2 features
	anomalyPipeline    *AnomalyPipeline
	anomalyCorrelation *AnomalyCorrelationEngine
	cloudRelay         *CloudRelay
	playground         *Playground
	queryPlanner       *QueryPlanner
	connectorHub       *ConnectorHub
	autoscaler         *PredictiveAutoscaler
	notebookEngine     *NotebookEngine
	saasControlPlane   *SaaSControlPlane
	gitopsEngine       *GitOpsEngine
	federatedML        *FederatedMLTrainer

	// Next-gen v4 features
	hardeningSuite   *HardeningSuite
	otelDistro       *OTelDistro
	embeddedCluster  *EmbeddedCluster
	smartRetention   *SmartRetentionEngine
	dashboard        *EmbeddableDashboard
	lspEnhanced      *LSPEnhancedServer
	etlManager       *ETLPipelineManager
	cloudSyncFabric  *CloudSyncFabric

	// Next-gen v3 features
	edgeMesh           *EdgeMesh
	queryCompiler      *QueryCompiler
	edgePlatform       *EdgePlatformManager
	tsRAG              *TSRAGEngine
	pluginRegistry     *PluginRegistry
	pluginMarketplace  *PluginMarketplace
	matViewV2          *MaterializedViewV2Engine
	clusterReconciler  *ClusterReconciler
	adaptiveV3         *AdaptiveCompressorV3
	multiModelGraph    *MultiModelGraphStore
	fleetManager       *SaaSFleetManager

	mu sync.RWMutex
}

// FeatureManagerConfig holds configuration for feature management.
type FeatureManagerConfig struct {
	ExemplarConfig     ExemplarConfig
	CardinalityConfig  CardinalityConfig
	StrictSchema       bool
	Schemas            []MetricSchema
	CQL                CQLConfig
	Observability      ObservabilitySuiteConfig
	MaterializedViews  MaterializedViewConfig
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

	// Initialize next-gen features
	fm.cqlEngine = NewCQLEngine(db, cfg.CQL)
	fm.observability = NewObservabilitySuite(cfg.Observability)
	fm.materializedViews = NewMaterializedViewEngine(db, cfg.MaterializedViews)
	fm.chaosInjector = NewFaultInjector(DefaultChaosConfig())
	fm.offlineSync = NewOfflineSyncManager(DefaultOfflineSyncConfig())

	// Initialize next-gen v2 features
	fm.anomalyPipeline = NewAnomalyPipeline(db, DefaultAnomalyPipelineConfig())
	fm.anomalyCorrelation = NewAnomalyCorrelationEngine(db, DefaultAnomalyCorrelationConfig())
	fm.cloudRelay = NewCloudRelay(db, DefaultCloudRelayConfig())
	fm.playground = NewPlayground(db, DefaultPlaygroundConfig())
	fm.queryPlanner = NewQueryPlanner(db, DefaultQueryPlannerConfig())
	fm.connectorHub = NewConnectorHub(db, DefaultConnectorHubConfig())
	fm.autoscaler = NewPredictiveAutoscaler(db, DefaultPredictiveAutoscalingConfig())
	fm.notebookEngine = NewNotebookEngine(db, DefaultNotebookConfig())
	fm.saasControlPlane = NewSaaSControlPlane(db, DefaultSaaSControlPlaneConfig())
	fm.gitopsEngine = NewGitOpsEngine(db, DefaultGitOpsConfig())
	fm.federatedML = NewFederatedMLTrainer(db, DefaultFederatedMLConfig())

	// Initialize next-gen v3 features
	fm.queryCompiler = NewQueryCompiler(db, DefaultQueryCompilerConfig())
	fm.edgePlatform = NewEdgePlatformManager()
	fm.tsRAG = NewTSRAGEngine(db, DefaultTSRAGConfig())
	fm.pluginRegistry = NewPluginRegistry(DefaultPluginSDKConfig())
	fm.pluginMarketplace = NewPluginMarketplace(DefaultPluginSDKConfig().MarketplaceURL, fm.pluginRegistry)
	fm.matViewV2 = NewMaterializedViewV2Engine(db, DefaultMaterializedViewV2Config())
	fm.clusterReconciler = NewClusterReconciler()
	fm.adaptiveV3 = NewAdaptiveCompressorV3(DefaultAdaptiveCompressionV3Config())
	fm.multiModelGraph = NewMultiModelGraphStore(db)
	fm.fleetManager = NewSaaSFleetManager(DefaultSaaSFleetConfig())

	// Initialize next-gen v4 features
	fm.hardeningSuite = NewHardeningSuite(db, DefaultHardeningConfig())
	fm.otelDistro = NewOTelDistro(db, DefaultOTelDistroConfig())
	fm.embeddedCluster = NewEmbeddedCluster(db, DefaultEmbeddedClusterConfig())
	fm.smartRetention = NewSmartRetentionEngine(db, DefaultSmartRetentionConfig())
	fm.dashboard = NewEmbeddableDashboard(db, DefaultDashboardConfig())
	fm.lspEnhanced = NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())
	fm.etlManager = NewETLPipelineManager(db, DefaultETLPipelineManagerConfig())
	fm.cloudSyncFabric = NewCloudSyncFabric(db, DefaultCloudSyncFabricConfig())

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
func (fm *FeatureManager) CQLEngine() *CQLEngine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.cqlEngine
}

// Observability returns the observability suite.
func (fm *FeatureManager) Observability() *ObservabilitySuite {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.observability
}

// MaterializedViews returns the materialized view engine.
func (fm *FeatureManager) MaterializedViews() *MaterializedViewEngine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.materializedViews
}

// ChaosInjector returns the fault injector.
func (fm *FeatureManager) ChaosInjector() *FaultInjector {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.chaosInjector
}

// OfflineSync returns the offline sync manager.
func (fm *FeatureManager) OfflineSync() *OfflineSyncManager {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.offlineSync
}

// AnomalyPipeline returns the streaming anomaly detection pipeline.
func (fm *FeatureManager) AnomalyPipeline() *AnomalyPipeline {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.anomalyPipeline
}

// AnomalyCorrelation returns the anomaly correlation engine.
func (fm *FeatureManager) AnomalyCorrelation() *AnomalyCorrelationEngine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.anomalyCorrelation
}

// CloudRelay returns the cloud relay agent.
func (fm *FeatureManager) CloudRelay() *CloudRelay {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.cloudRelay
}

// Playground returns the query playground.
func (fm *FeatureManager) Playground() *Playground {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.playground
}

// QueryPlanner returns the adaptive query planner.
func (fm *FeatureManager) QueryPlanner() *QueryPlanner {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.queryPlanner
}

// ConnectorHub returns the connector hub.
func (fm *FeatureManager) ConnectorHub() *ConnectorHub {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.connectorHub
}

// Autoscaler returns the predictive autoscaler.
func (fm *FeatureManager) Autoscaler() *PredictiveAutoscaler {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.autoscaler
}

// NotebookEngine returns the notebook engine.
func (fm *FeatureManager) NotebookEngine() *NotebookEngine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.notebookEngine
}

// SaaSControlPlane returns the SaaS control plane.
func (fm *FeatureManager) SaaSControlPlane() *SaaSControlPlane {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.saasControlPlane
}

// GitOpsEngine returns the GitOps engine.
func (fm *FeatureManager) GitOpsEngine() *GitOpsEngine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.gitopsEngine
}

// FederatedMLTrainer returns the federated ML trainer.
func (fm *FeatureManager) FederatedMLTrainer() *FederatedMLTrainer {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.federatedML
}

// EdgeMesh returns the edge mesh network manager.
func (fm *FeatureManager) EdgeMesh() *EdgeMesh {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.edgeMesh
}

// QueryCompiler returns the unified query compiler.
func (fm *FeatureManager) QueryCompiler() *QueryCompiler {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.queryCompiler
}

// EdgePlatform returns the edge platform manager.
func (fm *FeatureManager) EdgePlatform() *EdgePlatformManager {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.edgePlatform
}

// TSRAG returns the time-series RAG engine.
func (fm *FeatureManager) TSRAG() *TSRAGEngine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.tsRAG
}

// PluginRegistry returns the plugin registry.
func (fm *FeatureManager) PluginRegistry() *PluginRegistry {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.pluginRegistry
}

// PluginMarketplace returns the plugin marketplace.
func (fm *FeatureManager) PluginMarketplace() *PluginMarketplace {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.pluginMarketplace
}

// MaterializedViewsV2 returns the v2 materialized view engine.
func (fm *FeatureManager) MaterializedViewsV2() *MaterializedViewV2Engine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.matViewV2
}

// ClusterReconciler returns the K8s cluster reconciler.
func (fm *FeatureManager) ClusterReconciler() *ClusterReconciler {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.clusterReconciler
}

// AdaptiveCompressorV3 returns the ML-driven adaptive compressor.
func (fm *FeatureManager) AdaptiveCompressorV3() *AdaptiveCompressorV3 {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.adaptiveV3
}

// MultiModelGraph returns the multi-model graph+document store.
func (fm *FeatureManager) MultiModelGraph() *MultiModelGraphStore {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.multiModelGraph
}

// FleetManager returns the fleet manager.
func (fm *FeatureManager) FleetManager() *SaaSFleetManager {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.fleetManager
}

// HardeningSuite returns the production hardening suite.
func (fm *FeatureManager) HardeningSuite() *HardeningSuite {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.hardeningSuite
}

// OTelDistro returns the OpenTelemetry distribution.
func (fm *FeatureManager) OTelDistro() *OTelDistro {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.otelDistro
}

// EmbeddedCluster returns the embedded cluster manager.
func (fm *FeatureManager) EmbeddedCluster() *EmbeddedCluster {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.embeddedCluster
}

// SmartRetention returns the smart retention engine.
func (fm *FeatureManager) SmartRetention() *SmartRetentionEngine {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.smartRetention
}

// Dashboard returns the embeddable dashboard.
func (fm *FeatureManager) Dashboard() *EmbeddableDashboard {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.dashboard
}

// LSPEnhanced returns the enhanced LSP server.
func (fm *FeatureManager) LSPEnhanced() *LSPEnhancedServer {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.lspEnhanced
}

// ETLManager returns the ETL pipeline manager.
func (fm *FeatureManager) ETLManager() *ETLPipelineManager {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.etlManager
}

// CloudSyncFabric returns the multi-cloud sync fabric.
func (fm *FeatureManager) CloudSyncFabric() *CloudSyncFabric {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.cloudSyncFabric
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
