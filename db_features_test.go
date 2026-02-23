package chronicle

import (
	"reflect"
	"testing"
)

// TestDBFeaturesNilFeatureManager verifies that all feature accessor methods
// that delegate through db.features return nil when the feature manager is nil.
func TestDBFeaturesNilFeatureManager(t *testing.T) {
	db := &DB{} // features is nil

	nilChecks := []struct {
		name string
		fn   func() interface{}
	}{
		{"CQLEngine", func() interface{} { return db.CQLEngine() }},
		{"Observability", func() interface{} { return db.Observability() }},
		{"MaterializedViews", func() interface{} { return db.MaterializedViews() }},
		{"AnomalyPipeline", func() interface{} { return db.AnomalyPipeline() }},
		{"AnomalyCorrelation", func() interface{} { return db.AnomalyCorrelation() }},
		{"QueryPlanner", func() interface{} { return db.QueryPlanner() }},
		{"ConnectorHub", func() interface{} { return db.ConnectorHub() }},
		{"NotebookEngine", func() interface{} { return db.NotebookEngine() }},
		{"QueryCompiler", func() interface{} { return db.QueryCompiler() }},
		{"TSRAG", func() interface{} { return db.TSRAG() }},
		{"PluginRegistry", func() interface{} { return db.PluginRegistry() }},
		{"MaterializedViewsV2", func() interface{} { return db.MaterializedViewsV2() }},
		{"MultiModelGraph", func() interface{} { return db.MultiModelGraph() }},
		{"FleetManager", func() interface{} { return db.FleetManager() }},
		{"HardeningSuite", func() interface{} { return db.HardeningSuite() }},
		{"OTelDistro", func() interface{} { return db.OTelDistro() }},
		{"EmbeddedCluster", func() interface{} { return db.EmbeddedCluster() }},
		{"SmartRetention", func() interface{} { return db.SmartRetention() }},
		{"Dashboard", func() interface{} { return db.Dashboard() }},
		{"LSPEnhanced", func() interface{} { return db.LSPEnhanced() }},
		{"ETLManager", func() interface{} { return db.ETLManager() }},
		{"CloudSyncFabric", func() interface{} { return db.CloudSyncFabric() }},
		{"DataMesh", func() interface{} { return db.DataMesh() }},
		{"FoundationModel", func() interface{} { return db.FoundationModel() }},
		{"DataContracts", func() interface{} { return db.DataContracts() }},
		{"QueryCache", func() interface{} { return db.QueryCache() }},
		{"SQLPipelines", func() interface{} { return db.SQLPipelines() }},
		{"MultiModelStore", func() interface{} { return db.MultiModelStore() }},
		{"AdaptiveOptimizer", func() interface{} { return db.AdaptiveOptimizer() }},
		{"ComplianceAutomation", func() interface{} { return db.ComplianceAutomation() }},
		{"SchemaDesigner", func() interface{} { return db.SchemaDesigner() }},
		{"MobileSDK", func() interface{} { return db.MobileSDK() }},
		{"StreamProcessing", func() interface{} { return db.StreamProcessing() }},
		{"TimeTravelDebug", func() interface{} { return db.TimeTravelDebug() }},
		{"AutoSharding", func() interface{} { return db.AutoSharding() }},
		{"RootCauseAnalysis", func() interface{} { return db.RootCauseAnalysis() }},
		{"CrossCloudTiering", func() interface{} { return db.CrossCloudTiering() }},
		{"DeclarativeAlerting", func() interface{} { return db.DeclarativeAlerting() }},
		{"MetricsCatalog", func() interface{} { return db.MetricsCatalog() }},
		{"CompressionAdvisor", func() interface{} { return db.CompressionAdvisor() }},
		{"TSDiffMerge", func() interface{} { return db.TSDiffMerge() }},
		{"CompliancePacks", func() interface{} { return db.CompliancePacks() }},
		{"BlockchainAudit", func() interface{} { return db.BlockchainAudit() }},
		{"ChronicleStudio", func() interface{} { return db.ChronicleStudio() }},
		{"IoTDeviceSDK", func() interface{} { return db.IoTDeviceSDK() }},
		{"MultiRegionReplication", func() interface{} { return db.MultiRegionReplication() }},
		{"UniversalSDK", func() interface{} { return db.UniversalSDK() }},
		{"StudioEnhanced", func() interface{} { return db.StudioEnhanced() }},
		{"SchemaInference", func() interface{} { return db.SchemaInference() }},
		{"CloudSaaS", func() interface{} { return db.CloudSaaS() }},
		{"StreamDSLV2", func() interface{} { return db.StreamDSLV2() }},
		{"AnomalyExplainability", func() interface{} { return db.AnomalyExplainability() }},
		{"HWAcceleratedQuery", func() interface{} { return db.HWAcceleratedQuery() }},
		{"Marketplace", func() interface{} { return db.Marketplace() }},
		{"RegulatoryCompliance", func() interface{} { return db.RegulatoryCompliance() }},
		{"GRPCIngestion", func() interface{} { return db.GRPCIngestion() }},
		{"ClusterEngine", func() interface{} { return db.ClusterEngine() }},
		{"AnomalyV2", func() interface{} { return db.AnomalyV2() }},
		{"DuckDBBackend", func() interface{} { return db.DuckDBBackend() }},
		{"WASMUDF", func() interface{} { return db.WASMUDF() }},
		{"PromDropIn", func() interface{} { return db.PromDropIn() }},
		{"SchemaEvolution", func() interface{} { return db.SchemaEvolution() }},
		{"EdgeCloudFabric", func() interface{} { return db.EdgeCloudFabric() }},
		{"QueryProfiler", func() interface{} { return db.QueryProfiler() }},
		{"MetricsSDK", func() interface{} { return db.MetricsSDK() }},
		{"DistributedQuery", func() interface{} { return db.DistributedQuery() }},
		{"ContinuousAgg", func() interface{} { return db.ContinuousAgg() }},
		{"DataLineage", func() interface{} { return db.DataLineage() }},
		{"SmartCompaction", func() interface{} { return db.SmartCompaction() }},
		{"MetricCorrelation", func() interface{} { return db.MetricCorrelation() }},
		{"AdaptiveSampling", func() interface{} { return db.AdaptiveSampling() }},
		{"TSDiff", func() interface{} { return db.TSDiff() }},
		{"DataQuality", func() interface{} { return db.DataQuality() }},
		{"QueryCost", func() interface{} { return db.QueryCost() }},
		{"StreamReplay", func() interface{} { return db.StreamReplay() }},
		{"TagIndex", func() interface{} { return db.TagIndex() }},
		{"WritePipeline", func() interface{} { return db.WritePipeline() }},
		{"MetricLifecycle", func() interface{} { return db.MetricLifecycle() }},
		{"RateController", func() interface{} { return db.RateController() }},
		{"HotBackup", func() interface{} { return db.HotBackup() }},
		{"CrossAlert", func() interface{} { return db.CrossAlert() }},
		{"ResultCache", func() interface{} { return db.ResultCache() }},
		{"WebhookSystem", func() interface{} { return db.WebhookSystem() }},
		{"RetentionOptimizer", func() interface{} { return db.RetentionOptimizer() }},
		{"BenchRunner", func() interface{} { return db.BenchRunner() }},
		{"MetricMetadataStore", func() interface{} { return db.MetricMetadataStore() }},
		{"PointValidator", func() interface{} { return db.PointValidator() }},
		{"ConfigReload", func() interface{} { return db.ConfigReload() }},
		{"HealthCheck", func() interface{} { return db.HealthCheck() }},
		{"AuditLog", func() interface{} { return db.AuditLog() }},
		{"SeriesDedup", func() interface{} { return db.SeriesDedup() }},
		{"PartitionPruner", func() interface{} { return db.PartitionPruner() }},
		{"QueryMiddleware", func() interface{} { return db.QueryMiddleware() }},
		{"ConnectionPool", func() interface{} { return db.ConnectionPool() }},
		{"StorageStatsCollector", func() interface{} { return db.StorageStatsCollector() }},
		{"WireProtocol", func() interface{} { return db.WireProtocol() }},
		{"WALSnapshot", func() interface{} { return db.WALSnapshot() }},
		{"PromScraper", func() interface{} { return db.PromScraper() }},
		{"ForecastV2", func() interface{} { return db.ForecastV2() }},
		{"TenantIsolation", func() interface{} { return db.TenantIsolation() }},
		{"IncrementalBackup", func() interface{} { return db.IncrementalBackup() }},
		{"OTLPProto", func() interface{} { return db.OTLPProto() }},
		{"QueryPlanViz", func() interface{} { return db.QueryPlanViz() }},
		{"DataRehydration", func() interface{} { return db.DataRehydration() }},
		{"DataMasking", func() interface{} { return db.DataMasking() }},
		{"MigrationTool", func() interface{} { return db.MigrationTool() }},
		{"ChaosRecovery", func() interface{} { return db.ChaosRecovery() }},
		{"FeatureFlags", func() interface{} { return db.FeatureFlags() }},
		{"SelfInstrumentation", func() interface{} { return db.SelfInstrumentation() }},
		{"Deprecation", func() interface{} { return db.Deprecation() }},
	}

	for _, tc := range nilChecks {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.fn()
			if v != nil && !reflect.ValueOf(v).IsNil() {
				t.Errorf("%s: expected nil, got %v", tc.name, v)
			}
		})
	}
}

// TestDBFeaturesWithDB verifies that feature accessors return non-nil
// values when the DB is properly initialized via Open.
func TestDBFeaturesWithDB(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if db.Features() == nil {
		t.Fatal("Features() should not be nil on initialized DB")
	}

	// Spot-check a few feature delegation methods that should be initialized.
	checks := []struct {
		name string
		fn   func() interface{}
	}{
		{"CQLEngine", func() interface{} { return db.CQLEngine() }},
		{"Observability", func() interface{} { return db.Observability() }},
		{"QueryCompiler", func() interface{} { return db.QueryCompiler() }},
		{"HealthCheck", func() interface{} { return db.HealthCheck() }},
		{"AuditLog", func() interface{} { return db.AuditLog() }},
	}

	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			if v := tc.fn(); v == nil {
				t.Errorf("%s: expected non-nil on initialized DB", tc.name)
			}
		})
	}
}

// TestDBFeaturesRegistry verifies that Registry() returns a non-nil
// feature registry on a properly initialized DB.
func TestDBFeaturesRegistry(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if db.Registry() == nil {
		t.Fatal("Registry() should not be nil on initialized DB")
	}
}

// TestDBFeaturesDirectAccess verifies direct field accessors return
// non-nil values on a properly initialized DB.
func TestDBFeaturesDirectAccess(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if db.ExemplarStore() == nil {
		t.Error("ExemplarStore() should not be nil")
	}
	if db.HistogramStore() == nil {
		t.Error("HistogramStore() should not be nil")
	}
	if db.CardinalityTracker() == nil {
		t.Error("CardinalityTracker() should not be nil")
	}
	if db.AlertManager() == nil {
		t.Error("AlertManager() should not be nil")
	}
}

// TestDBCardinalityStatsNilTracker verifies that CardinalityStats returns
// a zero-value struct when the cardinality tracker is nil.
func TestDBCardinalityStatsNilTracker(t *testing.T) {
	db := &DB{} // cardinalityTracker is nil

	stats := db.CardinalityStats()
	if stats.TotalSeries != 0 || stats.MetricCount != 0 || stats.TopMetrics != nil {
		t.Errorf("expected zero CardinalityStats, got %+v", stats)
	}
}

// TestDBWriteHistogramNoFeatures verifies that WriteHistogram and
// WriteExemplar return errors when the underlying stores are nil.
func TestDBWriteHistogramNoFeatures(t *testing.T) {
	db := &DB{} // histogramStore and exemplarStore are nil

	if err := db.WriteHistogram(HistogramPoint{}); err == nil {
		t.Error("WriteHistogram: expected error when histogram store is nil")
	}

	if err := db.WriteExemplar(ExemplarPoint{}); err == nil {
		t.Error("WriteExemplar: expected error when exemplar store is nil")
	}
}
