package chronicle

import (
	"reflect"
	"sync"
	"testing"
)

// TestFeatureAccessorsAfterOpen verifies that all feature accessors return
// non-nil values (or expected defaults) after a normal Open().
func TestFeatureAccessorsAfterOpen(t *testing.T) {
	db := setupTestDB(t)

	// Table-driven test for all accessor methods that return a pointer.
	// Each accessor should return non-nil after Open().
	accessors := []struct {
		name   string
		result interface{}
	}{
		{"Registry", db.Registry()},
		{"Features", db.Features()},
		{"ExemplarStore", db.ExemplarStore()},
		{"HistogramStore", db.HistogramStore()},
		{"CardinalityTracker", db.CardinalityTracker()},
		{"AlertManager", db.AlertManager()},
	}

	for _, tc := range accessors {
		t.Run(tc.name, func(t *testing.T) {
			if tc.result == nil || reflect.ValueOf(tc.result).IsNil() {
				t.Errorf("%s() returned nil after Open()", tc.name)
			}
		})
	}
}

// TestFeatureAccessorsViaDelegation verifies feature accessors that delegate
// to db.features return non-nil when features is non-nil.
func TestFeatureAccessorsViaDelegation(t *testing.T) {
	db := setupTestDB(t)

	if db.Features() == nil {
		t.Fatal("Features() is nil, cannot test delegation")
	}

	// These accessors delegate to db.features and may return nil if the
	// sub-feature was not initialized. We just verify they don't panic.
	delegatedAccessors := []string{
		"CQLEngine",
		"Observability",
		"MaterializedViews",
		"AnomalyPipeline",
		"AnomalyCorrelation",
		"QueryPlanner",
		"ConnectorHub",
		"NotebookEngine",
		"QueryCompiler",
		"TSRAG",
		"PluginRegistry",
		"MaterializedViewsV2",
		"MultiModelGraph",
		"FleetManager",
		"HardeningSuite",
		"OTelDistro",
		"EmbeddedCluster",
		"SmartRetention",
		"Dashboard",
		"LSPEnhanced",
		"ETLManager",
		"CloudSyncFabric",
		"DataMesh",
		"FoundationModel",
		"DataContracts",
		"QueryCache",
		"SQLPipelines",
		"MultiModelStore",
		"AdaptiveOptimizer",
		"ComplianceAutomation",
		"SchemaDesigner",
		"MobileSDK",
		"StreamProcessing",
		"TimeTravelDebug",
		"AutoSharding",
		"RootCauseAnalysis",
		"CrossCloudTiering",
		"DeclarativeAlerting",
		"MetricsCatalog",
		"CompressionAdvisor",
		"TSDiffMerge",
		"CompliancePacks",
		"BlockchainAudit",
		"ChronicleStudio",
		"IoTDeviceSDK",
	}

	dbVal := reflect.ValueOf(db)
	for _, name := range delegatedAccessors {
		t.Run(name, func(t *testing.T) {
			method := dbVal.MethodByName(name)
			if !method.IsValid() {
				t.Fatalf("Method %s not found on DB", name)
			}
			// Call the method and ensure it doesn't panic.
			results := method.Call(nil)
			if len(results) != 1 {
				t.Fatalf("Expected 1 return value, got %d", len(results))
			}
		})
	}
}

// TestFeatureAccessorsNilFeatures verifies accessors return nil when
// db.features is nil (simulating uninitialized state).
func TestFeatureAccessorsNilFeatures(t *testing.T) {
	db := &DB{} // Zero-value DB, features is nil.

	nilAccessors := []struct {
		name   string
		result interface{}
	}{
		{"CQLEngine", db.CQLEngine()},
		{"Observability", db.Observability()},
		{"MaterializedViews", db.MaterializedViews()},
		{"AnomalyPipeline", db.AnomalyPipeline()},
		{"AnomalyCorrelation", db.AnomalyCorrelation()},
		{"QueryPlanner", db.QueryPlanner()},
		{"ConnectorHub", db.ConnectorHub()},
		{"NotebookEngine", db.NotebookEngine()},
		{"QueryCompiler", db.QueryCompiler()},
		{"TSRAG", db.TSRAG()},
		{"PluginRegistry", db.PluginRegistry()},
		{"MaterializedViewsV2", db.MaterializedViewsV2()},
		{"MultiModelGraph", db.MultiModelGraph()},
		{"FleetManager", db.FleetManager()},
		{"HardeningSuite", db.HardeningSuite()},
		{"OTelDistro", db.OTelDistro()},
		{"EmbeddedCluster", db.EmbeddedCluster()},
		{"SmartRetention", db.SmartRetention()},
		{"Dashboard", db.Dashboard()},
		{"LSPEnhanced", db.LSPEnhanced()},
		{"ETLManager", db.ETLManager()},
		{"CloudSyncFabric", db.CloudSyncFabric()},
		{"DataMesh", db.DataMesh()},
		{"FoundationModel", db.FoundationModel()},
		{"DataContracts", db.DataContracts()},
		{"QueryCache", db.QueryCache()},
		{"SQLPipelines", db.SQLPipelines()},
		{"MultiModelStore", db.MultiModelStore()},
		{"AdaptiveOptimizer", db.AdaptiveOptimizer()},
		{"ComplianceAutomation", db.ComplianceAutomation()},
		{"SchemaDesigner", db.SchemaDesigner()},
		{"MobileSDK", db.MobileSDK()},
		{"StreamProcessing", db.StreamProcessing()},
		{"TimeTravelDebug", db.TimeTravelDebug()},
		{"AutoSharding", db.AutoSharding()},
		{"RootCauseAnalysis", db.RootCauseAnalysis()},
		{"CrossCloudTiering", db.CrossCloudTiering()},
		{"DeclarativeAlerting", db.DeclarativeAlerting()},
		{"MetricsCatalog", db.MetricsCatalog()},
		{"CompressionAdvisor", db.CompressionAdvisor()},
		{"TSDiffMerge", db.TSDiffMerge()},
		{"CompliancePacks", db.CompliancePacks()},
		{"BlockchainAudit", db.BlockchainAudit()},
		{"ChronicleStudio", db.ChronicleStudio()},
		{"IoTDeviceSDK", db.IoTDeviceSDK()},
	}

	for _, tc := range nilAccessors {
		t.Run(tc.name+"_nil", func(t *testing.T) {
			v := reflect.ValueOf(tc.result)
			if v.IsValid() && !v.IsNil() {
				t.Errorf("%s() should return nil when features is nil", tc.name)
			}
		})
	}
}

// TestFeatureAccessorsConcurrency verifies thread-safety of accessor calls.
func TestFeatureAccessorsConcurrency(t *testing.T) {
	db := setupTestDB(t)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Registry()
			_ = db.Features()
			_ = db.ExemplarStore()
			_ = db.HistogramStore()
			_ = db.CardinalityTracker()
			_ = db.AlertManager()
			_ = db.CQLEngine()
			_ = db.Observability()
			_ = db.QueryPlanner()
			_ = db.ConnectorHub()
			_ = db.QueryCompiler()
			_ = db.TSRAG()
			_ = db.PluginRegistry()
		}()
	}
	wg.Wait()
}

// TestCardinalityStatsNilTracker verifies CardinalityStats returns zero value.
func TestCardinalityStatsNilTracker(t *testing.T) {
	db := &DB{}
	stats := db.CardinalityStats()
	if stats.TotalSeries != 0 {
		t.Errorf("Expected zero stats, got TotalSeries=%d", stats.TotalSeries)
	}
}

// TestWriteHistogramNilStore verifies error when histogramStore is nil.
func TestWriteHistogramNilStore(t *testing.T) {
	db := &DB{}
	err := db.WriteHistogram(HistogramPoint{})
	if err == nil {
		t.Error("Expected error when histogram store is nil")
	}
}

// TestWriteExemplarNilStore verifies error when exemplarStore is nil.
func TestWriteExemplarNilStore(t *testing.T) {
	db := &DB{}
	err := db.WriteExemplar(ExemplarPoint{})
	if err == nil {
		t.Error("Expected error when exemplar store is nil")
	}
}
