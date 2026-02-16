package chronicle

import (
	"fmt"
)

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

// MaterializedViews returns the materialized view engine.
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
