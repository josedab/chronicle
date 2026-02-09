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
