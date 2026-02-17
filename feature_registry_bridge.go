package chronicle

import "log"

// registeredCQLEngine wraps CQLEngine to implement the Feature interface.
type registeredCQLEngine struct {
	engine *CQLEngine
}

func (r *registeredCQLEngine) Name() string          { return "cql" }
func (r *registeredCQLEngine) Status() FeatureStatus {
	if r.engine != nil {
		return FeatureStatusActive
	}
	return FeatureStatusInactive
}

// registeredObservability wraps ObservabilitySuite to implement Feature + StartableFeature.
type registeredObservability struct {
	suite *ObservabilitySuite
}

func (r *registeredObservability) Name() string          { return "observability" }
func (r *registeredObservability) Status() FeatureStatus {
	if r.suite != nil {
		return FeatureStatusActive
	}
	return FeatureStatusInactive
}
func (r *registeredObservability) Start() {
	if r.suite != nil {
		r.suite.Start()
	}
}
func (r *registeredObservability) Stop() {
	if r.suite != nil {
		r.suite.Stop()
	}
}

// registeredAnomalyPipeline wraps AnomalyPipeline to implement Feature.
type registeredAnomalyPipeline struct {
	pipeline *AnomalyPipeline
}

func (r *registeredAnomalyPipeline) Name() string          { return "anomaly_pipeline" }
func (r *registeredAnomalyPipeline) Status() FeatureStatus {
	if r.pipeline != nil {
		return FeatureStatusActive
	}
	return FeatureStatusInactive
}

// registeredQueryPlanner wraps QueryPlanner to implement Feature.
type registeredQueryPlanner struct {
	planner *QueryPlanner
}

func (r *registeredQueryPlanner) Name() string          { return "query_planner" }
func (r *registeredQueryPlanner) Status() FeatureStatus {
	if r.planner != nil {
		return FeatureStatusActive
	}
	return FeatureStatusInactive
}

// registeredDashboard wraps EmbeddableDashboard to implement Feature + HTTPFeature.
type registeredDashboard struct {
	dashboard *EmbeddableDashboard
}

func (r *registeredDashboard) Name() string          { return "dashboard" }
func (r *registeredDashboard) Status() FeatureStatus {
	if r.dashboard != nil {
		return FeatureStatusActive
	}
	return FeatureStatusInactive
}

// RegisterCoreFeatures registers the 5 core features in the FeatureRegistry.
// This is called during DB initialization and bridges the FeatureManager
// to the new registry pattern. Over time, features will be created directly
// via the registry instead of through FeatureManager.
func RegisterCoreFeatures(db *DB) {
	if db.registry == nil || db.features == nil {
		return
	}

	// These access FeatureManager lazily — the features init on first access
	for _, f := range []Feature{
		&registeredCQLEngine{engine: db.features.CQLEngine()},
		&registeredObservability{suite: db.features.Observability()},
		&registeredAnomalyPipeline{pipeline: db.features.AnomalyPipeline()},
		&registeredQueryPlanner{planner: db.features.QueryPlanner()},
		&registeredDashboard{dashboard: db.features.Dashboard()},
	} {
		if err := db.registry.Register(f); err != nil {
			log.Printf("[WARN] chronicle: failed to register feature %s: %v", f.Name(), err)
		}
	}
}
