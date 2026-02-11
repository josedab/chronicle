package chronicle

// Engine is the common interface for Chronicle subsystems that have a start/stop lifecycle.
// All engine types that manage background goroutines or hold external resources should
// implement this interface for uniform lifecycle management.
//
// This interface is the foundation for the package restructuring plan documented in
// docs/PACKAGES.md — future engine extractions will depend on this contract.
type Engine interface {
	// Start initializes the engine and begins any background processing.
	Start() error

	// Stop gracefully shuts down the engine, releasing resources and stopping goroutines.
	Stop() error
}

// EngineHealthChecker extends Engine with a health check capability.
type EngineHealthChecker interface {
	Engine

	// Healthy returns true if the engine is operating normally.
	Healthy() bool
}

// Compile-time interface compliance checks.
var (
	_ Engine = (*AdaptiveCompressionEngine)(nil)
	_ Engine = (*StreamProcessingEngine)(nil)
	_ Engine = (*AutoShardingEngine)(nil)
	_ Engine = (*GrafanaBackend)(nil)
	_ Engine = (*EmbeddedCluster)(nil)
	_ Engine = (*CloudSyncFabric)(nil)
	_ Engine = (*ETLPipelineManager)(nil)
	_ Engine = (*OTelDistro)(nil)
	_ Engine = (*ArrowFlightServer)(nil)
	_ Engine = (*K8sOperator)(nil)
	_ Engine = (*EdgeMesh)(nil)
)
