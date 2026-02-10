package chronicle

// API Stability Classification
//
// Chronicle classifies every exported symbol into one of three stability tiers:
//
//   - Stable: Covered by semver. Breaking changes only in major versions.
//   - Beta: May change between minor versions with migration guidance.
//   - Experimental: May change or be removed without notice.
//
// This file documents the stable public API surface. All types and functions
// listed here are safe to depend on for production use.

// ---------------------------------------------------------------------------
// Stable API — Core Types
// ---------------------------------------------------------------------------
//
// The following types form the foundation of Chronicle's public API:
//
//   DB            — The database handle. Created via Open(), closed via Close().
//   Point         — A single time-series data point.
//   Query         — A query specification for reading data.
//   Aggregation   — Aggregation parameters for a query.
//   AggFunc       — Aggregation function enum (AggCount, AggSum, AggMean, AggMin, AggMax).
//   Result        — Query results containing matched Points.
//   Config        — Database configuration.
//   StorageConfig — Storage subsystem settings.
//   WALConfig     — Write-ahead log settings.
//   RetentionConfig — Data retention settings.
//   QueryConfig   — Query engine settings.
//   HTTPConfig    — HTTP server settings.
//   TagFilter     — Tag-based query filter.
//
// ---------------------------------------------------------------------------
// Stable API — Core Functions
// ---------------------------------------------------------------------------
//
//   Open(path, config)        → (*DB, error)        Create or open a database
//   DefaultConfig(path)       → Config               Sensible default config
//   db.Close()                → error                 Close the database
//   db.Write(Point)           → error                 Write a single point
//   db.WriteBatch([]Point)    → error                 Write multiple points
//   db.Execute(*Query)        → (*Result, error)      Execute a query
//   db.Metrics()              → []string              List known metric names
//
// ---------------------------------------------------------------------------
// Beta API
// ---------------------------------------------------------------------------
//
// The following are considered beta — API may evolve between minor versions:
//
//   QueryConsole, QueryConsoleConfig    — Web query console
//   AutoscaleEngine, AutoscaleConfig   — Predictive autoscaling
//   CDCEngine, CDCConfig               — Change data capture
//   PipelineDSL, ParsePipelineDSL      — YAML pipeline DSL
//   ParquetBridge, ParquetBridgeConfig  — Parquet lakehouse bridge
//   FleetManager, FleetConfig           — Edge fleet management
//   SLOTracker, SLOConfig               — SLO tracking
//   WorkloadLearner                     — Adaptive compression v2
//
// ---------------------------------------------------------------------------
// Experimental API
// ---------------------------------------------------------------------------
//
// Everything not listed above is experimental. This includes internal
// pseudo-packages (admin_ui_*, cql_*, raft_consensus_*, otel_distro_*, etc.)
// and advanced features still under active development.

// APIVersion is the current API version of Chronicle.
const APIVersion = "0.1.0"

// StabilityTier classifies the stability of an API symbol.
type StabilityTier int

const (
	// StabilityStable symbols are covered by semver guarantees.
	StabilityStable StabilityTier = iota
	// StabilityBeta symbols may change between minor versions.
	StabilityBeta
	// StabilityExperimental symbols may change or be removed at any time.
	StabilityExperimental
)

func (s StabilityTier) String() string {
	switch s {
	case StabilityStable:
		return "stable"
	case StabilityBeta:
		return "beta"
	case StabilityExperimental:
		return "experimental"
	default:
		return "unknown"
	}
}

// APISymbol describes a public API symbol and its stability tier.
type APISymbol struct {
	Name      string        `json:"name"`
	Kind      string        `json:"kind"` // "type", "func", "const", "var"
	Stability StabilityTier `json:"stability"`
	Since     string        `json:"since"`
}

// StableAPI returns the list of all stable API symbols.
func StableAPI() []APISymbol {
	return []APISymbol{
		// Core types
		{Name: "DB", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "Point", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "Query", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "Aggregation", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "AggFunc", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "Result", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "Config", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "StorageConfig", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "WALConfig", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "RetentionConfig", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "QueryConfig", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "HTTPConfig", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "TagFilter", Kind: "type", Stability: StabilityStable, Since: "0.1.0"},

		// Core functions
		{Name: "Open", Kind: "func", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "DefaultConfig", Kind: "func", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "DB.Close", Kind: "func", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "DB.Write", Kind: "func", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "DB.WriteBatch", Kind: "func", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "DB.Execute", Kind: "func", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "DB.Metrics", Kind: "func", Stability: StabilityStable, Since: "0.1.0"},

		// Core constants
		{Name: "AggCount", Kind: "const", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "AggSum", Kind: "const", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "AggMean", Kind: "const", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "AggMin", Kind: "const", Stability: StabilityStable, Since: "0.1.0"},
		{Name: "AggMax", Kind: "const", Stability: StabilityStable, Since: "0.1.0"},
	}
}

// BetaAPI returns the list of all beta API symbols.
func BetaAPI() []APISymbol {
	return []APISymbol{
		{Name: "QueryConsole", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "AutoscaleEngine", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "CDCEngine", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "PipelineDSL", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "ParquetBridge", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "FleetManager", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "SLOTracker", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "WorkloadLearner", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
		{Name: "Forecaster", Kind: "type", Stability: StabilityBeta, Since: "0.1.0"},
	}
}

// ExperimentalAPI returns the list of all experimental API symbols.
func ExperimentalAPI() []APISymbol {
	return []APISymbol{
		{Name: "StreamProcessingEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "TimeTravelDebugEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "AutoShardingEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "RootCauseAnalysisEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "CrossCloudTieringEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "DeclarativeAlertingEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "MetricsCatalog", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "CompressionAdvisor", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "TSDiffMergeEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "CompliancePacksEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.2.0"},
		{Name: "BlockchainAuditTrail", Kind: "type", Stability: StabilityExperimental, Since: "0.3.0"},
		{Name: "ChronicleStudio", Kind: "type", Stability: StabilityExperimental, Since: "0.3.0"},
		{Name: "IoTDeviceSDK", Kind: "type", Stability: StabilityExperimental, Since: "0.3.0"},
		{Name: "MultiRegionReplicationEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "UniversalSDKEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "StudioEnhancedEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "SchemaInferenceEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "CloudSaaSEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "StreamDSLV2Engine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "AnomalyExplainabilityEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "HWAcceleratedQueryEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "MarketplaceEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
		{Name: "RegulatoryComplianceEngine", Kind: "type", Stability: StabilityExperimental, Since: "0.4.0"},
	}
}
