// Package compat provides a migration bridge for the Chronicle module split.
//
// When Chronicle reorganizes into core + x/ sub-modules, this package provides
// re-exports and compatibility shims so existing code continues to work.
//
// Usage:
//
//	import chronicle "github.com/chronicle-db/chronicle/compat"
//
// This re-exports all stable core types. For experimental features, import the
// specific x/ sub-module directly:
//
//	import "github.com/chronicle-db/chronicle/x/analytics"
//	import "github.com/chronicle-db/chronicle/x/ml"
package compat

import (
	chronicle "github.com/chronicle-db/chronicle"
)

// --- Stable Core Type Aliases ---

// DB is the database handle.
type DB = chronicle.DB

// Point is a single time-series data point.
type Point = chronicle.Point

// Query is a query specification for reading data.
type Query = chronicle.Query

// Result contains query results.
type Result = chronicle.Result

// Config is the database configuration.
type Config = chronicle.Config

// Aggregation defines aggregation parameters.
type Aggregation = chronicle.Aggregation

// TagFilter defines a tag-based query filter.
type TagFilter = chronicle.TagFilter

// StorageConfig contains storage subsystem settings.
type StorageConfig = chronicle.StorageConfig

// WALConfig contains write-ahead log settings.
type WALConfig = chronicle.WALConfig

// RetentionConfig contains data retention settings.
type RetentionConfig = chronicle.RetentionConfig

// QueryConfig contains query engine settings.
type QueryConfig = chronicle.QueryConfig

// HTTPConfig contains HTTP server settings.
type HTTPConfig = chronicle.HTTPConfig

// --- Stable Core Function Re-exports ---

// Open creates or opens a Chronicle database at the given path.
var Open = chronicle.Open

// DefaultConfig returns a sensible default configuration.
var DefaultConfig = chronicle.DefaultConfig

// --- Aggregation Function Constants ---

const (
	AggCount  = chronicle.AggCount
	AggSum    = chronicle.AggSum
	AggMean   = chronicle.AggMean
	AggMin    = chronicle.AggMin
	AggMax    = chronicle.AggMax
	AggStddev = chronicle.AggStddev
	AggRate   = chronicle.AggRate
)

// --- Module Boundary Metadata ---

// ModuleInfo describes a sub-module in the split architecture.
type ModuleInfo struct {
	// Name is the module import path suffix (e.g., "x/analytics").
	Name string `json:"name"`

	// Description is a brief description of the module.
	Description string `json:"description"`

	// Stability is the stability tier: "stable", "beta", "experimental".
	Stability string `json:"stability"`

	// DependsOn lists the modules this module depends on.
	DependsOn []string `json:"depends_on,omitempty"`

	// Symbols lists the exported symbols belonging to this module.
	Symbols []string `json:"symbols,omitempty"`
}

// PlannedModules returns the planned module split boundaries.
func PlannedModules() []ModuleInfo {
	return []ModuleInfo{
		{
			Name:        "core",
			Description: "Stable core database engine with minimal dependencies",
			Stability:   "stable",
			Symbols: []string{
				"DB", "Point", "Query", "Result", "Config", "Aggregation",
				"Open", "DefaultConfig", "TagFilter", "StorageConfig",
				"WALConfig", "RetentionConfig", "QueryConfig", "HTTPConfig",
			},
		},
		{
			Name:        "x/analytics",
			Description: "Advanced analytics: forecasting, anomaly detection, ML inference",
			Stability:   "experimental",
			DependsOn:   []string{"core"},
			Symbols: []string{
				"ForecastEngine", "AnomalyDetector", "AutoMLEngine",
				"TSRAGEngine", "NLQueryEngine", "MetricCorrelation",
			},
		},
		{
			Name:        "x/edge",
			Description: "Edge computing, IoT, and mobile SDK features",
			Stability:   "experimental",
			DependsOn:   []string{"core"},
			Symbols: []string{
				"EdgeSyncEngine", "IoTDeviceSDK", "MobileSDK",
				"TinyMLEngine", "OfflineSyncEngine", "EdgeMesh",
			},
		},
		{
			Name:        "x/ml",
			Description: "Machine learning, federated learning, and inference",
			Stability:   "experimental",
			DependsOn:   []string{"core"},
			Symbols: []string{
				"MLInferenceEngine", "FederatedLearningEngine",
				"FoundationModelEngine", "FeatureStore",
			},
		},
		{
			Name:        "x/integrations",
			Description: "Third-party integrations: Grafana, K8s, OTel, DuckDB",
			Stability:   "beta",
			DependsOn:   []string{"core"},
			Symbols: []string{
				"GrafanaBackend", "K8sOperator", "OTelCollectorDistro",
				"DuckDBBackend", "ClickHouseParser", "ArrowFlightServer",
			},
		},
		{
			Name:        "x/security",
			Description: "Security, privacy, compliance features",
			Stability:   "experimental",
			DependsOn:   []string{"core"},
			Symbols: []string{
				"PrivacyFederation", "ConfidentialCompute",
				"RegulatoryCompliance", "DataMasking", "ZKQueryEngine",
			},
		},
		{
			Name:        "x/ops",
			Description: "Operational tooling: chaos testing, profiling, fleet management",
			Stability:   "beta",
			DependsOn:   []string{"core"},
			Symbols: []string{
				"ChaosEngine", "ContinuousProfilingEngine",
				"FleetManager", "AutoRemediationEngine",
			},
		},
	}
}

// DependencyMapEntry represents a file-to-module mapping for the split.
type DependencyMapEntry struct {
	File          string   `json:"file"`
	Module        string   `json:"module"`
	ExportedTypes []string `json:"exported_types,omitempty"`
	ImportsFrom   []string `json:"imports_from,omitempty"`
}

// GenerateDependencyMap returns a mapping of key files to their target modules.
func GenerateDependencyMap() []DependencyMapEntry {
	return []DependencyMapEntry{
		// Core module files
		{File: "db_core.go", Module: "core", ExportedTypes: []string{"DB"}},
		{File: "point.go", Module: "core", ExportedTypes: []string{"Point"}},
		{File: "query.go", Module: "core", ExportedTypes: []string{"Query", "Result"}},
		{File: "config.go", Module: "core", ExportedTypes: []string{"Config", "StorageConfig"}},
		{File: "storage.go", Module: "core", ExportedTypes: []string{"StorageBackend"}},
		{File: "wal.go", Module: "core", ExportedTypes: []string{"WAL"}},
		{File: "index.go", Module: "core", ExportedTypes: []string{"Index"}},
		{File: "codec.go", Module: "core", ExportedTypes: []string{"Codec"}},
		{File: "errors.go", Module: "core", ExportedTypes: []string{"Error types"}},

		// Analytics module
		{File: "forecast.go", Module: "x/analytics", ExportedTypes: []string{"ForecastEngine"}, ImportsFrom: []string{"core"}},
		{File: "anomaly_detection_v2.go", Module: "x/analytics", ExportedTypes: []string{"AnomalyDetector"}, ImportsFrom: []string{"core"}},
		{File: "ts_rag.go", Module: "x/analytics", ExportedTypes: []string{"TSRAGEngine"}, ImportsFrom: []string{"core"}},
		{File: "nl_query.go", Module: "x/analytics", ExportedTypes: []string{"NLQueryEngine"}, ImportsFrom: []string{"core"}},

		// Edge module
		{File: "edge_sync.go", Module: "x/edge", ExportedTypes: []string{"EdgeSyncEngine"}, ImportsFrom: []string{"core"}},
		{File: "iot_device_sdk.go", Module: "x/edge", ExportedTypes: []string{"IoTDeviceSDK"}, ImportsFrom: []string{"core"}},
		{File: "mobile_sdk.go", Module: "x/edge", ExportedTypes: []string{"MobileSDK"}, ImportsFrom: []string{"core"}},
		{File: "tinyml.go", Module: "x/edge", ExportedTypes: []string{"TinyMLEngine"}, ImportsFrom: []string{"core"}},

		// ML module
		{File: "ml_inference.go", Module: "x/ml", ExportedTypes: []string{"MLInferenceEngine"}, ImportsFrom: []string{"core"}},
		{File: "federated_learning.go", Module: "x/ml", ExportedTypes: []string{"FederatedLearningEngine"}, ImportsFrom: []string{"core"}},
		{File: "feature_store.go", Module: "x/ml", ExportedTypes: []string{"FeatureStore"}, ImportsFrom: []string{"core"}},

		// Integrations module
		{File: "grafana_backend.go", Module: "x/integrations", ExportedTypes: []string{"GrafanaBackend"}, ImportsFrom: []string{"core"}},
		{File: "k8s_operator.go", Module: "x/integrations", ExportedTypes: []string{"K8sOperator"}, ImportsFrom: []string{"core"}},
		{File: "otel_collector_distro.go", Module: "x/integrations", ExportedTypes: []string{"OTelCollectorDistro"}, ImportsFrom: []string{"core"}},

		// Security module
		{File: "privacy_federation.go", Module: "x/security", ExportedTypes: []string{"PrivacyFederation"}, ImportsFrom: []string{"core"}},
		{File: "data_masking.go", Module: "x/security", ExportedTypes: []string{"DataMasking"}, ImportsFrom: []string{"core"}},

		// Ops module
		{File: "chaos.go", Module: "x/ops", ExportedTypes: []string{"ChaosEngine"}, ImportsFrom: []string{"core"}},
		{File: "continuous_profiling.go", Module: "x/ops", ExportedTypes: []string{"ContinuousProfilingEngine"}, ImportsFrom: []string{"core"}},
	}
}

// GenerateCIConfig returns a GitHub Actions workflow snippet for multi-module testing.
func GenerateCIConfig() string {
	return `name: Multi-Module CI

on: [push, pull_request]

jobs:
  test-core:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with:
        go-version: '1.22'
    - name: Test core module
      run: go test ./... -short -count=1

  test-x-modules:
    runs-on: ubuntu-latest
    needs: test-core
    strategy:
      matrix:
        module: [analytics, edge, ml, integrations, security, ops]
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with:
        go-version: '1.22'
    - name: Test x/${{ matrix.module }}
      run: |
        cd x/${{ matrix.module }}
        go test ./... -count=1

  compat-check:
    runs-on: ubuntu-latest
    needs: test-core
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with:
        go-version: '1.22'
    - name: Test compat bridge
      run: go test ./compat/ -count=1
`
}

// GenerateGoModForModule returns a go.mod template for a sub-module.
func GenerateGoModForModule(module ModuleInfo) string {
	deps := "require github.com/chronicle-db/chronicle v0.0.0\n"
	for _, dep := range module.DependsOn {
		if dep != "core" {
			deps += "require github.com/chronicle-db/chronicle/x/" + dep + " v0.0.0\n"
		}
	}
	return "module github.com/chronicle-db/chronicle/" + module.Name + "\n\ngo 1.22\n\n" + deps
}

// CoreFileList returns the set of files that belong to the stable core module.
// These are the minimum files needed for the core DB engine.
func CoreFileList() []string {
	return []string{
		"db_core.go", "db_open_helpers.go", "db_write.go", "db_recovery.go",
		"db_schema.go", "db_retention.go", "db_metrics.go", "db_features.go",
		"point.go", "point_validator.go",
		"query.go", "query_engine.go", "query_planner.go", "query_cache.go",
		"config.go", "config_builder.go",
		"storage.go", "storage_engine.go", "storage_backend_file.go",
		"storage_backend_memory.go", "storage_backend_interface.go",
		"wal.go", "wal_snapshot.go",
		"index.go", "tags.go", "columns.go",
		"codec.go", "column_codec.go", "gorilla.go", "delta.go", "dictionary.go",
		"buffer.go", "engine.go",
		"errors.go", "util.go", "doc.go",
		"partition_core.go", "partitioning.go",
		"schema.go", "schema_compat.go",
		"http_core.go", "http_server.go", "http_helpers.go",
		"api_stability.go", "api_aliases.go",
		"retention_optimizer.go", "series_dedup.go",
	}
}

// VerifyCoreFileCount checks that the core module stays within the target file count.
// Returns the count and whether it's within the limit.
func VerifyCoreFileCount(maxFiles int) (int, bool) {
	if maxFiles <= 0 {
		maxFiles = 50
	}
	count := len(CoreFileList())
	return count, count <= maxFiles
}

// MigrationGuide returns a migration guide for moving from monolith to split modules.
func MigrationGuide() string {
	return `# Chronicle Module Split Migration Guide

## Overview
Chronicle is splitting from a single module into core + x/ sub-modules.
The core module contains the stable database engine (~45 files).
Experimental features move to x/ sub-modules.

## Migration Steps

### Step 1: Update imports
Replace direct imports of experimental types with x/ module imports:
` + "```go" + `
// Before
import "github.com/chronicle-db/chronicle"
engine := chronicle.NewForecastEngine(db, cfg)

// After
import "github.com/chronicle-db/chronicle/x/analytics"
engine := analytics.NewForecastEngine(db, cfg)
` + "```" + `

### Step 2: Use the compat bridge (temporary)
For gradual migration, use the compat package:
` + "```go" + `
import chronicle "github.com/chronicle-db/chronicle/compat"
// All stable types are re-exported
db, err := chronicle.Open(path, chronicle.DefaultConfig(path))
` + "```" + `

### Step 3: Update go.mod
Add x/ module dependencies as needed:
` + "```" + `
require (
    github.com/chronicle-db/chronicle v1.0.0
    github.com/chronicle-db/chronicle/x/analytics v0.1.0
    github.com/chronicle-db/chronicle/x/integrations v0.1.0
)
` + "```" + `

## Module Boundaries
- core: DB, Point, Query, Config, Storage, WAL, Index
- x/analytics: Forecast, Anomaly, RAG, NL Query
- x/edge: EdgeSync, IoT, Mobile, TinyML
- x/ml: MLInference, FederatedLearning, FeatureStore
- x/integrations: Grafana, K8s, OTel, DuckDB, Arrow Flight
- x/security: Privacy, Encryption, Data Masking, ZK Proofs
- x/ops: Chaos, Profiling, Fleet, Auto-Remediation
`
}
