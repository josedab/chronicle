package chronicle

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// httpRouteRegistrar is implemented by features that register their own HTTP handlers.
type httpRouteRegistrar interface {
	RegisterHTTPHandlers(mux *http.ServeMux)
}

// registerFeatureRoutes registers HTTP handlers for features that implement httpRouteRegistrar.
// It safely skips nil features. Body size limits are enforced at the server level
// via the global MaxBytesReader wrapper in http_server.go.
func registerFeatureRoutes(mux *http.ServeMux, registrars ...httpRouteRegistrar) {
	for _, r := range registrars {
		if r != nil {
			r.RegisterHTTPHandlers(mux)
		}
	}
}

// featureGuard wraps an HTTP handler with a feature flag check.
// If the feature is disabled, it returns 403 with a structured error.
func featureGuard(db *DB, featureName string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if db.features != nil {
			flags := db.features.FeatureFlags()
			if flags != nil && !flags.IsEnabled(featureName) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				json.NewEncoder(w).Encode(map[string]string{
					"status":  "error",
					"error":   fmt.Sprintf("feature %q is disabled", featureName),
					"feature": featureName,
				})
				return
			}
		}
		handler(w, r)
	}
}

// setupNextGenRoutes configures next-generation feature endpoints
func setupNextGenRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	// CQL query endpoint
	if db.features != nil && db.features.CQLEngine() != nil {
		cqlEngine := db.features.CQLEngine()
		mux.HandleFunc("/api/v1/cql", wrap(featureGuard(db, "CQLEngine", func(w http.ResponseWriter, r *http.Request) {
			handleCQLQuery(cqlEngine, w, r)
		})))
		mux.HandleFunc("/api/v1/cql/validate", wrap(featureGuard(db, "CQLEngine", func(w http.ResponseWriter, r *http.Request) {
			handleCQLValidate(cqlEngine, w, r)
		})))
		mux.HandleFunc("/api/v1/cql/explain", wrap(featureGuard(db, "CQLEngine", func(w http.ResponseWriter, r *http.Request) {
			handleCQLExplain(cqlEngine, w, r)
		})))
	}

	// Observability endpoints
	if db.features != nil && db.features.Observability() != nil {
		db.features.Observability().RegisterHTTPHandlers(mux)
	}

	// OpenAPI spec
	specGen := NewOpenAPIGenerator(DefaultOpenAPIGeneratorConfig())
	mux.HandleFunc("/openapi.json", wrap(OpenAPIHandler(specGen)))
	mux.HandleFunc("/swagger", wrap(SwaggerUIHandler()))

	// Materialized views
	if db.features != nil && db.features.MaterializedViews() != nil {
		mvEngine := db.features.MaterializedViews()
		mux.HandleFunc("/api/v1/views", wrap(featureGuard(db, "MaterializedViews", func(w http.ResponseWriter, r *http.Request) {
			handleMaterializedViews(mvEngine, w, r)
		})))
	}

	// Playground
	if db.features != nil && db.features.Playground() != nil {
		db.features.Playground().RegisterHTTPHandlers(mux)
	}

	// Query Planner
	if db.features != nil && db.features.QueryPlanner() != nil {
		qp := db.features.QueryPlanner()
		mux.HandleFunc("/api/v1/planner/stats", wrap(featureGuard(db, "QueryPlanner", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, qp.GetPlannerStats())
		})))
	}

	// Connector Hub
	if db.features != nil && db.features.ConnectorHub() != nil {
		hub := db.features.ConnectorHub()
		mux.HandleFunc("/api/v1/connectors", wrap(featureGuard(db, "ConnectorHub", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, hub.ListConnectors())
		})))
		mux.HandleFunc("/api/v1/connectors/drivers", wrap(featureGuard(db, "ConnectorHub", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, hub.ListDrivers())
		})))
	}

	// Anomaly Correlation
	if db.features != nil && db.features.AnomalyCorrelation() != nil {
		ac := db.features.AnomalyCorrelation()
		mux.HandleFunc("/api/v1/incidents", wrap(featureGuard(db, "AnomalyCorrelation", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, ac.ListIncidents())
		})))
	}

	// Anomaly Pipeline endpoints
	if db.features != nil && db.features.AnomalyPipeline() != nil {
		ap := db.features.AnomalyPipeline()
		mux.HandleFunc("/api/v1/anomalies", wrap(featureGuard(db, "AnomalyPipeline", func(w http.ResponseWriter, r *http.Request) {
			metric := r.URL.Query().Get("metric")
			var since time.Time
			if s := r.URL.Query().Get("since"); s != "" {
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					since = t
				}
			}
			limit := 100
			if l := r.URL.Query().Get("limit"); l != "" {
				if n, err := fmt.Sscanf(l, "%d", &limit); err != nil || n != 1 {
					limit = 100
				}
			}
			writeJSON(w, ap.ListAnomalies(metric, since, limit))
		})))
		mux.HandleFunc("/api/v1/anomalies/stats", wrap(featureGuard(db, "AnomalyPipeline", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, ap.Stats())
		})))
		mux.HandleFunc("/api/v1/anomalies/baseline/", wrap(featureGuard(db, "AnomalyPipeline", func(w http.ResponseWriter, r *http.Request) {
			metric := r.URL.Path[len("/api/v1/anomalies/baseline/"):]
			if metric == "" {
				writeError(w, "metric name required", http.StatusBadRequest)
				return
			}
			b := ap.GetBaseline(metric)
			if b == nil {
				writeError(w, "no baseline for metric", http.StatusNotFound)
				return
			}
			writeJSON(w, map[string]any{
				"metric":       metric,
				"mean":         b.mean,
				"stddev":       b.stddev,
				"q1":           b.q1,
				"q3":           b.q3,
				"count":        b.count,
				"last_updated": b.lastUpdated,
				"window_size":  len(b.values),
			})
		})))
	}

	// Notebooks
	if db.features != nil && db.features.NotebookEngine() != nil {
		nb := db.features.NotebookEngine()
		mux.HandleFunc("/api/v1/notebooks", wrap(featureGuard(db, "NotebookEngine", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, nb.ListNotebooks())
		})))
	}

	// Query Compiler
	if db.features != nil && db.features.QueryCompiler() != nil {
		compiler := db.features.QueryCompiler()
		mux.HandleFunc("/api/v1/compile", wrap(featureGuard(db, "QueryCompiler", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				writeError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				writeError(w, "failed to read body", http.StatusBadRequest)
				return
			}
			plan, err := compiler.Compile(string(body))
			if err != nil {
				writeError(w, "bad request", http.StatusBadRequest)
				return
			}
			writeJSON(w, plan)
		})))
		mux.HandleFunc("/api/v1/compile/stats", wrap(featureGuard(db, "QueryCompiler", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, compiler.Stats())
		})))
	}

	// Time-Series RAG
	if db.features != nil && db.features.TSRAG() != nil {
		rag := db.features.TSRAG()
		mux.HandleFunc("/api/v1/rag/ask", wrap(featureGuard(db, "TSRAG", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				writeError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var query RAGQuery
			if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
				writeError(w, "invalid request", http.StatusBadRequest)
				return
			}
			resp, err := rag.Ask(r.Context(), query)
			if err != nil {
				internalError(w, err, "internal error")
				return
			}
			writeJSON(w, resp)
		})))
		mux.HandleFunc("/api/v1/rag/stats", wrap(featureGuard(db, "TSRAG", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, rag.Stats())
		})))
	}

	// Plugin Registry
	if db.features != nil && db.features.PluginRegistry() != nil {
		registry := db.features.PluginRegistry()
		mux.HandleFunc("/api/v1/plugins", wrap(featureGuard(db, "PluginRegistry", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, registry.List())
		})))
	}

	// Materialized Views V2
	if db.features != nil && db.features.MaterializedViewsV2() != nil {
		mvV2 := db.features.MaterializedViewsV2()
		mux.HandleFunc("/api/v2/views", wrap(featureGuard(db, "MaterializedViewsV2", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, mvV2.ListViews())
		})))
	}

	// Fleet Manager
	if db.features != nil && db.features.FleetManager() != nil {
		fleet := db.features.FleetManager()
		mux.HandleFunc("/api/v1/fleet/agents", wrap(featureGuard(db, "FleetManager", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, fleet.ListAgents(""))
		})))
		mux.HandleFunc("/api/v1/fleet/stats", wrap(featureGuard(db, "FleetManager", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, fleet.Stats())
		})))
	}

	// OTel Distribution
	if db.features != nil && db.features.OTelDistro() != nil {
		db.features.OTelDistro().RegisterHTTPHandlers(mux)
	}

	// Embedded Cluster
	if db.features != nil && db.features.EmbeddedCluster() != nil {
		db.features.EmbeddedCluster().RegisterHTTPHandlers(mux)
	}

	// Smart Retention
	if db.features != nil && db.features.SmartRetention() != nil {
		sr := db.features.SmartRetention()
		mux.HandleFunc("/api/v1/retention/stats", wrap(featureGuard(db, "SmartRetention", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, sr.Stats())
		})))
		mux.HandleFunc("/api/v1/retention/profiles", wrap(featureGuard(db, "SmartRetention", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, sr.ListProfiles())
		})))
		mux.HandleFunc("/api/v1/retention/evaluate", wrap(featureGuard(db, "SmartRetention", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				writeError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			recs := sr.Evaluate()
			writeJSON(w, recs)
		})))
	}

	// Production Hardening Suite
	if db.features != nil && db.features.HardeningSuite() != nil {
		hs := db.features.HardeningSuite()
		mux.HandleFunc("/api/v1/hardening/run", wrap(featureGuard(db, "HardeningSuite", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				writeError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			results := hs.RunAll()
			writeJSON(w, results)
		})))
		mux.HandleFunc("/api/v1/hardening/summary", wrap(featureGuard(db, "HardeningSuite", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, hs.Summary())
		})))
	}

	// Register features that implement httpRouteRegistrar via consolidated helper.
	if db.features != nil {
		registerFeatureRoutes(mux,
			db.features.Dashboard(),
			db.features.ETLManager(),
			db.features.CloudSyncFabric(),
			db.features.DataMesh(),
			db.features.FoundationModel(),
			db.features.DataContracts(),
			db.features.QueryCache(),
			db.features.SQLPipelines(),
			db.features.MultiModelStore(),
			db.features.AdaptiveOptimizer(),
			db.features.ComplianceAutomation(),
			db.features.SchemaDesigner(),
			db.features.MobileSDK(),
			db.features.StreamProcessing(),
			db.features.TimeTravelDebug(),
			db.features.AutoSharding(),
			db.features.RootCauseAnalysis(),
			db.features.CrossCloudTiering(),
			db.features.DeclarativeAlerting(),
			db.features.MetricsCatalog(),
			db.features.CompressionAdvisor(),
			db.features.TSDiffMerge(),
			db.features.CompliancePacks(),
			db.features.BlockchainAudit(),
			db.features.ChronicleStudio(),
			db.features.IoTDeviceSDK(),
			db.features.MultiRegionReplication(),
			db.features.UniversalSDK(),
			db.features.StudioEnhanced(),
			db.features.SchemaInference(),
			db.features.CloudSaaS(),
			db.features.StreamDSLV2(),
			db.features.AnomalyExplainability(),
			db.features.HWAcceleratedQuery(),
			db.features.Marketplace(),
			db.features.RegulatoryCompliance(),
			db.features.GRPCIngestion(),
			db.features.ClusterEngine(),
			db.features.AnomalyV2(),
			db.features.DuckDBBackend(),
			db.features.WASMUDF(),
			db.features.PromDropIn(),
			db.features.SchemaEvolution(),
			db.features.EdgeCloudFabric(),
			db.features.QueryProfiler(),
			db.features.MetricsSDK(),
			db.features.DistributedQuery(),
			db.features.ContinuousAgg(),
			db.features.DataLineage(),
			db.features.SmartCompaction(),
			db.features.MetricCorrelation(),
			db.features.AdaptiveSampling(),
			db.features.TSDiff(),
			db.features.DataQuality(),
			db.features.QueryCost(),
			db.features.StreamReplay(),
			db.features.TagIndex(),
			db.features.WritePipeline(),
			db.features.MetricLifecycle(),
			db.features.RateController(),
			db.features.HotBackup(),
			db.features.CrossAlert(),
			db.features.ResultCache(),
			db.features.WebhookSystem(),
			db.features.RetentionOptimizer(),
			db.features.BenchRunner(),
			db.features.MetricMetadataStore(),
			db.features.PointValidator(),
			db.features.ConfigReload(),
			db.features.HealthCheck(),
			db.features.AuditLog(),
			db.features.SeriesDedup(),
			db.features.PartitionPruner(),
			db.features.QueryMiddleware(),
			db.features.ConnectionPool(),
			db.features.StorageStatsCollector(),
			db.features.WireProtocol(),
			db.features.WALSnapshot(),
			db.features.PromScraper(),
			db.features.ForecastV2(),
			db.features.TenantIsolation(),
			db.features.IncrementalBackup(),
			db.features.OTLPProto(),
			db.features.QueryPlanViz(),
			db.features.DataRehydration(),
			db.features.DataMasking(),
			db.features.MigrationTool(),
			db.features.ChaosRecovery(),
			db.features.FeatureFlags(),
			db.features.SelfInstrumentation(),
			db.features.Deprecation(),
		)
	}
}

func handleCQLQuery(engine *CQLEngine, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, "failed to read body", http.StatusBadRequest)
		return
	}
	result, err := engine.Execute(r.Context(), string(body))
	if err != nil {
		writeError(w, "bad request", http.StatusBadRequest)
		return
	}
	writeJSON(w, result)
}

func handleCQLValidate(engine *CQLEngine, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if err := engine.Validate(string(body)); err != nil {
		writeJSON(w, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"valid": true})
}

func handleCQLExplain(engine *CQLEngine, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, "failed to read body", http.StatusBadRequest)
		return
	}
	result, err := engine.Explain(string(body))
	if err != nil {
		writeError(w, "bad request", http.StatusBadRequest)
		return
	}
	writeJSON(w, result)
}

func handleMaterializedViews(engine *MaterializedViewEngine, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		views := engine.ListViews()
		writeJSON(w, views)
	default:
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
