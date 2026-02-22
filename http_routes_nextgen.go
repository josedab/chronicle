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
// It safely skips nil features.
func registerFeatureRoutes(mux *http.ServeMux, registrars ...httpRouteRegistrar) {
	for _, r := range registrars {
		if r != nil {
			r.RegisterHTTPHandlers(mux)
		}
	}
}

// setupNextGenRoutes configures next-generation feature endpoints
func setupNextGenRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	// CQL query endpoint
	if db.features != nil && db.features.CQLEngine() != nil {
		cqlEngine := db.features.CQLEngine()
		mux.HandleFunc("/api/v1/cql", wrap(func(w http.ResponseWriter, r *http.Request) {
			handleCQLQuery(cqlEngine, w, r)
		}))
		mux.HandleFunc("/api/v1/cql/validate", wrap(func(w http.ResponseWriter, r *http.Request) {
			handleCQLValidate(cqlEngine, w, r)
		}))
		mux.HandleFunc("/api/v1/cql/explain", wrap(func(w http.ResponseWriter, r *http.Request) {
			handleCQLExplain(cqlEngine, w, r)
		}))
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
		mux.HandleFunc("/api/v1/views", wrap(func(w http.ResponseWriter, r *http.Request) {
			handleMaterializedViews(mvEngine, w, r)
		}))
	}

	// Playground
	if db.features != nil && db.features.Playground() != nil {
		db.features.Playground().RegisterHTTPHandlers(mux)
	}

	// Query Planner
	if db.features != nil && db.features.QueryPlanner() != nil {
		qp := db.features.QueryPlanner()
		mux.HandleFunc("/api/v1/planner/stats", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(qp.GetPlannerStats())
		}))
	}

	// Connector Hub
	if db.features != nil && db.features.ConnectorHub() != nil {
		hub := db.features.ConnectorHub()
		mux.HandleFunc("/api/v1/connectors", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(hub.ListConnectors())
		}))
		mux.HandleFunc("/api/v1/connectors/drivers", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(hub.ListDrivers())
		}))
	}

	// Anomaly Correlation
	if db.features != nil && db.features.AnomalyCorrelation() != nil {
		ac := db.features.AnomalyCorrelation()
		mux.HandleFunc("/api/v1/incidents", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ac.ListIncidents())
		}))
	}

	// Anomaly Pipeline endpoints
	if db.features != nil && db.features.AnomalyPipeline() != nil {
		ap := db.features.AnomalyPipeline()
		mux.HandleFunc("/api/v1/anomalies", wrap(func(w http.ResponseWriter, r *http.Request) {
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
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ap.ListAnomalies(metric, since, limit))
		}))
		mux.HandleFunc("/api/v1/anomalies/stats", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ap.Stats())
		}))
		mux.HandleFunc("/api/v1/anomalies/baseline/", wrap(func(w http.ResponseWriter, r *http.Request) {
			metric := r.URL.Path[len("/api/v1/anomalies/baseline/"):]
			if metric == "" {
				http.Error(w, "metric name required", http.StatusBadRequest)
				return
			}
			b := ap.GetBaseline(metric)
			if b == nil {
				http.Error(w, "no baseline for metric", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"metric":       metric,
				"mean":         b.mean,
				"stddev":       b.stddev,
				"q1":           b.q1,
				"q3":           b.q3,
				"count":        b.count,
				"last_updated": b.lastUpdated,
				"window_size":  len(b.values),
			})
		}))
	}

	// Notebooks
	if db.features != nil && db.features.NotebookEngine() != nil {
		nb := db.features.NotebookEngine()
		mux.HandleFunc("/api/v1/notebooks", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(nb.ListNotebooks())
		}))
	}

	// Query Compiler
	if db.features != nil && db.features.QueryCompiler() != nil {
		compiler := db.features.QueryCompiler()
		mux.HandleFunc("/api/v1/compile", wrap(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}
			plan, err := compiler.Compile(string(body))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(plan)
		}))
		mux.HandleFunc("/api/v1/compile/stats", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(compiler.Stats())
		}))
	}

	// Time-Series RAG
	if db.features != nil && db.features.TSRAG() != nil {
		rag := db.features.TSRAG()
		mux.HandleFunc("/api/v1/rag/ask", wrap(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var query RAGQuery
			if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			resp, err := rag.Ask(r.Context(), query)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		mux.HandleFunc("/api/v1/rag/stats", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(rag.Stats())
		}))
	}

	// Plugin Registry
	if db.features != nil && db.features.PluginRegistry() != nil {
		registry := db.features.PluginRegistry()
		mux.HandleFunc("/api/v1/plugins", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(registry.List())
		}))
	}

	// Materialized Views V2
	if db.features != nil && db.features.MaterializedViewsV2() != nil {
		mvV2 := db.features.MaterializedViewsV2()
		mux.HandleFunc("/api/v2/views", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(mvV2.ListViews())
		}))
	}

	// Fleet Manager
	if db.features != nil && db.features.FleetManager() != nil {
		fleet := db.features.FleetManager()
		mux.HandleFunc("/api/v1/fleet/agents", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(fleet.ListAgents(""))
		}))
		mux.HandleFunc("/api/v1/fleet/stats", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(fleet.Stats())
		}))
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
		mux.HandleFunc("/api/v1/retention/stats", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(sr.Stats())
		}))
		mux.HandleFunc("/api/v1/retention/profiles", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(sr.ListProfiles())
		}))
		mux.HandleFunc("/api/v1/retention/evaluate", wrap(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			recs := sr.Evaluate()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(recs)
		}))
	}

	// Production Hardening Suite
	if db.features != nil && db.features.HardeningSuite() != nil {
		hs := db.features.HardeningSuite()
		mux.HandleFunc("/api/v1/hardening/run", wrap(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			results := hs.RunAll()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(results)
		}))
		mux.HandleFunc("/api/v1/hardening/summary", wrap(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(hs.Summary())
		}))
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	result, err := engine.Execute(r.Context(), string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleCQLValidate(engine *CQLEngine, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if err := engine.Validate(string(body)); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"valid": false, "error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"valid": true})
}

func handleCQLExplain(engine *CQLEngine, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	result, err := engine.Explain(string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleMaterializedViews(engine *MaterializedViewEngine, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		views := engine.ListViews()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(views)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
