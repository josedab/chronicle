package chronicle

import (
	"encoding/json"
	"io"
	"net/http"
)

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
		json.NewEncoder(w).Encode(map[string]interface{}{"valid": false, "error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"valid": true})
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
