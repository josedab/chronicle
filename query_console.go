package chronicle

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sync"
	"time"
)

//go:embed admin_ui/templates/console.html
var consoleTemplateFS embed.FS

// QueryConsoleConfig configures the web query console.
type QueryConsoleConfig struct {
	Bind         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxQueryLen  int
	EnableCORS   bool

	// AllowedOrigins restricts CORS to these origins when EnableCORS is true.
	// An empty list defaults to same-origin only (no wildcard).
	AllowedOrigins []string
}

// DefaultQueryConsoleConfig returns sensible defaults.
func DefaultQueryConsoleConfig() QueryConsoleConfig {
	return QueryConsoleConfig{
		Bind:         ":9090",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		MaxQueryLen:  8192,
		EnableCORS:   false,
	}
}

// QueryConsole serves a browser-based CQL editor.
//
// 🔬 BETA: API may evolve between minor versions with migration guidance.
// See api_stability.go for stability classifications.
type QueryConsole struct {
	db     *DB
	config QueryConsoleConfig
	mux    *http.ServeMux
	tmpl   *template.Template
	mu     sync.RWMutex
}

// NewQueryConsole creates a new web query console.
func NewQueryConsole(db *DB, config QueryConsoleConfig) (*QueryConsole, error) {
	tmpl, err := template.ParseFS(consoleTemplateFS, "admin_ui/templates/console.html")
	if err != nil {
		return nil, fmt.Errorf("query console: template parse error: %w", err)
	}

	qc := &QueryConsole{
		db:     db,
		config: config,
		mux:    http.NewServeMux(),
		tmpl:   tmpl,
	}

	qc.mux.HandleFunc("/", qc.handleIndex)
	qc.mux.HandleFunc("/api/query", qc.handleQuery)
	qc.mux.HandleFunc("/api/metrics", qc.handleMetrics)
	qc.mux.HandleFunc("/api/health", qc.handleHealth)

	return qc, nil
}

// Handler returns the HTTP handler for embedding in existing servers.
func (qc *QueryConsole) Handler() http.Handler {
	return qc.mux
}

// ListenAndServe starts the console HTTP server.
func (qc *QueryConsole) ListenAndServe() error {
	srv := &http.Server{
		Addr:         qc.config.Bind,
		Handler:      qc.corsMiddleware(qc.mux),
		ReadTimeout:  qc.config.ReadTimeout,
		WriteTimeout: qc.config.WriteTimeout,
	}
	return srv.ListenAndServe()
}

func (qc *QueryConsole) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := qc.tmpl.Execute(w, nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type consoleQueryRequest struct {
	Query string `json:"query"`
}

type consoleQueryResponse struct {
	Points []consolePoint `json:"points,omitempty"`
	Error  string         `json:"error,omitempty"`
}

type consolePoint struct {
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (qc *QueryConsole) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONStatus(w, http.StatusMethodNotAllowed, consoleQueryResponse{Error: "POST required"})
		return
	}

	var req consoleQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, consoleQueryResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	if len(req.Query) == 0 {
		writeJSONStatus(w, http.StatusBadRequest, consoleQueryResponse{Error: "query is required"})
		return
	}
	if len(req.Query) > qc.config.MaxQueryLen {
		writeJSONStatus(w, http.StatusBadRequest, consoleQueryResponse{
			Error: fmt.Sprintf("query exceeds max length (%d)", qc.config.MaxQueryLen),
		})
		return
	}

	if qc.db == nil {
		writeJSONStatus(w, http.StatusServiceUnavailable, consoleQueryResponse{Error: "database not available"})
		return
	}

	q := &Query{Metric: req.Query}
	result, err := qc.db.Execute(q)
	if err != nil {
		writeJSONStatus(w, http.StatusOK, consoleQueryResponse{Error: err.Error()})
		return
	}

	points := make([]consolePoint, 0, len(result.Points))
	for _, p := range result.Points {
		points = append(points, consolePoint{
			Metric:    p.Metric,
			Value:     p.Value,
			Timestamp: p.Timestamp,
			Tags:      p.Tags,
		})
	}

	writeJSONStatus(w, http.StatusOK, consoleQueryResponse{Points: points})
}

type consoleMetricsResponse struct {
	Metrics []string `json:"metrics"`
}

func (qc *QueryConsole) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if qc.db == nil {
		writeJSONStatus(w, http.StatusServiceUnavailable, map[string]string{"error": "database not available"})
		return
	}
	metrics := qc.db.Metrics()
	writeJSONStatus(w, http.StatusOK, consoleMetricsResponse{Metrics: metrics})
}

func (qc *QueryConsole) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (qc *QueryConsole) corsMiddleware(next http.Handler) http.Handler {
	if !qc.config.EnableCORS {
		return next
	}
	allowed := qc.config.AllowedOrigins
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if len(allowed) == 0 {
			// No origins configured: allow same-origin only (no header set).
		} else {
			for _, o := range allowed {
				if o == origin {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					break
				}
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
