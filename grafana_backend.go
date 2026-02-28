package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GrafanaBackendConfig configures the Grafana backend plugin.
type GrafanaBackendConfig struct {
	// Enabled enables the Grafana backend
	Enabled bool `json:"enabled"`

	// HTTPPort for the Grafana datasource API
	HTTPPort int `json:"http_port"`

	// MaxDataPoints limits the maximum points returned
	MaxDataPoints int `json:"max_data_points"`

	// DefaultInterval for aggregation
	DefaultInterval time.Duration `json:"default_interval"`

	// EnableStreaming enables real-time streaming support
	EnableStreaming bool `json:"enable_streaming"`

	// EnableAnnotations enables annotation support
	EnableAnnotations bool `json:"enable_annotations"`

	// EnableVariables enables template variable support
	EnableVariables bool `json:"enable_variables"`

	// CacheDuration for metric metadata
	CacheDuration time.Duration `json:"cache_duration"`

	// AllowedOrigins restricts CORS to these origins.
	// An empty list defaults to same-origin only (no wildcard).
	AllowedOrigins []string `json:"allowed_origins"`
}

// DefaultGrafanaBackendConfig returns default configuration.
func DefaultGrafanaBackendConfig() GrafanaBackendConfig {
	return GrafanaBackendConfig{
		Enabled:           true,
		HTTPPort:          3001,
		MaxDataPoints:     2000,
		DefaultInterval:   time.Minute,
		EnableStreaming:   true,
		EnableAnnotations: true,
		EnableVariables:   true,
		CacheDuration:     time.Minute,
	}
}

// GrafanaBackend provides Grafana datasource backend functionality.
type GrafanaBackend struct {
	db     *DB
	config GrafanaBackendConfig
	server *http.Server
	hub    *StreamHub
	wg     sync.WaitGroup

	// Alert management
	alertMgr   *AlertManager
	alertRules []GrafanaAlertRule
	alertMu    sync.RWMutex

	// Caches
	metricCache   []string
	metricCacheMu sync.RWMutex
	metricCacheAt time.Time

	// Annotations storage
	annotations   []GrafanaAnnotation
	annotationsMu sync.RWMutex
}

// GrafanaQuery represents a Grafana query request.
type GrafanaQuery struct {
	RefID         string            `json:"refId"`
	Metric        string            `json:"metric"`
	Tags          map[string]string `json:"tags,omitempty"`
	RawQuery      bool              `json:"rawQuery"`
	QueryText     string            `json:"queryText,omitempty"`
	Aggregation   string            `json:"aggregation,omitempty"`
	Window        string            `json:"window,omitempty"`
	IntervalMs    int64             `json:"intervalMs"`
	MaxDataPoints int               `json:"maxDataPoints"`
	Hide          bool              `json:"hide"`
}

// GrafanaQueryRequest is the Grafana query request format.
type GrafanaQueryRequest struct {
	From    string         `json:"from"`
	To      string         `json:"to"`
	Queries []GrafanaQuery `json:"queries"`
}

// GrafanaQueryResponse is the Grafana query response format.
type GrafanaQueryResponse struct {
	Results map[string]GrafanaQueryResult `json:"results"`
}

// GrafanaQueryResult is a single query result.
type GrafanaQueryResult struct {
	RefID  string             `json:"refId"`
	Frames []GrafanaDataFrame `json:"frames"`
	Error  string             `json:"error,omitempty"`
}

// GrafanaDataFrame is a Grafana data frame.
type GrafanaDataFrame struct {
	Schema GrafanaFrameSchema `json:"schema"`
	Data   GrafanaFrameData   `json:"data"`
}

// GrafanaFrameSchema describes the frame structure.
type GrafanaFrameSchema struct {
	RefID  string               `json:"refId,omitempty"`
	Name   string               `json:"name,omitempty"`
	Fields []GrafanaFieldSchema `json:"fields"`
}

// GrafanaFieldSchema describes a field.
type GrafanaFieldSchema struct {
	Name   string            `json:"name"`
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels,omitempty"`
}

// GrafanaFrameData contains the actual data.
type GrafanaFrameData struct {
	Values [][]any `json:"values"`
}

// GrafanaAnnotation represents a Grafana annotation.
type GrafanaAnnotation struct {
	ID          int64          `json:"id"`
	DashboardID int64          `json:"dashboardId"`
	PanelID     int64          `json:"panelId"`
	Time        int64          `json:"time"`
	TimeEnd     int64          `json:"timeEnd,omitempty"`
	Text        string         `json:"text"`
	Tags        []string       `json:"tags,omitempty"`
	Data        map[string]any `json:"data,omitempty"`
}

// GrafanaMetricFindQuery represents a metric find request.
type GrafanaMetricFindQuery struct {
	Query string `json:"query"`
	Type  string `json:"type"`
}

// GrafanaMetricFindValue represents a metric find result.
type GrafanaMetricFindValue struct {
	Text  string `json:"text"`
	Value string `json:"value"`
}

// NewGrafanaBackend creates a new Grafana backend.
func NewGrafanaBackend(db *DB, config GrafanaBackendConfig) *GrafanaBackend {
	return &GrafanaBackend{
		db:     db,
		config: config,
	}
}

// Start starts the Grafana backend HTTP server.
func (g *GrafanaBackend) Start() error {
	mux := http.NewServeMux()

	// Grafana datasource endpoints
	mux.HandleFunc("/api/v1/grafana/", g.handleRoot)
	mux.HandleFunc("/api/v1/grafana/health", g.handleHealth)
	mux.HandleFunc("/api/v1/grafana/query", g.handleQuery)
	mux.HandleFunc("/api/v1/grafana/metrics", g.handleMetrics)
	mux.HandleFunc("/api/v1/grafana/tag-keys", g.handleTagKeys)
	mux.HandleFunc("/api/v1/grafana/tag-values", g.handleTagValues)
	mux.HandleFunc("/api/v1/grafana/annotations", g.handleAnnotations)
	mux.HandleFunc("/api/v1/grafana/search", g.handleSearch)
	mux.HandleFunc("/api/v1/grafana/variable", g.handleVariable)

	// Streaming endpoint
	if g.config.EnableStreaming {
		mux.HandleFunc("/api/v1/grafana/stream", g.handleStream)
	}

	// Alert management endpoints
	mux.HandleFunc("/alerts", g.handleAlerts)

	g.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", g.config.HTTPPort),
		Handler: g.corsMiddleware(mux),
	}

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := g.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("grafana backend server error", "err", err)
		}
	}()

	return nil
}

// Stop stops the Grafana backend.
func (g *GrafanaBackend) Stop() error {
	if g.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := g.server.Shutdown(ctx)
	g.wg.Wait()
	return err
}

func (g *GrafanaBackend) corsMiddleware(next http.Handler) http.Handler {
	allowed := g.config.AllowedOrigins
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		originAllowed := false
		if len(allowed) > 0 && origin != "" {
			for _, o := range allowed {
				if o == "*" {
					slog.Warn("wildcard '*' in AllowedOrigins is rejected; configure explicit origins")
					break
				}
				if o == origin {
					originAllowed = true
					w.Header().Set("Access-Control-Allow-Origin", origin)
					break
				}
			}
		}

		if originAllowed {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Grafana-Org-Id")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (g *GrafanaBackend) handleRoot(w http.ResponseWriter, r *http.Request) {
	g.writeJSON(w, map[string]any{
		"status":  "ok",
		"name":    "Chronicle Grafana Datasource",
		"version": "1.0.0",
	})
}

func (g *GrafanaBackend) handleHealth(w http.ResponseWriter, r *http.Request) {
	g.writeJSON(w, map[string]string{"status": "ok"})
}

func (g *GrafanaBackend) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GrafanaQueryRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Parse time range
	from, to, err := g.parseTimeRange(req.From, req.To)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	response := GrafanaQueryResponse{
		Results: make(map[string]GrafanaQueryResult),
	}

	for _, query := range req.Queries {
		if query.Hide {
			continue
		}

		result := g.executeQuery(query, from, to)
		response.Results[query.RefID] = result
	}

	g.writeJSON(w, response)
}

func (g *GrafanaBackend) executeQuery(query GrafanaQuery, from, to int64) GrafanaQueryResult {
	result := GrafanaQueryResult{RefID: query.RefID}

	// Parse query
	var chronicleQuery *Query
	if query.RawQuery && query.QueryText != "" {
		// Parse SQL/PromQL query
		parsedQuery, err := g.parseQueryText(query.QueryText)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		chronicleQuery = parsedQuery
		chronicleQuery.Start = from
		chronicleQuery.End = to
	} else {
		// Build query from fields
		chronicleQuery = &Query{
			Metric: query.Metric,
			Tags:   query.Tags,
			Start:  from,
			End:    to,
		}

		// Add aggregation if specified
		if query.Aggregation != "" {
			window := g.parseWindow(query.Window)
			if window == 0 {
				window = g.config.DefaultInterval
			}
			chronicleQuery.Aggregation = &Aggregation{
				Function: g.parseAggFunc(query.Aggregation),
				Window:   window,
			}
		}
	}

	// Execute query
	queryResult, err := g.db.Execute(chronicleQuery)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	// Apply max data points limit
	points := queryResult.Points
	maxPoints := query.MaxDataPoints
	if maxPoints == 0 {
		maxPoints = g.config.MaxDataPoints
	}
	if len(points) > maxPoints {
		points = g.downsamplePoints(points, maxPoints)
	}

	// Build data frame
	frame := g.buildDataFrame(query.RefID, query.Metric, points)
	result.Frames = []GrafanaDataFrame{frame}

	return result
}

func (g *GrafanaBackend) buildDataFrame(refID, metric string, points []Point) GrafanaDataFrame {
	times := make([]any, len(points))
	values := make([]any, len(points))

	for i, pt := range points {
		// Convert nanoseconds to milliseconds for Grafana
		times[i] = pt.Timestamp / 1000000
		values[i] = pt.Value
	}

	return GrafanaDataFrame{
		Schema: GrafanaFrameSchema{
			RefID: refID,
			Name:  metric,
			Fields: []GrafanaFieldSchema{
				{Name: "time", Type: "time"},
				{Name: "value", Type: "number"},
			},
		},
		Data: GrafanaFrameData{
			Values: [][]any{times, values},
		},
	}
}

func (g *GrafanaBackend) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := g.getMetricsCached()
	g.writeJSON(w, metrics)
}

func (g *GrafanaBackend) handleTagKeys(w http.ResponseWriter, r *http.Request) {
	// Get unique tag keys from all metrics
	tagKeys := g.getTagKeys()

	result := make([]GrafanaMetricFindValue, len(tagKeys))
	for i, key := range tagKeys {
		result[i] = GrafanaMetricFindValue{Text: key, Value: key}
	}

	g.writeJSON(w, result)
}

func (g *GrafanaBackend) handleTagValues(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key parameter required", http.StatusBadRequest)
		return
	}

	values := g.getTagValues(key)

	result := make([]GrafanaMetricFindValue, len(values))
	for i, val := range values {
		result[i] = GrafanaMetricFindValue{Text: val, Value: val}
	}

	g.writeJSON(w, result)
}

func (g *GrafanaBackend) handleAnnotations(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.getAnnotations(w, r)
	case http.MethodPost:
		g.createAnnotation(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (g *GrafanaBackend) getAnnotations(w http.ResponseWriter, r *http.Request) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	from, err := strconv.ParseInt(fromStr, 10, 64)
	if err != nil || from < 0 {
		http.Error(w, "invalid 'from' parameter", http.StatusBadRequest)
		return
	}
	to, err := strconv.ParseInt(toStr, 10, 64)
	if err != nil || to < 0 {
		http.Error(w, "invalid 'to' parameter", http.StatusBadRequest)
		return
	}

	g.annotationsMu.RLock()
	defer g.annotationsMu.RUnlock()

	var filtered []GrafanaAnnotation
	for _, ann := range g.annotations {
		if ann.Time >= from && ann.Time <= to {
			filtered = append(filtered, ann)
		}
	}

	g.writeJSON(w, filtered)
}

func (g *GrafanaBackend) createAnnotation(w http.ResponseWriter, r *http.Request) {
	var ann GrafanaAnnotation
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&ann); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	g.annotationsMu.Lock()
	ann.ID = int64(len(g.annotations) + 1)
	if ann.Time == 0 {
		ann.Time = time.Now().UnixMilli()
	}
	g.annotations = append(g.annotations, ann)
	g.annotationsMu.Unlock()

	g.writeJSON(w, ann)
}

func (g *GrafanaBackend) handleSearch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Target string `json:"target"`
	}

	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON request body", http.StatusBadRequest)
			return
		}
	} else {
		req.Target = r.URL.Query().Get("target")
	}

	metrics := g.getMetricsCached()

	// Filter by target if specified
	if req.Target != "" {
		var filtered []string
		target := strings.ToLower(req.Target)
		for _, m := range metrics {
			if strings.Contains(strings.ToLower(m), target) {
				filtered = append(filtered, m)
			}
		}
		metrics = filtered
	}

	g.writeJSON(w, metrics)
}

func (g *GrafanaBackend) handleVariable(w http.ResponseWriter, r *http.Request) {
	var req GrafanaMetricFindQuery
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON request body", http.StatusBadRequest)
			return
		}
	} else {
		req.Query = r.URL.Query().Get("query")
		req.Type = r.URL.Query().Get("type")
	}

	var values []GrafanaMetricFindValue

	switch req.Type {
	case "metrics":
		metrics := g.getMetricsCached()
		for _, m := range metrics {
			values = append(values, GrafanaMetricFindValue{Text: m, Value: m})
		}

	case "tag_keys":
		keys := g.getTagKeys()
		for _, k := range keys {
			values = append(values, GrafanaMetricFindValue{Text: k, Value: k})
		}

	case "tag_values":
		// Extract key from query
		vals := g.getTagValues(req.Query)
		for _, v := range vals {
			values = append(values, GrafanaMetricFindValue{Text: v, Value: v})
		}

	default:
		// Default to metric search
		metrics := g.getMetricsCached()
		for _, m := range metrics {
			if strings.Contains(m, req.Query) {
				values = append(values, GrafanaMetricFindValue{Text: m, Value: m})
			}
		}
	}

	g.writeJSON(w, values)
}
