package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// QueryExecutor provides query execution capabilities.
// This interface allows HTTP handlers to be tested independently of the DB.
type QueryExecutor interface {
	Execute(q *Query) (*Result, error)
	ExecuteContext(ctx context.Context, q *Query) (*Result, error)
}

// DataWriter provides data writing capabilities.
// This interface allows HTTP handlers to be tested independently of the DB.
type DataWriter interface {
	Write(p Point) error
	WriteBatch(points []Point) error
}

// MetricLister provides metric listing capabilities.
type MetricLister interface {
	Metrics() []string
}

// SchemaManager provides schema management capabilities.
type SchemaManager interface {
	RegisterSchema(schema MetricSchema) error
	UnregisterSchema(name string)
	GetSchema(name string) *MetricSchema
	ListSchemas() []MetricSchema
}

// Ensure DB implements the interfaces
var (
	_ QueryExecutor = (*DB)(nil)
	_ DataWriter    = (*DB)(nil)
	_ MetricLister  = (*DB)(nil)
	_ SchemaManager = (*DB)(nil)
)

type httpServer struct {
	srv *http.Server
}

type writeRequest struct {
	Points []Point `json:"points"`
}

type queryRequest struct {
	Query  string            `json:"query"`
	Metric string            `json:"metric"`
	Start  int64             `json:"start"`
	End    int64             `json:"end"`
	Tags   map[string]string `json:"tags"`
}

type queryResponse struct {
	Points []Point `json:"points"`
}

const (
	// maxBodySize is the maximum allowed request body size (10MB)
	maxBodySize = 10 * 1024 * 1024
)

// rateLimiter implements a simple token bucket rate limiter per IP
type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	rate     int           // requests per window
	window   time.Duration // time window
	cleanup  time.Duration // cleanup interval
}

type visitor struct {
	tokens    int
	lastReset time.Time
}

// newRateLimiter creates a rate limiter with the given rate per window.
func newRateLimiter(rate int, window time.Duration) *rateLimiter {
	rl := &rateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		window:   window,
		cleanup:  window * 2,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *rateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanup)
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, v := range rl.visitors {
			if now.Sub(v.lastReset) > rl.cleanup {
				delete(rl.visitors, ip)
			}
		}
		rl.mu.Unlock()
	}
}

func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	v, exists := rl.visitors[ip]
	if !exists {
		rl.visitors[ip] = &visitor{tokens: rl.rate - 1, lastReset: now}
		return true
	}

	if now.Sub(v.lastReset) >= rl.window {
		v.tokens = rl.rate - 1
		v.lastReset = now
		return true
	}

	if v.tokens > 0 {
		v.tokens--
		return true
	}

	return false
}

// getClientIP extracts the client IP from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// Check X-Real-IP header
	xri := r.Header.Get("X-Real-IP")
	if xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// rateLimitMiddleware wraps a handler with rate limiting
func rateLimitMiddleware(rl *rateLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := getClientIP(r)
		if !rl.allow(ip) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

// authenticator handles API key authentication
type authenticator struct {
	enabled      bool
	apiKeys      map[string]bool
	readOnlyKeys map[string]bool
	excludePaths map[string]bool
}

func newAuthenticator(cfg *AuthConfig) *authenticator {
	a := &authenticator{
		apiKeys:      make(map[string]bool),
		readOnlyKeys: make(map[string]bool),
		excludePaths: make(map[string]bool),
	}

	if cfg == nil || !cfg.Enabled {
		a.enabled = false
		return a
	}

	a.enabled = true
	for _, key := range cfg.APIKeys {
		a.apiKeys[key] = true
	}
	for _, key := range cfg.ReadOnlyKeys {
		a.readOnlyKeys[key] = true
	}
	for _, path := range cfg.ExcludePaths {
		a.excludePaths[path] = true
	}
	// Always allow health endpoint without auth
	a.excludePaths["/health"] = true

	return a
}

// extractAPIKey extracts the API key from the request
func extractAPIKey(r *http.Request) string {
	// Check Authorization header (Bearer token)
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}

	// Check X-API-Key header
	if key := r.Header.Get("X-API-Key"); key != "" {
		return key
	}

	// Check query parameter
	return r.URL.Query().Get("api_key")
}

// isWriteOperation returns true if the request is a write operation
func isWriteOperation(r *http.Request) bool {
	if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
		path := r.URL.Path
		// Query endpoints are reads even with POST
		if strings.HasPrefix(path, "/query") ||
			strings.HasPrefix(path, "/api/v1/query") ||
			strings.HasPrefix(path, "/graphql") {
			return false
		}
		return true
	}
	return false
}

// authMiddleware wraps a handler with authentication
func authMiddleware(auth *authenticator, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !auth.enabled {
			next(w, r)
			return
		}

		// Check if path is excluded from auth
		if auth.excludePaths[r.URL.Path] {
			next(w, r)
			return
		}

		apiKey := extractAPIKey(r)
		if apiKey == "" {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}

		// Check if it's a full-access key
		if auth.apiKeys[apiKey] {
			next(w, r)
			return
		}

		// Check if it's a read-only key
		if auth.readOnlyKeys[apiKey] {
			if isWriteOperation(r) {
				http.Error(w, "read-only API key cannot perform write operations", http.StatusForbidden)
				return
			}
			next(w, r)
			return
		}

		http.Error(w, "invalid API key", http.StatusUnauthorized)
	}
}

// middlewareWrapper wraps handlers with authentication and rate limiting
type middlewareWrapper func(h http.HandlerFunc) http.HandlerFunc

// setupWriteRoutes configures write-related endpoints
func setupWriteRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	mux.HandleFunc("/write", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer func() { _ = gz.Close() }()
			reader = io.LimitReader(gz, maxBodySize)
		}

		body, err := io.ReadAll(reader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		var req writeRequest
		if err := json.NewDecoder(bytes.NewReader(body)).Decode(&req); err == nil && len(req.Points) > 0 {
			for i := range req.Points {
				if err := ValidatePoint(&req.Points[i]); err != nil {
					http.Error(w, fmt.Sprintf("invalid point %d: %v", i, err), http.StatusBadRequest)
					return
				}
			}
			if err := db.WriteBatch(req.Points); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}

		points, err := parseLineProtocol(string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(points) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		for i := range points {
			if err := ValidatePoint(&points[i]); err != nil {
				http.Error(w, fmt.Sprintf("invalid point %d: %v", i, err), http.StatusBadRequest)
				return
			}
		}
		if err := db.WriteBatch(points); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
}

// setupQueryRoutes configures query-related endpoints
func setupQueryRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	mux.HandleFunc("/query", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var req queryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var q *Query
		if req.Query != "" {
			parser := &QueryParser{}
			parsed, err := parser.Parse(req.Query)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			q = parsed
		} else {
			q = &Query{
				Metric: req.Metric,
				Start:  req.Start,
				End:    req.End,
				Tags:   req.Tags,
			}
		}

		result, err := db.Execute(q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, queryResponse{Points: result.Points})
	}))
}

// setupPrometheusRoutes configures Prometheus-compatible endpoints
func setupPrometheusRoutes(mux *http.ServeMux, db *DB) {
	mux.HandleFunc("/prometheus/write", func(w http.ResponseWriter, r *http.Request) {
		if !db.config.PrometheusRemoteWriteEnabled {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		decoded, err := snappy.Decode(nil, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req prompb.WriteRequest
		if err := req.Unmarshal(decoded); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		points := convertPromWrite(&req)
		if err := db.WriteBatch(points); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	mux.HandleFunc("/api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		handlePromQuery(db, w, r, false)
	})

	mux.HandleFunc("/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		handlePromQuery(db, w, r, true)
	})
}

// setupAdminRoutes configures admin and operational endpoints
func setupAdminRoutes(mux *http.ServeMux, db *DB) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, db.Metrics())
	})

	mux.HandleFunc("/schemas", func(w http.ResponseWriter, r *http.Request) {
		handleSchemas(db, w, r)
	})

	adminUI := NewAdminUI(db, AdminConfig{Prefix: "/admin"})
	mux.Handle("/admin", adminUI)
	mux.Handle("/admin/", adminUI)
}

// setupAlertingRoutes configures alerting-related endpoints
func setupAlertingRoutes(mux *http.ServeMux, db *DB) {
	mux.HandleFunc("/api/v1/alerts", func(w http.ResponseWriter, r *http.Request) {
		handleAlerts(db, w, r)
	})

	mux.HandleFunc("/api/v1/rules", func(w http.ResponseWriter, r *http.Request) {
		handleRules(db, w, r)
	})
}

// setupFeatureRoutes configures advanced feature endpoints
func setupFeatureRoutes(mux *http.ServeMux, db *DB) {
	// OTLP metrics endpoint
	otlpReceiver := NewOTLPReceiver(db, DefaultOTLPConfig())
	mux.HandleFunc("/v1/metrics", otlpReceiver.Handler())

	// Streaming WebSocket endpoint
	streamHub := NewStreamHub(db, DefaultStreamConfig())
	mux.HandleFunc("/stream", streamHub.WebSocketHandler())

	// GraphQL endpoint
	graphqlServer := NewGraphQLServer(db)
	mux.Handle("/graphql", graphqlServer.Handler())
	mux.Handle("/graphql/playground", graphqlServer.ServePlayground())

	// Forecast endpoint
	mux.HandleFunc("/api/v1/forecast", func(w http.ResponseWriter, r *http.Request) {
		handleForecast(db, w, r)
	})

	// Histogram endpoints
	mux.HandleFunc("/api/v1/histogram", func(w http.ResponseWriter, r *http.Request) {
		handleHistogram(db, w, r)
	})
}

func startHTTPServer(db *DB, port int) (*httpServer, error) {
	if port <= 0 || port > 65535 {
		port = 8086
	}

	// Rate limiter from config or default
	rateLimit := db.config.RateLimitPerSecond
	if rateLimit <= 0 {
		rateLimit = 1000
	}
	var rl *rateLimiter
	if rateLimit > 0 {
		rl = newRateLimiter(rateLimit, time.Second)
	}

	// Authentication from config
	auth := newAuthenticator(db.config.Auth)

	// Helper to wrap handlers with middleware
	wrap := func(h http.HandlerFunc) http.HandlerFunc {
		h = authMiddleware(auth, h)
		if rl != nil {
			h = rateLimitMiddleware(rl, h)
		}
		return h
	}

	mux := http.NewServeMux()

	// Setup route groups
	setupWriteRoutes(mux, db, wrap)
	setupQueryRoutes(mux, db, wrap)
	setupPrometheusRoutes(mux, db)
	setupAdminRoutes(mux, db)
	setupAlertingRoutes(mux, db)
	setupFeatureRoutes(mux, db)

	// Setup ClickHouse-compatible routes if enabled
	if db.config.ClickHouse != nil && db.config.ClickHouse.Enabled {
		setupClickHouseRoutes(mux, db, *db.config.ClickHouse)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		_ = srv.Serve(listener)
	}()

	return &httpServer{srv: srv}, nil
}

func (s *httpServer) Close() error {
	if s == nil || s.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

func parseLineProtocol(body string) ([]Point, error) {
	lines := strings.Split(strings.TrimSpace(body), "\n")
	points := make([]Point, 0, len(lines))
	now := time.Now().UnixNano()

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid line protocol")
		}

		measurementAndTags := fields[0]
		fieldSet := fields[1]
		timestamp := now
		if len(fields) >= 3 {
			ts, err := strconv.ParseInt(fields[2], 10, 64)
			if err == nil {
				timestamp = ts
			}
		}

		metric, tags := parseMeasurementTags(measurementAndTags)
		fieldKey, fieldValue, err := parseFieldSet(fieldSet)
		if err != nil {
			return nil, err
		}

		_ = fieldKey
		points = append(points, Point{
			Metric:    metric,
			Tags:      tags,
			Value:     fieldValue,
			Timestamp: timestamp,
		})
	}

	return points, nil
}

func parseMeasurementTags(input string) (metric string, tags map[string]string) {
	parts := strings.Split(input, ",")
	metric = parts[0]
	tags = make(map[string]string)
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}
	return metric, tags
}

func parseFieldSet(input string) (key string, value float64, err error) {
	kv := strings.SplitN(input, "=", 2)
	if len(kv) != 2 {
		return "", 0, fmt.Errorf("invalid field set")
	}
	val := strings.TrimSuffix(kv[1], "i")
	floatVal, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return "", 0, err
	}
	return kv[0], floatVal, nil
}

func convertPromWrite(req *prompb.WriteRequest) []Point {
	points := make([]Point, 0, len(req.Timeseries))
	for i := range req.Timeseries {
		ts := &req.Timeseries[i]
		metric := ""
		tags := make(map[string]string)
		for _, label := range ts.Labels {
			if label.Name == "__name__" {
				metric = label.Value
			} else {
				tags[label.Name] = label.Value
			}
		}
		for _, sample := range ts.Samples {
			points = append(points, Point{
				Metric:    metric,
				Tags:      tags,
				Value:     sample.Value,
				Timestamp: sample.Timestamp * int64(time.Millisecond),
			})
		}
	}
	return points
}

// handlePromQuery handles Prometheus-compatible /api/v1/query and /api/v1/query_range
func handlePromQuery(db *DB, w http.ResponseWriter, r *http.Request, isRange bool) {
	var query string
	var start, end int64
	var step time.Duration

	if r.Method == http.MethodGet {
		query = r.URL.Query().Get("query")
		if s := r.URL.Query().Get("time"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
				end = start
			}
		}
		if s := r.URL.Query().Get("start"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
			}
		}
		if s := r.URL.Query().Get("end"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				end = int64(t * 1e9)
			}
		}
		if s := r.URL.Query().Get("step"); s != "" {
			if d, err := time.ParseDuration(s); err == nil {
				step = d
			} else if f, err := strconv.ParseFloat(s, 64); err == nil {
				step = time.Duration(f * float64(time.Second))
			}
		}
	} else if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			promError(w, "bad_data", err.Error())
			return
		}
		query = r.FormValue("query")
		if s := r.FormValue("time"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
				end = start
			}
		}
		if s := r.FormValue("start"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				start = int64(t * 1e9)
			}
		}
		if s := r.FormValue("end"); s != "" {
			if t, err := strconv.ParseFloat(s, 64); err == nil {
				end = int64(t * 1e9)
			}
		}
		if s := r.FormValue("step"); s != "" {
			if d, err := time.ParseDuration(s); err == nil {
				step = d
			}
		}
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if query == "" {
		promError(w, "bad_data", "query parameter is required")
		return
	}

	// Parse PromQL
	parser := &PromQLParser{}
	pq, err := parser.Parse(query)
	if err != nil {
		promError(w, "bad_data", err.Error())
		return
	}

	// Set defaults
	if start == 0 {
		start = time.Now().Add(-time.Hour).UnixNano()
	}
	if end == 0 {
		end = time.Now().UnixNano()
	}
	if step == 0 {
		step = time.Minute
	}

	// Convert to Chronicle query
	cq := pq.ToChronicleQuery(start, end)
	if cq.Aggregation != nil && cq.Aggregation.Window <= 0 {
		cq.Aggregation.Window = step
	}

	result, err := db.Execute(cq)
	if err != nil {
		promError(w, "execution", err.Error())
		return
	}

	// Format response
	resp := promResponse(result, pq.Metric, isRange)
	writeJSON(w, resp)
}

func promError(w http.ResponseWriter, errorType, message string) {
	jsonError(w, http.StatusBadRequest, errorType, message)
}

func promResponse(result *Result, metric string, isRange bool) map[string]interface{} {
	data := make([]map[string]interface{}, 0)

	// Group points by tags
	groups := make(map[string][]Point)
	for _, p := range result.Points {
		key := formatTags(p.Tags)
		groups[key] = append(groups[key], p)
	}

	for _, points := range groups {
		if len(points) == 0 {
			continue
		}

		labels := make(map[string]string)
		labels["__name__"] = metric
		for k, v := range points[0].Tags {
			labels[k] = v
		}

		entry := map[string]interface{}{
			"metric": labels,
		}

		if isRange {
			values := make([][]interface{}, len(points))
			for i, p := range points {
				values[i] = []interface{}{
					float64(p.Timestamp) / 1e9,
					strconv.FormatFloat(p.Value, 'f', -1, 64),
				}
			}
			entry["values"] = values
		} else {
			if len(points) > 0 {
				p := points[len(points)-1]
				entry["value"] = []interface{}{
					float64(p.Timestamp) / 1e9,
					strconv.FormatFloat(p.Value, 'f', -1, 64),
				}
			}
		}

		data = append(data, entry)
	}

	resultType := "vector"
	if isRange {
		resultType = "matrix"
	}

	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": resultType,
			"result":     data,
		},
	}
}

func formatTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tags))
	for k, v := range tags {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

// handleSchemas handles schema registry operations
func handleSchemas(db *DB, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		schemas := db.ListSchemas()
		writeJSON(w, schemas)

	case http.MethodPost:
		var schema MetricSchema
		if err := json.NewDecoder(r.Body).Decode(&schema); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := db.RegisterSchema(schema); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeError(w, "name parameter required", http.StatusBadRequest)
			return
		}
		db.UnregisterSchema(name)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleAlerts handles alert listing
func handleAlerts(db *DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	am := db.AlertManager()
	if am == nil {
		writeError(w, "alert manager not initialized", http.StatusServiceUnavailable)
		return
	}
	alerts := am.ListAlerts()

	resp := make([]map[string]interface{}, len(alerts))
	for i, a := range alerts {
		resp[i] = map[string]interface{}{
			"name":    a.Rule.Name,
			"state":   a.State.String(),
			"value":   a.Value,
			"firedAt": a.FiredAt,
			"labels":  a.Labels,
		}
	}

	writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   map[string]interface{}{"alerts": resp},
	})
}

// handleRules handles alert rule management
func handleRules(db *DB, w http.ResponseWriter, r *http.Request) {
	am := db.AlertManager()
	if am == nil {
		writeError(w, "alert manager not initialized", http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodGet:
		rules := am.ListRules()
		resp := make([]map[string]interface{}, len(rules))
		for i, r := range rules {
			resp[i] = map[string]interface{}{
				"name":        r.Name,
				"metric":      r.Metric,
				"condition":   conditionString(r.Condition),
				"threshold":   r.Threshold,
				"forDuration": r.ForDuration.String(),
			}
		}
		writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   map[string]interface{}{"groups": []interface{}{map[string]interface{}{"rules": resp}}},
		})

	case http.MethodPost:
		var rule AlertRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := am.AddRule(rule); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeError(w, "name parameter required", http.StatusBadRequest)
			return
		}
		am.RemoveRule(name)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleForecast handles forecast API requests
func handleForecast(db *DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Metric  string `json:"metric"`
		Start   int64  `json:"start"`
		End     int64  `json:"end"`
		Periods int    `json:"periods"`
		Method  string `json:"method"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Periods <= 0 {
		req.Periods = 10
	}

	// Query historical data
	query := &Query{
		Metric: req.Metric,
		Start:  req.Start,
		End:    req.End,
	}

	result, err := db.Execute(query)
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(result.Points) < 2 {
		writeError(w, "insufficient data for forecasting", http.StatusBadRequest)
		return
	}

	// Convert to TimeSeriesData
	data := TimeSeriesData{
		Timestamps: make([]int64, len(result.Points)),
		Values:     make([]float64, len(result.Points)),
	}
	for i, p := range result.Points {
		data.Timestamps[i] = p.Timestamp
		data.Values[i] = p.Value
	}

	// Configure forecaster
	config := DefaultForecastConfig()
	switch req.Method {
	case "simple":
		config.Method = ForecastMethodSimpleExponential
	case "double":
		config.Method = ForecastMethodDoubleExponential
	case "moving_average":
		config.Method = ForecastMethodMovingAverage
	default:
		config.Method = ForecastMethodHoltWinters
	}

	forecaster := NewForecaster(config)
	forecast, err := forecaster.Forecast(data, req.Periods)
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"predictions": forecast.Predictions,
		"anomalies":   forecast.Anomalies,
		"rmse":        forecast.RMSE,
		"mae":         forecast.MAE,
	})
}

// handleHistogram handles histogram API requests
func handleHistogram(db *DB, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeError(w, "name parameter required", http.StatusBadRequest)
			return
		}

		if db.histogramStore == nil {
			writeError(w, "histogram store not initialized", http.StatusServiceUnavailable)
			return
		}

		results, err := db.histogramStore.Query(name, nil, 0, 0)
		if err != nil || len(results) == 0 {
			writeError(w, "histogram not found", http.StatusNotFound)
			return
		}

		h := results[len(results)-1].Histogram
		writeJSON(w, map[string]interface{}{
			"name":    name,
			"count":   h.Count,
			"sum":     h.Sum,
			"buckets": h.PositiveBuckets,
		})

	case http.MethodPost:
		var req struct {
			Name  string  `json:"name"`
			Value float64 `json:"value"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		if db.histogramStore == nil {
			writeError(w, "histogram store not initialized", http.StatusServiceUnavailable)
			return
		}

		h := NewHistogram(3)
		h.Observe(req.Value)

		if err := db.histogramStore.Write(HistogramPoint{
			Metric:    req.Name,
			Histogram: h,
			Timestamp: time.Now().UnixNano(),
		}); err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
