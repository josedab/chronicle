package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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
	db       *DB
	config   GrafanaBackendConfig
	server   *http.Server
	hub      *StreamHub

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
	From    string          `json:"from"`
	To      string          `json:"to"`
	Queries []GrafanaQuery  `json:"queries"`
}

// GrafanaQueryResponse is the Grafana query response format.
type GrafanaQueryResponse struct {
	Results map[string]GrafanaQueryResult `json:"results"`
}

// GrafanaQueryResult is a single query result.
type GrafanaQueryResult struct {
	RefID  string              `json:"refId"`
	Frames []GrafanaDataFrame  `json:"frames"`
	Error  string              `json:"error,omitempty"`
}

// GrafanaDataFrame is a Grafana data frame.
type GrafanaDataFrame struct {
	Schema GrafanaFrameSchema `json:"schema"`
	Data   GrafanaFrameData   `json:"data"`
}

// GrafanaFrameSchema describes the frame structure.
type GrafanaFrameSchema struct {
	RefID  string              `json:"refId,omitempty"`
	Name   string              `json:"name,omitempty"`
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
	Values [][]interface{} `json:"values"`
}

// GrafanaAnnotation represents a Grafana annotation.
type GrafanaAnnotation struct {
	ID          int64             `json:"id"`
	DashboardID int64             `json:"dashboardId"`
	PanelID     int64             `json:"panelId"`
	Time        int64             `json:"time"`
	TimeEnd     int64             `json:"timeEnd,omitempty"`
	Text        string            `json:"text"`
	Tags        []string          `json:"tags,omitempty"`
	Data        map[string]interface{} `json:"data,omitempty"`
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
	mux.HandleFunc("/", g.handleRoot)
	mux.HandleFunc("/health", g.handleHealth)
	mux.HandleFunc("/query", g.handleQuery)
	mux.HandleFunc("/metrics", g.handleMetrics)
	mux.HandleFunc("/tag-keys", g.handleTagKeys)
	mux.HandleFunc("/tag-values", g.handleTagValues)
	mux.HandleFunc("/annotations", g.handleAnnotations)
	mux.HandleFunc("/search", g.handleSearch)
	mux.HandleFunc("/variable", g.handleVariable)

	// Streaming endpoint
	if g.config.EnableStreaming {
		mux.HandleFunc("/stream", g.handleStream)
	}

	// Alert management endpoints
	mux.HandleFunc("/alerts", g.handleAlerts)

	g.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", g.config.HTTPPort),
		Handler: g.corsMiddleware(mux),
	}

	go func() {
		if err := g.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error
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
	return g.server.Shutdown(ctx)
}

func (g *GrafanaBackend) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Grafana-Org-Id")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (g *GrafanaBackend) handleRoot(w http.ResponseWriter, r *http.Request) {
	g.writeJSON(w, map[string]interface{}{
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse time range
	from, to, err := g.parseTimeRange(req.From, req.To)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	times := make([]interface{}, len(points))
	values := make([]interface{}, len(points))

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
			Values: [][]interface{}{times, values},
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

	from, _ := strconv.ParseInt(fromStr, 10, 64)
	to, _ := strconv.ParseInt(toStr, 10, 64)

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
	if err := json.NewDecoder(r.Body).Decode(&ann); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
		json.NewDecoder(r.Body).Decode(&req)
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
		json.NewDecoder(r.Body).Decode(&req)
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

func (g *GrafanaBackend) handleStream(w http.ResponseWriter, r *http.Request) {
	if g.hub == nil {
		http.Error(w, "streaming not enabled", http.StatusServiceUnavailable)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Track subscriptions for this connection
	connSubs := make(map[string]*Subscription)
	var connMu sync.Mutex

	// Read commands from client
	go func() {
		defer cancel()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var cmd StreamMessage
			if err := json.Unmarshal(msg, &cmd); err != nil {
				resp, _ := json.Marshal(StreamMessage{Type: "error", Error: "invalid message format"})
				_ = conn.WriteMessage(1, resp)
				continue
			}

			switch cmd.Type {
			case "subscribe":
				sub := g.hub.Subscribe(cmd.Metric, cmd.Tags)
				connMu.Lock()
				connSubs[sub.ID] = sub
				connMu.Unlock()

				resp, _ := json.Marshal(StreamMessage{Type: "subscribed", SubID: sub.ID})
				_ = conn.WriteMessage(1, resp)

				go g.forwardStreamPoints(ctx, conn, sub)

			case "unsubscribe":
				connMu.Lock()
				if sub, ok := connSubs[cmd.SubID]; ok {
					delete(connSubs, cmd.SubID)
					g.hub.Unsubscribe(sub.ID)
				}
				connMu.Unlock()

				resp, _ := json.Marshal(StreamMessage{Type: "unsubscribed", SubID: cmd.SubID})
				_ = conn.WriteMessage(1, resp)

			default:
				resp, _ := json.Marshal(StreamMessage{Type: "error", Error: "unknown command: " + cmd.Type})
				_ = conn.WriteMessage(1, resp)
			}
		}
	}()

	<-ctx.Done()

	// Cleanup all subscriptions
	connMu.Lock()
	for _, sub := range connSubs {
		g.hub.Unsubscribe(sub.ID)
	}
	connMu.Unlock()
}

func (g *GrafanaBackend) forwardStreamPoints(ctx context.Context, conn *websocket.Conn, sub *Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.done:
			return
		case p, ok := <-sub.ch:
			if !ok {
				return
			}
			msg, _ := json.Marshal(StreamMessage{
				Type:  "point",
				SubID: sub.ID,
				Point: &p,
			})
			if err := conn.WriteMessage(1, msg); err != nil {
				return
			}
		}
	}
}

func (g *GrafanaBackend) handleAlerts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.listAlertRules(w, r)
	case http.MethodPost:
		g.createAlertRule(w, r)
	case http.MethodDelete:
		g.deleteAlertRule(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (g *GrafanaBackend) createAlertRule(w http.ResponseWriter, r *http.Request) {
	var rule GrafanaAlertRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if rule.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if rule.ID == "" {
		rule.ID = fmt.Sprintf("alert-%d", time.Now().UnixNano())
	}

	// Register with AlertManager if available
	if g.alertMgr != nil {
		alertRule := AlertRule{
			Name:        rule.Name,
			Metric:      rule.Query.Metric,
			Tags:        rule.Query.Tags,
			Condition:   g.parseAlertCondition(rule.Condition),
			Threshold:   rule.Threshold,
			Labels:      rule.Labels,
			Annotations: rule.Annotations,
		}
		if rule.For != "" {
			alertRule.ForDuration, _ = g.parseDuration(rule.For)
		}
		if err := g.alertMgr.AddRule(alertRule); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	g.alertMu.Lock()
	g.alertRules = append(g.alertRules, rule)
	g.alertMu.Unlock()

	g.writeJSON(w, rule)
}

func (g *GrafanaBackend) listAlertRules(w http.ResponseWriter, r *http.Request) {
	g.alertMu.RLock()
	rules := g.alertRules
	g.alertMu.RUnlock()

	if rules == nil {
		rules = []GrafanaAlertRule{}
	}

	g.writeJSON(w, rules)
}

func (g *GrafanaBackend) deleteAlertRule(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		// Try to extract from path
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/alerts/"), "/")
		if len(parts) > 0 && parts[0] != "" {
			id = parts[0]
		}
	}

	if id == "" {
		http.Error(w, "id parameter required", http.StatusBadRequest)
		return
	}

	g.alertMu.Lock()
	found := false
	for i, rule := range g.alertRules {
		if rule.ID == id {
			g.alertRules = append(g.alertRules[:i], g.alertRules[i+1:]...)
			found = true
			// Remove from AlertManager if available
			if g.alertMgr != nil {
				g.alertMgr.RemoveRule(rule.Name)
			}
			break
		}
	}
	g.alertMu.Unlock()

	if !found {
		http.Error(w, "alert rule not found", http.StatusNotFound)
		return
	}

	g.writeJSON(w, map[string]string{"status": "deleted", "id": id})
}

func (g *GrafanaBackend) parseAlertCondition(s string) AlertCondition {
	switch strings.ToLower(s) {
	case "above", "gt", ">":
		return AlertConditionAbove
	case "below", "lt", "<":
		return AlertConditionBelow
	case "equal", "eq", "=":
		return AlertConditionEqual
	case "not_equal", "ne", "!=":
		return AlertConditionNotEqual
	case "absent", "no_data":
		return AlertConditionAbsent
	default:
		return AlertConditionAbove
	}
}

// Helper methods

func (g *GrafanaBackend) parseTimeRange(from, to string) (int64, int64, error) {
	// Handle relative time strings like "now-1h"
	fromTime, err := g.parseTime(from)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid from time: %w", err)
	}

	toTime, err := g.parseTime(to)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid to time: %w", err)
	}

	return fromTime.UnixNano(), toTime.UnixNano(), nil
}

func (g *GrafanaBackend) parseTime(s string) (time.Time, error) {
	// Try numeric timestamp first
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ts), nil
	}

	// Handle "now" and relative time
	if strings.HasPrefix(s, "now") {
		now := time.Now()
		if s == "now" {
			return now, nil
		}

		// Parse relative offset (now-1h, now-30m, etc.)
		offset := strings.TrimPrefix(s, "now")
		if strings.HasPrefix(offset, "-") || strings.HasPrefix(offset, "+") {
			duration, err := g.parseDuration(offset[1:])
			if err != nil {
				return time.Time{}, err
			}
			if offset[0] == '-' {
				return now.Add(-duration), nil
			}
			return now.Add(duration), nil
		}
	}

	// Try parsing as RFC3339
	return time.Parse(time.RFC3339, s)
}

func (g *GrafanaBackend) parseDuration(s string) (time.Duration, error) {
	// Handle common formats: 1h, 30m, 1d, 1w
	if len(s) < 2 {
		return 0, errors.New("invalid duration")
	}

	unit := s[len(s)-1]
	value, err := strconv.Atoi(s[:len(s)-1])
	if err != nil {
		return 0, err
	}

	switch unit {
	case 's':
		return time.Duration(value) * time.Second, nil
	case 'm':
		return time.Duration(value) * time.Minute, nil
	case 'h':
		return time.Duration(value) * time.Hour, nil
	case 'd':
		return time.Duration(value) * 24 * time.Hour, nil
	case 'w':
		return time.Duration(value) * 7 * 24 * time.Hour, nil
	default:
		return time.ParseDuration(s)
	}
}

func (g *GrafanaBackend) parseWindow(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, _ := g.parseDuration(s)
	return d
}

func (g *GrafanaBackend) parseAggFunc(s string) AggFunc {
	switch strings.ToLower(s) {
	case "mean", "avg", "average":
		return AggMean
	case "sum":
		return AggSum
	case "count":
		return AggCount
	case "min":
		return AggMin
	case "max":
		return AggMax
	case "first":
		return AggFirst
	case "last":
		return AggLast
	default:
		return AggMean
	}
}

func (g *GrafanaBackend) parseQueryText(text string) (*Query, error) {
	// Basic SQL-like query parsing
	// Format: SELECT agg(field) FROM metric WHERE tags GROUP BY time(window)
	text = strings.TrimSpace(text)

	query := &Query{}

	// Extract metric name (FROM clause)
	fromIdx := strings.Index(strings.ToUpper(text), "FROM ")
	if fromIdx == -1 {
		return nil, errors.New("missing FROM clause")
	}

	afterFrom := text[fromIdx+5:]
	endIdx := strings.IndexAny(afterFrom, " \t\n")
	if endIdx == -1 {
		query.Metric = strings.TrimSpace(afterFrom)
	} else {
		query.Metric = strings.TrimSpace(afterFrom[:endIdx])
	}

	// Extract aggregation (SELECT clause)
	selectIdx := strings.Index(strings.ToUpper(text), "SELECT ")
	if selectIdx != -1 {
		selectClause := text[selectIdx+7 : fromIdx]
		// Parse aggregation function
		if parenIdx := strings.Index(selectClause, "("); parenIdx != -1 {
			funcName := strings.TrimSpace(selectClause[:parenIdx])
			query.Aggregation = &Aggregation{
				Function: g.parseAggFunc(funcName),
				Window:   time.Minute, // Default
			}
		}
	}

	// Extract GROUP BY time window
	groupByIdx := strings.Index(strings.ToUpper(text), "GROUP BY ")
	if groupByIdx != -1 && query.Aggregation != nil {
		groupByClause := text[groupByIdx+9:]
		if timeIdx := strings.Index(strings.ToLower(groupByClause), "time("); timeIdx != -1 {
			start := timeIdx + 5
			end := strings.Index(groupByClause[start:], ")")
			if end != -1 {
				windowStr := groupByClause[start : start+end]
				if d, err := g.parseDuration(windowStr); err == nil {
					query.Aggregation.Window = d
				}
			}
		}
	}

	return query, nil
}

func (g *GrafanaBackend) getMetricsCached() []string {
	g.metricCacheMu.RLock()
	if time.Since(g.metricCacheAt) < g.config.CacheDuration && g.metricCache != nil {
		defer g.metricCacheMu.RUnlock()
		return g.metricCache
	}
	g.metricCacheMu.RUnlock()

	// Refresh cache
	metrics := g.db.Metrics()
	sort.Strings(metrics)

	g.metricCacheMu.Lock()
	g.metricCache = metrics
	g.metricCacheAt = time.Now()
	g.metricCacheMu.Unlock()

	return metrics
}

func (g *GrafanaBackend) getTagKeys() []string {
	if g.db == nil {
		return []string{}
	}
	keys := g.db.TagKeys()
	if len(keys) == 0 {
		return []string{}
	}
	sort.Strings(keys)
	return keys
}

func (g *GrafanaBackend) getTagValues(key string) []string {
	if g.db == nil {
		return []string{}
	}
	values := g.db.TagValues(key)
	if len(values) == 0 {
		return []string{}
	}
	sort.Strings(values)
	return values
}

func (g *GrafanaBackend) downsamplePoints(points []Point, maxPoints int) []Point {
	if len(points) <= maxPoints {
		return points
	}

	// Simple downsampling by taking every Nth point
	step := len(points) / maxPoints
	if step < 1 {
		step = 1
	}

	result := make([]Point, 0, maxPoints)
	for i := 0; i < len(points); i += step {
		if len(result) >= maxPoints {
			break
		}
		result = append(result, points[i])
	}

	return result
}

func (g *GrafanaBackend) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// GrafanaAlertRule represents a Grafana alerting rule.
type GrafanaAlertRule struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Query       GrafanaQuery       `json:"query"`
	Condition   string             `json:"condition"`
	Threshold   float64            `json:"threshold"`
	For         string             `json:"for"`
	Labels      map[string]string  `json:"labels,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

// GrafanaAlertState represents alert state.
type GrafanaAlertState string

const (
	GrafanaAlertStateOK       GrafanaAlertState = "ok"
	GrafanaAlertStatePending  GrafanaAlertState = "pending"
	GrafanaAlertStateAlerting GrafanaAlertState = "alerting"
)

// AddAnnotation adds an annotation programmatically.
func (g *GrafanaBackend) AddAnnotation(ann GrafanaAnnotation) {
	g.annotationsMu.Lock()
	defer g.annotationsMu.Unlock()

	ann.ID = int64(len(g.annotations) + 1)
	if ann.Time == 0 {
		ann.Time = time.Now().UnixMilli()
	}
	g.annotations = append(g.annotations, ann)
}

// GetAnnotations returns annotations in a time range.
func (g *GrafanaBackend) GetAnnotations(from, to int64) []GrafanaAnnotation {
	g.annotationsMu.RLock()
	defer g.annotationsMu.RUnlock()

	var result []GrafanaAnnotation
	for _, ann := range g.annotations {
		if ann.Time >= from && ann.Time <= to {
			result = append(result, ann)
		}
	}
	return result
}
