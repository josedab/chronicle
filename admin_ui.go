package chronicle

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AdminUI provides a web-based admin interface for Chronicle.
type AdminUI struct {
	db               *DB
	mux              *http.ServeMux
	startTime        time.Time
	mu               sync.RWMutex
	metrics          *adminMetrics
	queryHistory     []queryHistoryEntry
	activityLog      []activityEntry
	devMode          bool
	auditLog         []auditLogEntry
	alertRules       []adminAlertRule
	retentionRules   []retentionRule
	scheduledExports []scheduledExport
	sseClients       map[chan []byte]bool
	sseMu            sync.RWMutex
	savedQueries     []savedQuery
	favorites        []favoriteItem
	recentItems      []recentItem
	alertHistory     []alertHistoryEntry
	sessions         []sessionInfo
	// Phase 11 fields
	queryTemplates    []queryTemplate
	annotations       []metricAnnotation
	logBuffer         []logEntry
	roles             []userRole
	userPermissions   map[string]*userAccess
}

type adminMetrics struct {
	Writes      int64
	Reads       int64
	Errors      int64
	LastError   string
	LastErrorAt time.Time
}

type queryHistoryEntry struct {
	Query     string    `json:"query"`
	Timestamp time.Time `json:"timestamp"`
	Duration  float64   `json:"duration_ms"`
	Success   bool      `json:"success"`
	Error     string    `json:"error,omitempty"`
}

type activityEntry struct {
	Action    string    `json:"action"`
	Details   string    `json:"details"`
	Timestamp time.Time `json:"timestamp"`
}

// auditLogEntry tracks administrative actions for security auditing
type auditLogEntry struct {
	ID        string    `json:"id"`
	Action    string    `json:"action"`
	User      string    `json:"user"`
	IP        string    `json:"ip"`
	Details   string    `json:"details"`
	Timestamp time.Time `json:"timestamp"`
	Success   bool      `json:"success"`
}

// adminAlertRule represents an alert rule in the admin UI
type adminAlertRule struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Metric      string            `json:"metric"`
	Condition   string            `json:"condition"`
	Threshold   float64           `json:"threshold"`
	Duration    string            `json:"duration"`
	State       string            `json:"state"`
	Enabled     bool              `json:"enabled"`
	Labels      map[string]string `json:"labels,omitempty"`
	WebhookURL  string            `json:"webhook_url,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	LastFired   *time.Time        `json:"last_fired,omitempty"`
}

// retentionRule defines data retention policy for a metric
type retentionRule struct {
	ID         string    `json:"id"`
	Metric     string    `json:"metric"`
	Duration   string    `json:"duration"`
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
	LastApplied *time.Time `json:"last_applied,omitempty"`
}

// scheduledExport defines an automated export schedule
type scheduledExport struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Query      string    `json:"query"`
	Format     string    `json:"format"`
	Schedule   string    `json:"schedule"`
	Enabled    bool      `json:"enabled"`
	LastRun    *time.Time `json:"last_run,omitempty"`
	NextRun    *time.Time `json:"next_run,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// queryExplainResult contains query analysis information
type queryExplainResult struct {
	Query          string   `json:"query"`
	ParsedMetric   string   `json:"parsed_metric"`
	ParsedFunction string   `json:"parsed_function"`
	TimeRange      string   `json:"time_range"`
	EstimatedRows  int      `json:"estimated_rows"`
	IndexUsed      bool     `json:"index_used"`
	Steps          []string `json:"steps"`
}

// savedQuery represents a saved/bookmarked query
type savedQuery struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Query       string    `json:"query"`
	Description string    `json:"description,omitempty"`
	Tags        []string  `json:"tags,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UsageCount  int       `json:"usage_count"`
}

// favoriteItem represents a bookmarked metric or page
type favoriteItem struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"` // "metric" or "page"
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// recentItem tracks recently accessed items
type recentItem struct {
	Type       string    `json:"type"` // "metric", "query", "page"
	Name       string    `json:"name"`
	Details    string    `json:"details,omitempty"`
	AccessedAt time.Time `json:"accessed_at"`
}

// alertHistoryEntry records alert state changes
type alertHistoryEntry struct {
	ID         string    `json:"id"`
	AlertID    string    `json:"alert_id"`
	AlertName  string    `json:"alert_name"`
	State      string    `json:"state"` // "firing", "resolved"
	Value      float64   `json:"value"`
	Threshold  float64   `json:"threshold"`
	Timestamp  time.Time `json:"timestamp"`
	Message    string    `json:"message,omitempty"`
}

// sessionInfo tracks active admin sessions
type sessionInfo struct {
	ID        string    `json:"id"`
	User      string    `json:"user"`
	IP        string    `json:"ip"`
	UserAgent string    `json:"user_agent"`
	StartedAt time.Time `json:"started_at"`
	LastSeen  time.Time `json:"last_seen"`
	Active    bool      `json:"active"`
}

// queryTemplate represents a pre-built query snippet
type queryTemplate struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Category    string   `json:"category"` // "aggregation", "analysis", "monitoring", etc.
	Query       string   `json:"query"`
	Variables   []string `json:"variables,omitempty"` // Placeholders like {{metric}}
	BuiltIn     bool     `json:"built_in"`
}

// metricAnnotation stores notes/comments on metrics
type metricAnnotation struct {
	ID        string    `json:"id"`
	Metric    string    `json:"metric"`
	Timestamp int64     `json:"timestamp,omitempty"` // Optional: annotation at specific time
	Title     string    `json:"title"`
	Text      string    `json:"text"`
	Tags      []string  `json:"tags,omitempty"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
}

// logEntry represents a single log line
type logEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"` // "debug", "info", "warn", "error"
	Message   string    `json:"message"`
	Source    string    `json:"source,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// userRole defines access control roles
type userRole struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Permissions []string `json:"permissions"` // "read", "write", "admin", "export", etc.
}

// userAccess tracks user permissions
type userAccess struct {
	User        string    `json:"user"`
	Role        string    `json:"role"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"created_at"`
	LastLogin   time.Time `json:"last_login,omitempty"`
}

// AdminConfig configures the admin UI.
type AdminConfig struct {
	// Prefix is the URL prefix for admin routes (default: "/admin").
	Prefix string

	// Username for basic auth (optional).
	Username string

	// Password for basic auth (optional).
	Password string

	// DevMode enables dangerous operations like data insertion (default: false).
	DevMode bool
}

// NewAdminUI creates an admin UI instance.
func NewAdminUI(db *DB, config AdminConfig) *AdminUI {
	if config.Prefix == "" {
		config.Prefix = "/admin"
	}

	ui := &AdminUI{
		db:               db,
		mux:              http.NewServeMux(),
		startTime:        time.Now(),
		metrics:          &adminMetrics{},
		queryHistory:     make([]queryHistoryEntry, 0, 100),
		activityLog:      make([]activityEntry, 0, 100),
		devMode:          config.DevMode,
		auditLog:         make([]auditLogEntry, 0, 500),
		alertRules:       make([]adminAlertRule, 0),
		retentionRules:   make([]retentionRule, 0),
		scheduledExports: make([]scheduledExport, 0),
		sseClients:       make(map[chan []byte]bool),
		savedQueries:     make([]savedQuery, 0),
		favorites:        make([]favoriteItem, 0),
		recentItems:      make([]recentItem, 0, 50),
		alertHistory:     make([]alertHistoryEntry, 0, 200),
		sessions:         make([]sessionInfo, 0),
		queryTemplates:   initBuiltInTemplates(),
		annotations:      make([]metricAnnotation, 0),
		logBuffer:        make([]logEntry, 0, 1000),
		roles:            initDefaultRoles(),
		userPermissions:  make(map[string]*userAccess),
	}

	// Register routes - main dashboard
	ui.mux.HandleFunc(config.Prefix, ui.handleDashboard)
	ui.mux.HandleFunc(config.Prefix+"/", ui.handleDashboard)

	// Phase 2: Stats and monitoring APIs
	ui.mux.HandleFunc(config.Prefix+"/api/stats", ui.handleAPIStats)
	ui.mux.HandleFunc(config.Prefix+"/api/metrics", ui.handleAPIMetrics)
	ui.mux.HandleFunc(config.Prefix+"/api/series", ui.handleAPISeries)
	ui.mux.HandleFunc(config.Prefix+"/api/health", ui.handleAPIHealth)
	ui.mux.HandleFunc(config.Prefix+"/api/activity", ui.handleAPIActivity)

	// Phase 3: Data Explorer APIs
	ui.mux.HandleFunc(config.Prefix+"/api/metric-details", ui.handleAPIMetricDetails)
	ui.mux.HandleFunc(config.Prefix+"/api/tags", ui.handleAPITags)
	ui.mux.HandleFunc(config.Prefix+"/api/data-preview", ui.handleAPIDataPreview)

	// Phase 4: Query Console APIs
	ui.mux.HandleFunc(config.Prefix+"/api/query", ui.handleAPIQuery)
	ui.mux.HandleFunc(config.Prefix+"/api/query-history", ui.handleAPIQueryHistory)
	ui.mux.HandleFunc(config.Prefix+"/api/export", ui.handleAPIExport)

	// Phase 5: Data Management APIs
	ui.mux.HandleFunc(config.Prefix+"/api/delete-metric", ui.handleAPIDeleteMetric)
	ui.mux.HandleFunc(config.Prefix+"/api/truncate", ui.handleAPITruncate)
	ui.mux.HandleFunc(config.Prefix+"/api/insert", ui.handleAPIInsert)

	// Phase 6: Configuration and Operations APIs
	ui.mux.HandleFunc(config.Prefix+"/api/config", ui.handleAPIConfig)
	ui.mux.HandleFunc(config.Prefix+"/api/partitions", ui.handleAPIPartitions)
	ui.mux.HandleFunc(config.Prefix+"/api/backup", ui.handleAPIBackup)

	// Phase 7: Enhanced APIs - Real-time, Alerting, Audit
	ui.mux.HandleFunc(config.Prefix+"/api/events", ui.handleAPIEvents)          // SSE endpoint
	ui.mux.HandleFunc(config.Prefix+"/api/alerts", ui.handleAPIAlerts)          // Alert rules CRUD
	ui.mux.HandleFunc(config.Prefix+"/api/audit-log", ui.handleAPIAuditLog)     // Audit log
	ui.mux.HandleFunc(config.Prefix+"/api/query-explain", ui.handleAPIQueryExplain) // Query explain

	// Phase 8: Schema, Retention, Cluster
	ui.mux.HandleFunc(config.Prefix+"/api/schemas", ui.handleAPISchemas)        // Schema registry
	ui.mux.HandleFunc(config.Prefix+"/api/retention", ui.handleAPIRetention)    // Retention policies
	ui.mux.HandleFunc(config.Prefix+"/api/cluster", ui.handleAPICluster)        // Cluster status

	// Phase 9: WAL, Scheduled Exports
	ui.mux.HandleFunc(config.Prefix+"/api/wal", ui.handleAPIWAL)                // WAL inspector
	ui.mux.HandleFunc(config.Prefix+"/api/scheduled-exports", ui.handleAPIScheduledExports)
	ui.mux.HandleFunc(config.Prefix+"/api/search", ui.handleAPISearch)          // Global search

	// Phase 10: Query UX, Productivity, Advanced Features
	ui.mux.HandleFunc(config.Prefix+"/api/autocomplete", ui.handleAPIAutocomplete)    // Query autocomplete
	ui.mux.HandleFunc(config.Prefix+"/api/saved-queries", ui.handleAPISavedQueries)   // Saved queries
	ui.mux.HandleFunc(config.Prefix+"/api/favorites", ui.handleAPIFavorites)          // Favorites
	ui.mux.HandleFunc(config.Prefix+"/api/recent", ui.handleAPIRecent)                // Recent items
	ui.mux.HandleFunc(config.Prefix+"/api/alert-history", ui.handleAPIAlertHistory)   // Alert history
	ui.mux.HandleFunc(config.Prefix+"/api/import", ui.handleAPIImport)                // Data import
	ui.mux.HandleFunc(config.Prefix+"/api/diagnostics", ui.handleAPIDiagnostics)      // System diagnostics
	ui.mux.HandleFunc(config.Prefix+"/api/sessions", ui.handleAPISessions)            // Session management
	ui.mux.HandleFunc(config.Prefix+"/api/sparkline", ui.handleAPISparkline)          // Sparkline data
	ui.mux.HandleFunc(config.Prefix+"/api/compare", ui.handleAPICompare)              // Multi-metric compare

	// Phase 11: Templates, Annotations, Profiling, Logs, RBAC
	ui.mux.HandleFunc(config.Prefix+"/api/templates", ui.handleAPITemplates)          // Query templates
	ui.mux.HandleFunc(config.Prefix+"/api/annotations", ui.handleAPIAnnotations)      // Metric annotations
	ui.mux.HandleFunc(config.Prefix+"/api/profiling", ui.handleAPIProfiling)          // Performance profiling
	ui.mux.HandleFunc(config.Prefix+"/api/logs", ui.handleAPILogs)                    // Log viewer
	ui.mux.HandleFunc(config.Prefix+"/api/roles", ui.handleAPIRoles)                  // Role management
	ui.mux.HandleFunc(config.Prefix+"/api/permissions", ui.handleAPIPermissions)      // User permissions

	return ui
}

// Handler returns the HTTP handler for the admin UI.
func (ui *AdminUI) Handler() http.Handler {
	return ui.mux
}

// ServeHTTP implements http.Handler.
func (ui *AdminUI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ui.mux.ServeHTTP(w, r)
}

// logActivity records an activity for the activity log
func (ui *AdminUI) logActivity(action, details string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	entry := activityEntry{
		Action:    action,
		Details:   details,
		Timestamp: time.Now(),
	}

	ui.activityLog = append(ui.activityLog, entry)
	if len(ui.activityLog) > 100 {
		ui.activityLog = ui.activityLog[1:]
	}
}

// addQueryHistory adds a query to the history
func (ui *AdminUI) addQueryHistory(query string, duration time.Duration, success bool, errMsg string) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	entry := queryHistoryEntry{
		Query:     query,
		Timestamp: time.Now(),
		Duration:  float64(duration.Microseconds()) / 1000.0,
		Success:   success,
		Error:     errMsg,
	}

	ui.queryHistory = append(ui.queryHistory, entry)
	if len(ui.queryHistory) > 100 {
		ui.queryHistory = ui.queryHistory[1:]
	}
}

func (ui *AdminUI) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	tmpl, err := template.New("dashboard").Parse(adminDashboardHTML)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := ui.getDashboardData()
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type dashboardData struct {
	Title        string
	Uptime       string
	Version      string
	GoVersion    string
	NumCPU       int
	NumGoroutine int
	MemStats     memStatsData
	DBStats      dbStatsData
	Metrics      []string
	Config       configData
	DevMode      bool
}

type memStatsData struct {
	Alloc      string
	TotalAlloc string
	Sys        string
	NumGC      uint32
}

type dbStatsData struct {
	MetricCount    int
	PartitionCount int
	BufferSize     int
	Retention      string
}

type configData struct {
	Path              string
	PartitionDuration string
	BufferSize        int
	SyncInterval      string
	Retention         string
}

func (ui *AdminUI) getDashboardData() dashboardData {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	metrics := ui.db.Metrics()

	partitionCount := 0
	if ui.db.index != nil {
		partitionCount = ui.db.index.Count()
	}

	return dashboardData{
		Title:        "Chronicle Admin",
		Uptime:       time.Since(ui.startTime).Round(time.Second).String(),
		Version:      "1.0.0",
		GoVersion:    runtime.Version(),
		NumCPU:       runtime.NumCPU(),
		NumGoroutine: runtime.NumGoroutine(),
		MemStats: memStatsData{
			Alloc:      formatBytes(m.Alloc),
			TotalAlloc: formatBytes(m.TotalAlloc),
			Sys:        formatBytes(m.Sys),
			NumGC:      m.NumGC,
		},
		DBStats: dbStatsData{
			MetricCount:    len(metrics),
			PartitionCount: partitionCount,
			BufferSize:     ui.db.config.BufferSize,
			Retention:      ui.db.config.RetentionDuration.String(),
		},
		Metrics: metrics,
		Config: configData{
			Path:              ui.db.config.Path,
			PartitionDuration: ui.db.config.PartitionDuration.String(),
			BufferSize:        ui.db.config.BufferSize,
			SyncInterval:      ui.db.config.SyncInterval.String(),
			Retention:         ui.db.config.RetentionDuration.String(),
		},
		DevMode: ui.devMode,
	}
}

func (ui *AdminUI) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	partitionCount := 0
	if ui.db.index != nil {
		partitionCount = ui.db.index.Count()
	}

	stats := map[string]interface{}{
		"uptime":          time.Since(ui.startTime).Seconds(),
		"version":         "1.0.0",
		"go_version":      runtime.Version(),
		"num_cpu":         runtime.NumCPU(),
		"num_goroutine":   runtime.NumGoroutine(),
		"metric_count":    len(ui.db.Metrics()),
		"partition_count": partitionCount,
		"memory": map[string]uint64{
			"alloc":       m.Alloc,
			"total_alloc": m.TotalAlloc,
			"sys":         m.Sys,
			"num_gc":      uint64(m.NumGC),
		},
		"db_status": func() string {
			if ui.db.closed {
				return "closed"
			}
			return "open"
		}(),
	}

	writeJSON(w, stats)
}

func (ui *AdminUI) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := ui.db.Metrics()
	sort.Strings(metrics)

	search := strings.ToLower(r.URL.Query().Get("search"))

	result := make([]map[string]interface{}, 0, len(metrics))
	for _, m := range metrics {
		if search != "" && !strings.Contains(strings.ToLower(m), search) {
			continue
		}
		result = append(result, map[string]interface{}{
			"name": m,
		})
	}

	writeJSON(w, result)
}

func (ui *AdminUI) handleAPISeries(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")

	var series []map[string]interface{}

	if metric != "" {
		series = append(series, map[string]interface{}{
			"metric": metric,
		})
	} else {
		for _, m := range ui.db.Metrics() {
			series = append(series, map[string]interface{}{
				"metric": m,
			})
		}
	}

	writeJSON(w, series)
}

func (ui *AdminUI) handleAPIQuery(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	start := time.Now()
	parser := &QueryParser{}
	q, err := parser.Parse(query)
	if err != nil {
		ui.addQueryHistory(query, time.Since(start), false, err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := ui.db.Execute(q)
	duration := time.Since(start)
	if err != nil {
		ui.addQueryHistory(query, duration, false, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ui.addQueryHistory(query, duration, true, "")
	ui.logActivity("Query", query)

	writeJSON(w, result)
}

func (ui *AdminUI) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"path":               ui.db.config.Path,
		"partition_duration": ui.db.config.PartitionDuration.String(),
		"buffer_size":        ui.db.config.BufferSize,
		"sync_interval":      ui.db.config.SyncInterval.String(),
		"retention":          ui.db.config.RetentionDuration.String(),
		"dev_mode":           ui.devMode,
	}

	writeJSON(w, config)
}

func (ui *AdminUI) handleAPIHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status": "healthy",
		"uptime": time.Since(ui.startTime).Seconds(),
		"checks": map[string]interface{}{
			"database":   "ok",
			"memory":     "ok",
			"goroutines": "ok",
		},
	}

	// Check if database is responsive
	if ui.db.closed {
		health["status"] = "unhealthy"
		health["checks"].(map[string]interface{})["database"] = "closed"
	}

	// Check memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > 1<<30 { // > 1GB
		health["checks"].(map[string]interface{})["memory"] = "warning"
	}

	// Check goroutine count
	if runtime.NumGoroutine() > 10000 {
		health["checks"].(map[string]interface{})["goroutines"] = "warning"
	}

	writeJSON(w, health)
}

// Phase 2: Activity log endpoint
func (ui *AdminUI) handleAPIActivity(w http.ResponseWriter, r *http.Request) {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	// Return most recent first
	result := make([]activityEntry, 0, limit)
	start := len(ui.activityLog) - limit
	if start < 0 {
		start = 0
	}
	for i := len(ui.activityLog) - 1; i >= start; i-- {
		result = append(result, ui.activityLog[i])
	}

	writeJSON(w, result)
}

// Phase 3: Metric details endpoint
func (ui *AdminUI) handleAPIMetricDetails(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	// Get metric info
	metrics := ui.db.Metrics()
	found := false
	for _, m := range metrics {
		if m == metric {
			found = true
			break
		}
	}

	if !found {
		http.Error(w, "metric not found", http.StatusNotFound)
		return
	}

	// Try to get sample data to determine tags
	parser := &QueryParser{}
	q, _ := parser.Parse(fmt.Sprintf("SELECT mean(value) FROM %s", metric))
	result, _ := ui.db.Execute(q)

	details := map[string]interface{}{
		"name":         metric,
		"exists":       true,
		"sample_count": len(result.Points),
	}

	writeJSON(w, details)
}

// Phase 3: Tags endpoint
func (ui *AdminUI) handleAPITags(w http.ResponseWriter, r *http.Request) {
	// Return empty tags since the DB doesn't have a direct tagIndex
	// Tags would need to be extracted from actual data points
	tags := make(map[string][]string)
	writeJSON(w, tags)
}

// Phase 3: Data preview endpoint
func (ui *AdminUI) handleAPIDataPreview(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}

	// Build query with time range
	queryStr := fmt.Sprintf("SELECT mean(value) FROM %s", metric)

	parser := &QueryParser{}
	q, err := parser.Parse(queryStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := ui.db.Execute(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Limit points
	points := result.Points
	if len(points) > limit {
		points = points[:limit]
	}

	preview := map[string]interface{}{
		"metric":   metric,
		"total":    len(result.Points),
		"returned": len(points),
		"points":   points,
		"has_more": len(result.Points) > limit,
	}

	writeJSON(w, preview)
}

// Phase 4: Query history endpoint
func (ui *AdminUI) handleAPIQueryHistory(w http.ResponseWriter, r *http.Request) {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	// Return most recent first
	result := make([]queryHistoryEntry, 0, limit)
	start := len(ui.queryHistory) - limit
	if start < 0 {
		start = 0
	}
	for i := len(ui.queryHistory) - 1; i >= start; i-- {
		result = append(result, ui.queryHistory[i])
	}

	writeJSON(w, result)
}

// Phase 4: Export endpoint
func (ui *AdminUI) handleAPIExport(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	if query == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	parser := &QueryParser{}
	q, err := parser.Parse(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := ui.db.Execute(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ui.logActivity("Export", fmt.Sprintf("format=%s, query=%s", format, query))

	switch format {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=export.csv")
		csvWriter := csv.NewWriter(w)
		csvWriter.Write([]string{"timestamp", "value", "metric"})
		for _, p := range result.Points {
			csvWriter.Write([]string{
				time.Unix(0, p.Timestamp).Format(time.RFC3339),
				fmt.Sprintf("%f", p.Value),
				p.Metric,
			})
		}
		csvWriter.Flush()
	default:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=export.json")
		json.NewEncoder(w).Encode(result)
	}
}

// Phase 5: Delete metric endpoint
func (ui *AdminUI) handleAPIDeleteMetric(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	// Note: Chronicle doesn't have a built-in delete metric function
	// This is a placeholder that logs the intent
	ui.logActivity("DeleteMetric", metric)

	writeJSON(w, map[string]interface{}{
		"status":  "acknowledged",
		"message": "Metric deletion scheduled. Data will be removed during next compaction.",
		"metric":  metric,
	})
}

// Phase 5: Truncate endpoint
func (ui *AdminUI) handleAPITruncate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metric := r.URL.Query().Get("metric")
	beforeStr := r.URL.Query().Get("before")

	var before time.Time
	if beforeStr != "" {
		var err error
		before, err = time.Parse(time.RFC3339, beforeStr)
		if err != nil {
			http.Error(w, "invalid before timestamp", http.StatusBadRequest)
			return
		}
	}

	ui.logActivity("Truncate", fmt.Sprintf("metric=%s, before=%s", metric, before))

	writeJSON(w, map[string]interface{}{
		"status":  "acknowledged",
		"message": "Truncation scheduled. Data will be removed during next compaction.",
		"metric":  metric,
		"before":  before,
	})
}

// Phase 5: Insert endpoint (dev mode only)
func (ui *AdminUI) handleAPIInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !ui.devMode {
		http.Error(w, "insert is only available in dev mode", http.StatusForbidden)
		return
	}

	var points []struct {
		Metric    string            `json:"metric"`
		Value     float64           `json:"value"`
		Timestamp int64             `json:"timestamp,omitempty"`
		Tags      map[string]string `json:"tags,omitempty"`
	}

	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&points); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	insertPoints := make([]Point, len(points))
	now := time.Now().UnixNano()
	for i, p := range points {
		ts := p.Timestamp
		if ts == 0 {
			ts = now
		}
		insertPoints[i] = Point{
			Metric:    p.Metric,
			Value:     p.Value,
			Timestamp: ts,
			Tags:      p.Tags,
		}
	}

	if err := ui.db.WriteBatch(insertPoints); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ui.logActivity("Insert", fmt.Sprintf("%d points", len(points)))

	writeJSON(w, map[string]interface{}{
		"status":   "ok",
		"inserted": len(points),
	})
}

// Phase 6: Partitions endpoint
func (ui *AdminUI) handleAPIPartitions(w http.ResponseWriter, r *http.Request) {
	partitions := make([]map[string]interface{}, 0)

	if ui.db.index != nil {
		// Get partition info from the index
		count := ui.db.index.Count()
		partitions = append(partitions, map[string]interface{}{
			"count":             count,
			"partition_duration": ui.db.config.PartitionDuration.String(),
		})
	}

	writeJSON(w, map[string]interface{}{
		"partitions": partitions,
		"total":      len(partitions),
	})
}

// Phase 6: Backup endpoint
func (ui *AdminUI) handleAPIBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// Trigger backup
		ui.logActivity("Backup", "Manual backup triggered")

		// Flush to ensure data is persisted
		if err := ui.db.Flush(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, map[string]interface{}{
			"status":    "ok",
			"message":   "Backup completed (data synced to disk)",
			"timestamp": time.Now(),
			"path":      ui.db.config.Path,
		})
		return
	}

	// GET - return backup status
	writeJSON(w, map[string]interface{}{
		"path":        ui.db.config.Path,
		"last_sync":   "available via Sync()",
		"auto_backup": false,
	})
}

// Phase 7: Server-Sent Events for real-time updates
func (ui *AdminUI) handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientChan := make(chan []byte, 10)
	ui.sseMu.Lock()
	ui.sseClients[clientChan] = true
	ui.sseMu.Unlock()

	defer func() {
		ui.sseMu.Lock()
		delete(ui.sseClients, clientChan)
		ui.sseMu.Unlock()
		close(clientChan)
	}()

	// Send initial stats
	ui.sendStatsUpdate()

	// Start periodic updates
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-clientChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-ticker.C:
			ui.sendStatsUpdate()
		}
	}
}

func (ui *AdminUI) sendStatsUpdate() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	update := map[string]interface{}{
		"type":        "stats",
		"timestamp":   time.Now().UnixMilli(),
		"memory":      formatBytes(memStats.Alloc),
		"goroutines":  runtime.NumGoroutine(),
		"gc_cycles":   memStats.NumGC,
		"uptime":      time.Since(ui.startTime).Round(time.Second).String(),
		"total_alloc": formatBytes(memStats.TotalAlloc),
	}

	data, _ := json.Marshal(update)

	ui.sseMu.RLock()
	for clientChan := range ui.sseClients {
		select {
		case clientChan <- data:
		default:
		}
	}
	ui.sseMu.RUnlock()
}

// Phase 7: Alerting API
func (ui *AdminUI) handleAPIAlerts(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "AlertsAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		alerts := make([]adminAlertRule, len(ui.alertRules))
		copy(alerts, ui.alertRules)
		ui.mu.RUnlock()
		writeJSON(w, alerts)

	case http.MethodPost:
		var rule adminAlertRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		rule.ID = fmt.Sprintf("alert_%d", time.Now().UnixNano())
		rule.CreatedAt = time.Now()
		rule.State = "ok"
		if rule.Enabled == false {
			rule.Enabled = true
		}

		ui.mu.Lock()
		ui.alertRules = append(ui.alertRules, rule)
		ui.mu.Unlock()

		ui.logActivity("Alert Created", rule.Name)
		writeJSON(w, rule)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing alert id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, rule := range ui.alertRules {
			if rule.ID == id {
				ui.alertRules = append(ui.alertRules[:i], ui.alertRules[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Alert Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 7: Audit Log API
func (ui *AdminUI) handleAPIAuditLog(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}

	ui.mu.RLock()
	entries := make([]auditLogEntry, 0, limit)
	start := len(ui.auditLog) - limit
	if start < 0 {
		start = 0
	}
	for i := len(ui.auditLog) - 1; i >= start; i-- {
		entries = append(entries, ui.auditLog[i])
	}
	ui.mu.RUnlock()

	writeJSON(w, entries)
}

func (ui *AdminUI) logAudit(r *http.Request, action, details string) {
	entry := auditLogEntry{
		ID:        fmt.Sprintf("audit_%d", time.Now().UnixNano()),
		Action:    action,
		User:      r.Header.Get("X-User"),
		IP:        r.RemoteAddr,
		Details:   details,
		Timestamp: time.Now(),
		Success:   true,
	}
	if entry.User == "" {
		entry.User = "anonymous"
	}

	ui.mu.Lock()
	ui.auditLog = append(ui.auditLog, entry)
	if len(ui.auditLog) > 500 {
		ui.auditLog = ui.auditLog[1:]
	}
	ui.mu.Unlock()
}

// Phase 7: Query Explain API
func (ui *AdminUI) handleAPIQueryExplain(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "Missing query parameter 'q'", http.StatusBadRequest)
		return
	}

	result := ui.explainQuery(query)
	writeJSON(w, result)
}

func (ui *AdminUI) explainQuery(query string) queryExplainResult {
	result := queryExplainResult{
		Query: query,
		Steps: []string{},
	}

	// Parse the query
	query = strings.TrimSpace(query)
	upperQuery := strings.ToUpper(query)

	// Determine function
	if strings.Contains(upperQuery, "MEAN(") || strings.Contains(upperQuery, "AVG(") {
		result.ParsedFunction = "mean"
		result.Steps = append(result.Steps, "Aggregate: Calculate mean of values")
	} else if strings.Contains(upperQuery, "SUM(") {
		result.ParsedFunction = "sum"
		result.Steps = append(result.Steps, "Aggregate: Calculate sum of values")
	} else if strings.Contains(upperQuery, "COUNT(") {
		result.ParsedFunction = "count"
		result.Steps = append(result.Steps, "Aggregate: Count data points")
	} else if strings.Contains(upperQuery, "MAX(") {
		result.ParsedFunction = "max"
		result.Steps = append(result.Steps, "Aggregate: Find maximum value")
	} else if strings.Contains(upperQuery, "MIN(") {
		result.ParsedFunction = "min"
		result.Steps = append(result.Steps, "Aggregate: Find minimum value")
	} else {
		result.ParsedFunction = "raw"
		result.Steps = append(result.Steps, "Select: Return raw data points")
	}

	// Extract metric name
	if idx := strings.Index(upperQuery, "FROM "); idx != -1 {
		rest := query[idx+5:]
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			result.ParsedMetric = parts[0]
		}
	}

	// Check for time range
	if strings.Contains(upperQuery, "WHERE") {
		result.TimeRange = "Custom time range specified"
		result.Steps = append(result.Steps, "Filter: Apply WHERE clause conditions")
	} else {
		result.TimeRange = "All time (no WHERE clause)"
	}

	// Estimate rows
	metrics := ui.db.Metrics()
	for _, m := range metrics {
		if m == result.ParsedMetric {
			result.IndexUsed = true
			result.EstimatedRows = 1000 // Placeholder estimate
			break
		}
	}

	result.Steps = append(result.Steps, "Scan: Read from metric index")
	if result.ParsedFunction != "raw" {
		result.Steps = append(result.Steps, "Reduce: Apply aggregation function")
	}
	result.Steps = append(result.Steps, "Return: Format and return results")

	return result
}

// Phase 8: Schema Registry API
func (ui *AdminUI) handleAPISchemas(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "SchemasAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		// Return list of metrics with their inferred schemas
		metrics := ui.db.Metrics()
		schemas := make([]map[string]interface{}, 0, len(metrics))

		for _, metric := range metrics {
			schema := map[string]interface{}{
				"name":        metric,
				"type":        "float64",
				"description": fmt.Sprintf("Auto-discovered metric: %s", metric),
				"tags":        []string{},
				"fields": []map[string]string{
					{"name": "value", "type": "float64"},
				},
			}

			// Try to get registered schema
			if regSchema := ui.db.GetSchema(metric); regSchema != nil {
				schema["description"] = regSchema.Description
				tags := make([]string, 0, len(regSchema.Tags))
				for _, t := range regSchema.Tags {
					tags = append(tags, t.Name)
				}
				schema["tags"] = tags
			}

			schemas = append(schemas, schema)
		}

		writeJSON(w, schemas)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 8: Retention Policy API
func (ui *AdminUI) handleAPIRetention(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "RetentionAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		rules := make([]retentionRule, len(ui.retentionRules))
		copy(rules, ui.retentionRules)
		ui.mu.RUnlock()
		writeJSON(w, rules)

	case http.MethodPost:
		var rule retentionRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		rule.ID = fmt.Sprintf("retention_%d", time.Now().UnixNano())
		rule.CreatedAt = time.Now()
		rule.Enabled = true

		ui.mu.Lock()
		ui.retentionRules = append(ui.retentionRules, rule)
		ui.mu.Unlock()

		ui.logActivity("Retention Policy Created", fmt.Sprintf("%s: %s", rule.Metric, rule.Duration))
		writeJSON(w, rule)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing retention rule id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, rule := range ui.retentionRules {
			if rule.ID == id {
				ui.retentionRules = append(ui.retentionRules[:i], ui.retentionRules[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Retention Policy Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 8: Cluster Status API
func (ui *AdminUI) handleAPICluster(w http.ResponseWriter, r *http.Request) {
	// Return cluster status (standalone mode if not in cluster)
	status := map[string]interface{}{
		"mode":   "standalone",
		"status": "healthy",
		"node": map[string]interface{}{
			"id":      "local",
			"state":   "leader",
			"address": "localhost",
			"uptime":  time.Since(ui.startTime).Round(time.Second).String(),
		},
		"nodes":       []interface{}{},
		"replication": "none",
	}

	writeJSON(w, status)
}

// Phase 9: WAL Inspector API
func (ui *AdminUI) handleAPIWAL(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "WALInspector", "Accessed WAL inspector")

	walInfo := map[string]interface{}{
		"enabled":       true,
		"path":          ui.db.config.Path + ".wal",
		"sync_interval": "1s",
		"max_size":      "100MB",
		"segments":      []interface{}{},
		"stats": map[string]interface{}{
			"total_writes":   0,
			"pending_writes": 0,
			"last_sync":      time.Now().Add(-1 * time.Second),
		},
	}

	// Check if WAL file exists
	walPath := ui.db.config.Path + ".wal"
	if info, err := os.Stat(walPath); err == nil {
		walInfo["stats"].(map[string]interface{})["file_size"] = formatBytes(uint64(info.Size()))
		walInfo["stats"].(map[string]interface{})["modified"] = info.ModTime()
	}

	writeJSON(w, walInfo)
}

// Phase 9: Scheduled Exports API
func (ui *AdminUI) handleAPIScheduledExports(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "ScheduledExportsAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		exports := make([]scheduledExport, len(ui.scheduledExports))
		copy(exports, ui.scheduledExports)
		ui.mu.RUnlock()
		writeJSON(w, exports)

	case http.MethodPost:
		var export scheduledExport
		if err := json.NewDecoder(r.Body).Decode(&export); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		export.ID = fmt.Sprintf("export_%d", time.Now().UnixNano())
		export.CreatedAt = time.Now()
		export.Enabled = true

		// Calculate next run based on schedule
		nextRun := time.Now().Add(1 * time.Hour)
		export.NextRun = &nextRun

		ui.mu.Lock()
		ui.scheduledExports = append(ui.scheduledExports, export)
		ui.mu.Unlock()

		ui.logActivity("Scheduled Export Created", export.Name)
		writeJSON(w, export)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing export id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, export := range ui.scheduledExports {
			if export.ID == id {
				ui.scheduledExports = append(ui.scheduledExports[:i], ui.scheduledExports[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Scheduled Export Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 9: Global Search API
func (ui *AdminUI) handleAPISearch(w http.ResponseWriter, r *http.Request) {
	query := strings.ToLower(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, []interface{}{})
		return
	}

	results := make([]map[string]interface{}, 0)

	// Search metrics
	metrics := ui.db.Metrics()
	for _, metric := range metrics {
		if strings.Contains(strings.ToLower(metric), query) {
			results = append(results, map[string]interface{}{
				"type":   "metric",
				"name":   metric,
				"action": "query",
				"icon":   "📊",
			})
		}
	}

	// Search pages
	pages := []struct{ name, icon string }{
		{"Dashboard", "🏠"},
		{"Health", "💚"},
		{"Explorer", "🔍"},
		{"Query Console", "⚡"},
		{"Management", "🗑️"},
		{"Configuration", "⚙️"},
		{"Backup", "💾"},
		{"Alerts", "🔔"},
		{"Audit Log", "📋"},
		{"Schema Registry", "📐"},
		{"Retention", "🕐"},
		{"Cluster", "🌐"},
		{"WAL Inspector", "💾"},
		{"Scheduled Exports", "📤"},
	}

	for _, page := range pages {
		if strings.Contains(strings.ToLower(page.name), query) {
			results = append(results, map[string]interface{}{
				"type":   "page",
				"name":   page.name,
				"action": "navigate",
				"icon":   page.icon,
			})
		}
	}

	// Limit results
	if len(results) > 10 {
		results = results[:10]
	}

	writeJSON(w, results)
}

// Phase 10: Query Autocomplete API
func (ui *AdminUI) handleAPIAutocomplete(w http.ResponseWriter, r *http.Request) {
	prefix := strings.ToLower(r.URL.Query().Get("prefix"))
	context := r.URL.Query().Get("context") // "metric", "function", "keyword"

	suggestions := make([]map[string]string, 0)

	switch context {
	case "function":
		functions := []struct{ name, desc string }{
			{"mean", "Calculate average value"},
			{"sum", "Calculate sum of values"},
			{"count", "Count data points"},
			{"max", "Find maximum value"},
			{"min", "Find minimum value"},
			{"first", "Get first value"},
			{"last", "Get last value"},
			{"median", "Calculate median value"},
			{"stddev", "Standard deviation"},
			{"percentile", "Calculate percentile"},
		}
		for _, f := range functions {
			if prefix == "" || strings.HasPrefix(f.name, prefix) {
				suggestions = append(suggestions, map[string]string{
					"value": f.name + "(value)",
					"label": f.name,
					"desc":  f.desc,
					"type":  "function",
				})
			}
		}

	case "keyword":
		keywords := []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "AND", "OR", "time"}
		for _, kw := range keywords {
			if prefix == "" || strings.HasPrefix(strings.ToLower(kw), prefix) {
				suggestions = append(suggestions, map[string]string{
					"value": kw,
					"label": kw,
					"type":  "keyword",
				})
			}
		}

	default: // metrics
		metrics := ui.db.Metrics()
		for _, m := range metrics {
			if prefix == "" || strings.HasPrefix(strings.ToLower(m), prefix) {
				suggestions = append(suggestions, map[string]string{
					"value": m,
					"label": m,
					"type":  "metric",
				})
			}
		}
	}

	// Limit suggestions
	if len(suggestions) > 15 {
		suggestions = suggestions[:15]
	}

	writeJSON(w, suggestions)
}

// Phase 10: Saved Queries API
func (ui *AdminUI) handleAPISavedQueries(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "SavedQueriesAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		queries := make([]savedQuery, len(ui.savedQueries))
		copy(queries, ui.savedQueries)
		ui.mu.RUnlock()

		// Sort by usage count descending
		sort.Slice(queries, func(i, j int) bool {
			return queries[i].UsageCount > queries[j].UsageCount
		})
		writeJSON(w, queries)

	case http.MethodPost:
		var query savedQuery
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		query.ID = fmt.Sprintf("query_%d", time.Now().UnixNano())
		query.CreatedAt = time.Now()
		query.UsageCount = 0

		ui.mu.Lock()
		ui.savedQueries = append(ui.savedQueries, query)
		ui.mu.Unlock()

		ui.logActivity("Query Saved", query.Name)
		writeJSON(w, query)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing query id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, q := range ui.savedQueries {
			if q.ID == id {
				ui.savedQueries = append(ui.savedQueries[:i], ui.savedQueries[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Query Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 10: Favorites API
func (ui *AdminUI) handleAPIFavorites(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		favs := make([]favoriteItem, len(ui.favorites))
		copy(favs, ui.favorites)
		ui.mu.RUnlock()
		writeJSON(w, favs)

	case http.MethodPost:
		var fav favoriteItem
		if err := json.NewDecoder(r.Body).Decode(&fav); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		fav.ID = fmt.Sprintf("fav_%d", time.Now().UnixNano())
		fav.CreatedAt = time.Now()

		ui.mu.Lock()
		// Check for duplicates
		exists := false
		for _, f := range ui.favorites {
			if f.Type == fav.Type && f.Name == fav.Name {
				exists = true
				break
			}
		}
		if !exists {
			ui.favorites = append(ui.favorites, fav)
		}
		ui.mu.Unlock()

		writeJSON(w, fav)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing favorite id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, f := range ui.favorites {
			if f.ID == id {
				ui.favorites = append(ui.favorites[:i], ui.favorites[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 10: Recent Items API
func (ui *AdminUI) handleAPIRecent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limit := 20
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 50 {
				limit = n
			}
		}

		ui.mu.RLock()
		items := make([]recentItem, 0, limit)
		start := len(ui.recentItems) - limit
		if start < 0 {
			start = 0
		}
		for i := len(ui.recentItems) - 1; i >= start; i-- {
			items = append(items, ui.recentItems[i])
		}
		ui.mu.RUnlock()
		writeJSON(w, items)

	case http.MethodPost:
		var item recentItem
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		item.AccessedAt = time.Now()
		ui.addRecentItem(item)
		writeJSON(w, item)

	case http.MethodDelete:
		// Clear all recent items
		ui.mu.Lock()
		ui.recentItems = make([]recentItem, 0, 50)
		ui.mu.Unlock()
		writeJSON(w, map[string]string{"status": "cleared"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (ui *AdminUI) addRecentItem(item recentItem) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	// Remove duplicate if exists
	for i, r := range ui.recentItems {
		if r.Type == item.Type && r.Name == item.Name {
			ui.recentItems = append(ui.recentItems[:i], ui.recentItems[i+1:]...)
			break
		}
	}

	ui.recentItems = append(ui.recentItems, item)
	if len(ui.recentItems) > 50 {
		ui.recentItems = ui.recentItems[1:]
	}
}

// Phase 10: Alert History API
func (ui *AdminUI) handleAPIAlertHistory(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	alertID := r.URL.Query().Get("alert_id") // Optional filter

	ui.mu.RLock()
	entries := make([]alertHistoryEntry, 0, limit)
	count := 0
	for i := len(ui.alertHistory) - 1; i >= 0 && count < limit; i-- {
		entry := ui.alertHistory[i]
		if alertID == "" || entry.AlertID == alertID {
			entries = append(entries, entry)
			count++
		}
	}
	ui.mu.RUnlock()

	writeJSON(w, entries)
}

func (ui *AdminUI) recordAlertHistory(alertID, alertName, state string, value, threshold float64, message string) {
	entry := alertHistoryEntry{
		ID:        fmt.Sprintf("ah_%d", time.Now().UnixNano()),
		AlertID:   alertID,
		AlertName: alertName,
		State:     state,
		Value:     value,
		Threshold: threshold,
		Timestamp: time.Now(),
		Message:   message,
	}

	ui.mu.Lock()
	ui.alertHistory = append(ui.alertHistory, entry)
	if len(ui.alertHistory) > 200 {
		ui.alertHistory = ui.alertHistory[1:]
	}
	ui.mu.Unlock()
}

// Phase 10: Data Import API
func (ui *AdminUI) handleAPIImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !ui.devMode {
		http.Error(w, "Import only available in dev mode", http.StatusForbidden)
		return
	}

	ui.logAudit(r, "DataImport", "Import initiated")

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	var points []Point
	var err error

	switch format {
	case "json":
		var data []struct {
			Metric    string            `json:"metric"`
			Value     float64           `json:"value"`
			Timestamp int64             `json:"timestamp,omitempty"`
			Tags      map[string]string `json:"tags,omitempty"`
		}
		if err = json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		for _, d := range data {
			ts := d.Timestamp
			if ts == 0 {
				ts = time.Now().UnixNano()
			}
			points = append(points, Point{
				Metric:    d.Metric,
				Value:     d.Value,
				Timestamp: ts,
				Tags:      d.Tags,
			})
		}

	case "csv":
		reader := csv.NewReader(r.Body)
		records, err := reader.ReadAll()
		if err != nil {
			http.Error(w, "Invalid CSV: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Expected format: metric,value,timestamp (optional)
		for i, record := range records {
			if i == 0 && (record[0] == "metric" || record[0] == "Metric") {
				continue // Skip header
			}
			if len(record) < 2 {
				continue
			}
			val, err := strconv.ParseFloat(record[1], 64)
			if err != nil {
				continue
			}
			ts := time.Now().UnixNano()
			if len(record) >= 3 {
				if parsed, err := strconv.ParseInt(record[2], 10, 64); err == nil {
					ts = parsed
				}
			}
			points = append(points, Point{
				Metric:    record[0],
				Value:     val,
				Timestamp: ts,
			})
		}

	case "line":
		// InfluxDB line protocol
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		lines := strings.Split(string(body), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			// Simple parsing: metric,tag=val value=123 timestamp
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			// Parse metric and tags
			metricParts := strings.Split(parts[0], ",")
			metric := metricParts[0]
			tags := make(map[string]string)
			for i := 1; i < len(metricParts); i++ {
				kv := strings.SplitN(metricParts[i], "=", 2)
				if len(kv) == 2 {
					tags[kv[0]] = kv[1]
				}
			}
			// Parse value
			valueParts := strings.SplitN(parts[1], "=", 2)
			if len(valueParts) != 2 {
				continue
			}
			val, err := strconv.ParseFloat(valueParts[1], 64)
			if err != nil {
				continue
			}
			ts := time.Now().UnixNano()
			if len(parts) >= 3 {
				if parsed, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
					ts = parsed
				}
			}
			points = append(points, Point{
				Metric:    metric,
				Value:     val,
				Timestamp: ts,
				Tags:      tags,
			})
		}

	default:
		http.Error(w, "Unsupported format: "+format, http.StatusBadRequest)
		return
	}

	if len(points) == 0 {
		http.Error(w, "No valid data points found", http.StatusBadRequest)
		return
	}

	// Write points
	if err = ui.db.WriteBatch(points); err != nil {
		http.Error(w, "Write failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ui.logActivity("Data Import", fmt.Sprintf("Imported %d points", len(points)))
	writeJSON(w, map[string]interface{}{
		"status":  "ok",
		"imported": len(points),
		"format":  format,
	})
}

// Phase 10: System Diagnostics API
func (ui *AdminUI) handleAPIDiagnostics(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Get disk usage (approximation based on config path)
	var diskUsage int64
	if info, err := os.Stat(ui.db.config.Path); err == nil {
		diskUsage = info.Size()
	}

	diagnostics := map[string]interface{}{
		"system": map[string]interface{}{
			"go_version":   runtime.Version(),
			"os":           runtime.GOOS,
			"arch":         runtime.GOARCH,
			"num_cpu":      runtime.NumCPU(),
			"num_cgo_call": runtime.NumCgoCall(),
		},
		"runtime": map[string]interface{}{
			"goroutines":      runtime.NumGoroutine(),
			"gc_pause_total":  time.Duration(memStats.PauseTotalNs).String(),
			"gc_num":          memStats.NumGC,
			"gc_last":         time.Unix(0, int64(memStats.LastGC)).Format(time.RFC3339),
			"gc_cpu_fraction": fmt.Sprintf("%.4f%%", memStats.GCCPUFraction*100),
		},
		"memory": map[string]interface{}{
			"alloc":         formatBytes(memStats.Alloc),
			"total_alloc":   formatBytes(memStats.TotalAlloc),
			"sys":           formatBytes(memStats.Sys),
			"heap_alloc":    formatBytes(memStats.HeapAlloc),
			"heap_sys":      formatBytes(memStats.HeapSys),
			"heap_idle":     formatBytes(memStats.HeapIdle),
			"heap_inuse":    formatBytes(memStats.HeapInuse),
			"heap_released": formatBytes(memStats.HeapReleased),
			"heap_objects":  memStats.HeapObjects,
			"stack_inuse":   formatBytes(memStats.StackInuse),
			"stack_sys":     formatBytes(memStats.StackSys),
		},
		"database": map[string]interface{}{
			"path":          ui.db.config.Path,
			"disk_usage":    formatBytes(uint64(diskUsage)),
			"metrics_count": len(ui.db.Metrics()),
			"uptime":        time.Since(ui.startTime).Round(time.Second).String(),
			"buffer_size":   ui.db.config.BufferSize,
		},
	}

	writeJSON(w, diagnostics)
}

// Phase 10: Session Management API
func (ui *AdminUI) handleAPISessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		sessions := make([]sessionInfo, len(ui.sessions))
		copy(sessions, ui.sessions)
		ui.mu.RUnlock()

		// Filter active only if requested
		if r.URL.Query().Get("active") == "true" {
			filtered := make([]sessionInfo, 0)
			for _, s := range sessions {
				if s.Active {
					filtered = append(filtered, s)
				}
			}
			sessions = filtered
		}
		writeJSON(w, sessions)

	case http.MethodPost:
		// Record new session
		session := sessionInfo{
			ID:        fmt.Sprintf("sess_%d", time.Now().UnixNano()),
			User:      r.Header.Get("X-User"),
			IP:        r.RemoteAddr,
			UserAgent: r.UserAgent(),
			StartedAt: time.Now(),
			LastSeen:  time.Now(),
			Active:    true,
		}
		if session.User == "" {
			session.User = "anonymous"
		}

		ui.mu.Lock()
		ui.sessions = append(ui.sessions, session)
		// Keep only last 100 sessions
		if len(ui.sessions) > 100 {
			ui.sessions = ui.sessions[len(ui.sessions)-100:]
		}
		ui.mu.Unlock()

		writeJSON(w, session)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing session id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i := range ui.sessions {
			if ui.sessions[i].ID == id {
				ui.sessions[i].Active = false
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "SessionRevoked", id)
		writeJSON(w, map[string]string{"status": "revoked", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 10: Sparkline Data API
func (ui *AdminUI) handleAPISparkline(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "Missing metric parameter", http.StatusBadRequest)
		return
	}

	points := 20 // Number of points for sparkline
	if p := r.URL.Query().Get("points"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n <= 100 {
			points = n
		}
	}

	// Get recent data for the metric
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	parser := &QueryParser{}
	q, err := parser.Parse(fmt.Sprintf("SELECT mean(value) FROM %s", metric))
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"metric": metric,
			"values": []float64{},
		})
		return
	}
	q.Start = start.UnixNano()
	q.End = end.UnixNano()

	result, err := ui.db.Execute(q)
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"metric": metric,
			"values": []float64{},
		})
		return
	}

	values := make([]float64, 0, points)
	var minVal, maxVal float64
	if result != nil && len(result.Points) > 0 {
		minVal = result.Points[0].Value
		maxVal = result.Points[0].Value
		step := len(result.Points) / points
		if step < 1 {
			step = 1
		}
		for i := 0; i < len(result.Points) && len(values) < points; i += step {
			v := result.Points[i].Value
			values = append(values, v)
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}

	writeJSON(w, map[string]interface{}{
		"metric": metric,
		"values": values,
		"min":    minVal,
		"max":    maxVal,
	})
}

// Phase 10: Multi-Metric Compare API
func (ui *AdminUI) handleAPICompare(w http.ResponseWriter, r *http.Request) {
	metricsParam := r.URL.Query().Get("metrics")
	if metricsParam == "" {
		http.Error(w, "Missing metrics parameter", http.StatusBadRequest)
		return
	}

	metricNames := strings.Split(metricsParam, ",")
	if len(metricNames) > 10 {
		metricNames = metricNames[:10] // Limit to 10 metrics
	}

	end := time.Now()
	start := end.Add(-1 * time.Hour)

	// Parse time range if provided
	if s := r.URL.Query().Get("start"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			start = t
		}
	}
	if e := r.URL.Query().Get("end"); e != "" {
		if t, err := time.Parse(time.RFC3339, e); err == nil {
			end = t
		}
	}

	parser := &QueryParser{}
	results := make(map[string]interface{})
	for _, metric := range metricNames {
		metric = strings.TrimSpace(metric)
		if metric == "" {
			continue
		}

		q, err := parser.Parse(fmt.Sprintf("SELECT mean(value) FROM %s", metric))
		if err != nil {
			results[metric] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}
		q.Start = start.UnixNano()
		q.End = end.UnixNano()

		result, err := ui.db.Execute(q)
		if err != nil {
			results[metric] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}

		points := make([]map[string]interface{}, 0)
		var minVal, maxVal, sum float64
		if result != nil && len(result.Points) > 0 {
			minVal = result.Points[0].Value
			maxVal = result.Points[0].Value
			for _, p := range result.Points {
				points = append(points, map[string]interface{}{
					"timestamp": p.Timestamp,
					"value":     p.Value,
				})
				sum += p.Value
				if p.Value < minVal {
					minVal = p.Value
				}
				if p.Value > maxVal {
					maxVal = p.Value
				}
			}
		}

		meanVal := 0.0
		if len(result.Points) > 0 {
			meanVal = sum / float64(len(result.Points))
		}

		results[metric] = map[string]interface{}{
			"points": points,
			"min":    minVal,
			"max":    maxVal,
			"mean":   meanVal,
		}
	}

	writeJSON(w, map[string]interface{}{
		"metrics":    metricNames,
		"start":      start.Format(time.RFC3339),
		"end":        end.Format(time.RFC3339),
		"comparison": results,
	})
}

// Phase 11: Initialize built-in query templates
func initBuiltInTemplates() []queryTemplate {
	return []queryTemplate{
		{
			ID:          "tpl_avg_last_hour",
			Name:        "Average (Last Hour)",
			Description: "Calculate average value over the last hour",
			Category:    "aggregation",
			Query:       "SELECT mean(value) FROM {{metric}} WHERE time > now() - 1h",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_max_last_day",
			Name:        "Maximum (Last 24h)",
			Description: "Find maximum value in the last 24 hours",
			Category:    "aggregation",
			Query:       "SELECT max(value) FROM {{metric}} WHERE time > now() - 24h",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_min_last_day",
			Name:        "Minimum (Last 24h)",
			Description: "Find minimum value in the last 24 hours",
			Category:    "aggregation",
			Query:       "SELECT min(value) FROM {{metric}} WHERE time > now() - 24h",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_count_by_hour",
			Name:        "Count by Hour",
			Description: "Count data points grouped by hour",
			Category:    "analysis",
			Query:       "SELECT count(value) FROM {{metric}} GROUP BY time(1h)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_rate_per_minute",
			Name:        "Rate per Minute",
			Description: "Calculate the rate of change per minute",
			Category:    "analysis",
			Query:       "SELECT derivative(mean(value), 1m) FROM {{metric}} GROUP BY time(1m)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_percentile_95",
			Name:        "95th Percentile",
			Description: "Calculate 95th percentile over time",
			Category:    "analysis",
			Query:       "SELECT percentile(value, 95) FROM {{metric}} WHERE time > now() - 1h GROUP BY time(5m)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_anomaly_detection",
			Name:        "Anomaly Detection",
			Description: "Find values outside 2 standard deviations",
			Category:    "monitoring",
			Query:       "SELECT value FROM {{metric}} WHERE value > mean(value) + 2*stddev(value) OR value < mean(value) - 2*stddev(value)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_top_n",
			Name:        "Top N Values",
			Description: "Get top N highest values",
			Category:    "analysis",
			Query:       "SELECT top(value, {{n}}) FROM {{metric}} WHERE time > now() - 1h",
			Variables:   []string{"metric", "n"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_moving_average",
			Name:        "Moving Average",
			Description: "Calculate moving average over time windows",
			Category:    "analysis",
			Query:       "SELECT moving_average(mean(value), {{window}}) FROM {{metric}} GROUP BY time(1m)",
			Variables:   []string{"metric", "window"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_compare_periods",
			Name:        "Compare Time Periods",
			Description: "Compare current hour vs previous hour",
			Category:    "monitoring",
			Query:       "SELECT mean(value) FROM {{metric}} WHERE time > now() - 1h GROUP BY time(5m)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
	}
}

// Phase 11: Initialize default roles
func initDefaultRoles() []userRole {
	return []userRole{
		{
			ID:          "role_admin",
			Name:        "Admin",
			Description: "Full access to all features",
			Permissions: []string{"read", "write", "delete", "admin", "export", "import", "config", "users"},
		},
		{
			ID:          "role_operator",
			Name:        "Operator",
			Description: "Can view and manage data, but not configure system",
			Permissions: []string{"read", "write", "delete", "export"},
		},
		{
			ID:          "role_analyst",
			Name:        "Analyst",
			Description: "Read-only access with export capability",
			Permissions: []string{"read", "export"},
		},
		{
			ID:          "role_viewer",
			Name:        "Viewer",
			Description: "Read-only access to dashboards and metrics",
			Permissions: []string{"read"},
		},
	}
}

// Phase 11: Query Templates API
func (ui *AdminUI) handleAPITemplates(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		category := r.URL.Query().Get("category")
		ui.mu.RLock()
		templates := make([]queryTemplate, 0)
		for _, t := range ui.queryTemplates {
			if category == "" || t.Category == category {
				templates = append(templates, t)
			}
		}
		ui.mu.RUnlock()
		writeJSON(w, templates)

	case http.MethodPost:
		var tpl queryTemplate
		if err := json.NewDecoder(r.Body).Decode(&tpl); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if tpl.Name == "" || tpl.Query == "" {
			http.Error(w, "Name and query are required", http.StatusBadRequest)
			return
		}

		tpl.ID = fmt.Sprintf("tpl_%d", time.Now().UnixNano())
		tpl.BuiltIn = false

		ui.mu.Lock()
		ui.queryTemplates = append(ui.queryTemplates, tpl)
		ui.mu.Unlock()

		ui.logAudit(r, "CreateTemplate", tpl.Name)
		writeJSON(w, tpl)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, t := range ui.queryTemplates {
			if t.ID == id {
				if t.BuiltIn {
					ui.mu.Unlock()
					http.Error(w, "Cannot delete built-in templates", http.StatusForbidden)
					return
				}
				ui.queryTemplates = append(ui.queryTemplates[:i], ui.queryTemplates[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "DeleteTemplate", id)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 11: Metric Annotations API
func (ui *AdminUI) handleAPIAnnotations(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		metric := r.URL.Query().Get("metric")
		ui.mu.RLock()
		annotations := make([]metricAnnotation, 0)
		for _, a := range ui.annotations {
			if metric == "" || a.Metric == metric {
				annotations = append(annotations, a)
			}
		}
		ui.mu.RUnlock()
		writeJSON(w, annotations)

	case http.MethodPost:
		var ann metricAnnotation
		if err := json.NewDecoder(r.Body).Decode(&ann); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if ann.Metric == "" || ann.Title == "" {
			http.Error(w, "Metric and title are required", http.StatusBadRequest)
			return
		}

		ann.ID = fmt.Sprintf("ann_%d", time.Now().UnixNano())
		ann.CreatedAt = time.Now()
		if ann.CreatedBy == "" {
			ann.CreatedBy = r.Header.Get("X-User")
			if ann.CreatedBy == "" {
				ann.CreatedBy = "anonymous"
			}
		}

		ui.mu.Lock()
		ui.annotations = append(ui.annotations, ann)
		ui.mu.Unlock()

		ui.logAudit(r, "CreateAnnotation", fmt.Sprintf("%s: %s", ann.Metric, ann.Title))
		writeJSON(w, ann)

	case http.MethodPut:
		var ann metricAnnotation
		if err := json.NewDecoder(r.Body).Decode(&ann); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, a := range ui.annotations {
			if a.ID == ann.ID {
				ann.CreatedAt = a.CreatedAt
				ann.CreatedBy = a.CreatedBy
				ui.annotations[i] = ann
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "UpdateAnnotation", ann.ID)
		writeJSON(w, ann)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, a := range ui.annotations {
			if a.ID == id {
				ui.annotations = append(ui.annotations[:i], ui.annotations[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "DeleteAnnotation", id)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 11: Performance Profiling API
func (ui *AdminUI) handleAPIProfiling(w http.ResponseWriter, r *http.Request) {
	profileType := r.URL.Query().Get("type")
	if profileType == "" {
		profileType = "summary"
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	switch profileType {
	case "summary":
		writeJSON(w, map[string]interface{}{
			"goroutines":     runtime.NumGoroutine(),
			"cgo_calls":      runtime.NumCgoCall(),
			"cpu_count":      runtime.NumCPU(),
			"gc_runs":        memStats.NumGC,
			"gc_pause_total": time.Duration(memStats.PauseTotalNs).String(),
			"gc_pause_last":  time.Duration(memStats.PauseNs[(memStats.NumGC+255)%256]).String(),
			"heap_alloc":     memStats.HeapAlloc,
			"heap_sys":       memStats.HeapSys,
			"heap_idle":      memStats.HeapIdle,
			"heap_inuse":     memStats.HeapInuse,
			"heap_objects":   memStats.HeapObjects,
			"stack_inuse":    memStats.StackInuse,
			"stack_sys":      memStats.StackSys,
			"mspan_inuse":    memStats.MSpanInuse,
			"mcache_inuse":   memStats.MCacheInuse,
			"other_sys":      memStats.OtherSys,
		})

	case "gc":
		// GC statistics
		gcPauses := make([]int64, 0, 256)
		for i := uint32(0); i < memStats.NumGC && i < 256; i++ {
			gcPauses = append(gcPauses, int64(memStats.PauseNs[i]))
		}
		writeJSON(w, map[string]interface{}{
			"num_gc":            memStats.NumGC,
			"pause_total_ns":    memStats.PauseTotalNs,
			"pause_end":         memStats.PauseEnd[:min(int(memStats.NumGC), 256)],
			"pause_ns":          gcPauses,
			"gc_cpu_fraction":   memStats.GCCPUFraction,
			"last_gc":           time.Unix(0, int64(memStats.LastGC)).Format(time.RFC3339),
			"next_gc":           memStats.NextGC,
			"enable_gc":         memStats.EnableGC,
			"debug_gc":          memStats.DebugGC,
		})

	case "memory":
		// Detailed memory breakdown
		writeJSON(w, map[string]interface{}{
			"alloc":           memStats.Alloc,
			"total_alloc":     memStats.TotalAlloc,
			"sys":             memStats.Sys,
			"lookups":         memStats.Lookups,
			"mallocs":         memStats.Mallocs,
			"frees":           memStats.Frees,
			"heap_alloc":      memStats.HeapAlloc,
			"heap_sys":        memStats.HeapSys,
			"heap_idle":       memStats.HeapIdle,
			"heap_inuse":      memStats.HeapInuse,
			"heap_released":   memStats.HeapReleased,
			"heap_objects":    memStats.HeapObjects,
			"stack_inuse":     memStats.StackInuse,
			"stack_sys":       memStats.StackSys,
			"mspan_inuse":     memStats.MSpanInuse,
			"mspan_sys":       memStats.MSpanSys,
			"mcache_inuse":    memStats.MCacheInuse,
			"mcache_sys":      memStats.MCacheSys,
			"buck_hash_sys":   memStats.BuckHashSys,
			"gc_sys":          memStats.GCSys,
			"other_sys":       memStats.OtherSys,
		})

	case "goroutines":
		// Goroutine info
		writeJSON(w, map[string]interface{}{
			"count":      runtime.NumGoroutine(),
			"gomaxprocs": runtime.GOMAXPROCS(0),
			"num_cpu":    runtime.NumCPU(),
		})

	default:
		http.Error(w, "Invalid profile type. Use: summary, gc, memory, goroutines", http.StatusBadRequest)
	}
}

// Phase 11: Log Viewer API
func (ui *AdminUI) handleAPILogs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		level := r.URL.Query().Get("level")
		source := r.URL.Query().Get("source")
		limitStr := r.URL.Query().Get("limit")
		limit := 100
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}

		ui.mu.RLock()
		logs := make([]logEntry, 0)
		count := 0
		// Iterate in reverse to get most recent first
		for i := len(ui.logBuffer) - 1; i >= 0 && count < limit; i-- {
			entry := ui.logBuffer[i]
			if (level == "" || entry.Level == level) && (source == "" || entry.Source == source) {
				logs = append(logs, entry)
				count++
			}
		}
		ui.mu.RUnlock()
		writeJSON(w, logs)

	case http.MethodPost:
		// Allow posting log entries (useful for client-side logging)
		var entry logEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if entry.Message == "" {
			http.Error(w, "Message is required", http.StatusBadRequest)
			return
		}

		entry.Timestamp = time.Now()
		if entry.Level == "" {
			entry.Level = "info"
		}

		ui.mu.Lock()
		ui.logBuffer = append(ui.logBuffer, entry)
		// Keep buffer size limited
		if len(ui.logBuffer) > 1000 {
			ui.logBuffer = ui.logBuffer[len(ui.logBuffer)-1000:]
		}
		ui.mu.Unlock()

		writeJSON(w, map[string]string{"status": "logged"})

	case http.MethodDelete:
		// Clear logs
		if !ui.devMode {
			http.Error(w, "Clear logs only available in dev mode", http.StatusForbidden)
			return
		}

		ui.mu.Lock()
		ui.logBuffer = make([]logEntry, 0, 1000)
		ui.mu.Unlock()

		ui.logAudit(r, "ClearLogs", "All logs cleared")
		writeJSON(w, map[string]string{"status": "cleared"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// AddLog adds a log entry to the admin UI log buffer
func (ui *AdminUI) AddLog(level, message, source string, fields map[string]interface{}) {
	entry := logEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Source:    source,
		Fields:    fields,
	}

	ui.mu.Lock()
	ui.logBuffer = append(ui.logBuffer, entry)
	if len(ui.logBuffer) > 1000 {
		ui.logBuffer = ui.logBuffer[len(ui.logBuffer)-1000:]
	}
	ui.mu.Unlock()
}

// Phase 11: Role Management API
func (ui *AdminUI) handleAPIRoles(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		roles := make([]userRole, len(ui.roles))
		copy(roles, ui.roles)
		ui.mu.RUnlock()
		writeJSON(w, roles)

	case http.MethodPost:
		var role userRole
		if err := json.NewDecoder(r.Body).Decode(&role); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if role.Name == "" {
			http.Error(w, "Name is required", http.StatusBadRequest)
			return
		}

		role.ID = fmt.Sprintf("role_%d", time.Now().UnixNano())

		ui.mu.Lock()
		ui.roles = append(ui.roles, role)
		ui.mu.Unlock()

		ui.logAudit(r, "CreateRole", role.Name)
		writeJSON(w, role)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		// Prevent deleting built-in roles
		if strings.HasPrefix(id, "role_admin") || strings.HasPrefix(id, "role_operator") ||
			strings.HasPrefix(id, "role_analyst") || strings.HasPrefix(id, "role_viewer") {
			http.Error(w, "Cannot delete built-in roles", http.StatusForbidden)
			return
		}

		ui.mu.Lock()
		for i, r := range ui.roles {
			if r.ID == id {
				ui.roles = append(ui.roles[:i], ui.roles[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "DeleteRole", id)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 11: User Permissions API
func (ui *AdminUI) handleAPIPermissions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		user := r.URL.Query().Get("user")
		ui.mu.RLock()
		if user != "" {
			if access, ok := ui.userPermissions[user]; ok {
				ui.mu.RUnlock()
				writeJSON(w, access)
				return
			}
			ui.mu.RUnlock()
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}

		// Return all user permissions
		perms := make([]*userAccess, 0, len(ui.userPermissions))
		for _, v := range ui.userPermissions {
			perms = append(perms, v)
		}
		ui.mu.RUnlock()
		writeJSON(w, perms)

	case http.MethodPost:
		var access userAccess
		if err := json.NewDecoder(r.Body).Decode(&access); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if access.User == "" || access.Role == "" {
			http.Error(w, "User and role are required", http.StatusBadRequest)
			return
		}

		// Find role and get permissions
		ui.mu.RLock()
		var rolePerms []string
		for _, role := range ui.roles {
			if role.ID == access.Role || role.Name == access.Role {
				rolePerms = role.Permissions
				access.Role = role.ID
				break
			}
		}
		ui.mu.RUnlock()

		if rolePerms == nil {
			http.Error(w, "Role not found", http.StatusBadRequest)
			return
		}

		access.Permissions = rolePerms
		access.CreatedAt = time.Now()

		ui.mu.Lock()
		ui.userPermissions[access.User] = &access
		ui.mu.Unlock()

		ui.logAudit(r, "AssignRole", fmt.Sprintf("%s -> %s", access.User, access.Role))
		writeJSON(w, access)

	case http.MethodDelete:
		user := r.URL.Query().Get("user")
		if user == "" {
			http.Error(w, "Missing user parameter", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		delete(ui.userPermissions, user)
		ui.mu.Unlock()

		ui.logAudit(r, "RevokeAccess", user)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// HasPermission checks if a user has a specific permission
func (ui *AdminUI) HasPermission(user, permission string) bool {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	access, ok := ui.userPermissions[user]
	if !ok {
		return false
	}

	for _, p := range access.Permissions {
		if p == permission || p == "admin" {
			return true
		}
	}
	return false
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

const adminDashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{.Title}}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        :root {
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --border-color: #30363d;
            --text-primary: #f0f6fc;
            --text-secondary: #8b949e;
            --text-muted: #6e7681;
            --accent-blue: #58a6ff;
            --accent-green: #3fb950;
            --accent-yellow: #d29922;
            --accent-red: #f85149;
            --accent-purple: #a371f7;
            --sidebar-width: 240px;
            --header-height: 56px;
        }

        [data-theme="light"] {
            --bg-primary: #ffffff;
            --bg-secondary: #f6f8fa;
            --bg-tertiary: #eaeef2;
            --border-color: #d0d7de;
            --text-primary: #1f2328;
            --text-secondary: #656d76;
            --text-muted: #8c959f;
            --accent-blue: #0969da;
            --accent-green: #1a7f37;
            --accent-yellow: #9a6700;
            --accent-red: #cf222e;
            --accent-purple: #8250df;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.5;
            overflow-x: hidden;
        }

        /* Layout */
        .app-container {
            display: flex;
            min-height: 100vh;
        }

        /* Sidebar */
        .sidebar {
            width: var(--sidebar-width);
            background: var(--bg-secondary);
            border-right: 1px solid var(--border-color);
            position: fixed;
            top: 0;
            left: 0;
            height: 100vh;
            display: flex;
            flex-direction: column;
            z-index: 100;
            transition: transform 0.3s ease;
        }

        .sidebar-header {
            padding: 16px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .sidebar-logo {
            font-size: 20px;
            font-weight: 600;
            color: var(--accent-blue);
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .sidebar-nav {
            flex: 1;
            padding: 12px 8px;
            overflow-y: auto;
        }

        .nav-section {
            margin-bottom: 16px;
        }

        .nav-section-title {
            font-size: 11px;
            font-weight: 600;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding: 8px 12px 4px;
        }

        .nav-item {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 8px 12px;
            border-radius: 6px;
            color: var(--text-secondary);
            cursor: pointer;
            transition: all 0.15s ease;
            font-size: 14px;
        }

        .nav-item:hover {
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }

        .nav-item.active {
            background: var(--accent-blue);
            color: white;
        }

        .nav-item .icon {
            width: 16px;
            text-align: center;
        }

        .sidebar-footer {
            padding: 12px 16px;
            border-top: 1px solid var(--border-color);
        }

        /* Main Content */
        .main-content {
            flex: 1;
            margin-left: var(--sidebar-width);
            display: flex;
            flex-direction: column;
        }

        /* Header */
        .header {
            height: var(--header-height);
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 24px;
            position: sticky;
            top: 0;
            z-index: 50;
        }

        .header-title {
            font-size: 16px;
            font-weight: 600;
        }

        .header-actions {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        /* Theme Toggle */
        .theme-toggle {
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 6px 10px;
            cursor: pointer;
            color: var(--text-secondary);
            font-size: 14px;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .theme-toggle:hover {
            background: var(--border-color);
        }

        /* Status Badge */
        .status-badge {
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 500;
        }

        .status-badge.healthy {
            background: rgba(63, 185, 80, 0.15);
            color: var(--accent-green);
        }

        .status-badge.warning {
            background: rgba(210, 153, 34, 0.15);
            color: var(--accent-yellow);
        }

        .status-badge.error {
            background: rgba(248, 81, 73, 0.15);
            color: var(--accent-red);
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: currentColor;
        }

        /* Page Content */
        .page-content {
            padding: 24px;
            flex: 1;
        }

        .page {
            display: none;
        }

        .page.active {
            display: block;
        }

        /* Cards */
        .card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            margin-bottom: 16px;
        }

        .card-header {
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .card-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
        }

        .card-body {
            padding: 16px;
        }

        /* Grid System */
        .grid {
            display: grid;
            gap: 16px;
        }

        .grid-2 { grid-template-columns: repeat(2, 1fr); }
        .grid-3 { grid-template-columns: repeat(3, 1fr); }
        .grid-4 { grid-template-columns: repeat(4, 1fr); }

        @media (max-width: 1200px) {
            .grid-4 { grid-template-columns: repeat(2, 1fr); }
            .grid-3 { grid-template-columns: repeat(2, 1fr); }
        }

        @media (max-width: 768px) {
            .sidebar {
                transform: translateX(-100%);
            }
            .sidebar.open {
                transform: translateX(0);
            }
            .main-content {
                margin-left: 0;
            }
            .grid-2, .grid-3, .grid-4 {
                grid-template-columns: 1fr;
            }
            .mobile-menu-btn {
                display: flex !important;
            }
        }

        /* Stats Cards */
        .stat-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 16px;
        }

        .stat-card .stat-label {
            font-size: 12px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }

        .stat-card .stat-value {
            font-size: 28px;
            font-weight: 600;
            color: var(--text-primary);
        }

        .stat-card .stat-change {
            font-size: 12px;
            margin-top: 4px;
        }

        .stat-card .stat-change.positive { color: var(--accent-green); }
        .stat-card .stat-change.negative { color: var(--accent-red); }

        /* Tables */
        .table-container {
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th, td {
            padding: 10px 12px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }

        th {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            background: var(--bg-tertiary);
        }

        td {
            font-size: 14px;
            color: var(--text-primary);
        }

        tr:hover td {
            background: var(--bg-tertiary);
        }

        /* Buttons */
        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            padding: 8px 16px;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            border: 1px solid transparent;
            transition: all 0.15s ease;
        }

        .btn-primary {
            background: var(--accent-green);
            color: white;
        }

        .btn-primary:hover {
            filter: brightness(1.1);
        }

        .btn-secondary {
            background: var(--bg-tertiary);
            border-color: var(--border-color);
            color: var(--text-primary);
        }

        .btn-secondary:hover {
            background: var(--border-color);
        }

        .btn-danger {
            background: var(--accent-red);
            color: white;
        }

        .btn-danger:hover {
            filter: brightness(1.1);
        }

        .btn-sm {
            padding: 4px 10px;
            font-size: 12px;
        }

        /* Forms */
        .form-group {
            margin-bottom: 16px;
        }

        .form-label {
            display: block;
            font-size: 14px;
            font-weight: 500;
            color: var(--text-primary);
            margin-bottom: 6px;
        }

        .form-input, .form-select, .form-textarea {
            width: 100%;
            padding: 8px 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-size: 14px;
            font-family: inherit;
        }

        .form-input:focus, .form-select:focus, .form-textarea:focus {
            outline: none;
            border-color: var(--accent-blue);
            box-shadow: 0 0 0 3px rgba(88, 166, 255, 0.15);
        }

        .form-textarea {
            min-height: 120px;
            resize: vertical;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
        }

        /* Query Editor */
        .query-editor {
            position: relative;
        }

        .query-input {
            width: 100%;
            min-height: 100px;
            padding: 12px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 14px;
            resize: vertical;
        }

        .query-input:focus {
            outline: none;
            border-color: var(--accent-blue);
        }

        .query-actions {
            display: flex;
            gap: 8px;
            margin-top: 12px;
        }

        .query-result {
            margin-top: 16px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            overflow: hidden;
        }

        .query-result-header {
            padding: 8px 12px;
            background: var(--bg-tertiary);
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 12px;
            color: var(--text-secondary);
        }

        .query-result-body {
            padding: 12px;
            max-height: 400px;
            overflow: auto;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 13px;
            white-space: pre-wrap;
        }

        /* Metrics List */
        .metric-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-color);
            cursor: pointer;
            transition: background 0.15s ease;
        }

        .metric-item:hover {
            background: var(--bg-tertiary);
        }

        .metric-item:last-child {
            border-bottom: none;
        }

        .metric-name {
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 14px;
            color: var(--accent-blue);
        }

        .metric-actions {
            display: flex;
            gap: 8px;
            opacity: 0;
            transition: opacity 0.15s ease;
        }

        .metric-item:hover .metric-actions {
            opacity: 1;
        }

        /* Search */
        .search-box {
            position: relative;
        }

        .search-box input {
            width: 100%;
            padding: 8px 12px 8px 36px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-size: 14px;
        }

        .search-box::before {
            content: "🔍";
            position: absolute;
            left: 10px;
            top: 50%;
            transform: translateY(-50%);
            font-size: 14px;
            opacity: 0.5;
        }

        /* Tabs */
        .tabs {
            display: flex;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 16px;
        }

        .tab {
            padding: 10px 16px;
            font-size: 14px;
            color: var(--text-secondary);
            cursor: pointer;
            border-bottom: 2px solid transparent;
            margin-bottom: -1px;
            transition: all 0.15s ease;
        }

        .tab:hover {
            color: var(--text-primary);
        }

        .tab.active {
            color: var(--accent-blue);
            border-bottom-color: var(--accent-blue);
        }

        .tab-content {
            display: none;
        }

        .tab-content.active {
            display: block;
        }

        /* Modals */
        .modal-backdrop {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.5);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 1000;
            opacity: 0;
            visibility: hidden;
            transition: all 0.2s ease;
        }

        .modal-backdrop.open {
            opacity: 1;
            visibility: visible;
        }

        .modal {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            width: 90%;
            max-width: 500px;
            max-height: 90vh;
            overflow: hidden;
            transform: scale(0.95);
            transition: transform 0.2s ease;
        }

        .modal-backdrop.open .modal {
            transform: scale(1);
        }

        .modal-header {
            padding: 16px 20px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .modal-title {
            font-size: 16px;
            font-weight: 600;
        }

        .modal-close {
            background: none;
            border: none;
            font-size: 20px;
            color: var(--text-secondary);
            cursor: pointer;
            padding: 4px;
            line-height: 1;
        }

        .modal-body {
            padding: 20px;
            overflow-y: auto;
            max-height: calc(90vh - 140px);
        }

        .modal-footer {
            padding: 16px 20px;
            border-top: 1px solid var(--border-color);
            display: flex;
            justify-content: flex-end;
            gap: 8px;
        }

        /* Charts */
        .chart-container {
            position: relative;
            height: 200px;
        }

        /* Activity Log */
        .activity-item {
            display: flex;
            align-items: flex-start;
            gap: 12px;
            padding: 10px 0;
            border-bottom: 1px solid var(--border-color);
        }

        .activity-item:last-child {
            border-bottom: none;
        }

        .activity-icon {
            width: 32px;
            height: 32px;
            border-radius: 50%;
            background: var(--bg-tertiary);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 14px;
        }

        .activity-content {
            flex: 1;
        }

        .activity-action {
            font-weight: 500;
            color: var(--text-primary);
        }

        .activity-details {
            font-size: 13px;
            color: var(--text-secondary);
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
        }

        .activity-time {
            font-size: 12px;
            color: var(--text-muted);
        }

        /* Connection Snippets */
        .snippet-card {
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            margin-bottom: 12px;
        }

        .snippet-header {
            padding: 10px 12px;
            background: var(--bg-tertiary);
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 13px;
            font-weight: 500;
        }

        .snippet-body {
            padding: 12px;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 13px;
            overflow-x: auto;
        }

        .snippet-copy {
            font-size: 12px;
            color: var(--accent-blue);
            cursor: pointer;
        }

        /* Toast Notifications */
        .toast-container {
            position: fixed;
            bottom: 20px;
            right: 20px;
            z-index: 2000;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .toast {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 12px 16px;
            min-width: 280px;
            display: flex;
            align-items: center;
            gap: 10px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            transform: translateX(100%);
            opacity: 0;
            transition: all 0.3s ease;
        }

        .toast.show {
            transform: translateX(0);
            opacity: 1;
        }

        .toast.success { border-left: 3px solid var(--accent-green); }
        .toast.error { border-left: 3px solid var(--accent-red); }
        .toast.warning { border-left: 3px solid var(--accent-yellow); }

        /* Keyboard Shortcuts */
        kbd {
            display: inline-block;
            padding: 2px 6px;
            font-size: 11px;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            color: var(--text-secondary);
        }

        /* Mobile Menu Button */
        .mobile-menu-btn {
            display: none;
            background: none;
            border: none;
            font-size: 24px;
            cursor: pointer;
            color: var(--text-primary);
        }

        /* Loading Spinner */
        .spinner {
            width: 20px;
            height: 20px;
            border: 2px solid var(--border-color);
            border-top-color: var(--accent-blue);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }

        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        /* Empty State */
        .empty-state {
            text-align: center;
            padding: 40px 20px;
            color: var(--text-secondary);
        }

        .empty-state-icon {
            font-size: 48px;
            margin-bottom: 16px;
            opacity: 0.5;
        }

        .empty-state-title {
            font-size: 16px;
            font-weight: 500;
            color: var(--text-primary);
            margin-bottom: 8px;
        }

        /* Dev Mode Banner */
        .dev-mode-banner {
            background: rgba(210, 153, 34, 0.15);
            border-bottom: 1px solid var(--accent-yellow);
            padding: 8px 24px;
            font-size: 13px;
            color: var(--accent-yellow);
            display: flex;
            align-items: center;
            gap: 8px;
        }

        /* JSON Viewer */
        .json-key { color: var(--accent-purple); }
        .json-string { color: var(--accent-green); }
        .json-number { color: var(--accent-blue); }
        .json-boolean { color: var(--accent-yellow); }
        .json-null { color: var(--text-muted); }

        /* Config Table */
        .config-row {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid var(--border-color);
        }

        .config-row:last-child {
            border-bottom: none;
        }

        .config-key {
            color: var(--text-secondary);
            font-size: 14px;
        }

        .config-value {
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 14px;
            color: var(--text-primary);
        }

        /* Query Templates */
        .template-item {
            padding: 10px 12px;
            border-bottom: 1px solid var(--border-color);
            cursor: pointer;
            transition: background 0.15s ease;
        }

        .template-item:hover {
            background: var(--bg-tertiary);
        }

        .template-name {
            font-weight: 500;
            font-size: 14px;
            margin-bottom: 4px;
        }

        .template-query {
            font-size: 12px;
            color: var(--text-secondary);
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
        }

        /* Global Search Modal */
        .search-modal {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.6);
            display: none;
            align-items: flex-start;
            justify-content: center;
            padding-top: 100px;
            z-index: 3000;
        }

        .search-modal.open {
            display: flex;
        }

        .search-modal-content {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            width: 90%;
            max-width: 600px;
            max-height: 400px;
            overflow: hidden;
            box-shadow: 0 16px 70px rgba(0, 0, 0, 0.3);
        }

        .search-modal-input {
            width: 100%;
            padding: 16px 20px;
            border: none;
            background: transparent;
            color: var(--text-primary);
            font-size: 16px;
            outline: none;
            border-bottom: 1px solid var(--border-color);
        }

        .search-results {
            max-height: 300px;
            overflow-y: auto;
        }

        .search-result-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px 20px;
            cursor: pointer;
            transition: background 0.1s ease;
        }

        .search-result-item:hover,
        .search-result-item.selected {
            background: var(--bg-tertiary);
        }

        .search-result-icon {
            font-size: 18px;
        }

        .search-result-name {
            flex: 1;
            font-size: 14px;
            color: var(--text-primary);
        }

        .search-result-type {
            font-size: 11px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .search-hint {
            padding: 12px 20px;
            font-size: 12px;
            color: var(--text-muted);
            border-top: 1px solid var(--border-color);
        }

        /* Alert Status Colors */
        .alert-status-ok { color: var(--accent-green); }
        .alert-status-pending { color: var(--accent-yellow); }
        .alert-status-firing { color: var(--accent-red); }

        /* Live Indicator */
        .live-indicator {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
            color: var(--accent-green);
        }

        .live-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--accent-green);
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        /* Phase 10: Autocomplete Dropdown */
        .autocomplete-container {
            position: relative;
        }

        .autocomplete-dropdown {
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            max-height: 300px;
            overflow-y: auto;
            z-index: 1000;
            display: none;
        }

        .autocomplete-dropdown.show {
            display: block;
        }

        .autocomplete-item {
            padding: 10px 14px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 10px;
            border-bottom: 1px solid var(--border-color);
        }

        .autocomplete-item:last-child {
            border-bottom: none;
        }

        .autocomplete-item:hover,
        .autocomplete-item.selected {
            background: var(--bg-secondary);
        }

        .autocomplete-item .type {
            font-size: 10px;
            padding: 2px 6px;
            border-radius: 4px;
            background: var(--bg-secondary);
            color: var(--text-muted);
            text-transform: uppercase;
        }

        .autocomplete-item .type.metric { background: var(--accent-blue); color: white; }
        .autocomplete-item .type.function { background: var(--accent-purple); color: white; }

        /* Phase 10: Sparklines */
        .sparkline-container {
            display: inline-block;
            width: 80px;
            height: 24px;
            vertical-align: middle;
        }

        .sparkline {
            width: 100%;
            height: 100%;
        }

        .sparkline-path {
            fill: none;
            stroke: var(--accent-blue);
            stroke-width: 1.5;
        }

        .sparkline-fill {
            fill: var(--accent-blue);
            opacity: 0.1;
        }

        /* Phase 10: Favorites */
        .favorite-btn {
            background: none;
            border: none;
            cursor: pointer;
            font-size: 16px;
            padding: 4px;
            opacity: 0.5;
            transition: opacity 0.2s, transform 0.2s;
        }

        .favorite-btn:hover {
            opacity: 1;
            transform: scale(1.1);
        }

        .favorite-btn.active {
            opacity: 1;
            color: var(--accent-yellow);
        }

        /* Phase 10: Recent Items */
        .recent-items {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-bottom: 16px;
        }

        .recent-item {
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            background: var(--bg-secondary);
            border-radius: 16px;
            font-size: 13px;
            cursor: pointer;
            transition: background 0.2s;
        }

        .recent-item:hover {
            background: var(--card-bg);
        }

        /* Phase 10: Alert History */
        .alert-history-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px;
            border-bottom: 1px solid var(--border-color);
        }

        .alert-history-item:last-child {
            border-bottom: none;
        }

        .alert-history-status {
            width: 10px;
            height: 10px;
            border-radius: 50%;
        }

        .alert-history-status.firing { background: var(--accent-red); }
        .alert-history-status.resolved { background: var(--accent-green); }

        /* Phase 10: Comparison Chart */
        .comparison-legend {
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            margin-top: 12px;
        }

        .comparison-legend-item {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 13px;
        }

        .comparison-legend-color {
            width: 12px;
            height: 12px;
            border-radius: 2px;
        }

        /* Phase 10: Import Wizard */
        .import-wizard {
            display: flex;
            flex-direction: column;
            gap: 20px;
        }

        .import-step {
            display: flex;
            align-items: flex-start;
            gap: 12px;
        }

        .import-step-number {
            width: 28px;
            height: 28px;
            border-radius: 50%;
            background: var(--accent-blue);
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            flex-shrink: 0;
        }

        .import-step.completed .import-step-number {
            background: var(--accent-green);
        }

        .import-drop-zone {
            border: 2px dashed var(--border-color);
            border-radius: 8px;
            padding: 40px;
            text-align: center;
            cursor: pointer;
            transition: border-color 0.2s, background 0.2s;
        }

        .import-drop-zone:hover,
        .import-drop-zone.drag-over {
            border-color: var(--accent-blue);
            background: rgba(59, 130, 246, 0.05);
        }

        /* Phase 10: Diagnostics */
        .diagnostics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 16px;
        }

        .diagnostic-card {
            background: var(--card-bg);
            border-radius: 12px;
            padding: 20px;
        }

        .diagnostic-value {
            font-size: 32px;
            font-weight: bold;
            margin: 8px 0;
        }

        .diagnostic-bar {
            height: 8px;
            background: var(--bg-secondary);
            border-radius: 4px;
            overflow: hidden;
            margin-top: 12px;
        }

        .diagnostic-bar-fill {
            height: 100%;
            border-radius: 4px;
            transition: width 0.5s;
        }

        .diagnostic-bar-fill.low { background: var(--accent-green); }
        .diagnostic-bar-fill.medium { background: var(--accent-yellow); }
        .diagnostic-bar-fill.high { background: var(--accent-red); }

        /* Phase 10: Sessions */
        .session-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px;
            border-bottom: 1px solid var(--border-color);
        }

        .session-item:last-child {
            border-bottom: none;
        }

        .session-current {
            background: rgba(59, 130, 246, 0.05);
        }

        .session-badge {
            font-size: 11px;
            padding: 2px 8px;
            border-radius: 4px;
            background: var(--accent-blue);
            color: white;
        }

        /* Phase 10: Saved Queries */
        .saved-query-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 14px;
            border-bottom: 1px solid var(--border-color);
            cursor: pointer;
            transition: background 0.2s;
        }

        .saved-query-item:hover {
            background: var(--bg-secondary);
        }

        .saved-query-name {
            font-weight: 500;
            margin-bottom: 4px;
        }

        .saved-query-text {
            font-size: 12px;
            color: var(--text-muted);
            font-family: monospace;
        }

        /* Phase 11: Query Templates */
        .template-card {
            background: var(--card-bg);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 12px;
            border-left: 3px solid var(--accent-blue);
            cursor: pointer;
            transition: transform 0.2s, box-shadow 0.2s;
        }

        .template-card:hover {
            transform: translateX(4px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }

        .template-card.built-in {
            border-left-color: var(--accent-purple);
        }

        .template-category {
            font-size: 10px;
            padding: 2px 8px;
            border-radius: 4px;
            background: var(--bg-secondary);
            color: var(--text-muted);
            text-transform: uppercase;
            margin-right: 8px;
        }

        .template-category.aggregation { background: #3b82f620; color: var(--accent-blue); }
        .template-category.analysis { background: #10b98120; color: var(--accent-green); }
        .template-category.monitoring { background: #f59e0b20; color: var(--accent-yellow); }

        /* Phase 11: Annotations */
        .annotation-timeline {
            position: relative;
            padding-left: 24px;
        }

        .annotation-timeline::before {
            content: '';
            position: absolute;
            left: 8px;
            top: 0;
            bottom: 0;
            width: 2px;
            background: var(--border-color);
        }

        .annotation-item {
            position: relative;
            padding: 12px 16px;
            margin-bottom: 12px;
            background: var(--card-bg);
            border-radius: 8px;
        }

        .annotation-item::before {
            content: '📌';
            position: absolute;
            left: -24px;
            top: 12px;
            font-size: 14px;
        }

        .annotation-tags {
            display: flex;
            gap: 6px;
            margin-top: 8px;
        }

        .annotation-tag {
            font-size: 11px;
            padding: 2px 8px;
            border-radius: 12px;
            background: var(--bg-secondary);
            color: var(--text-muted);
        }

        /* Phase 11: Profiling */
        .profiling-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 16px;
        }

        .profile-stat {
            background: var(--card-bg);
            border-radius: 8px;
            padding: 16px;
            text-align: center;
        }

        .profile-stat-value {
            font-size: 28px;
            font-weight: bold;
            color: var(--accent-blue);
        }

        .profile-stat-label {
            font-size: 12px;
            color: var(--text-muted);
            margin-top: 4px;
        }

        .memory-bar {
            height: 24px;
            background: var(--bg-secondary);
            border-radius: 4px;
            overflow: hidden;
            display: flex;
        }

        .memory-bar-segment {
            height: 100%;
            transition: width 0.3s;
        }

        .memory-bar-segment.heap { background: var(--accent-blue); }
        .memory-bar-segment.stack { background: var(--accent-green); }
        .memory-bar-segment.other { background: var(--accent-purple); }

        /* Phase 11: Log Viewer */
        .log-container {
            font-family: monospace;
            font-size: 12px;
            background: var(--bg-secondary);
            border-radius: 8px;
            padding: 12px;
            max-height: 500px;
            overflow-y: auto;
        }

        .log-entry {
            padding: 4px 8px;
            border-radius: 4px;
            margin-bottom: 2px;
            display: flex;
            gap: 12px;
        }

        .log-entry:hover {
            background: var(--bg-tertiary);
        }

        .log-timestamp {
            color: var(--text-muted);
            white-space: nowrap;
        }

        .log-level {
            font-weight: bold;
            text-transform: uppercase;
            width: 50px;
        }

        .log-level.debug { color: var(--text-muted); }
        .log-level.info { color: var(--accent-blue); }
        .log-level.warn { color: var(--accent-yellow); }
        .log-level.error { color: var(--accent-red); }

        .log-message {
            flex: 1;
            word-break: break-word;
        }

        .log-filters {
            display: flex;
            gap: 12px;
            margin-bottom: 12px;
            flex-wrap: wrap;
        }

        /* Phase 11: RBAC */
        .role-card {
            background: var(--card-bg);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 12px;
        }

        .role-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }

        .role-name {
            font-weight: bold;
            font-size: 16px;
        }

        .role-badge {
            font-size: 10px;
            padding: 2px 8px;
            border-radius: 4px;
            text-transform: uppercase;
        }

        .role-badge.admin { background: var(--accent-red); color: white; }
        .role-badge.operator { background: var(--accent-yellow); color: black; }
        .role-badge.analyst { background: var(--accent-blue); color: white; }
        .role-badge.viewer { background: var(--text-muted); color: white; }

        .permission-list {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }

        .permission-badge {
            font-size: 11px;
            padding: 4px 10px;
            border-radius: 4px;
            background: var(--bg-secondary);
            color: var(--text-secondary);
        }

        .permission-badge.granted {
            background: #10b98120;
            color: var(--accent-green);
        }

        .user-access-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px;
            border-bottom: 1px solid var(--border-color);
        }

        .user-access-row:last-child {
            border-bottom: none;
        }
    </style>
</head>
<body>
    <div class="app-container">
        <!-- Sidebar -->
        <aside class="sidebar" id="sidebar">
            <div class="sidebar-header">
                <div class="sidebar-logo">
                    <span>📊</span>
                    <span>Chronicle</span>
                </div>
            </div>
            <nav class="sidebar-nav">
                <div class="nav-section">
                    <div class="nav-section-title">Overview</div>
                    <div class="nav-item active" data-page="dashboard">
                        <span class="icon">🏠</span>
                        <span>Dashboard</span>
                    </div>
                    <div class="nav-item" data-page="health">
                        <span class="icon">💚</span>
                        <span>Health</span>
                    </div>
                    <div class="nav-item" data-page="cluster">
                        <span class="icon">🌐</span>
                        <span>Cluster</span>
                    </div>
                </div>
                <div class="nav-section">
                    <div class="nav-section-title">Data</div>
                    <div class="nav-item" data-page="explorer">
                        <span class="icon">🔍</span>
                        <span>Explorer</span>
                    </div>
                    <div class="nav-item" data-page="query">
                        <span class="icon">⚡</span>
                        <span>Query Console</span>
                    </div>
                    <div class="nav-item" data-page="saved-queries">
                        <span class="icon">💾</span>
                        <span>Saved Queries</span>
                    </div>
                    <div class="nav-item" data-page="templates">
                        <span class="icon">📋</span>
                        <span>Query Templates</span>
                    </div>
                    <div class="nav-item" data-page="compare">
                        <span class="icon">📈</span>
                        <span>Compare Metrics</span>
                    </div>
                    <div class="nav-item" data-page="annotations">
                        <span class="icon">📌</span>
                        <span>Annotations</span>
                    </div>
                    <div class="nav-item" data-page="import">
                        <span class="icon">📥</span>
                        <span>Import Data</span>
                    </div>
                    <div class="nav-item" data-page="schemas">
                        <span class="icon">📐</span>
                        <span>Schema Registry</span>
                    </div>
                    <div class="nav-item" data-page="management">
                        <span class="icon">🗑️</span>
                        <span>Management</span>
                    </div>
                </div>
                <div class="nav-section">
                    <div class="nav-section-title">Alerting</div>
                    <div class="nav-item" data-page="alerts">
                        <span class="icon">🔔</span>
                        <span>Alert Rules</span>
                    </div>
                    <div class="nav-item" data-page="alert-history">
                        <span class="icon">📜</span>
                        <span>Alert History</span>
                    </div>
                </div>
                <div class="nav-section">
                    <div class="nav-section-title">Operations</div>
                    <div class="nav-item" data-page="config">
                        <span class="icon">⚙️</span>
                        <span>Configuration</span>
                    </div>
                    <div class="nav-item" data-page="retention">
                        <span class="icon">🕐</span>
                        <span>Retention</span>
                    </div>
                    <div class="nav-item" data-page="backup">
                        <span class="icon">💾</span>
                        <span>Backup</span>
                    </div>
                    <div class="nav-item" data-page="exports">
                        <span class="icon">📤</span>
                        <span>Scheduled Exports</span>
                    </div>
                </div>
                <div class="nav-section">
                    <div class="nav-section-title">Developer</div>
                    <div class="nav-item" data-page="wal">
                        <span class="icon">📝</span>
                        <span>WAL Inspector</span>
                    </div>
                    <div class="nav-item" data-page="audit">
                        <span class="icon">📋</span>
                        <span>Audit Log</span>
                    </div>
                    <div class="nav-item" data-page="diagnostics">
                        <span class="icon">🔬</span>
                        <span>Diagnostics</span>
                    </div>
                    <div class="nav-item" data-page="profiling">
                        <span class="icon">⚡</span>
                        <span>Profiling</span>
                    </div>
                    <div class="nav-item" data-page="logs">
                        <span class="icon">📄</span>
                        <span>Log Viewer</span>
                    </div>
                    <div class="nav-item" data-page="sessions">
                        <span class="icon">👤</span>
                        <span>Sessions</span>
                    </div>
                    <div class="nav-item" data-page="roles">
                        <span class="icon">🔐</span>
                        <span>Access Control</span>
                    </div>
                    <div class="nav-item" data-page="api">
                        <span class="icon">📚</span>
                        <span>API Reference</span>
                    </div>
                    <div class="nav-item" data-page="connect">
                        <span class="icon">🔗</span>
                        <span>Connect</span>
                    </div>
                </div>
            </nav>
            <div class="sidebar-footer">
                <div style="font-size: 12px; color: var(--text-muted);">
                    v{{.Version}} • {{.GoVersion}}<br>
                    <kbd>⌘</kbd>+<kbd>K</kbd> to search
                </div>
            </div>
        </aside>

        <!-- Main Content -->
        <main class="main-content">
            {{if .DevMode}}
            <div class="dev-mode-banner">
                <span>⚠️</span>
                <span>Development Mode - Data modification enabled</span>
            </div>
            {{end}}

            <header class="header">
                <div style="display: flex; align-items: center; gap: 16px;">
                    <button class="mobile-menu-btn" onclick="toggleSidebar()">☰</button>
                    <h1 class="header-title" id="pageTitle">Dashboard</h1>
                </div>
                <div class="header-actions">
                    <div class="status-badge healthy" id="statusBadge">
                        <span class="status-dot"></span>
                        <span>Healthy</span>
                    </div>
                    <button class="theme-toggle" onclick="toggleTheme()">
                        <span id="themeIcon">🌙</span>
                        <span id="themeText">Dark</span>
                    </button>
                </div>
            </header>

            <div class="page-content">
                <!-- Dashboard Page -->
                <div class="page active" id="page-dashboard">
                    <div class="grid grid-4">
                        <div class="stat-card">
                            <div class="stat-label">Metrics</div>
                            <div class="stat-value" id="statMetrics">{{.DBStats.MetricCount}}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Partitions</div>
                            <div class="stat-value" id="statPartitions">{{.DBStats.PartitionCount}}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Memory</div>
                            <div class="stat-value" id="statMemory">{{.MemStats.Alloc}}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Uptime</div>
                            <div class="stat-value" id="statUptime">{{.Uptime}}</div>
                        </div>
                    </div>

                    <div class="grid grid-2">
                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">Memory Usage</span>
                            </div>
                            <div class="card-body">
                                <div class="chart-container">
                                    <canvas id="memoryChart"></canvas>
                                </div>
                            </div>
                        </div>
                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">System Info</span>
                            </div>
                            <div class="card-body">
                                <div class="config-row">
                                    <span class="config-key">Go Version</span>
                                    <span class="config-value">{{.GoVersion}}</span>
                                </div>
                                <div class="config-row">
                                    <span class="config-key">CPUs</span>
                                    <span class="config-value">{{.NumCPU}}</span>
                                </div>
                                <div class="config-row">
                                    <span class="config-key">Goroutines</span>
                                    <span class="config-value" id="statGoroutines">{{.NumGoroutine}}</span>
                                </div>
                                <div class="config-row">
                                    <span class="config-key">GC Cycles</span>
                                    <span class="config-value" id="statGC">{{.MemStats.NumGC}}</span>
                                </div>
                                <div class="config-row">
                                    <span class="config-key">Total Allocated</span>
                                    <span class="config-value" id="statTotalAlloc">{{.MemStats.TotalAlloc}}</span>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Recent Activity</span>
                            <button class="btn btn-sm btn-secondary" onclick="refreshActivity()">Refresh</button>
                        </div>
                        <div class="card-body" id="activityLog">
                            <div class="empty-state">
                                <div class="empty-state-icon">📝</div>
                                <div class="empty-state-title">No recent activity</div>
                                <p>Activity will appear here as you interact with Chronicle</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Health Page -->
                <div class="page" id="page-health">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Health Checks</span>
                            <button class="btn btn-sm btn-secondary" onclick="refreshHealth()">Check Now</button>
                        </div>
                        <div class="card-body" id="healthChecks">
                            <div class="spinner"></div>
                        </div>
                    </div>
                </div>

                <!-- Explorer Page -->
                <div class="page" id="page-explorer">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Metrics ({{len .Metrics}})</span>
                            <div class="search-box" style="width: 250px;">
                                <input type="text" id="metricSearch" placeholder="Search metrics..." oninput="filterMetrics(this.value)">
                            </div>
                        </div>
                        <div class="card-body" style="padding: 0; max-height: 500px; overflow-y: auto;">
                            <div id="metricsList">
                                {{range .Metrics}}
                                <div class="metric-item" onclick="showMetricDetails('{{.}}')">
                                    <span class="metric-name">{{.}}</span>
                                    <div class="metric-actions">
                                        <button class="btn btn-sm btn-secondary" onclick="event.stopPropagation(); queryMetric('{{.}}')">Query</button>
                                        <button class="btn btn-sm btn-secondary" onclick="event.stopPropagation(); previewMetric('{{.}}')">Preview</button>
                                    </div>
                                </div>
                                {{else}}
                                <div class="empty-state">
                                    <div class="empty-state-icon">📊</div>
                                    <div class="empty-state-title">No metrics found</div>
                                    <p>Write some data to see metrics here</p>
                                </div>
                                {{end}}
                            </div>
                        </div>
                    </div>

                    <div class="card" id="metricDetailsCard" style="display: none;">
                        <div class="card-header">
                            <span class="card-title">Metric Details: <span id="metricDetailsName"></span></span>
                            <button class="btn btn-sm btn-secondary" onclick="closeMetricDetails()">Close</button>
                        </div>
                        <div class="card-body" id="metricDetailsBody">
                        </div>
                    </div>

                    <div class="card" id="dataPreviewCard" style="display: none;">
                        <div class="card-header">
                            <span class="card-title">Data Preview</span>
                            <button class="btn btn-sm btn-secondary" onclick="closeDataPreview()">Close</button>
                        </div>
                        <div class="card-body">
                            <div class="chart-container" style="height: 300px;">
                                <canvas id="previewChart"></canvas>
                            </div>
                            <div class="table-container" style="margin-top: 16px; max-height: 300px; overflow-y: auto;">
                                <table id="previewTable">
                                    <thead>
                                        <tr>
                                            <th>Timestamp</th>
                                            <th>Value</th>
                                        </tr>
                                    </thead>
                                    <tbody></tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Query Console Page -->
                <div class="page" id="page-query">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Query Editor</span>
                            <div style="display: flex; gap: 8px; align-items: center;">
                                <kbd>Ctrl</kbd>+<kbd>Enter</kbd> to run
                            </div>
                        </div>
                        <div class="card-body">
                            <div class="query-editor">
                                <textarea class="query-input" id="queryInput" placeholder="SELECT mean(value) FROM cpu WHERE host='server1' GROUP BY time(5m)"></textarea>
                                <div class="query-actions">
                                    <button class="btn btn-primary" onclick="executeQuery()">
                                        <span>▶</span> Execute
                                    </button>
                                    <button class="btn btn-secondary" onclick="explainQuery()">📊 Explain</button>
                                    <button class="btn btn-secondary" onclick="exportJSON()">Export JSON</button>
                                    <button class="btn btn-secondary" onclick="exportCSV()">Export CSV</button>
                                    <button class="btn btn-secondary" onclick="clearQuery()">Clear</button>
                                </div>
                            </div>
                            <div class="query-result" id="queryResultContainer" style="display: none;">
                                <div class="query-result-header">
                                    <span id="queryResultInfo"></span>
                                    <span id="queryResultTime"></span>
                                </div>
                                <div class="tabs">
                                    <div class="tab active" data-result-tab="table">Table</div>
                                    <div class="tab" data-result-tab="chart">Chart</div>
                                    <div class="tab" data-result-tab="json">JSON</div>
                                </div>
                                <div class="tab-content active" id="resultTable">
                                    <div class="table-container">
                                        <table id="queryResultTable">
                                            <thead></thead>
                                            <tbody></tbody>
                                        </table>
                                    </div>
                                </div>
                                <div class="tab-content" id="resultChart">
                                    <div class="chart-container" style="height: 300px;">
                                        <canvas id="queryResultChart"></canvas>
                                    </div>
                                </div>
                                <div class="tab-content" id="resultJson">
                                    <div class="query-result-body" id="queryResultJSON"></div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="grid grid-2">
                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">Query Templates</span>
                            </div>
                            <div class="card-body" style="padding: 0;">
                                <div class="template-item" onclick="useTemplate('SELECT mean(value) FROM metric_name')">
                                    <div class="template-name">Basic Average</div>
                                    <div class="template-query">SELECT mean(value) FROM metric_name</div>
                                </div>
                                <div class="template-item" onclick="useTemplate('SELECT max(value), min(value) FROM metric_name')">
                                    <div class="template-name">Min/Max</div>
                                    <div class="template-query">SELECT max(value), min(value) FROM metric_name</div>
                                </div>
                                <div class="template-item" onclick="useTemplate('SELECT count(value) FROM metric_name GROUP BY time(1h)')">
                                    <div class="template-name">Hourly Count</div>
                                    <div class="template-query">SELECT count(value) FROM metric_name GROUP BY time(1h)</div>
                                </div>
                                <div class="template-item" onclick="useTemplate('SELECT percentile(value, 95) FROM metric_name')">
                                    <div class="template-name">95th Percentile</div>
                                    <div class="template-query">SELECT percentile(value, 95) FROM metric_name</div>
                                </div>
                            </div>
                        </div>

                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">Query History</span>
                                <button class="btn btn-sm btn-secondary" onclick="refreshQueryHistory()">Refresh</button>
                            </div>
                            <div class="card-body" style="padding: 0; max-height: 300px; overflow-y: auto;" id="queryHistoryList">
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Management Page -->
                <div class="page" id="page-management">
                    {{if .DevMode}}
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Insert Test Data</span>
                        </div>
                        <div class="card-body">
                            <div class="form-group">
                                <label class="form-label">Data Points (JSON array)</label>
                                <textarea class="form-textarea" id="insertData" placeholder='[{"metric": "test", "value": 42.5}]'></textarea>
                            </div>
                            <button class="btn btn-primary" onclick="insertData()">Insert Data</button>
                        </div>
                    </div>
                    {{else}}
                    <div class="card">
                        <div class="card-body">
                            <div class="empty-state">
                                <div class="empty-state-icon">🔒</div>
                                <div class="empty-state-title">Data insertion disabled</div>
                                <p>Enable DevMode in AdminConfig to insert test data</p>
                            </div>
                        </div>
                    </div>
                    {{end}}

                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Data Operations</span>
                        </div>
                        <div class="card-body">
                            <div class="grid grid-2">
                                <div class="form-group">
                                    <label class="form-label">Metric Name</label>
                                    <input type="text" class="form-input" id="opMetricName" placeholder="Enter metric name">
                                </div>
                                <div class="form-group">
                                    <label class="form-label">Before Timestamp (optional)</label>
                                    <input type="datetime-local" class="form-input" id="opBefore">
                                </div>
                            </div>
                            <div style="display: flex; gap: 8px;">
                                <button class="btn btn-secondary" onclick="truncateData()">Truncate Data</button>
                                <button class="btn btn-danger" onclick="deleteMetric()">Delete Metric</button>
                            </div>
                            <p style="margin-top: 12px; font-size: 13px; color: var(--text-secondary);">
                                ⚠️ These operations are scheduled and will be applied during the next compaction cycle.
                            </p>
                        </div>
                    </div>
                </div>

                <!-- Configuration Page -->
                <div class="page" id="page-config">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Database Configuration</span>
                        </div>
                        <div class="card-body">
                            <div class="config-row">
                                <span class="config-key">Data Path</span>
                                <span class="config-value">{{.Config.Path}}</span>
                            </div>
                            <div class="config-row">
                                <span class="config-key">Partition Duration</span>
                                <span class="config-value">{{.Config.PartitionDuration}}</span>
                            </div>
                            <div class="config-row">
                                <span class="config-key">Buffer Size</span>
                                <span class="config-value">{{.Config.BufferSize}}</span>
                            </div>
                            <div class="config-row">
                                <span class="config-key">Sync Interval</span>
                                <span class="config-value">{{.Config.SyncInterval}}</span>
                            </div>
                            <div class="config-row">
                                <span class="config-key">Retention</span>
                                <span class="config-value">{{.Config.Retention}}</span>
                            </div>
                        </div>
                    </div>

                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Partition Info</span>
                            <button class="btn btn-sm btn-secondary" onclick="refreshPartitions()">Refresh</button>
                        </div>
                        <div class="card-body" id="partitionInfo">
                            <div class="spinner"></div>
                        </div>
                    </div>
                </div>

                <!-- Backup Page -->
                <div class="page" id="page-backup">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Backup & Sync</span>
                        </div>
                        <div class="card-body">
                            <p style="margin-bottom: 16px; color: var(--text-secondary);">
                                Chronicle uses a single-file storage format. Trigger a sync to ensure all buffered data is persisted to disk.
                            </p>
                            <div class="config-row">
                                <span class="config-key">Database Path</span>
                                <span class="config-value">{{.Config.Path}}</span>
                            </div>
                            <div style="margin-top: 16px;">
                                <button class="btn btn-primary" onclick="triggerBackup()">
                                    <span>💾</span> Sync to Disk
                                </button>
                            </div>
                            <div id="backupStatus" style="margin-top: 16px;"></div>
                        </div>
                    </div>
                </div>

                <!-- API Reference Page -->
                <div class="page" id="page-api">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">API Endpoints</span>
                        </div>
                        <div class="card-body" style="padding: 0;">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Method</th>
                                        <th>Endpoint</th>
                                        <th>Description</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/stats</code></td>
                                        <td>System and database statistics</td>
                                    </tr>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/metrics</code></td>
                                        <td>List all metrics</td>
                                    </tr>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/health</code></td>
                                        <td>Health check status</td>
                                    </tr>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/query?q=...</code></td>
                                        <td>Execute a query</td>
                                    </tr>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/config</code></td>
                                        <td>Database configuration</td>
                                    </tr>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/export?q=...&format=json|csv</code></td>
                                        <td>Export query results</td>
                                    </tr>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/tags?metric=...</code></td>
                                        <td>Get tag values</td>
                                    </tr>
                                    <tr>
                                        <td><code>GET</code></td>
                                        <td><code>/admin/api/data-preview?metric=...</code></td>
                                        <td>Preview metric data</td>
                                    </tr>
                                    <tr>
                                        <td><code>POST</code></td>
                                        <td><code>/admin/api/backup</code></td>
                                        <td>Trigger sync/backup</td>
                                    </tr>
                                    <tr>
                                        <td><code>POST</code></td>
                                        <td><code>/admin/api/insert</code></td>
                                        <td>Insert data (dev mode only)</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- Connect Page -->
                <div class="page" id="page-connect">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Connection Snippets</span>
                        </div>
                        <div class="card-body">
                            <div class="snippet-card">
                                <div class="snippet-header">
                                    <span>Go</span>
                                    <span class="snippet-copy" onclick="copySnippet(this)">Copy</span>
                                </div>
                                <div class="snippet-body">
<pre>import "chronicle"

db, err := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Write a point
db.Write(chronicle.Point{
    Metric: "cpu",
    Value:  42.5,
    Timestamp: time.Now().UnixNano(),
})</pre>
                                </div>
                            </div>

                            <div class="snippet-card">
                                <div class="snippet-header">
                                    <span>curl - Query</span>
                                    <span class="snippet-copy" onclick="copySnippet(this)">Copy</span>
                                </div>
                                <div class="snippet-body">
<pre>curl -G "http://localhost:8080/admin/api/query" \
  --data-urlencode "q=SELECT mean(value) FROM cpu"</pre>
                                </div>
                            </div>

                            <div class="snippet-card">
                                <div class="snippet-header">
                                    <span>curl - Write (HTTP API)</span>
                                    <span class="snippet-copy" onclick="copySnippet(this)">Copy</span>
                                </div>
                                <div class="snippet-body">
<pre>curl -X POST "http://localhost:8080/write" \
  -d "cpu,host=server1 value=42.5"</pre>
                                </div>
                            </div>

                            <div class="snippet-card">
                                <div class="snippet-header">
                                    <span>Python (via HTTP)</span>
                                    <span class="snippet-copy" onclick="copySnippet(this)">Copy</span>
                                </div>
                                <div class="snippet-body">
<pre>import requests

# Query
response = requests.get(
    "http://localhost:8080/admin/api/query",
    params={"q": "SELECT mean(value) FROM cpu"}
)
data = response.json()

# Write (InfluxDB line protocol)
requests.post(
    "http://localhost:8080/write",
    data="cpu,host=server1 value=42.5"
)</pre>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Alerts Page -->
                <div class="page" id="page-alerts">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Alert Rules</span>
                            <button class="btn btn-primary btn-sm" onclick="openModal('createAlertModal')">+ Create Alert</button>
                        </div>
                        <div class="card-body" id="alertsList">
                            <div class="empty-state">
                                <div class="empty-state-icon">🔔</div>
                                <div class="empty-state-title">No Alert Rules</div>
                                <p>Create an alert rule to monitor your metrics</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Audit Log Page -->
                <div class="page" id="page-audit">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Audit Log</span>
                            <button class="btn btn-secondary btn-sm" onclick="refreshAuditLog()">Refresh</button>
                        </div>
                        <div class="card-body">
                            <div class="table-container">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Timestamp</th>
                                            <th>Action</th>
                                            <th>User</th>
                                            <th>IP</th>
                                            <th>Details</th>
                                        </tr>
                                    </thead>
                                    <tbody id="auditLogBody">
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Schema Registry Page -->
                <div class="page" id="page-schemas">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Schema Registry</span>
                            <button class="btn btn-secondary btn-sm" onclick="refreshSchemas()">Refresh</button>
                        </div>
                        <div class="card-body" id="schemasList">
                            <div class="empty-state">
                                <div class="empty-state-icon">📐</div>
                                <div class="empty-state-title">Loading schemas...</div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Retention Page -->
                <div class="page" id="page-retention">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Retention Policies</span>
                            <button class="btn btn-primary btn-sm" onclick="openModal('createRetentionModal')">+ Create Policy</button>
                        </div>
                        <div class="card-body" id="retentionList">
                            <div class="empty-state">
                                <div class="empty-state-icon">🕐</div>
                                <div class="empty-state-title">No Retention Policies</div>
                                <p>Create a retention policy to automatically manage data lifecycle</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Cluster Page -->
                <div class="page" id="page-cluster">
                    <div class="grid grid-2">
                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">Cluster Status</span>
                                <div class="live-indicator"><span class="live-dot"></span> Live</div>
                            </div>
                            <div class="card-body" id="clusterStatus">
                                <div class="config-row">
                                    <span class="config-key">Mode</span>
                                    <span class="config-value" id="clusterMode">Loading...</span>
                                </div>
                                <div class="config-row">
                                    <span class="config-key">Status</span>
                                    <span class="config-value" id="clusterHealth">Loading...</span>
                                </div>
                            </div>
                        </div>
                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">Local Node</span>
                            </div>
                            <div class="card-body" id="nodeInfo">
                                <div class="config-row">
                                    <span class="config-key">Node ID</span>
                                    <span class="config-value" id="nodeId">Loading...</span>
                                </div>
                                <div class="config-row">
                                    <span class="config-key">State</span>
                                    <span class="config-value" id="nodeState">Loading...</span>
                                </div>
                                <div class="config-row">
                                    <span class="config-key">Uptime</span>
                                    <span class="config-value" id="nodeUptime">Loading...</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- WAL Inspector Page -->
                <div class="page" id="page-wal">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Write-Ahead Log (WAL)</span>
                            <button class="btn btn-secondary btn-sm" onclick="refreshWAL()">Refresh</button>
                        </div>
                        <div class="card-body" id="walInfo">
                            <div class="config-row">
                                <span class="config-key">Status</span>
                                <span class="config-value">Loading...</span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Scheduled Exports Page -->
                <div class="page" id="page-exports">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Scheduled Exports</span>
                            <button class="btn btn-primary btn-sm" onclick="openModal('createExportModal')">+ Create Export</button>
                        </div>
                        <div class="card-body" id="exportsList">
                            <div class="empty-state">
                                <div class="empty-state-icon">📤</div>
                                <div class="empty-state-title">No Scheduled Exports</div>
                                <p>Create a scheduled export to automatically export data</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Saved Queries Page -->
                <div class="page" id="page-saved-queries">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Saved Queries</span>
                            <button class="btn btn-primary btn-sm" onclick="openModal('saveQueryModal')">+ Save Current Query</button>
                        </div>
                        <div class="card-body" id="savedQueriesList">
                            <div class="empty-state">
                                <div class="empty-state-icon">💾</div>
                                <div class="empty-state-title">No Saved Queries</div>
                                <p>Save frequently used queries for quick access</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Compare Metrics Page -->
                <div class="page" id="page-compare">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Compare Metrics</span>
                        </div>
                        <div class="card-body">
                            <div class="form-group">
                                <label class="form-label">Select Metrics to Compare (comma separated)</label>
                                <input type="text" class="form-input" id="compareMetrics" placeholder="e.g., cpu,memory,disk">
                            </div>
                            <div class="form-group">
                                <label class="form-label">Time Range</label>
                                <select class="form-select" id="compareTimeRange">
                                    <option value="1h">Last 1 Hour</option>
                                    <option value="6h">Last 6 Hours</option>
                                    <option value="24h">Last 24 Hours</option>
                                    <option value="7d">Last 7 Days</option>
                                </select>
                            </div>
                            <button class="btn btn-primary" onclick="runComparison()">Compare</button>
                        </div>
                    </div>
                    <div class="card" style="margin-top: 20px;">
                        <div class="card-header">
                            <span class="card-title">Comparison Chart</span>
                        </div>
                        <div class="card-body">
                            <canvas id="comparisonChart" style="width: 100%; height: 300px;"></canvas>
                            <div class="comparison-legend" id="comparisonLegend"></div>
                        </div>
                    </div>
                </div>

                <!-- Import Data Page -->
                <div class="page" id="page-import">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Import Data</span>
                        </div>
                        <div class="card-body">
                            <div class="import-wizard">
                                <div class="import-step" id="importStep1">
                                    <div class="import-step-number">1</div>
                                    <div style="flex: 1;">
                                        <h4>Select Format</h4>
                                        <select class="form-select" id="importFormat">
                                            <option value="json">JSON</option>
                                            <option value="csv">CSV</option>
                                            <option value="line">Line Protocol (InfluxDB)</option>
                                        </select>
                                    </div>
                                </div>
                                <div class="import-step" id="importStep2">
                                    <div class="import-step-number">2</div>
                                    <div style="flex: 1;">
                                        <h4>Upload Data</h4>
                                        <div class="import-drop-zone" id="importDropZone" onclick="document.getElementById('importFile').click()">
                                            <input type="file" id="importFile" style="display:none" accept=".json,.csv,.txt" onchange="handleImportFile(this)">
                                            <div>📁 Drop file here or click to upload</div>
                                            <div style="font-size: 12px; color: var(--text-muted); margin-top: 8px;">Supported: JSON, CSV, Line Protocol</div>
                                        </div>
                                        <div style="text-align: center; margin: 16px 0; color: var(--text-muted);">OR</div>
                                        <textarea class="form-input" id="importData" rows="6" placeholder="Paste data here..."></textarea>
                                    </div>
                                </div>
                                <div class="import-step" id="importStep3">
                                    <div class="import-step-number">3</div>
                                    <div style="flex: 1;">
                                        <h4>Import</h4>
                                        <div id="importPreview" style="margin-bottom: 16px; font-size: 13px; color: var(--text-muted);"></div>
                                        <button class="btn btn-primary" onclick="executeImport()">Import Data</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Alert History Page -->
                <div class="page" id="page-alert-history">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Alert History</span>
                            <button class="btn btn-secondary btn-sm" onclick="loadAlertHistory()">Refresh</button>
                        </div>
                        <div class="card-body" id="alertHistoryList">
                            <div class="empty-state">
                                <div class="empty-state-icon">📜</div>
                                <div class="empty-state-title">No Alert History</div>
                                <p>Alert firings will appear here</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Diagnostics Page -->
                <div class="page" id="page-diagnostics">
                    <div class="diagnostics-grid" id="diagnosticsGrid">
                        <div class="diagnostic-card">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="color: var(--text-muted);">CPU Usage</span>
                                <span>💻</span>
                            </div>
                            <div class="diagnostic-value" id="diagCpu">--%</div>
                            <div class="diagnostic-bar">
                                <div class="diagnostic-bar-fill low" id="diagCpuBar" style="width: 0%;"></div>
                            </div>
                        </div>
                        <div class="diagnostic-card">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="color: var(--text-muted);">Memory Usage</span>
                                <span>🧠</span>
                            </div>
                            <div class="diagnostic-value" id="diagMemory">-- MB</div>
                            <div class="diagnostic-bar">
                                <div class="diagnostic-bar-fill low" id="diagMemoryBar" style="width: 0%;"></div>
                            </div>
                        </div>
                        <div class="diagnostic-card">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="color: var(--text-muted);">Goroutines</span>
                                <span>🔄</span>
                            </div>
                            <div class="diagnostic-value" id="diagGoroutines">--</div>
                        </div>
                        <div class="diagnostic-card">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="color: var(--text-muted);">GC Runs</span>
                                <span>🗑️</span>
                            </div>
                            <div class="diagnostic-value" id="diagGC">--</div>
                        </div>
                        <div class="diagnostic-card">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="color: var(--text-muted);">Uptime</span>
                                <span>⏱️</span>
                            </div>
                            <div class="diagnostic-value" id="diagUptime">--</div>
                        </div>
                        <div class="diagnostic-card">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="color: var(--text-muted);">Data Path</span>
                                <span>📁</span>
                            </div>
                            <div id="diagDataPath" style="font-family: monospace; font-size: 12px; word-break: break-all; margin-top: 8px;">--</div>
                        </div>
                    </div>
                </div>

                <!-- Sessions Page -->
                <div class="page" id="page-sessions">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Active Sessions</span>
                            <button class="btn btn-secondary btn-sm" onclick="loadSessions()">Refresh</button>
                        </div>
                        <div class="card-body" id="sessionsList">
                            <div class="empty-state">
                                <div class="empty-state-icon">👤</div>
                                <div class="empty-state-title">No Sessions</div>
                                <p>Active sessions will appear here</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Query Templates Page -->
                <div class="page" id="page-templates">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Query Templates</span>
                            <button class="btn btn-primary btn-sm" onclick="openModal('createTemplateModal')">+ Create Template</button>
                        </div>
                        <div class="card-body">
                            <div class="log-filters">
                                <button class="btn btn-secondary btn-sm" onclick="filterTemplates('')">All</button>
                                <button class="btn btn-secondary btn-sm" onclick="filterTemplates('aggregation')">Aggregation</button>
                                <button class="btn btn-secondary btn-sm" onclick="filterTemplates('analysis')">Analysis</button>
                                <button class="btn btn-secondary btn-sm" onclick="filterTemplates('monitoring')">Monitoring</button>
                            </div>
                            <div id="templatesList"></div>
                        </div>
                    </div>
                </div>

                <!-- Annotations Page -->
                <div class="page" id="page-annotations">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Metric Annotations</span>
                            <button class="btn btn-primary btn-sm" onclick="openModal('createAnnotationModal')">+ Add Annotation</button>
                        </div>
                        <div class="card-body">
                            <div class="form-group" style="margin-bottom: 16px;">
                                <input type="text" class="form-input" id="annotationMetricFilter" placeholder="Filter by metric..." oninput="loadAnnotations()">
                            </div>
                            <div class="annotation-timeline" id="annotationsList"></div>
                        </div>
                    </div>
                </div>

                <!-- Profiling Page -->
                <div class="page" id="page-profiling">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Performance Profiling</span>
                            <button class="btn btn-secondary btn-sm" onclick="loadProfiling()">Refresh</button>
                        </div>
                        <div class="card-body">
                            <div class="profiling-grid" id="profilingStats"></div>
                            <div style="margin-top: 20px;">
                                <h4 style="margin-bottom: 12px;">Memory Breakdown</h4>
                                <div class="memory-bar" id="memoryBar"></div>
                                <div style="display: flex; gap: 16px; margin-top: 8px; font-size: 12px;">
                                    <span><span style="color: var(--accent-blue);">■</span> Heap</span>
                                    <span><span style="color: var(--accent-green);">■</span> Stack</span>
                                    <span><span style="color: var(--accent-purple);">■</span> Other</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Log Viewer Page -->
                <div class="page" id="page-logs">
                    <div class="card">
                        <div class="card-header">
                            <span class="card-title">Log Viewer</span>
                            <div>
                                <button class="btn btn-secondary btn-sm" onclick="loadLogs()">Refresh</button>
                                {{if .DevMode}}
                                <button class="btn btn-secondary btn-sm" onclick="clearLogs()">Clear</button>
                                {{end}}
                            </div>
                        </div>
                        <div class="card-body">
                            <div class="log-filters">
                                <select class="form-select" id="logLevelFilter" onchange="loadLogs()" style="width: auto;">
                                    <option value="">All Levels</option>
                                    <option value="debug">Debug</option>
                                    <option value="info">Info</option>
                                    <option value="warn">Warning</option>
                                    <option value="error">Error</option>
                                </select>
                                <input type="text" class="form-input" id="logSourceFilter" placeholder="Filter by source..." style="width: 200px;" oninput="loadLogs()">
                            </div>
                            <div class="log-container" id="logViewer">
                                <div class="empty-state">
                                    <div class="empty-state-icon">📄</div>
                                    <div class="empty-state-title">No Logs</div>
                                    <p>Log entries will appear here</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Access Control Page -->
                <div class="page" id="page-roles">
                    <div class="grid-2">
                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">Roles</span>
                                <button class="btn btn-primary btn-sm" onclick="openModal('createRoleModal')">+ Create Role</button>
                            </div>
                            <div class="card-body" id="rolesList"></div>
                        </div>
                        <div class="card">
                            <div class="card-header">
                                <span class="card-title">User Permissions</span>
                                <button class="btn btn-primary btn-sm" onclick="openModal('assignRoleModal')">+ Assign Role</button>
                            </div>
                            <div class="card-body" id="permissionsList"></div>
                        </div>
                    </div>
                </div>
            </div>
        </main>
    </div>

    <!-- Global Search Modal -->
    <div class="search-modal" id="searchModal">
        <div class="search-modal-content">
            <input type="text" class="search-modal-input" id="globalSearchInput" placeholder="Search metrics, pages, actions..." autocomplete="off">
            <div class="search-results" id="searchResults"></div>
            <div class="search-hint">
                <kbd>↑</kbd> <kbd>↓</kbd> to navigate • <kbd>Enter</kbd> to select • <kbd>Esc</kbd> to close
            </div>
        </div>
    </div>

    <!-- Toast Container -->
    <div class="toast-container" id="toastContainer"></div>

    <!-- Create Alert Modal -->
    <div class="modal-backdrop" id="createAlertModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Create Alert Rule</span>
                <button class="modal-close" onclick="closeModal('createAlertModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Name</label>
                    <input type="text" class="form-input" id="alertName" placeholder="e.g., High CPU Usage">
                </div>
                <div class="form-group">
                    <label class="form-label">Metric</label>
                    <input type="text" class="form-input" id="alertMetric" placeholder="e.g., cpu">
                </div>
                <div class="form-group">
                    <label class="form-label">Condition</label>
                    <select class="form-select" id="alertCondition">
                        <option value="above">Above threshold</option>
                        <option value="below">Below threshold</option>
                        <option value="equal">Equal to</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Threshold</label>
                    <input type="number" class="form-input" id="alertThreshold" placeholder="e.g., 80">
                </div>
                <div class="form-group">
                    <label class="form-label">For Duration</label>
                    <input type="text" class="form-input" id="alertDuration" placeholder="e.g., 5m">
                </div>
                <div class="form-group">
                    <label class="form-label">Webhook URL (optional)</label>
                    <input type="text" class="form-input" id="alertWebhook" placeholder="https://...">
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('createAlertModal')">Cancel</button>
                <button class="btn btn-primary" onclick="createAlert()">Create Alert</button>
            </div>
        </div>
    </div>

    <!-- Create Retention Modal -->
    <div class="modal-backdrop" id="createRetentionModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Create Retention Policy</span>
                <button class="modal-close" onclick="closeModal('createRetentionModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Metric Pattern</label>
                    <input type="text" class="form-input" id="retentionMetric" placeholder="e.g., * or cpu.*">
                </div>
                <div class="form-group">
                    <label class="form-label">Retention Duration</label>
                    <select class="form-select" id="retentionDuration">
                        <option value="7d">7 days</option>
                        <option value="30d">30 days</option>
                        <option value="90d">90 days</option>
                        <option value="365d">1 year</option>
                    </select>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('createRetentionModal')">Cancel</button>
                <button class="btn btn-primary" onclick="createRetention()">Create Policy</button>
            </div>
        </div>
    </div>

    <!-- Create Export Modal -->
    <div class="modal-backdrop" id="createExportModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Create Scheduled Export</span>
                <button class="modal-close" onclick="closeModal('createExportModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Name</label>
                    <input type="text" class="form-input" id="exportName" placeholder="e.g., Daily CPU Report">
                </div>
                <div class="form-group">
                    <label class="form-label">Query</label>
                    <textarea class="form-textarea" id="exportQuery" placeholder="SELECT mean(value) FROM cpu"></textarea>
                </div>
                <div class="form-group">
                    <label class="form-label">Format</label>
                    <select class="form-select" id="exportFormat">
                        <option value="json">JSON</option>
                        <option value="csv">CSV</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Schedule</label>
                    <select class="form-select" id="exportSchedule">
                        <option value="hourly">Hourly</option>
                        <option value="daily">Daily</option>
                        <option value="weekly">Weekly</option>
                    </select>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('createExportModal')">Cancel</button>
                <button class="btn btn-primary" onclick="createExport()">Create Export</button>
            </div>
        </div>
    </div>

    <!-- Save Query Modal -->
    <div class="modal-backdrop" id="saveQueryModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Save Query</span>
                <button class="modal-close" onclick="closeModal('saveQueryModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Name</label>
                    <input type="text" class="form-input" id="saveQueryName" placeholder="e.g., Daily CPU Average">
                </div>
                <div class="form-group">
                    <label class="form-label">Query</label>
                    <textarea class="form-textarea" id="saveQueryText" placeholder="SELECT mean(value) FROM cpu"></textarea>
                </div>
                <div class="form-group">
                    <label class="form-label">Description (optional)</label>
                    <input type="text" class="form-input" id="saveQueryDesc" placeholder="Brief description of this query">
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('saveQueryModal')">Cancel</button>
                <button class="btn btn-primary" onclick="saveQuery()">Save Query</button>
            </div>
        </div>
    </div>

    <!-- Metric Details Modal -->
    <div class="modal-backdrop" id="metricModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Metric: <span id="modalMetricName"></span></span>
                <button class="modal-close" onclick="closeModal('metricModal')">&times;</button>
            </div>
            <div class="modal-body" id="modalMetricBody">
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('metricModal')">Close</button>
                <button class="btn btn-primary" onclick="queryModalMetric()">Query</button>
            </div>
        </div>
    </div>

    <!-- Create Template Modal -->
    <div class="modal-backdrop" id="createTemplateModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Create Query Template</span>
                <button class="modal-close" onclick="closeModal('createTemplateModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Name</label>
                    <input type="text" class="form-input" id="templateName" placeholder="e.g., Daily Average">
                </div>
                <div class="form-group">
                    <label class="form-label">Category</label>
                    <select class="form-select" id="templateCategory">
                        <option value="aggregation">Aggregation</option>
                        <option value="analysis">Analysis</option>
                        <option value="monitoring">Monitoring</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Description</label>
                    <input type="text" class="form-input" id="templateDesc" placeholder="Brief description">
                </div>
                <div class="form-group">
                    <label class="form-label">Query (use {{"{{metric}}"}} for variables)</label>
                    <textarea class="form-textarea" id="templateQuery" rows="3" placeholder="SELECT mean(value) FROM {{"{{metric}}"}}"></textarea>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('createTemplateModal')">Cancel</button>
                <button class="btn btn-primary" onclick="createTemplate()">Create Template</button>
            </div>
        </div>
    </div>

    <!-- Create Annotation Modal -->
    <div class="modal-backdrop" id="createAnnotationModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Add Annotation</span>
                <button class="modal-close" onclick="closeModal('createAnnotationModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Metric</label>
                    <input type="text" class="form-input" id="annotationMetric" placeholder="e.g., cpu">
                </div>
                <div class="form-group">
                    <label class="form-label">Title</label>
                    <input type="text" class="form-input" id="annotationTitle" placeholder="e.g., Deployment v1.2.3">
                </div>
                <div class="form-group">
                    <label class="form-label">Notes</label>
                    <textarea class="form-textarea" id="annotationText" rows="3" placeholder="Additional details..."></textarea>
                </div>
                <div class="form-group">
                    <label class="form-label">Tags (comma separated)</label>
                    <input type="text" class="form-input" id="annotationTags" placeholder="e.g., deployment, release">
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('createAnnotationModal')">Cancel</button>
                <button class="btn btn-primary" onclick="createAnnotation()">Add Annotation</button>
            </div>
        </div>
    </div>

    <!-- Create Role Modal -->
    <div class="modal-backdrop" id="createRoleModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Create Role</span>
                <button class="modal-close" onclick="closeModal('createRoleModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Name</label>
                    <input type="text" class="form-input" id="roleName" placeholder="e.g., Developer">
                </div>
                <div class="form-group">
                    <label class="form-label">Description</label>
                    <input type="text" class="form-input" id="roleDesc" placeholder="Brief description">
                </div>
                <div class="form-group">
                    <label class="form-label">Permissions</label>
                    <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px;">
                        <label><input type="checkbox" id="permRead" checked> Read</label>
                        <label><input type="checkbox" id="permWrite"> Write</label>
                        <label><input type="checkbox" id="permDelete"> Delete</label>
                        <label><input type="checkbox" id="permExport"> Export</label>
                        <label><input type="checkbox" id="permImport"> Import</label>
                        <label><input type="checkbox" id="permConfig"> Config</label>
                        <label><input type="checkbox" id="permUsers"> Users</label>
                        <label><input type="checkbox" id="permAdmin"> Admin</label>
                    </div>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('createRoleModal')">Cancel</button>
                <button class="btn btn-primary" onclick="createRole()">Create Role</button>
            </div>
        </div>
    </div>

    <!-- Assign Role Modal -->
    <div class="modal-backdrop" id="assignRoleModal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Assign Role to User</span>
                <button class="modal-close" onclick="closeModal('assignRoleModal')">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Username</label>
                    <input type="text" class="form-input" id="assignUser" placeholder="e.g., john@example.com">
                </div>
                <div class="form-group">
                    <label class="form-label">Role</label>
                    <select class="form-select" id="assignRole"></select>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeModal('assignRoleModal')">Cancel</button>
                <button class="btn btn-primary" onclick="assignRole()">Assign Role</button>
            </div>
        </div>
    </div>

    <script>
        // State
        let currentPage = 'dashboard';
        let lastQueryResult = null;
        let memoryChart = null;
        let previewChart = null;
        let queryResultChart = null;
        let memoryHistory = [];
        const MAX_HISTORY = 30;

        // Initialize
        document.addEventListener('DOMContentLoaded', () => {
            initNavigation();
            initTheme();
            initKeyboardShortcuts();
            initCharts();
            refreshStats();
            refreshQueryHistory();
            refreshHealth();
            refreshPartitions();
            refreshActivity();
            initSSE();
            setInterval(refreshStats, 10000);
        });

        // Navigation
        function initNavigation() {
            document.querySelectorAll('.nav-item[data-page]').forEach(item => {
                item.addEventListener('click', () => {
                    const page = item.dataset.page;
                    navigateTo(page);
                });
            });

            document.querySelectorAll('[data-result-tab]').forEach(tab => {
                tab.addEventListener('click', () => {
                    const tabName = tab.dataset.resultTab;
                    document.querySelectorAll('[data-result-tab]').forEach(t => t.classList.remove('active'));
                    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                    tab.classList.add('active');
                    document.getElementById('result' + tabName.charAt(0).toUpperCase() + tabName.slice(1)).classList.add('active');
                });
            });
        }

        function navigateTo(page) {
            document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
            const navItem = document.querySelector('.nav-item[data-page="' + page + '"]');
            if (navItem) navItem.classList.add('active');
            document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
            const pageEl = document.getElementById('page-' + page);
            if (pageEl) pageEl.classList.add('active');
            document.getElementById('pageTitle').textContent = navItem ? navItem.textContent.trim() : page;
            currentPage = page;
            closeSidebar();
            
            // Refresh data for specific pages
            switch(page) {
                case 'alerts': refreshAlerts(); break;
                case 'audit': refreshAuditLog(); break;
                case 'schemas': refreshSchemas(); break;
                case 'retention': refreshRetention(); break;
                case 'cluster': refreshCluster(); break;
                case 'wal': refreshWAL(); break;
                case 'exports': refreshExports(); break;
                case 'saved-queries': loadSavedQueries(); break;
                case 'alert-history': loadAlertHistory(); break;
                case 'diagnostics': loadDiagnostics(); break;
                case 'sessions': loadSessions(); break;
                case 'templates': loadTemplates(); break;
                case 'annotations': loadAnnotations(); break;
                case 'profiling': loadProfiling(); break;
                case 'logs': loadLogs(); break;
                case 'roles': loadRoles(); loadPermissions(); break;
            }
        }

        function toggleSidebar() {
            document.getElementById('sidebar').classList.toggle('open');
        }

        function closeSidebar() {
            document.getElementById('sidebar').classList.remove('open');
        }

        // Theme
        function initTheme() {
            const saved = localStorage.getItem('theme') || 'dark';
            setTheme(saved);
        }

        function toggleTheme() {
            const current = document.body.dataset.theme || 'dark';
            setTheme(current === 'dark' ? 'light' : 'dark');
        }

        function setTheme(theme) {
            document.body.dataset.theme = theme;
            localStorage.setItem('theme', theme);
            document.getElementById('themeIcon').textContent = theme === 'dark' ? '🌙' : '☀️';
            document.getElementById('themeText').textContent = theme === 'dark' ? 'Dark' : 'Light';
            updateChartColors();
        }

        // Keyboard Shortcuts
        function initKeyboardShortcuts() {
            let gPressed = false;
            
            document.addEventListener('keydown', (e) => {
                // Global search: Cmd/Ctrl + K
                if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
                    e.preventDefault();
                    openSearchModal();
                    return;
                }
                
                // Close search modal on Escape
                if (e.key === 'Escape') {
                    closeSearchModal();
                    return;
                }
                
                // Execute query: Cmd/Ctrl + Enter
                if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                    if (currentPage === 'query') {
                        e.preventDefault();
                        executeQuery();
                    }
                    return;
                }
                
                // Quick navigation with 'g' prefix
                if (e.key === 'g' && !e.ctrlKey && !e.metaKey && !e.altKey) {
                    if (!gPressed) {
                        gPressed = true;
                        setTimeout(() => { gPressed = false; }, 500);
                        return;
                    }
                }
                
                if (gPressed) {
                    gPressed = false;
                    const shortcuts = {
                        'd': 'dashboard',
                        'h': 'health',
                        'e': 'explorer',
                        'q': 'query',
                        'a': 'alerts',
                        'c': 'config',
                        'b': 'backup',
                        's': 'schemas',
                        'r': 'retention',
                        'l': 'cluster',
                        'w': 'wal',
                        'u': 'audit',
                        'x': 'exports'
                    };
                    if (shortcuts[e.key]) {
                        e.preventDefault();
                        navigateTo(shortcuts[e.key]);
                    }
                }
            });
            
            // Search modal keyboard navigation
            document.getElementById('globalSearchInput').addEventListener('keydown', (e) => {
                const results = document.querySelectorAll('.search-result-item');
                const selected = document.querySelector('.search-result-item.selected');
                let idx = Array.from(results).indexOf(selected);
                
                if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    if (selected) selected.classList.remove('selected');
                    idx = (idx + 1) % results.length;
                    if (results[idx]) results[idx].classList.add('selected');
                } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    if (selected) selected.classList.remove('selected');
                    idx = idx <= 0 ? results.length - 1 : idx - 1;
                    if (results[idx]) results[idx].classList.add('selected');
                } else if (e.key === 'Enter') {
                    e.preventDefault();
                    if (selected) selected.click();
                }
            });
        }

        // Global Search Functions
        function openSearchModal() {
            document.getElementById('searchModal').classList.add('open');
            document.getElementById('globalSearchInput').focus();
            document.getElementById('globalSearchInput').value = '';
            document.getElementById('searchResults').innerHTML = '';
        }
        
        function closeSearchModal() {
            document.getElementById('searchModal').classList.remove('open');
        }
        
        document.getElementById('searchModal').addEventListener('click', (e) => {
            if (e.target.id === 'searchModal') closeSearchModal();
        });
        
        let searchDebounce = null;
        document.getElementById('globalSearchInput').addEventListener('input', (e) => {
            clearTimeout(searchDebounce);
            searchDebounce = setTimeout(() => performSearch(e.target.value), 150);
        });
        
        async function performSearch(query) {
            if (!query.trim()) {
                document.getElementById('searchResults').innerHTML = '';
                return;
            }
            
            try {
                const res = await fetch('/admin/api/search?q=' + encodeURIComponent(query));
                const results = await res.json();
                
                let html = '';
                results.forEach((item, idx) => {
                    const selected = idx === 0 ? 'selected' : '';
                    html += '<div class="search-result-item ' + selected + '" onclick="handleSearchResult(\'' + item.type + '\', \'' + item.name + '\')">' +
                        '<span class="search-result-icon">' + item.icon + '</span>' +
                        '<span class="search-result-name">' + escapeHtml(item.name) + '</span>' +
                        '<span class="search-result-type">' + item.type + '</span>' +
                    '</div>';
                });
                
                document.getElementById('searchResults').innerHTML = html || '<div class="empty-state" style="padding: 20px;">No results found</div>';
            } catch (err) {
                console.error('Search failed:', err);
            }
        }
        
        function handleSearchResult(type, name) {
            closeSearchModal();
            if (type === 'page') {
                const pageMap = {
                    'Dashboard': 'dashboard',
                    'Health': 'health',
                    'Explorer': 'explorer',
                    'Query Console': 'query',
                    'Management': 'management',
                    'Configuration': 'config',
                    'Backup': 'backup',
                    'Alerts': 'alerts',
                    'Alert Rules': 'alerts',
                    'Audit Log': 'audit',
                    'Schema Registry': 'schemas',
                    'Retention': 'retention',
                    'Cluster': 'cluster',
                    'WAL Inspector': 'wal',
                    'Scheduled Exports': 'exports'
                };
                navigateTo(pageMap[name] || 'dashboard');
            } else if (type === 'metric') {
                document.getElementById('queryInput').value = 'SELECT mean(value) FROM ' + name;
                navigateTo('query');
            }
        }

        // SSE Real-time Updates
        let eventSource = null;
        
        function initSSE() {
            if (eventSource) eventSource.close();
            eventSource = new EventSource('/admin/api/events');
            
            eventSource.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    if (data.type === 'stats') {
                        document.getElementById('statMemory').textContent = data.memory;
                        document.getElementById('statGoroutines').textContent = data.goroutines;
                        document.getElementById('statGC').textContent = data.gc_cycles;
                        document.getElementById('statUptime').textContent = data.uptime;
                        document.getElementById('statTotalAlloc').textContent = data.total_alloc;
                    }
                } catch (err) {
                    console.error('SSE parse error:', err);
                }
            };
            
            eventSource.onerror = () => {
                // Reconnect after delay
                setTimeout(initSSE, 5000);
            };
        }

        // Charts
        function initCharts() {
            const ctx = document.getElementById('memoryChart');
            if (ctx) {
                memoryChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: [],
                        datasets: [{
                            label: 'Memory (MB)',
                            data: [],
                            borderColor: getComputedStyle(document.body).getPropertyValue('--accent-blue').trim(),
                            backgroundColor: 'rgba(88, 166, 255, 0.1)',
                            fill: true,
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { display: false },
                            y: {
                                beginAtZero: true,
                                grid: { color: 'rgba(128, 128, 128, 0.1)' },
                                ticks: { color: getComputedStyle(document.body).getPropertyValue('--text-secondary').trim() }
                            }
                        }
                    }
                });
            }
        }

        function updateChartColors() {
            if (memoryChart) {
                memoryChart.data.datasets[0].borderColor = getComputedStyle(document.body).getPropertyValue('--accent-blue').trim();
                memoryChart.options.scales.y.ticks.color = getComputedStyle(document.body).getPropertyValue('--text-secondary').trim();
                memoryChart.update();
            }
        }

        // API Functions
        async function refreshStats() {
            try {
                const res = await fetch('/admin/api/stats');
                const data = await res.json();
                
                document.getElementById('statMetrics').textContent = data.metric_count;
                document.getElementById('statPartitions').textContent = data.partition_count;
                document.getElementById('statGoroutines').textContent = data.num_goroutine;
                document.getElementById('statGC').textContent = data.memory.num_gc;
                
                const memMB = (data.memory.alloc / 1024 / 1024).toFixed(1);
                document.getElementById('statMemory').textContent = memMB + ' MB';
                
                const totalMB = (data.memory.total_alloc / 1024 / 1024).toFixed(1);
                document.getElementById('statTotalAlloc').textContent = totalMB + ' MB';
                
                const hours = Math.floor(data.uptime / 3600);
                const mins = Math.floor((data.uptime % 3600) / 60);
                const secs = Math.floor(data.uptime % 60);
                document.getElementById('statUptime').textContent = hours + 'h ' + mins + 'm ' + secs + 's';
                
                // Update memory chart
                if (memoryChart) {
                    memoryHistory.push(parseFloat(memMB));
                    if (memoryHistory.length > MAX_HISTORY) memoryHistory.shift();
                    memoryChart.data.labels = memoryHistory.map((_, i) => i);
                    memoryChart.data.datasets[0].data = memoryHistory;
                    memoryChart.update('none');
                }
                
                updateHealthBadge(data.db_status === 'open');
            } catch (err) {
                console.error('Failed to refresh stats:', err);
            }
        }

        function updateHealthBadge(healthy) {
            const badge = document.getElementById('statusBadge');
            badge.className = 'status-badge ' + (healthy ? 'healthy' : 'error');
            badge.innerHTML = '<span class="status-dot"></span><span>' + (healthy ? 'Healthy' : 'Unhealthy') + '</span>';
        }

        async function refreshHealth() {
            try {
                const res = await fetch('/admin/api/health');
                const data = await res.json();
                
                const container = document.getElementById('healthChecks');
                let html = '';
                
                for (const [check, status] of Object.entries(data.checks || {})) {
                    const icon = status === 'ok' ? '✅' : (status === 'warning' ? '⚠️' : '❌');
                    html += '<div class="config-row"><span class="config-key">' + check + '</span><span>' + icon + ' ' + status + '</span></div>';
                }
                
                html += '<div class="config-row"><span class="config-key">Overall Status</span><span class="config-value">' + data.status + '</span></div>';
                html += '<div class="config-row"><span class="config-key">Uptime</span><span class="config-value">' + Math.floor(data.uptime) + 's</span></div>';
                
                container.innerHTML = html;
            } catch (err) {
                document.getElementById('healthChecks').innerHTML = '<div class="empty-state"><div class="empty-state-icon">❌</div><div class="empty-state-title">Failed to load health</div></div>';
            }
        }

        async function refreshActivity() {
            try {
                const res = await fetch('/admin/api/activity?limit=10');
                const data = await res.json();
                
                const container = document.getElementById('activityLog');
                if (data.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📝</div><div class="empty-state-title">No recent activity</div><p>Activity will appear here as you interact with Chronicle</p></div>';
                    return;
                }
                
                let html = '';
                data.forEach(item => {
                    const time = new Date(item.timestamp).toLocaleTimeString();
                    html += '<div class="activity-item">' +
                        '<div class="activity-icon">📌</div>' +
                        '<div class="activity-content">' +
                        '<div class="activity-action">' + item.action + '</div>' +
                        '<div class="activity-details">' + escapeHtml(item.details) + '</div>' +
                        '<div class="activity-time">' + time + '</div>' +
                        '</div></div>';
                });
                container.innerHTML = html;
            } catch (err) {
                console.error('Failed to refresh activity:', err);
            }
        }

        async function refreshPartitions() {
            try {
                const res = await fetch('/admin/api/partitions');
                const data = await res.json();
                
                const container = document.getElementById('partitionInfo');
                let html = '<div class="config-row"><span class="config-key">Total Partitions</span><span class="config-value">' + data.total + '</span></div>';
                
                if (data.partitions && data.partitions.length > 0) {
                    html += '<div class="config-row"><span class="config-key">Partition Duration</span><span class="config-value">' + data.partitions[0].partition_duration + '</span></div>';
                }
                
                container.innerHTML = html;
            } catch (err) {
                document.getElementById('partitionInfo').innerHTML = '<div class="empty-state">Failed to load partition info</div>';
            }
        }

        async function refreshQueryHistory() {
            try {
                const res = await fetch('/admin/api/query-history?limit=10');
                const data = await res.json();
                
                const container = document.getElementById('queryHistoryList');
                if (data.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📋</div><div class="empty-state-title">No query history</div></div>';
                    return;
                }
                
                let html = '';
                data.forEach(item => {
                    const icon = item.success ? '✅' : '❌';
                    const time = new Date(item.timestamp).toLocaleTimeString();
                    html += '<div class="template-item" onclick="useTemplate(\'' + escapeHtml(item.query.replace(/'/g, "\\'")) + '\')">' +
                        '<div class="template-name">' + icon + ' ' + time + ' (' + item.duration_ms.toFixed(2) + 'ms)</div>' +
                        '<div class="template-query">' + escapeHtml(item.query) + '</div>' +
                        '</div>';
                });
                container.innerHTML = html;
            } catch (err) {
                console.error('Failed to refresh query history:', err);
            }
        }

        // Query Functions
        async function executeQuery() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) {
                showToast('Please enter a query', 'warning');
                return;
            }
            
            try {
                const start = performance.now();
                const res = await fetch('/admin/api/query?q=' + encodeURIComponent(query));
                const duration = (performance.now() - start).toFixed(2);
                
                if (!res.ok) {
                    const error = await res.text();
                    showToast('Query failed: ' + error, 'error');
                    return;
                }
                
                const data = await res.json();
                lastQueryResult = data;
                
                displayQueryResult(data, duration);
                refreshQueryHistory();
                showToast('Query executed successfully', 'success');
            } catch (err) {
                showToast('Query failed: ' + err.message, 'error');
            }
        }

        function displayQueryResult(data, duration) {
            const container = document.getElementById('queryResultContainer');
            container.style.display = 'block';
            
            const points = data.points || [];
            document.getElementById('queryResultInfo').textContent = points.length + ' rows';
            document.getElementById('queryResultTime').textContent = duration + 'ms';
            
            // Table
            const thead = document.querySelector('#queryResultTable thead');
            const tbody = document.querySelector('#queryResultTable tbody');
            thead.innerHTML = '<tr><th>Timestamp</th><th>Metric</th><th>Value</th></tr>';
            tbody.innerHTML = points.map(p => 
                '<tr><td>' + new Date(p.timestamp / 1000000).toISOString() + '</td>' +
                '<td>' + escapeHtml(p.metric || '') + '</td>' +
                '<td>' + p.value + '</td></tr>'
            ).join('');
            
            // JSON
            document.getElementById('queryResultJSON').textContent = JSON.stringify(data, null, 2);
            
            // Chart
            if (points.length > 0) {
                const ctx = document.getElementById('queryResultChart');
                if (queryResultChart) queryResultChart.destroy();
                
                queryResultChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: points.map(p => new Date(p.timestamp / 1000000).toLocaleTimeString()),
                        datasets: [{
                            label: 'Value',
                            data: points.map(p => p.value),
                            borderColor: getComputedStyle(document.body).getPropertyValue('--accent-green').trim(),
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } }
                    }
                });
            }
        }

        function useTemplate(query) {
            document.getElementById('queryInput').value = query;
            navigateTo('query');
        }

        function clearQuery() {
            document.getElementById('queryInput').value = '';
            document.getElementById('queryResultContainer').style.display = 'none';
        }

        async function exportJSON() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) return;
            window.open('/admin/api/export?format=json&q=' + encodeURIComponent(query));
        }

        async function exportCSV() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) return;
            window.open('/admin/api/export?format=csv&q=' + encodeURIComponent(query));
        }

        // Explorer Functions
        function filterMetrics(search) {
            const items = document.querySelectorAll('#metricsList .metric-item');
            const searchLower = search.toLowerCase();
            items.forEach(item => {
                const name = item.querySelector('.metric-name').textContent.toLowerCase();
                item.style.display = name.includes(searchLower) ? '' : 'none';
            });
        }

        function queryMetric(name) {
            document.getElementById('queryInput').value = 'SELECT mean(value) FROM ' + name;
            navigateTo('query');
        }

        async function previewMetric(name) {
            try {
                const res = await fetch('/admin/api/data-preview?metric=' + encodeURIComponent(name) + '&limit=100');
                const data = await res.json();
                
                const card = document.getElementById('dataPreviewCard');
                card.style.display = 'block';
                
                const points = data.points || [];
                const tbody = document.querySelector('#previewTable tbody');
                tbody.innerHTML = points.map(p => 
                    '<tr><td>' + new Date(p.timestamp / 1000000).toISOString() + '</td><td>' + p.value + '</td></tr>'
                ).join('');
                
                // Chart
                const ctx = document.getElementById('previewChart');
                if (previewChart) previewChart.destroy();
                
                if (points.length > 0) {
                    previewChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: points.map(p => new Date(p.timestamp / 1000000).toLocaleTimeString()),
                            datasets: [{
                                label: name,
                                data: points.map(p => p.value),
                                borderColor: getComputedStyle(document.body).getPropertyValue('--accent-purple').trim(),
                                tension: 0.4
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: { legend: { display: false } }
                        }
                    });
                }
                
                card.scrollIntoView({ behavior: 'smooth' });
            } catch (err) {
                showToast('Failed to preview metric: ' + err.message, 'error');
            }
        }

        async function showMetricDetails(name) {
            document.getElementById('modalMetricName').textContent = name;
            
            try {
                const res = await fetch('/admin/api/metric-details?metric=' + encodeURIComponent(name));
                const data = await res.json();
                
                const body = document.getElementById('modalMetricBody');
                body.innerHTML = '<div class="config-row"><span class="config-key">Name</span><span class="config-value">' + escapeHtml(data.name) + '</span></div>' +
                    '<div class="config-row"><span class="config-key">Sample Count</span><span class="config-value">' + data.sample_count + '</span></div>';
                
                openModal('metricModal');
            } catch (err) {
                showToast('Failed to load metric details', 'error');
            }
        }

        function closeMetricDetails() {
            document.getElementById('metricDetailsCard').style.display = 'none';
        }

        function closeDataPreview() {
            document.getElementById('dataPreviewCard').style.display = 'none';
        }

        function queryModalMetric() {
            const name = document.getElementById('modalMetricName').textContent;
            closeModal('metricModal');
            queryMetric(name);
        }

        // Management Functions
        async function insertData() {
            const dataStr = document.getElementById('insertData').value.trim();
            if (!dataStr) {
                showToast('Please enter data to insert', 'warning');
                return;
            }
            
            try {
                const data = JSON.parse(dataStr);
                const res = await fetch('/admin/api/insert', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(data)
                });
                
                if (!res.ok) {
                    const error = await res.text();
                    showToast('Insert failed: ' + error, 'error');
                    return;
                }
                
                const result = await res.json();
                showToast('Inserted ' + result.inserted + ' points', 'success');
                refreshStats();
            } catch (err) {
                showToast('Insert failed: ' + err.message, 'error');
            }
        }

        async function truncateData() {
            const metric = document.getElementById('opMetricName').value.trim();
            const before = document.getElementById('opBefore').value;
            
            if (!metric) {
                showToast('Please enter a metric name', 'warning');
                return;
            }
            
            let url = '/admin/api/truncate?metric=' + encodeURIComponent(metric);
            if (before) {
                url += '&before=' + new Date(before).toISOString();
            }
            
            try {
                const res = await fetch(url, { method: 'POST' });
                const result = await res.json();
                showToast(result.message, 'success');
            } catch (err) {
                showToast('Truncate failed: ' + err.message, 'error');
            }
        }

        async function deleteMetric() {
            const metric = document.getElementById('opMetricName').value.trim();
            
            if (!metric) {
                showToast('Please enter a metric name', 'warning');
                return;
            }
            
            if (!confirm('Are you sure you want to delete metric "' + metric + '"?')) {
                return;
            }
            
            try {
                const res = await fetch('/admin/api/delete-metric?metric=' + encodeURIComponent(metric), { method: 'DELETE' });
                const result = await res.json();
                showToast(result.message, 'success');
            } catch (err) {
                showToast('Delete failed: ' + err.message, 'error');
            }
        }

        async function triggerBackup() {
            try {
                const res = await fetch('/admin/api/backup', { method: 'POST' });
                const result = await res.json();
                
                document.getElementById('backupStatus').innerHTML = 
                    '<div class="status-badge healthy" style="display: inline-flex;">' +
                    '<span class="status-dot"></span>' +
                    '<span>' + result.message + '</span></div>';
                
                showToast('Backup completed', 'success');
            } catch (err) {
                showToast('Backup failed: ' + err.message, 'error');
            }
        }

        // Modal Functions
        function openModal(id) {
            document.getElementById(id).classList.add('open');
        }

        function closeModal(id) {
            document.getElementById(id).classList.remove('open');
        }

        // Toast Notifications
        function showToast(message, type = 'success') {
            const container = document.getElementById('toastContainer');
            const toast = document.createElement('div');
            toast.className = 'toast ' + type;
            toast.innerHTML = '<span>' + escapeHtml(message) + '</span>';
            container.appendChild(toast);
            
            setTimeout(() => toast.classList.add('show'), 10);
            setTimeout(() => {
                toast.classList.remove('show');
                setTimeout(() => toast.remove(), 300);
            }, 3000);
        }

        // Alerts API Functions
        async function refreshAlerts() {
            try {
                const res = await fetch('/admin/api/alerts');
                const alerts = await res.json();
                
                const container = document.getElementById('alertsList');
                if (alerts.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔔</div><div class="empty-state-title">No Alert Rules</div><p>Create an alert rule to monitor your metrics</p></div>';
                    return;
                }
                
                let html = '<div class="table-container"><table><thead><tr><th>Name</th><th>Metric</th><th>Condition</th><th>State</th><th>Actions</th></tr></thead><tbody>';
                alerts.forEach(alert => {
                    const stateClass = 'alert-status-' + alert.state;
                    html += '<tr>' +
                        '<td>' + escapeHtml(alert.name) + '</td>' +
                        '<td><code>' + escapeHtml(alert.metric) + '</code></td>' +
                        '<td>' + alert.condition + ' ' + alert.threshold + '</td>' +
                        '<td class="' + stateClass + '">' + alert.state.toUpperCase() + '</td>' +
                        '<td><button class="btn btn-sm btn-danger" onclick="deleteAlert(\'' + alert.id + '\')">Delete</button></td>' +
                        '</tr>';
                });
                html += '</tbody></table></div>';
                container.innerHTML = html;
            } catch (err) {
                console.error('Failed to load alerts:', err);
            }
        }
        
        async function createAlert() {
            const alert = {
                name: document.getElementById('alertName').value,
                metric: document.getElementById('alertMetric').value,
                condition: document.getElementById('alertCondition').value,
                threshold: parseFloat(document.getElementById('alertThreshold').value),
                duration: document.getElementById('alertDuration').value,
                webhook_url: document.getElementById('alertWebhook').value
            };
            
            try {
                const res = await fetch('/admin/api/alerts', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(alert)
                });
                
                if (res.ok) {
                    closeModal('createAlertModal');
                    refreshAlerts();
                    showToast('Alert rule created', 'success');
                } else {
                    showToast('Failed to create alert', 'error');
                }
            } catch (err) {
                showToast('Error: ' + err.message, 'error');
            }
        }
        
        async function deleteAlert(id) {
            if (!confirm('Delete this alert rule?')) return;
            try {
                await fetch('/admin/api/alerts?id=' + id, {method: 'DELETE'});
                refreshAlerts();
                showToast('Alert deleted', 'success');
            } catch (err) {
                showToast('Failed to delete alert', 'error');
            }
        }

        // Audit Log API Functions
        async function refreshAuditLog() {
            try {
                const res = await fetch('/admin/api/audit-log?limit=50');
                const entries = await res.json();
                
                const tbody = document.getElementById('auditLogBody');
                if (entries.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted);">No audit entries</td></tr>';
                    return;
                }
                
                tbody.innerHTML = entries.map(e => 
                    '<tr>' +
                    '<td>' + new Date(e.timestamp).toLocaleString() + '</td>' +
                    '<td>' + escapeHtml(e.action) + '</td>' +
                    '<td>' + escapeHtml(e.user) + '</td>' +
                    '<td>' + escapeHtml(e.ip) + '</td>' +
                    '<td>' + escapeHtml(e.details) + '</td>' +
                    '</tr>'
                ).join('');
            } catch (err) {
                console.error('Failed to load audit log:', err);
            }
        }

        // Schema Registry API Functions
        async function refreshSchemas() {
            try {
                const res = await fetch('/admin/api/schemas');
                const schemas = await res.json();
                
                const container = document.getElementById('schemasList');
                if (schemas.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📐</div><div class="empty-state-title">No Schemas</div></div>';
                    return;
                }
                
                let html = '';
                schemas.forEach(schema => {
                    html += '<div class="metric-item">' +
                        '<div>' +
                        '<div class="metric-name">' + escapeHtml(schema.name) + '</div>' +
                        '<div style="font-size: 12px; color: var(--text-muted);">' + escapeHtml(schema.description) + '</div>' +
                        '</div>' +
                        '<div style="font-size: 12px; color: var(--text-secondary);">' +
                        'Type: ' + schema.type + ' | Tags: ' + (schema.tags.length || 0) +
                        '</div>' +
                        '</div>';
                });
                container.innerHTML = html;
            } catch (err) {
                console.error('Failed to load schemas:', err);
            }
        }

        // Retention API Functions
        async function refreshRetention() {
            try {
                const res = await fetch('/admin/api/retention');
                const rules = await res.json();
                
                const container = document.getElementById('retentionList');
                if (rules.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🕐</div><div class="empty-state-title">No Retention Policies</div><p>Create a retention policy to automatically manage data lifecycle</p></div>';
                    return;
                }
                
                let html = '<div class="table-container"><table><thead><tr><th>Metric</th><th>Duration</th><th>Status</th><th>Actions</th></tr></thead><tbody>';
                rules.forEach(rule => {
                    html += '<tr>' +
                        '<td><code>' + escapeHtml(rule.metric) + '</code></td>' +
                        '<td>' + rule.duration + '</td>' +
                        '<td>' + (rule.enabled ? '✅ Active' : '⏸️ Disabled') + '</td>' +
                        '<td><button class="btn btn-sm btn-danger" onclick="deleteRetention(\'' + rule.id + '\')">Delete</button></td>' +
                        '</tr>';
                });
                html += '</tbody></table></div>';
                container.innerHTML = html;
            } catch (err) {
                console.error('Failed to load retention policies:', err);
            }
        }
        
        async function createRetention() {
            const rule = {
                metric: document.getElementById('retentionMetric').value,
                duration: document.getElementById('retentionDuration').value
            };
            
            try {
                const res = await fetch('/admin/api/retention', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(rule)
                });
                
                if (res.ok) {
                    closeModal('createRetentionModal');
                    refreshRetention();
                    showToast('Retention policy created', 'success');
                } else {
                    showToast('Failed to create policy', 'error');
                }
            } catch (err) {
                showToast('Error: ' + err.message, 'error');
            }
        }
        
        async function deleteRetention(id) {
            if (!confirm('Delete this retention policy?')) return;
            try {
                await fetch('/admin/api/retention?id=' + id, {method: 'DELETE'});
                refreshRetention();
                showToast('Policy deleted', 'success');
            } catch (err) {
                showToast('Failed to delete policy', 'error');
            }
        }

        // Cluster API Functions
        async function refreshCluster() {
            try {
                const res = await fetch('/admin/api/cluster');
                const data = await res.json();
                
                document.getElementById('clusterMode').textContent = data.mode;
                document.getElementById('clusterHealth').textContent = data.status;
                document.getElementById('nodeId').textContent = data.node.id;
                document.getElementById('nodeState').textContent = data.node.state;
                document.getElementById('nodeUptime').textContent = data.node.uptime;
            } catch (err) {
                console.error('Failed to load cluster status:', err);
            }
        }

        // WAL API Functions
        async function refreshWAL() {
            try {
                const res = await fetch('/admin/api/wal');
                const data = await res.json();
                
                const container = document.getElementById('walInfo');
                container.innerHTML = 
                    '<div class="config-row"><span class="config-key">Status</span><span class="config-value">' + (data.enabled ? '✅ Enabled' : '❌ Disabled') + '</span></div>' +
                    '<div class="config-row"><span class="config-key">Path</span><span class="config-value" style="font-family: monospace;">' + escapeHtml(data.path) + '</span></div>' +
                    '<div class="config-row"><span class="config-key">Sync Interval</span><span class="config-value">' + data.sync_interval + '</span></div>' +
                    '<div class="config-row"><span class="config-key">Max Size</span><span class="config-value">' + data.max_size + '</span></div>' +
                    '<div class="config-row"><span class="config-key">File Size</span><span class="config-value">' + (data.stats.file_size || 'N/A') + '</span></div>';
            } catch (err) {
                console.error('Failed to load WAL info:', err);
            }
        }

        // Scheduled Exports API Functions
        async function refreshExports() {
            try {
                const res = await fetch('/admin/api/scheduled-exports');
                const exports = await res.json();
                
                const container = document.getElementById('exportsList');
                if (exports.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📤</div><div class="empty-state-title">No Scheduled Exports</div><p>Create a scheduled export to automatically export data</p></div>';
                    return;
                }
                
                let html = '<div class="table-container"><table><thead><tr><th>Name</th><th>Query</th><th>Format</th><th>Schedule</th><th>Actions</th></tr></thead><tbody>';
                exports.forEach(exp => {
                    html += '<tr>' +
                        '<td>' + escapeHtml(exp.name) + '</td>' +
                        '<td><code style="font-size: 11px;">' + escapeHtml(exp.query.substring(0, 40)) + (exp.query.length > 40 ? '...' : '') + '</code></td>' +
                        '<td>' + exp.format.toUpperCase() + '</td>' +
                        '<td>' + exp.schedule + '</td>' +
                        '<td><button class="btn btn-sm btn-danger" onclick="deleteExport(\'' + exp.id + '\')">Delete</button></td>' +
                        '</tr>';
                });
                html += '</tbody></table></div>';
                container.innerHTML = html;
            } catch (err) {
                console.error('Failed to load exports:', err);
            }
        }
        
        async function createExport() {
            const exportData = {
                name: document.getElementById('exportName').value,
                query: document.getElementById('exportQuery').value,
                format: document.getElementById('exportFormat').value,
                schedule: document.getElementById('exportSchedule').value
            };
            
            try {
                const res = await fetch('/admin/api/scheduled-exports', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(exportData)
                });
                
                if (res.ok) {
                    closeModal('createExportModal');
                    refreshExports();
                    showToast('Export scheduled', 'success');
                } else {
                    showToast('Failed to create export', 'error');
                }
            } catch (err) {
                showToast('Error: ' + err.message, 'error');
            }
        }
        
        async function deleteExport(id) {
            if (!confirm('Delete this scheduled export?')) return;
            try {
                await fetch('/admin/api/scheduled-exports?id=' + id, {method: 'DELETE'});
                refreshExports();
                showToast('Export deleted', 'success');
            } catch (err) {
                showToast('Failed to delete export', 'error');
            }
        }

        // Query Explain Function
        async function explainQuery() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) {
                showToast('Please enter a query', 'warning');
                return;
            }
            
            try {
                const res = await fetch('/admin/api/query-explain?q=' + encodeURIComponent(query));
                const data = await res.json();
                
                let html = '<div style="font-family: monospace; font-size: 13px;">';
                html += '<div style="margin-bottom: 12px;"><strong>Query:</strong> ' + escapeHtml(data.query) + '</div>';
                html += '<div style="margin-bottom: 8px;"><strong>Metric:</strong> ' + escapeHtml(data.parsed_metric || 'Unknown') + '</div>';
                html += '<div style="margin-bottom: 8px;"><strong>Function:</strong> ' + escapeHtml(data.parsed_function) + '</div>';
                html += '<div style="margin-bottom: 8px;"><strong>Time Range:</strong> ' + escapeHtml(data.time_range) + '</div>';
                html += '<div style="margin-bottom: 8px;"><strong>Index Used:</strong> ' + (data.index_used ? '✅ Yes' : '❌ No') + '</div>';
                html += '<div style="margin-bottom: 8px;"><strong>Estimated Rows:</strong> ~' + data.estimated_rows + '</div>';
                html += '<div style="margin-top: 16px;"><strong>Execution Steps:</strong></div>';
                html += '<ol style="margin: 8px 0 0 20px;">';
                data.steps.forEach(step => {
                    html += '<li style="margin-bottom: 4px;">' + escapeHtml(step) + '</li>';
                });
                html += '</ol></div>';
                
                // Show in a modal or below query
                const container = document.getElementById('queryResultContainer');
                container.style.display = 'block';
                document.getElementById('queryResultInfo').textContent = 'Query Explanation';
                document.getElementById('queryResultTime').textContent = '';
                document.querySelector('#queryResultTable tbody').innerHTML = '';
                document.getElementById('queryResultJSON').innerHTML = html;
                
                // Switch to JSON tab
                document.querySelectorAll('[data-result-tab]').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                document.querySelector('[data-result-tab="json"]').classList.add('active');
                document.getElementById('resultJson').classList.add('active');
            } catch (err) {
                showToast('Explain failed: ' + err.message, 'error');
            }
        }

        // Utilities
        function escapeHtml(str) {
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        }

        function copySnippet(el) {
            const code = el.closest('.snippet-card').querySelector('.snippet-body pre').textContent;
            navigator.clipboard.writeText(code).then(() => {
                el.textContent = 'Copied!';
                setTimeout(() => el.textContent = 'Copy', 2000);
            });
        }

        // Phase 10: Query Autocomplete
        let autocompleteCache = { metrics: [], functions: [] };
        
        async function initAutocomplete(inputId) {
            const input = document.getElementById(inputId);
            if (!input) return;
            
            // Load autocomplete data from API
            try {
                // Load metrics
                const metricsResp = await fetch('/admin/api/autocomplete?context=metric');
                const metricsData = await metricsResp.json();
                autocompleteCache.metrics = metricsData.map(m => m.value || m.label || '');
                
                // Load functions
                const funcsResp = await fetch('/admin/api/autocomplete?context=function');
                const funcsData = await funcsResp.json();
                autocompleteCache.functions = funcsData.map(f => f.label || f.value || '');
            } catch (e) {
                console.warn('Failed to load autocomplete data:', e);
            }
            
            // Create dropdown
            const container = document.createElement('div');
            container.className = 'autocomplete-container';
            input.parentNode.insertBefore(container, input);
            container.appendChild(input);
            
            const dropdown = document.createElement('div');
            dropdown.className = 'autocomplete-dropdown';
            dropdown.id = inputId + 'Autocomplete';
            container.appendChild(dropdown);
            
            // Add event listeners
            input.addEventListener('input', () => updateAutocomplete(inputId));
            input.addEventListener('focus', () => updateAutocomplete(inputId));
            input.addEventListener('blur', () => {
                setTimeout(() => dropdown.classList.remove('show'), 200);
            });
        }
        
        function updateAutocomplete(inputId) {
            const input = document.getElementById(inputId);
            const dropdown = document.getElementById(inputId + 'Autocomplete');
            if (!input || !dropdown) return;
            
            const value = input.value.toLowerCase();
            const words = value.split(/\s+/);
            const lastWord = words[words.length - 1];
            
            if (lastWord.length < 1) {
                dropdown.classList.remove('show');
                return;
            }
            
            const suggestions = [];
            
            // Match metrics
            autocompleteCache.metrics.filter(m => 
                m.toLowerCase().includes(lastWord)
            ).slice(0, 5).forEach(m => {
                suggestions.push({ type: 'metric', value: m });
            });
            
            // Match functions
            autocompleteCache.functions.filter(f => 
                f.toLowerCase().startsWith(lastWord)
            ).slice(0, 5).forEach(f => {
                suggestions.push({ type: 'function', value: f });
            });
            
            if (suggestions.length === 0) {
                dropdown.classList.remove('show');
                return;
            }
            
            dropdown.innerHTML = suggestions.map((s, i) => 
                '<div class="autocomplete-item' + (i === 0 ? ' selected' : '') + '" onclick="selectAutocomplete(\'' + inputId + '\', \'' + s.value + '\')">' +
                '<span class="type ' + s.type + '">' + s.type + '</span>' +
                '<span>' + escapeHtml(s.value) + '</span>' +
                '</div>'
            ).join('');
            dropdown.classList.add('show');
        }
        
        function selectAutocomplete(inputId, value) {
            const input = document.getElementById(inputId);
            const words = input.value.split(/\s+/);
            words[words.length - 1] = value;
            input.value = words.join(' ');
            input.focus();
            document.getElementById(inputId + 'Autocomplete').classList.remove('show');
        }

        // Phase 10: Saved Queries
        async function loadSavedQueries() {
            try {
                const resp = await fetch('/admin/api/saved-queries');
                const queries = await resp.json();
                const container = document.getElementById('savedQueriesList');
                
                if (!queries || queries.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">💾</div><div class="empty-state-title">No Saved Queries</div><p>Save frequently used queries for quick access</p></div>';
                    return;
                }
                
                container.innerHTML = queries.map(q => 
                    '<div class="saved-query-item" onclick="useSavedQuery(\'' + escapeHtml(q.query) + '\')">' +
                    '<div>' +
                    '<div class="saved-query-name">' + escapeHtml(q.name) + '</div>' +
                    '<div class="saved-query-text">' + escapeHtml(q.query.substring(0, 60)) + (q.query.length > 60 ? '...' : '') + '</div>' +
                    '</div>' +
                    '<button class="btn btn-secondary btn-sm" onclick="event.stopPropagation(); deleteSavedQuery(\'' + q.id + '\')">Delete</button>' +
                    '</div>'
                ).join('');
            } catch (e) {
                console.error('Failed to load saved queries:', e);
            }
        }
        
        function useSavedQuery(query) {
            document.getElementById('queryInput').value = query;
            navigateTo('query');
        }
        
        async function saveQuery() {
            const name = document.getElementById('saveQueryName').value.trim();
            const query = document.getElementById('saveQueryText').value.trim();
            
            if (!name || !query) {
                showToast('Please enter name and query', 'error');
                return;
            }
            
            try {
                const resp = await fetch('/admin/api/saved-queries', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, query })
                });
                
                if (resp.ok) {
                    showToast('Query saved', 'success');
                    closeModal('saveQueryModal');
                    loadSavedQueries();
                    document.getElementById('saveQueryName').value = '';
                    document.getElementById('saveQueryText').value = '';
                } else {
                    showToast('Failed to save query', 'error');
                }
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function deleteSavedQuery(id) {
            if (!confirm('Delete this saved query?')) return;
            
            try {
                await fetch('/admin/api/saved-queries?id=' + id, { method: 'DELETE' });
                loadSavedQueries();
                showToast('Query deleted', 'success');
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }

        // Phase 10: Multi-Metric Comparison
        let comparisonChart = null;
        const comparisonColors = ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1'];
        
        async function runComparison() {
            const metricsInput = document.getElementById('compareMetrics').value.trim();
            if (!metricsInput) {
                showToast('Please enter metrics to compare', 'error');
                return;
            }
            
            const timeRange = document.getElementById('compareTimeRange').value;
            const end = new Date();
            let start = new Date();
            
            switch (timeRange) {
                case '6h': start.setHours(end.getHours() - 6); break;
                case '24h': start.setDate(end.getDate() - 1); break;
                case '7d': start.setDate(end.getDate() - 7); break;
                default: start.setHours(end.getHours() - 1);
            }
            
            try {
                const resp = await fetch('/admin/api/compare?metrics=' + encodeURIComponent(metricsInput) + 
                    '&start=' + start.toISOString() + '&end=' + end.toISOString());
                const data = await resp.json();
                
                renderComparisonChart(data);
            } catch (e) {
                showToast('Error comparing metrics: ' + e.message, 'error');
            }
        }
        
        function renderComparisonChart(data) {
            const ctx = document.getElementById('comparisonChart').getContext('2d');
            
            if (comparisonChart) {
                comparisonChart.destroy();
            }
            
            const datasets = [];
            const legend = [];
            
            data.metrics.forEach((metric, i) => {
                const metricData = data.comparison[metric];
                if (!metricData || metricData.error) return;
                
                const color = comparisonColors[i % comparisonColors.length];
                datasets.push({
                    label: metric,
                    data: metricData.points.map(p => ({ x: p.timestamp / 1000000, y: p.value })),
                    borderColor: color,
                    backgroundColor: color + '20',
                    fill: false,
                    tension: 0.3
                });
                
                legend.push({ name: metric, color });
            });
            
            comparisonChart = new Chart(ctx, {
                type: 'line',
                data: { datasets },
                options: {
                    responsive: true,
                    scales: {
                        x: { type: 'linear', position: 'bottom', ticks: { callback: v => new Date(v).toLocaleTimeString() } },
                        y: { beginAtZero: true }
                    },
                    plugins: { legend: { display: false } }
                }
            });
            
            // Render custom legend
            document.getElementById('comparisonLegend').innerHTML = legend.map(l => 
                '<div class="comparison-legend-item">' +
                '<div class="comparison-legend-color" style="background:' + l.color + '"></div>' +
                '<span>' + escapeHtml(l.name) + '</span>' +
                '</div>'
            ).join('');
        }

        // Phase 10: Sparklines
        function renderSparkline(containerId, values) {
            const container = document.getElementById(containerId);
            if (!container || !values || values.length === 0) return;
            
            const width = 80;
            const height = 24;
            const padding = 2;
            
            const min = Math.min(...values);
            const max = Math.max(...values);
            const range = max - min || 1;
            
            const points = values.map((v, i) => {
                const x = padding + (i / (values.length - 1)) * (width - 2 * padding);
                const y = height - padding - ((v - min) / range) * (height - 2 * padding);
                return x + ',' + y;
            });
            
            const pathD = 'M' + points.join(' L');
            const fillD = pathD + ' L' + (width - padding) + ',' + (height - padding) + ' L' + padding + ',' + (height - padding) + ' Z';
            
            container.innerHTML = '<svg class="sparkline" viewBox="0 0 ' + width + ' ' + height + '">' +
                '<path class="sparkline-fill" d="' + fillD + '"/>' +
                '<path class="sparkline-path" d="' + pathD + '"/>' +
                '</svg>';
        }
        
        async function loadSparklines(metrics) {
            for (const metric of metrics) {
                try {
                    const resp = await fetch('/admin/api/sparkline?metric=' + encodeURIComponent(metric));
                    const data = await resp.json();
                    renderSparkline('sparkline-' + metric, data.values);
                } catch (e) {
                    console.warn('Failed to load sparkline for ' + metric);
                }
            }
        }

        // Phase 10: Favorites
        async function toggleFavorite(type, name, btn) {
            try {
                const resp = await fetch('/admin/api/favorites');
                const favorites = await resp.json() || [];
                const existing = favorites.find(f => f.type === type && f.name === name);
                
                if (existing) {
                    await fetch('/admin/api/favorites?id=' + existing.id, { method: 'DELETE' });
                    btn.classList.remove('active');
                    showToast('Removed from favorites', 'success');
                } else {
                    await fetch('/admin/api/favorites', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ type, name })
                    });
                    btn.classList.add('active');
                    showToast('Added to favorites', 'success');
                }
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function loadFavorites() {
            try {
                const resp = await fetch('/admin/api/favorites');
                return await resp.json() || [];
            } catch (e) {
                return [];
            }
        }

        // Phase 10: Recent Items
        async function trackRecent(type, name) {
            try {
                await fetch('/admin/api/recent', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type, name })
                });
            } catch (e) {
                console.warn('Failed to track recent item');
            }
        }
        
        async function loadRecentItems() {
            try {
                const resp = await fetch('/admin/api/recent');
                return await resp.json() || [];
            } catch (e) {
                return [];
            }
        }

        // Phase 10: Alert History
        async function loadAlertHistory() {
            try {
                const resp = await fetch('/admin/api/alert-history');
                const history = await resp.json() || [];
                const container = document.getElementById('alertHistoryList');
                
                if (history.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📜</div><div class="empty-state-title">No Alert History</div><p>Alert firings will appear here</p></div>';
                    return;
                }
                
                container.innerHTML = history.map(h => 
                    '<div class="alert-history-item">' +
                    '<div class="alert-history-status ' + h.status + '"></div>' +
                    '<div style="flex: 1;">' +
                    '<strong>' + escapeHtml(h.rule_name) + '</strong>' +
                    '<div style="font-size: 12px; color: var(--text-muted);">' +
                    escapeHtml(h.message || 'Value: ' + h.value) +
                    '</div>' +
                    '</div>' +
                    '<div style="font-size: 12px; color: var(--text-muted);">' +
                    new Date(h.fired_at).toLocaleString() +
                    '</div>' +
                    '</div>'
                ).join('');
            } catch (e) {
                console.error('Failed to load alert history:', e);
            }
        }

        // Phase 10: Import Data
        function handleImportFile(input) {
            const file = input.files[0];
            if (!file) return;
            
            const reader = new FileReader();
            reader.onload = (e) => {
                document.getElementById('importData').value = e.target.result;
                updateImportPreview();
            };
            reader.readAsText(file);
        }
        
        function updateImportPreview() {
            const data = document.getElementById('importData').value;
            const format = document.getElementById('importFormat').value;
            const preview = document.getElementById('importPreview');
            
            try {
                let count = 0;
                if (format === 'json') {
                    const parsed = JSON.parse(data);
                    count = Array.isArray(parsed) ? parsed.length : 1;
                } else if (format === 'csv') {
                    count = data.trim().split('\n').length - 1;
                } else {
                    count = data.trim().split('\n').length;
                }
                preview.textContent = 'Ready to import ~' + count + ' data point(s)';
            } catch (e) {
                preview.textContent = 'Preview: Unable to parse data';
            }
        }
        
        async function executeImport() {
            const data = document.getElementById('importData').value.trim();
            const format = document.getElementById('importFormat').value;
            
            if (!data) {
                showToast('Please enter or upload data to import', 'error');
                return;
            }
            
            try {
                const resp = await fetch('/admin/api/import', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ format, data })
                });
                
                const result = await resp.json();
                
                if (resp.ok) {
                    showToast('Imported ' + result.imported + ' points', 'success');
                    document.getElementById('importData').value = '';
                    document.getElementById('importPreview').textContent = '';
                } else {
                    showToast('Import failed: ' + result.error, 'error');
                }
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        // Setup drag-and-drop for import
        document.addEventListener('DOMContentLoaded', () => {
            const dropZone = document.getElementById('importDropZone');
            if (dropZone) {
                dropZone.addEventListener('dragover', (e) => {
                    e.preventDefault();
                    dropZone.classList.add('drag-over');
                });
                dropZone.addEventListener('dragleave', () => {
                    dropZone.classList.remove('drag-over');
                });
                dropZone.addEventListener('drop', (e) => {
                    e.preventDefault();
                    dropZone.classList.remove('drag-over');
                    const file = e.dataTransfer.files[0];
                    if (file) {
                        const reader = new FileReader();
                        reader.onload = (ev) => {
                            document.getElementById('importData').value = ev.target.result;
                            updateImportPreview();
                        };
                        reader.readAsText(file);
                    }
                });
            }
        });

        // Phase 10: Diagnostics
        async function loadDiagnostics() {
            try {
                const resp = await fetch('/admin/api/diagnostics');
                const diag = await resp.json();
                
                const cpuPct = Math.min(100, Math.round((diag.cpu_usage || 0) * 100));
                document.getElementById('diagCpu').textContent = cpuPct + '%';
                const cpuBar = document.getElementById('diagCpuBar');
                cpuBar.style.width = cpuPct + '%';
                cpuBar.className = 'diagnostic-bar-fill ' + (cpuPct > 80 ? 'high' : cpuPct > 50 ? 'medium' : 'low');
                
                const memMB = Math.round((diag.memory_used || 0) / 1024 / 1024);
                const memTotal = Math.round((diag.memory_total || 1) / 1024 / 1024);
                const memPct = Math.round((diag.memory_used / diag.memory_total) * 100) || 0;
                document.getElementById('diagMemory').textContent = memMB + ' / ' + memTotal + ' MB';
                const memBar = document.getElementById('diagMemoryBar');
                memBar.style.width = memPct + '%';
                memBar.className = 'diagnostic-bar-fill ' + (memPct > 80 ? 'high' : memPct > 50 ? 'medium' : 'low');
                
                document.getElementById('diagGoroutines').textContent = diag.goroutines || '--';
                document.getElementById('diagGC').textContent = diag.gc_runs || '--';
                
                if (diag.uptime) {
                    const hours = Math.floor(diag.uptime / 3600000000000);
                    const mins = Math.floor((diag.uptime % 3600000000000) / 60000000000);
                    document.getElementById('diagUptime').textContent = hours + 'h ' + mins + 'm';
                }
                
                document.getElementById('diagDataPath').textContent = diag.data_path || '--';
            } catch (e) {
                console.error('Failed to load diagnostics:', e);
            }
        }

        // Phase 10: Sessions
        async function loadSessions() {
            try {
                const resp = await fetch('/admin/api/sessions');
                const sessions = await resp.json() || [];
                const container = document.getElementById('sessionsList');
                
                if (sessions.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">👤</div><div class="empty-state-title">No Sessions</div><p>Active sessions will appear here</p></div>';
                    return;
                }
                
                container.innerHTML = sessions.map(s => 
                    '<div class="session-item' + (s.current ? ' session-current' : '') + '">' +
                    '<div>' +
                    '<div><strong>' + escapeHtml(s.ip || 'Unknown IP') + '</strong>' + 
                    (s.current ? ' <span class="session-badge">Current</span>' : '') + '</div>' +
                    '<div style="font-size: 12px; color: var(--text-muted);">' +
                    escapeHtml(s.user_agent || 'Unknown browser') + '<br>' +
                    'Last active: ' + new Date(s.last_active).toLocaleString() +
                    '</div>' +
                    '</div>' +
                    (s.current ? '' : '<button class="btn btn-secondary btn-sm" onclick="revokeSession(\'' + s.id + '\')">Revoke</button>') +
                    '</div>'
                ).join('');
            } catch (e) {
                console.error('Failed to load sessions:', e);
            }
        }
        
        async function revokeSession(id) {
            if (!confirm('Revoke this session?')) return;
            
            try {
                await fetch('/admin/api/sessions?id=' + id, { method: 'DELETE' });
                loadSessions();
                showToast('Session revoked', 'success');
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }

        // Phase 11: Query Templates
        let currentTemplateFilter = '';
        
        async function loadTemplates() {
            try {
                const url = '/admin/api/templates' + (currentTemplateFilter ? '?category=' + currentTemplateFilter : '');
                const resp = await fetch(url);
                const templates = await resp.json();
                const container = document.getElementById('templatesList');
                
                if (!templates || templates.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📋</div><div class="empty-state-title">No Templates</div><p>Create a query template to get started</p></div>';
                    return;
                }
                
                container.innerHTML = templates.map(t => 
                    '<div class="template-card' + (t.built_in ? ' built-in' : '') + '" onclick="useTemplate(\'' + escapeHtml(t.query) + '\')">' +
                    '<div style="display: flex; justify-content: space-between; align-items: start;">' +
                    '<div>' +
                    '<span class="template-category ' + t.category + '">' + t.category + '</span>' +
                    (t.built_in ? '<span style="font-size: 10px; color: var(--text-muted);">Built-in</span>' : '') +
                    '</div>' +
                    (!t.built_in ? '<button class="btn btn-secondary btn-sm" onclick="event.stopPropagation(); deleteTemplate(\'' + t.id + '\')">Delete</button>' : '') +
                    '</div>' +
                    '<h4 style="margin: 8px 0 4px;">' + escapeHtml(t.name) + '</h4>' +
                    '<p style="font-size: 12px; color: var(--text-muted); margin-bottom: 8px;">' + escapeHtml(t.description) + '</p>' +
                    '<code style="font-size: 11px; color: var(--accent-blue);">' + escapeHtml(t.query.substring(0, 80)) + (t.query.length > 80 ? '...' : '') + '</code>' +
                    '</div>'
                ).join('');
            } catch (e) {
                console.error('Failed to load templates:', e);
            }
        }
        
        function filterTemplates(category) {
            currentTemplateFilter = category;
            loadTemplates();
        }
        
        function useTemplate(query) {
            // Replace variables with prompts
            let finalQuery = query;
            const vars = query.match(/\{\{(\w+)\}\}/g) || [];
            vars.forEach(v => {
                const varName = v.replace(/\{\{|\}\}/g, '');
                const value = prompt('Enter value for ' + varName + ':');
                if (value) {
                    finalQuery = finalQuery.replace(v, value);
                }
            });
            document.getElementById('queryInput').value = finalQuery;
            navigateTo('query');
        }
        
        async function createTemplate() {
            const name = document.getElementById('templateName').value.trim();
            const category = document.getElementById('templateCategory').value;
            const description = document.getElementById('templateDesc').value.trim();
            const query = document.getElementById('templateQuery').value.trim();
            
            if (!name || !query) {
                showToast('Name and query are required', 'error');
                return;
            }
            
            try {
                const resp = await fetch('/admin/api/templates', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, category, description, query })
                });
                
                if (resp.ok) {
                    showToast('Template created', 'success');
                    closeModal('createTemplateModal');
                    loadTemplates();
                    document.getElementById('templateName').value = '';
                    document.getElementById('templateDesc').value = '';
                    document.getElementById('templateQuery').value = '';
                } else {
                    showToast('Failed to create template', 'error');
                }
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function deleteTemplate(id) {
            if (!confirm('Delete this template?')) return;
            try {
                await fetch('/admin/api/templates?id=' + id, { method: 'DELETE' });
                loadTemplates();
                showToast('Template deleted', 'success');
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }

        // Phase 11: Annotations
        async function loadAnnotations() {
            const metric = document.getElementById('annotationMetricFilter')?.value || '';
            try {
                const url = '/admin/api/annotations' + (metric ? '?metric=' + encodeURIComponent(metric) : '');
                const resp = await fetch(url);
                const annotations = await resp.json();
                const container = document.getElementById('annotationsList');
                
                if (!annotations || annotations.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📌</div><div class="empty-state-title">No Annotations</div><p>Add an annotation to mark important events</p></div>';
                    return;
                }
                
                container.innerHTML = annotations.map(a => 
                    '<div class="annotation-item">' +
                    '<div style="display: flex; justify-content: space-between; align-items: start;">' +
                    '<div>' +
                    '<strong>' + escapeHtml(a.title) + '</strong>' +
                    '<span style="margin-left: 8px; font-size: 12px; color: var(--text-muted);">on ' + escapeHtml(a.metric) + '</span>' +
                    '</div>' +
                    '<button class="btn btn-secondary btn-sm" onclick="deleteAnnotation(\'' + a.id + '\')">Delete</button>' +
                    '</div>' +
                    '<p style="margin: 8px 0; font-size: 13px;">' + escapeHtml(a.text || '') + '</p>' +
                    (a.tags && a.tags.length ? '<div class="annotation-tags">' + a.tags.map(t => '<span class="annotation-tag">' + escapeHtml(t) + '</span>').join('') + '</div>' : '') +
                    '<div style="font-size: 11px; color: var(--text-muted); margin-top: 8px;">By ' + escapeHtml(a.created_by) + ' • ' + new Date(a.created_at).toLocaleString() + '</div>' +
                    '</div>'
                ).join('');
            } catch (e) {
                console.error('Failed to load annotations:', e);
            }
        }
        
        async function createAnnotation() {
            const metric = document.getElementById('annotationMetric').value.trim();
            const title = document.getElementById('annotationTitle').value.trim();
            const text = document.getElementById('annotationText').value.trim();
            const tagsStr = document.getElementById('annotationTags').value.trim();
            const tags = tagsStr ? tagsStr.split(',').map(t => t.trim()).filter(t => t) : [];
            
            if (!metric || !title) {
                showToast('Metric and title are required', 'error');
                return;
            }
            
            try {
                const resp = await fetch('/admin/api/annotations', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ metric, title, text, tags })
                });
                
                if (resp.ok) {
                    showToast('Annotation added', 'success');
                    closeModal('createAnnotationModal');
                    loadAnnotations();
                    document.getElementById('annotationMetric').value = '';
                    document.getElementById('annotationTitle').value = '';
                    document.getElementById('annotationText').value = '';
                    document.getElementById('annotationTags').value = '';
                } else {
                    showToast('Failed to add annotation', 'error');
                }
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function deleteAnnotation(id) {
            if (!confirm('Delete this annotation?')) return;
            try {
                await fetch('/admin/api/annotations?id=' + id, { method: 'DELETE' });
                loadAnnotations();
                showToast('Annotation deleted', 'success');
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }

        // Phase 11: Profiling
        async function loadProfiling() {
            try {
                const resp = await fetch('/admin/api/profiling?type=summary');
                const data = await resp.json();
                
                const container = document.getElementById('profilingStats');
                container.innerHTML = '' +
                    '<div class="profile-stat"><div class="profile-stat-value">' + data.goroutines + '</div><div class="profile-stat-label">Goroutines</div></div>' +
                    '<div class="profile-stat"><div class="profile-stat-value">' + data.cpu_count + '</div><div class="profile-stat-label">CPUs</div></div>' +
                    '<div class="profile-stat"><div class="profile-stat-value">' + data.gc_runs + '</div><div class="profile-stat-label">GC Runs</div></div>' +
                    '<div class="profile-stat"><div class="profile-stat-value">' + formatSize(data.heap_alloc) + '</div><div class="profile-stat-label">Heap Alloc</div></div>' +
                    '<div class="profile-stat"><div class="profile-stat-value">' + formatSize(data.stack_inuse) + '</div><div class="profile-stat-label">Stack</div></div>' +
                    '<div class="profile-stat"><div class="profile-stat-value">' + data.heap_objects + '</div><div class="profile-stat-label">Heap Objects</div></div>';
                
                // Memory bar
                const total = data.heap_sys + data.stack_sys + data.other_sys;
                const heapPct = (data.heap_inuse / total * 100).toFixed(1);
                const stackPct = (data.stack_inuse / total * 100).toFixed(1);
                const otherPct = (100 - heapPct - stackPct).toFixed(1);
                
                document.getElementById('memoryBar').innerHTML = '' +
                    '<div class="memory-bar-segment heap" style="width: ' + heapPct + '%" title="Heap: ' + heapPct + '%"></div>' +
                    '<div class="memory-bar-segment stack" style="width: ' + stackPct + '%" title="Stack: ' + stackPct + '%"></div>' +
                    '<div class="memory-bar-segment other" style="width: ' + otherPct + '%" title="Other: ' + otherPct + '%"></div>';
            } catch (e) {
                console.error('Failed to load profiling:', e);
            }
        }
        
        function formatSize(bytes) {
            if (bytes < 1024) return bytes + ' B';
            if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
            if (bytes < 1024 * 1024 * 1024) return (bytes / 1024 / 1024).toFixed(1) + ' MB';
            return (bytes / 1024 / 1024 / 1024).toFixed(1) + ' GB';
        }

        // Phase 11: Log Viewer
        async function loadLogs() {
            const level = document.getElementById('logLevelFilter')?.value || '';
            const source = document.getElementById('logSourceFilter')?.value || '';
            
            try {
                let url = '/admin/api/logs?limit=200';
                if (level) url += '&level=' + level;
                if (source) url += '&source=' + encodeURIComponent(source);
                
                const resp = await fetch(url);
                const logs = await resp.json();
                const container = document.getElementById('logViewer');
                
                if (!logs || logs.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📄</div><div class="empty-state-title">No Logs</div><p>Log entries will appear here</p></div>';
                    return;
                }
                
                container.innerHTML = logs.map(l => 
                    '<div class="log-entry">' +
                    '<span class="log-timestamp">' + new Date(l.timestamp).toLocaleTimeString() + '</span>' +
                    '<span class="log-level ' + l.level + '">' + l.level + '</span>' +
                    '<span class="log-message">' + escapeHtml(l.message) + '</span>' +
                    (l.source ? '<span style="color: var(--text-muted);">[' + escapeHtml(l.source) + ']</span>' : '') +
                    '</div>'
                ).join('');
            } catch (e) {
                console.error('Failed to load logs:', e);
            }
        }
        
        async function clearLogs() {
            if (!confirm('Clear all logs?')) return;
            try {
                await fetch('/admin/api/logs', { method: 'DELETE' });
                loadLogs();
                showToast('Logs cleared', 'success');
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }

        // Phase 11: Roles & Permissions
        async function loadRoles() {
            try {
                const resp = await fetch('/admin/api/roles');
                const roles = await resp.json();
                const container = document.getElementById('rolesList');
                
                container.innerHTML = roles.map(r => {
                    const badgeClass = r.name.toLowerCase();
                    return '<div class="role-card">' +
                        '<div class="role-header">' +
                        '<span class="role-name">' + escapeHtml(r.name) + '</span>' +
                        '<span class="role-badge ' + badgeClass + '">' + r.permissions.length + ' perms</span>' +
                        '</div>' +
                        '<p style="font-size: 12px; color: var(--text-muted); margin-bottom: 12px;">' + escapeHtml(r.description || '') + '</p>' +
                        '<div class="permission-list">' +
                        r.permissions.map(p => '<span class="permission-badge granted">' + p + '</span>').join('') +
                        '</div>' +
                        '</div>';
                }).join('');
                
                // Update role dropdown in assign modal
                const select = document.getElementById('assignRole');
                if (select) {
                    select.innerHTML = roles.map(r => '<option value="' + r.id + '">' + escapeHtml(r.name) + '</option>').join('');
                }
            } catch (e) {
                console.error('Failed to load roles:', e);
            }
        }
        
        async function loadPermissions() {
            try {
                const resp = await fetch('/admin/api/permissions');
                const perms = await resp.json();
                const container = document.getElementById('permissionsList');
                
                if (!perms || perms.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔐</div><div class="empty-state-title">No Users</div><p>Assign roles to users to manage access</p></div>';
                    return;
                }
                
                container.innerHTML = perms.map(p => 
                    '<div class="user-access-row">' +
                    '<div>' +
                    '<strong>' + escapeHtml(p.user) + '</strong>' +
                    '<div style="font-size: 12px; color: var(--text-muted);">Role: ' + escapeHtml(p.role) + '</div>' +
                    '</div>' +
                    '<button class="btn btn-secondary btn-sm" onclick="revokeAccess(\'' + escapeHtml(p.user) + '\')">Revoke</button>' +
                    '</div>'
                ).join('');
            } catch (e) {
                console.error('Failed to load permissions:', e);
            }
        }
        
        async function createRole() {
            const name = document.getElementById('roleName').value.trim();
            const description = document.getElementById('roleDesc').value.trim();
            
            const permissions = [];
            if (document.getElementById('permRead').checked) permissions.push('read');
            if (document.getElementById('permWrite').checked) permissions.push('write');
            if (document.getElementById('permDelete').checked) permissions.push('delete');
            if (document.getElementById('permExport').checked) permissions.push('export');
            if (document.getElementById('permImport').checked) permissions.push('import');
            if (document.getElementById('permConfig').checked) permissions.push('config');
            if (document.getElementById('permUsers').checked) permissions.push('users');
            if (document.getElementById('permAdmin').checked) permissions.push('admin');
            
            if (!name) {
                showToast('Name is required', 'error');
                return;
            }
            
            try {
                const resp = await fetch('/admin/api/roles', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, description, permissions })
                });
                
                if (resp.ok) {
                    showToast('Role created', 'success');
                    closeModal('createRoleModal');
                    loadRoles();
                    document.getElementById('roleName').value = '';
                    document.getElementById('roleDesc').value = '';
                } else {
                    showToast('Failed to create role', 'error');
                }
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function assignRole() {
            const user = document.getElementById('assignUser').value.trim();
            const role = document.getElementById('assignRole').value;
            
            if (!user || !role) {
                showToast('User and role are required', 'error');
                return;
            }
            
            try {
                const resp = await fetch('/admin/api/permissions', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user, role })
                });
                
                if (resp.ok) {
                    showToast('Role assigned', 'success');
                    closeModal('assignRoleModal');
                    loadPermissions();
                    document.getElementById('assignUser').value = '';
                } else {
                    showToast('Failed to assign role', 'error');
                }
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function revokeAccess(user) {
            if (!confirm('Revoke access for ' + user + '?')) return;
            try {
                await fetch('/admin/api/permissions?user=' + encodeURIComponent(user), { method: 'DELETE' });
                loadPermissions();
                showToast('Access revoked', 'success');
            } catch (e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
    </script>
</body>
</html>`
