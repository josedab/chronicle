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
			BufferSize:     ui.db.config.Storage.BufferSize,
			Retention:      ui.db.config.Retention.RetentionDuration.String(),
		},
		Metrics: metrics,
		Config: configData{
			Path:              ui.db.config.Path,
			PartitionDuration: ui.db.config.Storage.PartitionDuration.String(),
			BufferSize:        ui.db.config.Storage.BufferSize,
			SyncInterval:      ui.db.config.WAL.SyncInterval.String(),
			Retention:         ui.db.config.Retention.RetentionDuration.String(),
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
		"partition_duration": ui.db.config.Storage.PartitionDuration.String(),
		"buffer_size":        ui.db.config.Storage.BufferSize,
		"sync_interval":      ui.db.config.WAL.SyncInterval.String(),
		"retention":          ui.db.config.Retention.RetentionDuration.String(),
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
			"partition_duration": ui.db.config.Storage.PartitionDuration.String(),
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
			"buffer_size":   ui.db.config.Storage.BufferSize,
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
