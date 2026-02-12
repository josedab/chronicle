package adminui

import (
	"net/http"
	"time"
)

// NewAdminUI creates an admin UI instance.
func NewAdminUI(db AdminDB, config AdminConfig) *AdminUI {
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
	ui.mux.HandleFunc(config.Prefix+"/api/events", ui.handleAPIEvents)              // SSE endpoint
	ui.mux.HandleFunc(config.Prefix+"/api/alerts", ui.handleAPIAlerts)              // Alert rules CRUD
	ui.mux.HandleFunc(config.Prefix+"/api/audit-log", ui.handleAPIAuditLog)         // Audit log
	ui.mux.HandleFunc(config.Prefix+"/api/query-explain", ui.handleAPIQueryExplain) // Query explain

	// Phase 8: Schema, Retention, Cluster
	ui.mux.HandleFunc(config.Prefix+"/api/schemas", ui.handleAPISchemas)     // Schema registry
	ui.mux.HandleFunc(config.Prefix+"/api/retention", ui.handleAPIRetention) // Retention policies
	ui.mux.HandleFunc(config.Prefix+"/api/cluster", ui.handleAPICluster)     // Cluster status

	// Phase 9: WAL, Scheduled Exports
	ui.mux.HandleFunc(config.Prefix+"/api/wal", ui.handleAPIWAL) // WAL inspector
	ui.mux.HandleFunc(config.Prefix+"/api/scheduled-exports", ui.handleAPIScheduledExports)
	ui.mux.HandleFunc(config.Prefix+"/api/search", ui.handleAPISearch) // Global search

	// Phase 10: Query UX, Productivity, Advanced Features
	ui.mux.HandleFunc(config.Prefix+"/api/autocomplete", ui.handleAPIAutocomplete)  // Query autocomplete
	ui.mux.HandleFunc(config.Prefix+"/api/saved-queries", ui.handleAPISavedQueries) // Saved queries
	ui.mux.HandleFunc(config.Prefix+"/api/favorites", ui.handleAPIFavorites)        // Favorites
	ui.mux.HandleFunc(config.Prefix+"/api/recent", ui.handleAPIRecent)              // Recent items
	ui.mux.HandleFunc(config.Prefix+"/api/alert-history", ui.handleAPIAlertHistory) // Alert history
	ui.mux.HandleFunc(config.Prefix+"/api/import", ui.handleAPIImport)              // Data import
	ui.mux.HandleFunc(config.Prefix+"/api/diagnostics", ui.handleAPIDiagnostics)    // System diagnostics
	ui.mux.HandleFunc(config.Prefix+"/api/sessions", ui.handleAPISessions)          // Session management
	ui.mux.HandleFunc(config.Prefix+"/api/sparkline", ui.handleAPISparkline)        // Sparkline data
	ui.mux.HandleFunc(config.Prefix+"/api/compare", ui.handleAPICompare)            // Multi-metric compare

	// Phase 11: Templates, Annotations, Profiling, Logs, RBAC
	ui.mux.HandleFunc(config.Prefix+"/api/templates", ui.handleAPITemplates)     // Query templates
	ui.mux.HandleFunc(config.Prefix+"/api/annotations", ui.handleAPIAnnotations) // Metric annotations
	ui.mux.HandleFunc(config.Prefix+"/api/profiling", ui.handleAPIProfiling)     // Performance profiling
	ui.mux.HandleFunc(config.Prefix+"/api/logs", ui.handleAPILogs)               // Log viewer
	ui.mux.HandleFunc(config.Prefix+"/api/roles", ui.handleAPIRoles)             // Role management
	ui.mux.HandleFunc(config.Prefix+"/api/permissions", ui.handleAPIPermissions) // User permissions

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
