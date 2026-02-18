package adminui

import (
	"net/http"
	"sync"
	"time"
)

// AdminUI provides a web-based admin interface for Chronicle.
type AdminUI struct {
	db               AdminDB
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
	queryTemplates  []queryTemplate
	annotations     []metricAnnotation
	logBuffer       []logEntry
	roles           []userRole
	userPermissions map[string]*userAccess
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
	ID          string     `json:"id"`
	Metric      string     `json:"metric"`
	Duration    string     `json:"duration"`
	Enabled     bool       `json:"enabled"`
	CreatedAt   time.Time  `json:"created_at"`
	LastApplied *time.Time `json:"last_applied,omitempty"`
}

// scheduledExport defines an automated export schedule
type scheduledExport struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Query     string     `json:"query"`
	Format    string     `json:"format"`
	Schedule  string     `json:"schedule"`
	Enabled   bool       `json:"enabled"`
	LastRun   *time.Time `json:"last_run,omitempty"`
	NextRun   *time.Time `json:"next_run,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
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
	ID        string    `json:"id"`
	AlertID   string    `json:"alert_id"`
	AlertName string    `json:"alert_name"`
	State     string    `json:"state"` // "firing", "resolved"
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message,omitempty"`
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
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"` // "debug", "info", "warn", "error"
	Message   string         `json:"message"`
	Source    string         `json:"source,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
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
