package chronicle

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// StudioEnhancedConfig configures the next-gen Web IDE capabilities for Chronicle Studio.
type StudioEnhancedConfig struct {
	EnableCollaboration    bool          `json:"enable_collaboration"`
	MaxSessions            int           `json:"max_sessions"`
	SessionTimeout         time.Duration `json:"session_timeout"`
	EnableAutocomplete     bool          `json:"enable_autocomplete"`
	EnableSyntaxHighlight  bool          `json:"enable_syntax_highlighting"`
	Theme                  string        `json:"theme"`
	EnableRealtimePreview  bool          `json:"enable_real_time_preview"`
	MaxQueryHistory        int           `json:"max_query_history"`
	EnableDataExport       bool          `json:"enable_data_export"`
}

// DefaultStudioEnhancedConfig returns sensible defaults for StudioEnhanced.
func DefaultStudioEnhancedConfig() StudioEnhancedConfig {
	return StudioEnhancedConfig{
		EnableCollaboration:   true,
		MaxSessions:           100,
		SessionTimeout:        30 * time.Minute,
		EnableAutocomplete:    true,
		EnableSyntaxHighlight: true,
		Theme:                 "dark",
		EnableRealtimePreview: true,
		MaxQueryHistory:       500,
		EnableDataExport:      true,
	}
}

// StudioTabType represents the type of a tab in the studio.
type StudioTabType string

const (
	TabQuery     StudioTabType = "query"
	TabDashboard StudioTabType = "dashboard"
	TabSchema    StudioTabType = "schema"
	TabAlert     StudioTabType = "alert"
	TabNotebook  StudioTabType = "notebook"
)

// StudioTab represents a tab in a studio session workspace.
type StudioTab struct {
	ID       string        `json:"id"`
	Title    string        `json:"title"`
	Type     StudioTabType `json:"type"`
	Content  string        `json:"content"`
	Modified time.Time     `json:"modified"`
}

// StudioQueryHistoryEntry records a single executed query and its outcome in the enhanced studio.
type StudioQueryHistoryEntry struct {
	Query      string        `json:"query"`
	ExecutedAt time.Time     `json:"executed_at"`
	Duration   time.Duration `json:"duration"`
	RowCount   int           `json:"row_count"`
	Error      string        `json:"error,omitempty"`
}

// StudioWidget represents a widget in a dashboard layout.
type StudioWidget struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Title    string            `json:"title"`
	Config   map[string]string `json:"config,omitempty"`
	Position int               `json:"position"`
	Size     int               `json:"size"`
}

// StudioLayout represents a reusable dashboard layout configuration.
type StudioLayout struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	Widgets []StudioWidget `json:"widgets"`
	Columns int            `json:"columns"`
	Rows    int            `json:"rows"`
	Theme   string         `json:"theme"`
}

// StudioCollaboration holds real-time collaboration state for a session.
type StudioCollaboration struct {
	SessionID      string            `json:"session_id"`
	Participants   []string          `json:"participants"`
	CursorPositions map[string]int   `json:"cursor_positions"`
	Edits          []string          `json:"edits"`
}

// StudioSession represents an active workspace session in the enhanced studio.
type StudioSession struct {
	ID             string              `json:"id"`
	UserID         string              `json:"user_id"`
	CreatedAt      time.Time           `json:"created_at"`
	LastActive     time.Time           `json:"last_active"`
	QueryHistory   []StudioQueryHistoryEntry `json:"query_history"`
	ActiveTab      string              `json:"active_tab"`
	WorkspaceState map[string]string   `json:"workspace_state"`
}

// StudioQueryResult holds the result set of an executed query.
type StudioQueryResult struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	Duration  time.Duration   `json:"duration"`
	RowCount  int             `json:"row_count"`
	Truncated bool            `json:"truncated"`
}

// StudioEnhancedStats holds aggregate statistics for the enhanced studio.
type StudioEnhancedStats struct {
	ActiveSessions  int   `json:"active_sessions"`
	TotalLayouts    int   `json:"total_layouts"`
	SharedQueries   int   `json:"shared_queries"`
	QueriesExecuted int64 `json:"queries_executed"`
}

// StudioEnhancedEngine is the next-gen Web IDE engine for Chronicle.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type StudioEnhancedEngine struct {
	db              *DB
	config          StudioEnhancedConfig
	sessions        map[string]*StudioSession
	layouts         map[string]*StudioLayout
	sharedQueries   map[string]string
	queriesExecuted int64
	mu              sync.RWMutex
}

// NewStudioEnhancedEngine creates a new StudioEnhancedEngine instance.
func NewStudioEnhancedEngine(db *DB, cfg StudioEnhancedConfig) *StudioEnhancedEngine {
	return &StudioEnhancedEngine{
		db:            db,
		config:        cfg,
		sessions:      make(map[string]*StudioSession),
		layouts:       make(map[string]*StudioLayout),
		sharedQueries: make(map[string]string),
	}
}

// CreateSession creates a new workspace session for the given user.
func (e *StudioEnhancedEngine) CreateSession(userID string) (*StudioSession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if userID == "" {
		return nil, fmt.Errorf("user ID is required")
	}
	if len(e.sessions) >= e.config.MaxSessions {
		return nil, fmt.Errorf("maximum number of sessions (%d) reached", e.config.MaxSessions)
	}

	now := time.Now()
	s := &StudioSession{
		ID:             fmt.Sprintf("sess_%d", now.UnixNano()),
		UserID:         userID,
		CreatedAt:      now,
		LastActive:     now,
		QueryHistory:   []StudioQueryHistoryEntry{},
		ActiveTab:      "",
		WorkspaceState: make(map[string]string),
	}
	e.sessions[s.ID] = s
	return s, nil
}

// GetSession retrieves a session by ID.
func (e *StudioEnhancedEngine) GetSession(id string) (*StudioSession, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s, ok := e.sessions[id]
	if !ok {
		return nil, fmt.Errorf("session not found: %s", id)
	}
	return s, nil
}

// CloseSession ends and removes a session.
func (e *StudioEnhancedEngine) CloseSession(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.sessions[id]; !ok {
		return fmt.Errorf("session not found: %s", id)
	}
	delete(e.sessions, id)
	return nil
}

// ListSessions returns all active sessions.
func (e *StudioEnhancedEngine) ListSessions() []*StudioSession {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*StudioSession, 0, len(e.sessions))
	for _, s := range e.sessions {
		result = append(result, s)
	}
	return result
}

// ExecuteQuery executes a query within a session and records it in history.
func (e *StudioEnhancedEngine) ExecuteQuery(sessionID, query string) (*StudioQueryResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session not found: %s", sessionID)
	}
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}

	start := time.Now()

	// Simulate query execution against the database
	result := &StudioQueryResult{
		Columns:   []string{"time", "value"},
		Rows:      [][]interface{}{},
		Duration:  time.Since(start),
		RowCount:  0,
		Truncated: false,
	}

	entry := StudioQueryHistoryEntry{
		Query:      query,
		ExecutedAt: start,
		Duration:   result.Duration,
		RowCount:   result.RowCount,
	}

	if len(s.QueryHistory) >= e.config.MaxQueryHistory {
		s.QueryHistory = s.QueryHistory[1:]
	}
	s.QueryHistory = append(s.QueryHistory, entry)
	s.LastActive = time.Now()
	e.queriesExecuted++

	return result, nil
}

// GetQueryHistory returns the query history for a session.
func (e *StudioEnhancedEngine) GetQueryHistory(sessionID string) []StudioQueryHistoryEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil
	}
	return s.QueryHistory
}

// CreateLayout creates a new dashboard layout.
func (e *StudioEnhancedEngine) CreateLayout(name string) (*StudioLayout, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("layout name is required")
	}

	id := fmt.Sprintf("layout_%d", time.Now().UnixNano())
	layout := &StudioLayout{
		ID:      id,
		Name:    name,
		Widgets: []StudioWidget{},
		Columns: 12,
		Rows:    6,
		Theme:   e.config.Theme,
	}
	e.layouts[id] = layout
	return layout, nil
}

// SaveLayout persists an updated layout.
func (e *StudioEnhancedEngine) SaveLayout(layout StudioLayout) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if layout.ID == "" {
		return fmt.Errorf("layout ID is required")
	}
	if _, ok := e.layouts[layout.ID]; !ok {
		return fmt.Errorf("layout not found: %s", layout.ID)
	}
	saved := layout
	e.layouts[layout.ID] = &saved
	return nil
}

// ListLayouts returns all saved layouts.
func (e *StudioEnhancedEngine) ListLayouts() []*StudioLayout {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*StudioLayout, 0, len(e.layouts))
	for _, l := range e.layouts {
		result = append(result, l)
	}
	return result
}

// ShareQuery saves a named query for sharing across sessions.
func (e *StudioEnhancedEngine) ShareQuery(sessionID, name, query string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.sessions[sessionID]; !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	if name == "" {
		return fmt.Errorf("query name is required")
	}
	if query == "" {
		return fmt.Errorf("query is required")
	}
	e.sharedQueries[name] = query
	return nil
}

// ListSharedQueries returns all shared queries.
func (e *StudioEnhancedEngine) ListSharedQueries() map[string]string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make(map[string]string, len(e.sharedQueries))
	for k, v := range e.sharedQueries {
		out[k] = v
	}
	return out
}

// GetAutocompleteSuggestions returns metric name suggestions matching the prefix.
func (e *StudioEnhancedEngine) GetAutocompleteSuggestions(prefix string) []string {
	if !e.config.EnableAutocomplete {
		return nil
	}

	builtins := []string{
		"cpu.usage", "cpu.system", "cpu.user",
		"memory.used", "memory.free", "memory.cached",
		"disk.read", "disk.write", "disk.iops",
		"network.in", "network.out", "network.errors",
	}

	// Merge with actual DB metrics when available
	if e.db != nil {
		builtins = append(builtins, e.db.Metrics()...)
	}

	if prefix == "" {
		return builtins
	}

	lower := strings.ToLower(prefix)
	var matched []string
	for _, m := range builtins {
		if strings.HasPrefix(strings.ToLower(m), lower) {
			matched = append(matched, m)
		}
	}
	return matched
}

// ExportResults exports data in the specified format (csv or json).
func (e *StudioEnhancedEngine) ExportResults(format string, data interface{}) ([]byte, error) {
	if !e.config.EnableDataExport {
		return nil, fmt.Errorf("data export is disabled")
	}

	switch strings.ToLower(format) {
	case "json":
		return json.MarshalIndent(data, "", "  ")
	case "csv":
		rows, ok := data.([][]string)
		if !ok {
			return nil, fmt.Errorf("csv export requires [][]string data")
		}
		var b strings.Builder
		w := csv.NewWriter(&b)
		for _, row := range rows {
			if err := w.Write(row); err != nil {
				return nil, fmt.Errorf("csv write error: %w", err)
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return nil, fmt.Errorf("csv flush error: %w", err)
		}
		return []byte(b.String()), nil
	default:
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}
}

// Stats returns aggregate statistics for the enhanced studio.
func (e *StudioEnhancedEngine) Stats() StudioEnhancedStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return StudioEnhancedStats{
		ActiveSessions:  len(e.sessions),
		TotalLayouts:    len(e.layouts),
		SharedQueries:   len(e.sharedQueries),
		QueriesExecuted: e.queriesExecuted,
	}
}

// RegisterHTTPHandlers registers the enhanced studio HTTP API routes.
func (e *StudioEnhancedEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/studio-enhanced/sessions", e.handleSessions)
	mux.HandleFunc("/api/v1/studio-enhanced/session/", e.handleSession)
	mux.HandleFunc("/api/v1/studio-enhanced/query", e.handleExecuteQuery)
	mux.HandleFunc("/api/v1/studio-enhanced/query-history", e.handleQueryHistory)
	mux.HandleFunc("/api/v1/studio-enhanced/layouts", e.handleLayouts)
	mux.HandleFunc("/api/v1/studio-enhanced/shared-queries", e.handleSharedQueries)
	mux.HandleFunc("/api/v1/studio-enhanced/autocomplete", e.handleAutocomplete)
	mux.HandleFunc("/api/v1/studio-enhanced/export", e.handleExport)
	mux.HandleFunc("/api/v1/studio-enhanced/stats", e.handleStatsAPI)
}

func (e *StudioEnhancedEngine) handleSessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s, err := e.CreateSession(req.UserID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(s)
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListSessions())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *StudioEnhancedEngine) handleSession(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/studio-enhanced/session/")
	if id == "" {
		http.Error(w, "session ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s, err := e.GetSession(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s)
	case http.MethodDelete:
		if err := e.CloseSession(id); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *StudioEnhancedEngine) handleExecuteQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		SessionID string `json:"session_id"`
		Query     string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := e.ExecuteQuery(req.SessionID, req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (e *StudioEnhancedEngine) handleQueryHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := r.URL.Query().Get("session_id")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(e.GetQueryHistory(sessionID))
}

func (e *StudioEnhancedEngine) handleLayouts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		layout, err := e.CreateLayout(req.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(layout)
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListLayouts())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *StudioEnhancedEngine) handleSharedQueries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			SessionID string `json:"session_id"`
			Name      string `json:"name"`
			Query     string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := e.ShareQuery(req.SessionID, req.Name, req.Query); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListSharedQueries())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *StudioEnhancedEngine) handleAutocomplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(e.GetAutocompleteSuggestions(prefix))
}

func (e *StudioEnhancedEngine) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Format string      `json:"format"`
		Data   interface{} `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := e.ExportResults(req.Format, req.Data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(result)
}

func (e *StudioEnhancedEngine) handleStatsAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(e.Stats())
}
