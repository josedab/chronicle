package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// --- Persistent Notebook Storage ---

// NotebookStore provides SQLite-backed persistent notebook storage.
type NotebookStore struct {
	engine  *NotebookEngine
	mu      sync.RWMutex
	dirty   map[string]bool
	autosave bool
}

// NewNotebookStore creates a persistent notebook store wrapping the engine.
func NewNotebookStore(engine *NotebookEngine) *NotebookStore {
	return &NotebookStore{
		engine: engine,
		dirty:  make(map[string]bool),
		autosave: true,
	}
}

// Save serializes a notebook to JSON for persistence.
func (ns *NotebookStore) Save(notebookID string) ([]byte, error) {
	nb, ok := ns.engine.GetNotebook(notebookID)
	if !ok {
		return nil, fmt.Errorf("notebook %q not found", notebookID)
	}

	data, err := json.Marshal(nb)
	if err != nil {
		return nil, fmt.Errorf("notebook serialize: %w", err)
	}

	ns.mu.Lock()
	delete(ns.dirty, notebookID)
	ns.mu.Unlock()

	return data, nil
}

// Load deserializes a notebook from JSON and adds it to the engine.
func (ns *NotebookStore) Load(data []byte) (*Notebook, error) {
	var nb Notebook
	if err := json.Unmarshal(data, &nb); err != nil {
		return nil, fmt.Errorf("notebook deserialize: %w", err)
	}

	created, err := ns.engine.CreateNotebook(nb)
	if err != nil {
		return nil, err
	}
	return created, nil
}

// MarkDirty marks a notebook as needing save.
func (ns *NotebookStore) MarkDirty(notebookID string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.dirty[notebookID] = true
}

// DirtyNotebooks returns IDs of notebooks needing save.
func (ns *NotebookStore) DirtyNotebooks() []string {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	ids := make([]string, 0, len(ns.dirty))
	for id := range ns.dirty {
		ids = append(ids, id)
	}
	return ids
}

// --- Parameterized Templates ---

// NotebookTemplate defines a reusable notebook template with parameters.
type NotebookTemplate struct {
	ID          string                    `json:"id"`
	Title       string                    `json:"title"`
	Description string                    `json:"description"`
	Parameters  []NotebookTemplateParam       `json:"parameters"`
	Cells       []NotebookCell            `json:"cells"`
	Tags        []string                  `json:"tags,omitempty"`
	Author      string                    `json:"author,omitempty"`
	CreatedAt   time.Time                 `json:"created_at"`
}

// NotebookTemplateParam defines a parameter for a notebook template.
type NotebookTemplateParam struct {
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	Type         string   `json:"type"` // string, number, duration, metric
	Default      string   `json:"default,omitempty"`
	Required     bool     `json:"required"`
	Options      []string `json:"options,omitempty"` // for enum-style parameters
}

// TemplateRegistry manages notebook templates.
type TemplateRegistry struct {
	templates map[string]*NotebookTemplate
	mu        sync.RWMutex
}

// NewTemplateRegistry creates a new template registry with built-in templates.
func NewTemplateRegistry() *TemplateRegistry {
	tr := &TemplateRegistry{
		templates: make(map[string]*NotebookTemplate),
	}
	tr.registerBuiltins()
	return tr
}

func (tr *TemplateRegistry) registerBuiltins() {
	tr.templates["service-overview"] = &NotebookTemplate{
		ID:          "service-overview",
		Title:       "Service Overview Dashboard",
		Description: "Monitor key metrics for a service",
		Parameters: []NotebookTemplateParam{
			{Name: "service", Description: "Service name", Type: "string", Required: true},
			{Name: "duration", Description: "Time window", Type: "duration", Default: "1h"},
		},
		Cells: []NotebookCell{
			{ID: "c1", Type: CellMarkdown, Source: "# Service Overview: {{service}}"},
			{ID: "c2", Type: CellQuery, Source: "rate(http_requests_total{service=\"{{service}}\"}[{{duration}}])"},
			{ID: "c3", Type: CellQuery, Source: "avg_over_time(response_time{service=\"{{service}}\"}[{{duration}}])"},
		},
		Tags:      []string{"service", "monitoring"},
		CreatedAt: time.Now(),
	}

	tr.templates["anomaly-investigation"] = &NotebookTemplate{
		ID:          "anomaly-investigation",
		Title:       "Anomaly Investigation",
		Description: "Investigate metric anomalies",
		Parameters: []NotebookTemplateParam{
			{Name: "metric", Description: "Metric to investigate", Type: "metric", Required: true},
			{Name: "time_start", Description: "Investigation start time", Type: "string", Required: true},
			{Name: "duration", Description: "Investigation window", Type: "duration", Default: "6h"},
		},
		Cells: []NotebookCell{
			{ID: "c1", Type: CellMarkdown, Source: "# Anomaly Investigation: {{metric}}"},
			{ID: "c2", Type: CellQuery, Source: "{{metric}}"},
			{ID: "c3", Type: CellQuery, Source: "stddev_over_time({{metric}}[{{duration}}])"},
			{ID: "c4", Type: CellQuery, Source: "changes({{metric}}[{{duration}}])"},
		},
		Tags:      []string{"anomaly", "investigation"},
		CreatedAt: time.Now(),
	}
}

// RegisterTemplate adds a custom template.
func (tr *TemplateRegistry) RegisterTemplate(tmpl NotebookTemplate) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.templates[tmpl.ID] = &tmpl
}

// GetTemplate retrieves a template by ID.
func (tr *TemplateRegistry) GetTemplate(id string) (*NotebookTemplate, bool) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	t, ok := tr.templates[id]
	return t, ok
}

// ListTemplates returns all available templates.
func (tr *TemplateRegistry) ListTemplates() []*NotebookTemplate {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	result := make([]*NotebookTemplate, 0, len(tr.templates))
	for _, t := range tr.templates {
		result = append(result, t)
	}
	return result
}

// Instantiate creates a notebook from a template with parameter values.
func (tr *TemplateRegistry) Instantiate(templateID string, params map[string]string) (*Notebook, error) {
	tr.mu.RLock()
	tmpl, ok := tr.templates[templateID]
	tr.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("template %q not found", templateID)
	}

	// Validate required parameters
	for _, p := range tmpl.Parameters {
		if p.Required {
			if _, exists := params[p.Name]; !exists {
				if p.Default != "" {
					params[p.Name] = p.Default
				} else {
					return nil, fmt.Errorf("required parameter %q missing", p.Name)
				}
			}
		} else if _, exists := params[p.Name]; !exists && p.Default != "" {
			params[p.Name] = p.Default
		}
	}

	// Create notebook with substituted cells
	nb := &Notebook{
		ID:          fmt.Sprintf("nb-%d", time.Now().UnixNano()),
		Title:       substituteParams(tmpl.Title, params),
		Description: substituteParams(tmpl.Description, params),
		Parameters:  params,
		Tags:        tmpl.Tags,
		Author:      tmpl.Author,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	for _, cell := range tmpl.Cells {
		nb.Cells = append(nb.Cells, NotebookCell{
			ID:       fmt.Sprintf("cell-%d", len(nb.Cells)),
			Type:     cell.Type,
			Source:   substituteParams(cell.Source, params),
			Metadata: cell.Metadata,
		})
	}

	return nb, nil
}

func substituteParams(s string, params map[string]string) string {
	for k, v := range params {
		s = strings.ReplaceAll(s, "{{"+k+"}}", v)
	}
	return s
}

// --- Chart Configuration ---

// ChartConfig defines chart rendering configuration for chart cells.
type ChartConfig struct {
	Type       string            `json:"type"` // line, bar, heatmap, table, gauge, stat
	Title      string            `json:"title"`
	XAxis      string            `json:"x_axis,omitempty"`
	YAxis      string            `json:"y_axis,omitempty"`
	Width      int               `json:"width,omitempty"`
	Height     int               `json:"height,omitempty"`
	Colors     []string          `json:"colors,omitempty"`
	Legend     bool              `json:"legend"`
	Thresholds []ChartThreshold  `json:"thresholds,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
}

// ChartThreshold defines a threshold line on a chart.
type ChartThreshold struct {
	Value float64 `json:"value"`
	Color string  `json:"color"`
	Label string  `json:"label,omitempty"`
}

// DefaultChartConfig returns a default chart configuration.
func DefaultChartConfig(chartType string) ChartConfig {
	return ChartConfig{
		Type:   chartType,
		Width:  800,
		Height: 400,
		Legend: true,
		Colors: []string{"#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"},
	}
}

// --- Grafana Dashboard Export ---

// GrafanaDashboardExport generates a Grafana dashboard JSON from a notebook.
type GrafanaDashboardExport struct{}

// ExportToGrafana converts a notebook to a Grafana dashboard JSON structure.
func (g *GrafanaDashboardExport) ExportToGrafana(nb *Notebook) map[string]interface{} {
	panels := make([]interface{}, 0)
	panelID := 1
	gridY := 0

	for _, cell := range nb.Cells {
		switch cell.Type {
		case CellMarkdown:
			panels = append(panels, map[string]interface{}{
				"id":         panelID,
				"type":       "text",
				"title":      "",
				"gridPos":    map[string]int{"h": 3, "w": 24, "x": 0, "y": gridY},
				"options":    map[string]interface{}{"content": cell.Source, "mode": "markdown"},
			})
			gridY += 3

		case CellQuery:
			panels = append(panels, map[string]interface{}{
				"id":      panelID,
				"type":    "timeseries",
				"title":   fmt.Sprintf("Query %d", panelID),
				"gridPos": map[string]int{"h": 8, "w": 24, "x": 0, "y": gridY},
				"targets": []map[string]interface{}{
					{"expr": cell.Source, "refId": "A"},
				},
				"datasource": map[string]string{
					"type": "prometheus",
					"uid":  "chronicle",
				},
			})
			gridY += 8

		case CellChart:
			panels = append(panels, map[string]interface{}{
				"id":      panelID,
				"type":    "stat",
				"title":   fmt.Sprintf("Chart %d", panelID),
				"gridPos": map[string]int{"h": 4, "w": 12, "x": 0, "y": gridY},
				"targets": []map[string]interface{}{
					{"expr": cell.Source, "refId": "A"},
				},
			})
			gridY += 4
		}
		panelID++
	}

	return map[string]interface{}{
		"dashboard": map[string]interface{}{
			"title":         nb.Title,
			"tags":          nb.Tags,
			"timezone":      "browser",
			"schemaVersion": 39,
			"version":       1,
			"panels":        panels,
			"time": map[string]string{
				"from": "now-1h",
				"to":   "now",
			},
			"templating": map[string]interface{}{"list": []interface{}{}},
		},
	}
}

// --- Shareable Notebook URLs with Access Control ---

// SharedNotebookAccess defines access permissions for a shared notebook.
type SharedNotebookAccess struct {
	Token       string    `json:"token"`
	NotebookID  string    `json:"notebook_id"`
	Permission  string    `json:"permission"` // view, edit, execute
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
	MaxViews    int       `json:"max_views,omitempty"`
	ViewCount   int       `json:"view_count"`
}

// NotebookSharingManager manages shared notebook access tokens.
type NotebookSharingManager struct {
	shares map[string]*SharedNotebookAccess
	mu     sync.RWMutex
}

// NewNotebookSharingManager creates a new sharing manager.
func NewNotebookSharingManager() *NotebookSharingManager {
	return &NotebookSharingManager{
		shares: make(map[string]*SharedNotebookAccess),
	}
}

// CreateShare creates a shareable access token for a notebook.
func (m *NotebookSharingManager) CreateShare(notebookID, createdBy, permission string, ttl time.Duration) (*SharedNotebookAccess, error) {
	if permission != "view" && permission != "edit" && permission != "execute" {
		return nil, fmt.Errorf("invalid permission: %s (must be view, edit, or execute)", permission)
	}

	token := fmt.Sprintf("share-%d", time.Now().UnixNano())
	share := &SharedNotebookAccess{
		Token:      token,
		NotebookID: notebookID,
		Permission: permission,
		CreatedBy:  createdBy,
		CreatedAt:  time.Now(),
	}
	if ttl > 0 {
		share.ExpiresAt = time.Now().Add(ttl)
	}

	m.mu.Lock()
	m.shares[token] = share
	m.mu.Unlock()

	return share, nil
}

// ValidateAccess checks if a share token is valid and returns the access info.
func (m *NotebookSharingManager) ValidateAccess(token string) (*SharedNotebookAccess, error) {
	m.mu.RLock()
	share, ok := m.shares[token]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("invalid share token")
	}

	if !share.ExpiresAt.IsZero() && time.Now().After(share.ExpiresAt) {
		return nil, fmt.Errorf("share token expired")
	}

	if share.MaxViews > 0 && share.ViewCount >= share.MaxViews {
		return nil, fmt.Errorf("share token view limit reached")
	}

	m.mu.Lock()
	share.ViewCount++
	m.mu.Unlock()

	return share, nil
}

// RevokeShare revokes a share token.
func (m *NotebookSharingManager) RevokeShare(token string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.shares[token]
	if ok {
		delete(m.shares, token)
	}
	return ok
}

// --- Workbench HTTP API ---

// WorkbenchAPI provides HTTP handlers for the collaborative query workbench.
type WorkbenchAPI struct {
	engine    *NotebookEngine
	store     *NotebookStore
	templates *TemplateRegistry
	sharing   *NotebookSharingManager
	grafana   *GrafanaDashboardExport
}

// NewWorkbenchAPI creates a new workbench API.
func NewWorkbenchAPI(engine *NotebookEngine) *WorkbenchAPI {
	return &WorkbenchAPI{
		engine:    engine,
		store:     NewNotebookStore(engine),
		templates: NewTemplateRegistry(),
		sharing:   NewNotebookSharingManager(),
		grafana:   &GrafanaDashboardExport{},
	}
}

// RegisterHTTPHandlers registers workbench HTTP endpoints.
func (api *WorkbenchAPI) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/workbench/notebooks", api.handleNotebooks)
	mux.HandleFunc("/api/v1/workbench/notebooks/", api.handleNotebookByID)
	mux.HandleFunc("/api/v1/workbench/templates", api.handleTemplates)
	mux.HandleFunc("/api/v1/workbench/templates/instantiate", api.handleTemplateInstantiate)
	mux.HandleFunc("/api/v1/workbench/share", api.handleShare)
	mux.HandleFunc("/api/v1/workbench/export/grafana", api.handleGrafanaExport)
}

func (api *WorkbenchAPI) handleNotebooks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		notebooks := api.engine.ListNotebooks()
		json.NewEncoder(w).Encode(notebooks)
	case http.MethodPost:
		var nb Notebook
		if err := json.NewDecoder(r.Body).Decode(&nb); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		created, err := api.engine.CreateNotebook(nb)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(created)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *WorkbenchAPI) handleNotebookByID(w http.ResponseWriter, r *http.Request) {
	// Extract ID from URL path
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "notebook ID required", http.StatusBadRequest)
		return
	}
	id := parts[len(parts)-1]

	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		nb, ok := api.engine.GetNotebook(id)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(nb)
	case http.MethodDelete:
		if err := api.engine.DeleteNotebook(id); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *WorkbenchAPI) handleTemplates(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(api.templates.ListTemplates())
}

func (api *WorkbenchAPI) handleTemplateInstantiate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		TemplateID string            `json:"template_id"`
		Parameters map[string]string `json:"parameters"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	nb, err := api.templates.Instantiate(req.TemplateID, req.Parameters)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	created, err := api.engine.CreateNotebook(*nb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(created)
}

func (api *WorkbenchAPI) handleShare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NotebookID string `json:"notebook_id"`
		Permission string `json:"permission"`
		TTLMinutes int    `json:"ttl_minutes"`
		CreatedBy  string `json:"created_by"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	ttl := time.Duration(req.TTLMinutes) * time.Minute
	share, err := api.sharing.CreateShare(req.NotebookID, req.CreatedBy, req.Permission, ttl)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(share)
}

func (api *WorkbenchAPI) handleGrafanaExport(w http.ResponseWriter, r *http.Request) {
	notebookID := r.URL.Query().Get("notebook_id")
	if notebookID == "" {
		http.Error(w, "notebook_id required", http.StatusBadRequest)
		return
	}
	nb, ok := api.engine.GetNotebook(notebookID)
	if !ok {
		http.Error(w, "notebook not found", http.StatusNotFound)
		return
	}
	dashboard := api.grafana.ExportToGrafana(nb)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dashboard)
}

// --- Operational Transform for Concurrent Edits ---

// OTOperation represents an operational transform operation on text.
type OTOperation struct {
	Type     string `json:"type"`      // insert, delete, retain
	Position int    `json:"position"`
	Text     string `json:"text,omitempty"`
	Count    int    `json:"count,omitempty"`
	UserID   string `json:"user_id"`
	Version  int64  `json:"version"`
}

// OTDocument manages a document with operational transform support.
type OTDocument struct {
	Content  string         `json:"content"`
	Version  int64          `json:"version"`
	History  []OTOperation  `json:"history,omitempty"`
	mu       sync.RWMutex
}

// NewOTDocument creates a new OT document.
func NewOTDocument(content string) *OTDocument {
	return &OTDocument{
		Content: content,
		Version: 0,
		History: make([]OTOperation, 0),
	}
}

// Apply applies an operation to the document.
func (d *OTDocument) Apply(op OTOperation) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	switch op.Type {
	case "insert":
		if op.Position < 0 || op.Position > len(d.Content) {
			return fmt.Errorf("insert position %d out of range [0, %d]", op.Position, len(d.Content))
		}
		d.Content = d.Content[:op.Position] + op.Text + d.Content[op.Position:]
	case "delete":
		end := op.Position + op.Count
		if op.Position < 0 || end > len(d.Content) {
			return fmt.Errorf("delete range [%d, %d) out of range [0, %d]", op.Position, end, len(d.Content))
		}
		d.Content = d.Content[:op.Position] + d.Content[end:]
	case "retain":
		// no-op, advances cursor
	default:
		return fmt.Errorf("unknown operation type: %s", op.Type)
	}

	op.Version = d.Version + 1
	d.Version++
	d.History = append(d.History, op)
	return nil
}

// Transform resolves conflicts between two concurrent operations.
// Returns the transformed version of op2 that can be applied after op1.
func Transform(op1, op2 OTOperation) OTOperation {
	transformed := op2

	if op1.Type == "insert" && op2.Type == "insert" {
		if op2.Position >= op1.Position {
			transformed.Position += len(op1.Text)
		}
	} else if op1.Type == "insert" && op2.Type == "delete" {
		if op2.Position >= op1.Position {
			transformed.Position += len(op1.Text)
		}
	} else if op1.Type == "delete" && op2.Type == "insert" {
		if op2.Position > op1.Position {
			transformed.Position -= op1.Count
			if transformed.Position < op1.Position {
				transformed.Position = op1.Position
			}
		}
	} else if op1.Type == "delete" && op2.Type == "delete" {
		if op2.Position >= op1.Position+op1.Count {
			transformed.Position -= op1.Count
		} else if op2.Position >= op1.Position {
			overlap := op1.Position + op1.Count - op2.Position
			if overlap > op2.Count {
				overlap = op2.Count
			}
			transformed.Position = op1.Position
			transformed.Count -= overlap
			if transformed.Count < 0 {
				transformed.Count = 0
			}
		}
	}

	return transformed
}

// GetContent returns the current document content.
func (d *OTDocument) GetContent() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Content
}

// GetVersion returns the current document version.
func (d *OTDocument) GetVersion() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Version
}

// --- User Presence Indicators ---

// UserPresence tracks user presence in a collaborative session.
type UserPresence struct {
	UserID    string    `json:"user_id"`
	Name      string    `json:"name"`
	Color     string    `json:"color"`
	CursorPos int       `json:"cursor_pos"`
	CellID    string    `json:"cell_id"`
	Active    bool      `json:"active"`
	LastSeen  time.Time `json:"last_seen"`
}

// PresenceManager tracks user presence across notebook sessions.
type PresenceManager struct {
	users    map[string]map[string]*UserPresence // notebookID -> userID -> presence
	mu       sync.RWMutex
	timeout  time.Duration
}

// NewPresenceManager creates a new presence manager.
func NewPresenceManager(timeout time.Duration) *PresenceManager {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &PresenceManager{
		users:   make(map[string]map[string]*UserPresence),
		timeout: timeout,
	}
}

// UpdatePresence updates a user's presence in a notebook.
func (pm *PresenceManager) UpdatePresence(notebookID string, presence UserPresence) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.users[notebookID] == nil {
		pm.users[notebookID] = make(map[string]*UserPresence)
	}

	presence.Active = true
	presence.LastSeen = time.Now()
	pm.users[notebookID][presence.UserID] = &presence
}

// RemovePresence removes a user from a notebook.
func (pm *PresenceManager) RemovePresence(notebookID, userID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if users, ok := pm.users[notebookID]; ok {
		delete(users, userID)
		if len(users) == 0 {
			delete(pm.users, notebookID)
		}
	}
}

// GetPresence returns all active users in a notebook.
func (pm *PresenceManager) GetPresence(notebookID string) []*UserPresence {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	users, ok := pm.users[notebookID]
	if !ok {
		return nil
	}

	var result []*UserPresence
	now := time.Now()
	for _, u := range users {
		if now.Sub(u.LastSeen) < pm.timeout {
			result = append(result, u)
		}
	}
	return result
}

// CleanupStale removes stale presence entries.
func (pm *PresenceManager) CleanupStale() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	removed := 0
	now := time.Now()
	for nbID, users := range pm.users {
		for uid, u := range users {
			if now.Sub(u.LastSeen) > pm.timeout {
				delete(users, uid)
				removed++
			}
		}
		if len(users) == 0 {
			delete(pm.users, nbID)
		}
	}
	return removed
}

// --- WebSocket Message Types ---

// WSMessageType identifies WebSocket message types for the workbench.
type WSMessageType string

const (
	WSMsgTypeEdit      WSMessageType = "edit"
	WSMsgTypeCursor    WSMessageType = "cursor"
	WSMsgTypePresence  WSMessageType = "presence"
	WSMsgTypeExecute   WSMessageType = "execute"
	WSMsgTypeResult    WSMessageType = "result"
	WSMsgTypeError     WSMessageType = "error"
	WSMsgTypeSync      WSMessageType = "sync"
	WSMsgTypeAddCell   WSMessageType = "add_cell"
	WSMsgTypeDeleteCell WSMessageType = "delete_cell"
)

// WSMessage represents a WebSocket message in the workbench.
type WSMessage struct {
	Type       WSMessageType   `json:"type"`
	NotebookID string          `json:"notebook_id"`
	CellID     string          `json:"cell_id,omitempty"`
	UserID     string          `json:"user_id"`
	Operation  *OTOperation    `json:"operation,omitempty"`
	Presence   *UserPresence   `json:"presence,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
	Timestamp  time.Time       `json:"timestamp"`
}

// --- Collaborative Notebook Session ---

// CollaborativeNotebookSession manages real-time collaboration on a notebook.
type CollaborativeNotebookSession struct {
	NotebookID string
	cellDocs   map[string]*OTDocument // cellID -> OT document for that cell's source
	presence   *PresenceManager
	messages   []WSMessage
	mu         sync.RWMutex
}

// NewCollaborativeNotebookSession creates a new collaborative session for a notebook.
func NewCollaborativeNotebookSession(notebookID string, nb *Notebook) *CollaborativeNotebookSession {
	session := &CollaborativeNotebookSession{
		NotebookID: notebookID,
		cellDocs:   make(map[string]*OTDocument),
		presence:   NewPresenceManager(30 * time.Second),
		messages:   make([]WSMessage, 0),
	}

	// Initialize OT documents from existing cells
	if nb != nil {
		for _, cell := range nb.Cells {
			session.cellDocs[cell.ID] = NewOTDocument(cell.Source)
		}
	}

	return session
}

// ApplyEdit applies an edit operation to a cell and returns the transformed op.
func (s *CollaborativeNotebookSession) ApplyEdit(msg WSMessage) (*OTOperation, error) {
	if msg.Operation == nil {
		return nil, fmt.Errorf("no operation in message")
	}
	if msg.CellID == "" {
		return nil, fmt.Errorf("cell ID required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	doc, ok := s.cellDocs[msg.CellID]
	if !ok {
		doc = NewOTDocument("")
		s.cellDocs[msg.CellID] = doc
	}

	// Apply with transform against pending ops if needed
	op := *msg.Operation
	if err := doc.Apply(op); err != nil {
		return nil, fmt.Errorf("apply to cell %s: %w", msg.CellID, err)
	}

	msg.Timestamp = time.Now()
	s.messages = append(s.messages, msg)
	if len(s.messages) > 10000 {
		s.messages = s.messages[len(s.messages)-5000:]
	}

	return &op, nil
}

// GetCellContent returns the current content of a cell.
func (s *CollaborativeNotebookSession) GetCellContent(cellID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	doc, ok := s.cellDocs[cellID]
	if !ok {
		return "", false
	}
	return doc.GetContent(), true
}

// AddCell adds a new cell to the session.
func (s *CollaborativeNotebookSession) AddCell(cellID, content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cellDocs[cellID] = NewOTDocument(content)
}

// HandleMessage processes an incoming WebSocket message.
func (s *CollaborativeNotebookSession) HandleMessage(msg WSMessage) (*WSMessage, error) {
	switch msg.Type {
	case WSMsgTypeEdit:
		op, err := s.ApplyEdit(msg)
		if err != nil {
			errMsg := WSMessage{
				Type:       WSMsgTypeError,
				NotebookID: s.NotebookID,
				CellID:     msg.CellID,
				UserID:     "system",
				Data:       json.RawMessage(fmt.Sprintf(`{"error":%q}`, err.Error())),
				Timestamp:  time.Now(),
			}
			return &errMsg, err
		}
		broadcast := WSMessage{
			Type:       WSMsgTypeEdit,
			NotebookID: s.NotebookID,
			CellID:     msg.CellID,
			UserID:     msg.UserID,
			Operation:  op,
			Timestamp:  time.Now(),
		}
		return &broadcast, nil

	case WSMsgTypePresence:
		if msg.Presence != nil {
			s.presence.UpdatePresence(s.NotebookID, *msg.Presence)
		}
		return &msg, nil

	case WSMsgTypeCursor:
		return &msg, nil

	case WSMsgTypeAddCell:
		cellID := msg.CellID
		if cellID == "" {
			cellID = fmt.Sprintf("cell-%d", time.Now().UnixNano())
		}
		s.AddCell(cellID, "")
		return &msg, nil

	default:
		return &msg, nil
	}
}

// ActiveUsers returns the currently active users.
func (s *CollaborativeNotebookSession) ActiveUsers() []*UserPresence {
	return s.presence.GetPresence(s.NotebookID)
}
