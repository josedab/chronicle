package chronicle

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sync"
	"time"
)

// DashboardConfig configures the embeddable dashboard component.
type DashboardConfig struct {
	Enabled          bool          `json:"enabled"`
	Title            string        `json:"title"`
	RefreshInterval  time.Duration `json:"refresh_interval"`
	MaxPanels        int           `json:"max_panels"`
	Theme            string        `json:"theme"`
	DefaultTimeRange time.Duration `json:"default_time_range"`
	EnableExport     bool          `json:"enable_export"`
}

// DefaultDashboardConfig returns sensible defaults for the dashboard.
func DefaultDashboardConfig() DashboardConfig {
	return DashboardConfig{
		Enabled:          true,
		Title:            "Chronicle Dashboard",
		RefreshInterval:  10 * time.Second,
		MaxPanels:        20,
		Theme:            "light",
		DefaultTimeRange: time.Hour,
		EnableExport:     true,
	}
}

// DashboardPanelType defines the type of visualization panel.
type DashboardPanelType int

const (
	PanelTypeLine DashboardPanelType = iota
	PanelTypeArea
	PanelTypeBar
	PanelTypeStat
	PanelTypeTable
	PanelTypeHeatmap
	PanelTypeHistogram
	PanelTypeGauge
	PanelTypeSparkline
)

// String returns the string representation of PanelType.
func (pt DashboardPanelType) String() string {
	switch pt {
	case PanelTypeLine:
		return "line"
	case PanelTypeArea:
		return "area"
	case PanelTypeBar:
		return "bar"
	case PanelTypeStat:
		return "stat"
	case PanelTypeTable:
		return "table"
	case PanelTypeHeatmap:
		return "heatmap"
	case PanelTypeHistogram:
		return "histogram"
	case PanelTypeGauge:
		return "gauge"
	case PanelTypeSparkline:
		return "sparkline"
	default:
		return "line"
	}
}

// DashboardPanel represents a single panel in the dashboard.
type DashboardPanel struct {
	ID         string             `json:"id"`
	Title      string             `json:"title"`
	Type       DashboardPanelType `json:"type"`
	Metric     string             `json:"metric"`
	Tags       map[string]string  `json:"tags,omitempty"`
	AggFunc    string             `json:"agg_func,omitempty"`
	TimeRange  time.Duration      `json:"time_range,omitempty"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	Position   int                `json:"position"`
	Thresholds []PanelThreshold   `json:"thresholds,omitempty"`
}

// PanelThreshold defines a visual threshold on a panel.
type PanelThreshold struct {
	Value float64 `json:"value"`
	Color string  `json:"color"`
	Label string  `json:"label"`
}

// DashboardLayout holds the complete dashboard layout configuration.
type DashboardLayout struct {
	Title       string           `json:"title"`
	Description string           `json:"description"`
	Panels      []DashboardPanel `json:"panels"`
	Theme       string           `json:"theme"`
	Created     time.Time        `json:"created"`
	Updated     time.Time        `json:"updated"`
}

// EmbeddableDashboard is an embeddable web dashboard for Chronicle.
type EmbeddableDashboard struct {
	db      *DB
	config  DashboardConfig
	layouts map[string]*DashboardLayout
	mu      sync.RWMutex
}

// NewEmbeddableDashboard creates a new embeddable dashboard.
func NewEmbeddableDashboard(db *DB, cfg DashboardConfig) *EmbeddableDashboard {
	d := &EmbeddableDashboard{
		db:      db,
		config:  cfg,
		layouts: make(map[string]*DashboardLayout),
	}
	// Create default layout
	d.layouts["default"] = &DashboardLayout{
		Title:   cfg.Title,
		Theme:   cfg.Theme,
		Created: time.Now(),
		Updated: time.Now(),
	}
	return d
}

// AddPanel adds a panel to a dashboard layout.
func (d *EmbeddableDashboard) AddPanel(layoutID string, panel DashboardPanel) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	layout, exists := d.layouts[layoutID]
	if !exists {
		return fmt.Errorf("dashboard: layout %q not found", layoutID)
	}
	if len(layout.Panels) >= d.config.MaxPanels {
		return fmt.Errorf("dashboard: max panels (%d) reached", d.config.MaxPanels)
	}

	if panel.ID == "" {
		panel.ID = fmt.Sprintf("panel-%d", len(layout.Panels)+1)
	}
	panel.Position = len(layout.Panels)
	layout.Panels = append(layout.Panels, panel)
	layout.Updated = time.Now()
	return nil
}

// RemovePanel removes a panel from a layout.
func (d *EmbeddableDashboard) RemovePanel(layoutID, panelID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	layout, exists := d.layouts[layoutID]
	if !exists {
		return fmt.Errorf("dashboard: layout %q not found", layoutID)
	}

	for i, p := range layout.Panels {
		if p.ID == panelID {
			layout.Panels = append(layout.Panels[:i], layout.Panels[i+1:]...)
			layout.Updated = time.Now()
			return nil
		}
	}
	return fmt.Errorf("dashboard: panel %q not found", panelID)
}

// GetLayout returns a dashboard layout.
func (d *EmbeddableDashboard) GetLayout(layoutID string) (*DashboardLayout, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	layout, exists := d.layouts[layoutID]
	if !exists {
		return nil, fmt.Errorf("dashboard: layout %q not found", layoutID)
	}
	return layout, nil
}

// CreateLayout creates a new dashboard layout.
func (d *EmbeddableDashboard) CreateLayout(id, title, description string) *DashboardLayout {
	d.mu.Lock()
	defer d.mu.Unlock()

	layout := &DashboardLayout{
		Title:       title,
		Description: description,
		Theme:       d.config.Theme,
		Created:     time.Now(),
		Updated:     time.Now(),
	}
	d.layouts[id] = layout
	return layout
}

// ListLayouts returns all dashboard layouts.
func (d *EmbeddableDashboard) ListLayouts() map[string]*DashboardLayout {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]*DashboardLayout, len(d.layouts))
	for k, v := range d.layouts {
		out[k] = v
	}
	return out
}

// AutoDiscover automatically creates panels from database metrics.
func (d *EmbeddableDashboard) AutoDiscover() []DashboardPanel {
	metrics := d.db.Metrics()
	var panels []DashboardPanel

	for i, metric := range metrics {
		if i >= d.config.MaxPanels {
			break
		}
		panels = append(panels, DashboardPanel{
			ID:        fmt.Sprintf("auto-%d", i),
			Title:     metric,
			Type:      PanelTypeLine,
			Metric:    metric,
			TimeRange: d.config.DefaultTimeRange,
			Width:     6,
			Height:    4,
			Position:  i,
		})
	}
	return panels
}

// RegisterHTTPHandlers registers dashboard HTTP endpoints.
func (d *EmbeddableDashboard) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/dashboard/layouts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.ListLayouts())
	})

	mux.HandleFunc("/api/v1/dashboard/layout", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			id = "default"
		}
		layout, err := d.GetLayout(id)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(layout)
	})

	mux.HandleFunc("/api/v1/dashboard/discover", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.AutoDiscover())
	})

	mux.HandleFunc("/api/v1/dashboard/panel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var panel DashboardPanel
		if err := json.NewDecoder(r.Body).Decode(&panel); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		layoutID := r.URL.Query().Get("layout")
		if layoutID == "" {
			layoutID = "default"
		}
		if err := d.AddPanel(layoutID, panel); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		d.serveDashboardHTML(w, r)
	})
}

func (d *EmbeddableDashboard) serveDashboardHTML(w http.ResponseWriter, r *http.Request) {
	layout, _ := d.GetLayout("default")

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	dashboardTmpl.Execute(w, map[string]any{
		"Title":           d.config.Title,
		"Theme":           d.config.Theme,
		"RefreshInterval": int(d.config.RefreshInterval.Seconds()),
		"Layout":          layout,
		"Metrics":         d.db.Metrics(),
	})
}

var dashboardTmpl = template.Must(template.New("dashboard").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}}</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; }
.header { background: #1a1a2e; color: white; padding: 16px 24px; display: flex; justify-content: space-between; align-items: center; }
.header h1 { font-size: 20px; font-weight: 600; }
.header .badge { background: #0f3460; padding: 4px 12px; border-radius: 12px; font-size: 12px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(400px, 1fr)); gap: 16px; padding: 24px; }
.panel { background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 16px; }
.panel h3 { font-size: 14px; color: #666; margin-bottom: 8px; }
.panel .value { font-size: 32px; font-weight: 700; color: #1a1a2e; }
.panel .chart { height: 120px; background: #f8f9fa; border-radius: 4px; display: flex; align-items: end; padding: 8px; gap: 2px; }
.panel .bar { background: #4361ee; border-radius: 2px 2px 0 0; flex: 1; min-height: 4px; }
.metrics-list { padding: 24px; }
.metrics-list h2 { margin-bottom: 12px; }
.metric-item { padding: 8px 12px; background: white; margin-bottom: 4px; border-radius: 4px; font-family: monospace; }
</style>
</head>
<body>
<div class="header">
  <h1>{{.Title}}</h1>
  <span class="badge">Auto-refresh: {{.RefreshInterval}}s</span>
</div>
<div class="grid" id="panels">
  {{range .Metrics}}
  <div class="panel">
    <h3>{{.}}</h3>
    <div class="chart">
      <div class="bar" style="height:40%"></div>
      <div class="bar" style="height:60%"></div>
      <div class="bar" style="height:45%"></div>
      <div class="bar" style="height:80%"></div>
      <div class="bar" style="height:55%"></div>
      <div class="bar" style="height:70%"></div>
      <div class="bar" style="height:65%"></div>
      <div class="bar" style="height:90%"></div>
    </div>
  </div>
  {{end}}
</div>
<script>
setInterval(function(){location.reload()}, {{.RefreshInterval}}*1000);
</script>
</body>
</html>`))

// --- Declarative Dashboard Configuration ---

// DeclarativeDashboard allows defining dashboards through JSON/YAML configuration
// that can be version-controlled and applied programmatically.
type DeclarativeDashboard struct {
	APIVersion string                    `json:"api_version"`
	Kind       string                    `json:"kind"`
	Metadata   DeclarativeDashboardMeta  `json:"metadata"`
	Spec       DeclarativeDashboardSpec  `json:"spec"`
}

// DeclarativeDashboardMeta holds dashboard metadata.
type DeclarativeDashboardMeta struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// DeclarativeDashboardSpec is the dashboard specification.
type DeclarativeDashboardSpec struct {
	Title           string                `json:"title"`
	Description     string                `json:"description,omitempty"`
	RefreshInterval string                `json:"refresh_interval,omitempty"`
	TimeRange       string                `json:"time_range,omitempty"`
	Theme           DashboardTheme        `json:"theme"`
	Variables       []DashboardVariable   `json:"variables,omitempty"`
	Rows            []DashboardRow        `json:"rows"`
	DataSources     []DashboardDataSource `json:"data_sources,omitempty"`
}

// DashboardTheme defines the visual theme.
type DashboardTheme struct {
	Mode       string            `json:"mode"` // "light" or "dark"
	Colors     map[string]string `json:"colors,omitempty"`
	FontFamily string            `json:"font_family,omitempty"`
}

// DashboardVariable defines a template variable for dynamic dashboards.
type DashboardVariable struct {
	Name    string   `json:"name"`
	Label   string   `json:"label"`
	Type    string   `json:"type"` // "query", "custom", "interval"
	Query   string   `json:"query,omitempty"`
	Options []string `json:"options,omitempty"`
	Default string   `json:"default,omitempty"`
}

// DashboardRow is a row of panels in the dashboard grid.
type DashboardRow struct {
	Title     string           `json:"title,omitempty"`
	Collapsed bool             `json:"collapsed,omitempty"`
	Height    string           `json:"height,omitempty"`
	Panels    []DashboardPanel `json:"panels"`
}

// DashboardDataSource defines where panel data comes from.
type DashboardDataSource struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "chronicle", "prometheus", "graphql"
	URL      string `json:"url"`
	Default  bool   `json:"default,omitempty"`
}

// ApplyDeclarative creates or updates a dashboard from a declarative configuration.
func (d *EmbeddableDashboard) ApplyDeclarative(decl *DeclarativeDashboard) error {
	if decl == nil {
		return fmt.Errorf("nil declarative dashboard")
	}
	if decl.Metadata.Name == "" {
		return fmt.Errorf("dashboard name is required")
	}

	layout := d.CreateLayout(
		decl.Metadata.Name,
		decl.Spec.Title,
		decl.Spec.Description,
	)

	if decl.Spec.Theme.Mode != "" {
		layout.Theme = decl.Spec.Theme.Mode
	}

	for _, row := range decl.Spec.Rows {
		for _, panel := range row.Panels {
			if err := d.AddPanel(decl.Metadata.Name, panel); err != nil {
				return fmt.Errorf("add panel %s: %w", panel.ID, err)
			}
		}
	}

	return nil
}

// ExportDeclarative exports a dashboard layout as a declarative configuration.
func (d *EmbeddableDashboard) ExportDeclarative(layoutID string) (*DeclarativeDashboard, error) {
	layout, err := d.GetLayout(layoutID)
	if err != nil {
		return nil, err
	}

	return &DeclarativeDashboard{
		APIVersion: "chronicle.dev/v1",
		Kind:       "Dashboard",
		Metadata: DeclarativeDashboardMeta{
			Name: layoutID,
		},
		Spec: DeclarativeDashboardSpec{
			Title:       layout.Title,
			Description: layout.Description,
			Theme:       DashboardTheme{Mode: layout.Theme},
			Rows: []DashboardRow{
				{Panels: layout.Panels},
			},
		},
	}, nil
}

// --- WebSocket Data Binding ---

// DashboardWSMessage represents a WebSocket message for real-time data updates.
type DashboardWSMessage struct {
	Type      string      `json:"type"` // "data", "config", "error"
	PanelID   string      `json:"panel_id,omitempty"`
	Metric    string      `json:"metric,omitempty"`
	Timestamp int64       `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

// DashboardWSSubscription tracks a client's panel subscriptions.
type DashboardWSSubscription struct {
	PanelID  string        `json:"panel_id"`
	Metric   string        `json:"metric"`
	Interval time.Duration `json:"interval"`
}

// DashboardDataBinding manages REST/WebSocket data bindings for panels.
type DashboardDataBinding struct {
	db            *DB
	subscriptions map[string][]DashboardWSSubscription
	mu            sync.RWMutex
}

// NewDashboardDataBinding creates a new data binding manager.
func NewDashboardDataBinding(db *DB) *DashboardDataBinding {
	return &DashboardDataBinding{
		db:            db,
		subscriptions: make(map[string][]DashboardWSSubscription),
	}
}

// Subscribe registers a client subscription for a panel's data.
func (b *DashboardDataBinding) Subscribe(clientID string, sub DashboardWSSubscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscriptions[clientID] = append(b.subscriptions[clientID], sub)
}

// Unsubscribe removes all subscriptions for a client.
func (b *DashboardDataBinding) Unsubscribe(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subscriptions, clientID)
}

// GetPanelData fetches the current data for a panel's metric.
func (b *DashboardDataBinding) GetPanelData(panel DashboardPanel, timeRange time.Duration) ([]Point, error) {
	now := time.Now()
	q := &Query{
		Metric: panel.Metric,
		Tags:   panel.Tags,
		Start:  now.Add(-timeRange).UnixNano(),
		End:    now.UnixNano(),
	}

	result, err := b.db.Execute(q)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}

	points := make([]Point, len(result.Points))
	for i, rp := range result.Points {
		points[i] = Point{
			Metric:    panel.Metric,
			Value:     rp.Value,
			Timestamp: rp.Timestamp,
		}
	}
	return points, nil
}

// --- Web Component Wrapper ---

// WebComponentConfig configures the Web Component output for framework-agnostic embedding.
type WebComponentConfig struct {
	TagName    string `json:"tag_name"`    // e.g., "chronicle-chart"
	ShadowDOM  bool   `json:"shadow_dom"`
	Attributes []string `json:"attributes"` // observed attributes
}

// GenerateWebComponentJS generates a JavaScript Web Component wrapper
// that can be used in any framework (React, Vue, Angular, plain HTML).
func GenerateWebComponentJS(config WebComponentConfig) string {
	if config.TagName == "" {
		config.TagName = "chronicle-dashboard"
	}

	return fmt.Sprintf(`
class %sElement extends HTMLElement {
  static get observedAttributes() {
    return ['src', 'metric', 'time-range', 'refresh', 'theme'];
  }

  constructor() {
    super();
    this._shadow = this.attachShadow({ mode: 'open' });
    this._data = [];
    this._interval = null;
  }

  connectedCallback() {
    this.render();
    const refresh = parseInt(this.getAttribute('refresh') || '10000');
    this._interval = setInterval(() => this.fetchData(), refresh);
    this.fetchData();
  }

  disconnectedCallback() {
    if (this._interval) clearInterval(this._interval);
  }

  attributeChangedCallback(name, oldVal, newVal) {
    if (oldVal !== newVal) this.fetchData();
  }

  async fetchData() {
    const src = this.getAttribute('src') || '/api/v1/query';
    const metric = this.getAttribute('metric');
    if (!metric) return;

    try {
      const resp = await fetch(src, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ metric: metric })
      });
      this._data = await resp.json();
      this.render();
    } catch (e) {
      console.error('%s: fetch error', e);
    }
  }

  render() {
    const theme = this.getAttribute('theme') || 'light';
    const bg = theme === 'dark' ? '#1a1a2e' : '#ffffff';
    const fg = theme === 'dark' ? '#ffffff' : '#333333';

    this._shadow.innerHTML = ` + "`" + `
      <style>
        :host { display: block; font-family: system-ui, sans-serif; }
        .container { background: ${bg}; color: ${fg}; border-radius: 8px;
                     padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .title { font-size: 14px; opacity: 0.7; margin-bottom: 8px; }
        .value { font-size: 28px; font-weight: 700; }
        .chart { height: 80px; display: flex; align-items: flex-end; gap: 2px; margin-top: 8px; }
        .bar { flex: 1; background: #4361ee; border-radius: 2px 2px 0 0; min-height: 2px; }
      </style>
      <div class="container">
        <div class="title">${this.getAttribute('metric') || 'No metric'}</div>
        <div class="value">${this._data.length ? this._data[this._data.length-1]?.value?.toFixed(2) || '--' : '--'}</div>
        <div class="chart">
          ${(this._data.slice(-20) || []).map(d =>
            '<div class="bar" style="height:' + Math.max(5, (d.value/Math.max(...this._data.map(x=>x.value||1))*100)) + '%%"></div>'
          ).join('')}
        </div>
      </div>
    ` + "`" + `;
  }
}

customElements.define('%s', %sElement);
`, toClassName(config.TagName), config.TagName, config.TagName, toClassName(config.TagName))
}

func toClassName(tagName string) string {
	result := ""
	upper := true
	for _, ch := range tagName {
		if ch == '-' {
			upper = true
			continue
		}
		if upper {
			if ch >= 'a' && ch <= 'z' {
				ch -= 32
			}
			upper = false
		}
		result += string(ch)
	}
	return result
}