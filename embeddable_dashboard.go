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
	Enabled         bool          `json:"enabled"`
	Title           string        `json:"title"`
	RefreshInterval time.Duration `json:"refresh_interval"`
	MaxPanels       int           `json:"max_panels"`
	Theme           string        `json:"theme"`
	DefaultTimeRange time.Duration `json:"default_time_range"`
	EnableExport    bool          `json:"enable_export"`
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
	ID          string             `json:"id"`
	Title       string             `json:"title"`
	Type        DashboardPanelType `json:"type"`
	Metric      string             `json:"metric"`
	Tags        map[string]string  `json:"tags,omitempty"`
	AggFunc     string             `json:"agg_func,omitempty"`
	TimeRange   time.Duration      `json:"time_range,omitempty"`
	Width       int                `json:"width"`
	Height      int                `json:"height"`
	Position    int                `json:"position"`
	Thresholds  []PanelThreshold   `json:"thresholds,omitempty"`
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
			http.Error(w, err.Error(), http.StatusNotFound)
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
			http.Error(w, err.Error(), http.StatusBadRequest)
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
	dashboardTmpl.Execute(w, map[string]interface{}{
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
