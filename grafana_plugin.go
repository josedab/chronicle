package chronicle

import (
	"encoding/json"
)

// GrafanaPluginType identifies the Grafana plugin type.
type GrafanaPluginType string

const (
	GrafanaPluginDatasource GrafanaPluginType = "datasource"
	GrafanaPluginPanel      GrafanaPluginType = "panel"
	GrafanaPluginApp        GrafanaPluginType = "app"
)

// GrafanaPluginMeta describes the Chronicle Grafana plugin metadata.
type GrafanaPluginMeta struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Type        GrafanaPluginType `json:"type"`
	Version     string            `json:"version"`
	Description string            `json:"info.description"`
	Author      string            `json:"info.author.name"`
	Logos       GrafanaLogos      `json:"info.logos"`
	Backend     bool              `json:"backend"`
	Executable  string            `json:"executable,omitempty"`
	Alerting    bool              `json:"alerting"`
	Annotations bool              `json:"annotations"`
	Metrics     bool              `json:"metrics"`
}

// GrafanaLogos defines plugin logo paths.
type GrafanaLogos struct {
	Small string `json:"small"`
	Large string `json:"large"`
}

// DefaultGrafanaPluginMeta returns default metadata for the Chronicle datasource plugin.
func DefaultGrafanaPluginMeta() GrafanaPluginMeta {
	return GrafanaPluginMeta{
		ID:          "chronicle-datasource",
		Name:        "Chronicle TSDB",
		Type:        GrafanaPluginDatasource,
		Version:     "1.0.0",
		Description: "Chronicle embedded time-series database datasource",
		Author:      "Chronicle Authors",
		Backend:     true,
		Executable:  "gpx_chronicle-datasource",
		Alerting:    true,
		Annotations: true,
		Metrics:     true,
	}
}

// GrafanaDatasourcePluginConfig defines the datasource connection settings for the plugin.
type GrafanaDatasourcePluginConfig struct {
	URL        string `json:"url"`
	Database   string `json:"database,omitempty"`
	BasicAuth  bool   `json:"basicAuth"`
	TLSEnabled bool   `json:"tlsAuth"`
	MaxRetries int    `json:"maxRetries"`
	Timeout    int    `json:"timeout"` // seconds
}

// DefaultGrafanaDatasourcePluginConfig returns sensible defaults.
func DefaultGrafanaDatasourcePluginConfig() GrafanaDatasourcePluginConfig {
	return GrafanaDatasourcePluginConfig{
		URL:        "http://localhost:8080",
		MaxRetries: 3,
		Timeout:    30,
	}
}

// GrafanaVariable defines a template variable query.
type GrafanaVariable struct {
	Name   string `json:"name"`
	Query  string `json:"query"`
	Type   string `json:"type"` // "query", "custom", "constant"
	Multi  bool   `json:"multi"`
}

// GrafanaAnnotationQuery defines an annotation query.
type GrafanaAnnotationQuery struct {
	Name      string `json:"name"`
	Enable    bool   `json:"enable"`
	Query     string `json:"query"`
	IconColor string `json:"iconColor"`
}

// GrafanaDashboardTemplate defines a pre-built dashboard template.
type GrafanaDashboardTemplate struct {
	Title       string                 `json:"title"`
	Description string                 `json:"description"`
	Tags        []string               `json:"tags"`
	Panels      []GrafanaPanelTemplate `json:"panels"`
	Variables   []GrafanaVariable      `json:"templating"`
	Refresh     string                 `json:"refresh"`
}

// GrafanaPanelTemplate defines a dashboard panel.
type GrafanaPanelTemplate struct {
	Title       string         `json:"title"`
	Type        string         `json:"type"` // "graph", "stat", "table", "gauge", "heatmap"
	GridPos     GrafanaGridPos `json:"gridPos"`
	Targets     []GrafanaQuery `json:"targets"`
	Description string         `json:"description,omitempty"`
}

// GrafanaGridPos defines panel position in the dashboard grid.
type GrafanaGridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// ChronicleOverviewDashboard returns a pre-built overview dashboard template.
func ChronicleOverviewDashboard() GrafanaDashboardTemplate {
	return GrafanaDashboardTemplate{
		Title:       "Chronicle TSDB Overview",
		Description: "Overview of Chronicle database metrics and health",
		Tags:        []string{"chronicle", "tsdb", "overview"},
		Refresh:     "10s",
		Variables: []GrafanaVariable{
			{Name: "metric", Query: "metrics()", Type: "query", Multi: true},
			{Name: "interval", Query: "1m,5m,15m,1h", Type: "custom"},
		},
		Panels: []GrafanaPanelTemplate{
			{
				Title:   "Write Throughput",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "chronicle.writes_per_sec", Aggregation: "rate",
				}},
			},
			{
				Title:   "Query Latency P99",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "chronicle.query_latency_p99",
				}},
			},
			{
				Title:   "Active Series",
				Type:    "stat",
				GridPos: GrafanaGridPos{H: 4, W: 6, X: 0, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "chronicle.active_series", Aggregation: "last",
				}},
			},
			{
				Title:   "Storage Used",
				Type:    "gauge",
				GridPos: GrafanaGridPos{H: 4, W: 6, X: 6, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "chronicle.storage_bytes", Aggregation: "last",
				}},
			},
			{
				Title:   "Compaction Rate",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 4, W: 12, X: 12, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "chronicle.compaction_duration_ms",
				}},
			},
		},
	}
}

// IoTSensorDashboard returns a pre-built IoT sensor monitoring dashboard template.
func IoTSensorDashboard() GrafanaDashboardTemplate {
	return GrafanaDashboardTemplate{
		Title:       "IoT Sensor Dashboard",
		Description: "Real-time monitoring of IoT sensor data including temperature, humidity, and pressure",
		Tags:        []string{"chronicle", "iot", "sensors"},
		Refresh:     "5s",
		Variables: []GrafanaVariable{
			{Name: "sensor", Query: "tag_values(sensor_id)", Type: "query", Multi: true},
			{Name: "location", Query: "tag_values(location)", Type: "query", Multi: true},
		},
		Panels: []GrafanaPanelTemplate{
			{
				Title:   "Temperature",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 8, X: 0, Y: 0},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "sensor.temperature", Aggregation: "mean", Window: "1m",
				}},
				Description: "Temperature readings from IoT sensors in Celsius",
			},
			{
				Title:   "Humidity",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 8, X: 8, Y: 0},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "sensor.humidity", Aggregation: "mean", Window: "1m",
				}},
				Description: "Relative humidity percentage from IoT sensors",
			},
			{
				Title:   "Pressure",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 8, X: 16, Y: 0},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "sensor.pressure", Aggregation: "mean", Window: "1m",
				}},
				Description: "Atmospheric pressure readings in hPa",
			},
			{
				Title:   "Current Temperature",
				Type:    "stat",
				GridPos: GrafanaGridPos{H: 4, W: 8, X: 0, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "sensor.temperature", Aggregation: "last",
				}},
			},
			{
				Title:   "Current Humidity",
				Type:    "gauge",
				GridPos: GrafanaGridPos{H: 4, W: 8, X: 8, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "sensor.humidity", Aggregation: "last",
				}},
			},
			{
				Title:   "Current Pressure",
				Type:    "gauge",
				GridPos: GrafanaGridPos{H: 4, W: 8, X: 16, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "sensor.pressure", Aggregation: "last",
				}},
			},
		},
	}
}

// APIMonitoringDashboard returns a pre-built API monitoring dashboard template.
func APIMonitoringDashboard() GrafanaDashboardTemplate {
	return GrafanaDashboardTemplate{
		Title:       "API Monitoring Dashboard",
		Description: "Monitor API performance including request rate, error rate, and latency",
		Tags:        []string{"chronicle", "api", "monitoring"},
		Refresh:     "10s",
		Variables: []GrafanaVariable{
			{Name: "service", Query: "tag_values(service)", Type: "query", Multi: true},
			{Name: "endpoint", Query: "tag_values(endpoint)", Type: "query", Multi: true},
		},
		Panels: []GrafanaPanelTemplate{
			{
				Title:   "Request Rate",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 12, X: 0, Y: 0},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "api.request_rate", Aggregation: "rate", Window: "1m",
				}},
				Description: "Incoming API requests per second",
			},
			{
				Title:   "Error Rate",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 12, X: 12, Y: 0},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "api.error_rate", Aggregation: "rate", Window: "1m",
				}},
				Description: "API error responses per second",
			},
			{
				Title:   "Latency P99",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 12, X: 0, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "api.latency_p99", Aggregation: "max", Window: "1m",
				}},
				Description: "99th percentile API response latency in milliseconds",
			},
			{
				Title:   "Latency P50",
				Type:    "graph",
				GridPos: GrafanaGridPos{H: 8, W: 12, X: 12, Y: 8},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "api.latency_p50", Aggregation: "mean", Window: "1m",
				}},
				Description: "50th percentile API response latency in milliseconds",
			},
			{
				Title:   "Total Requests",
				Type:    "stat",
				GridPos: GrafanaGridPos{H: 4, W: 8, X: 0, Y: 16},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "api.request_rate", Aggregation: "sum",
				}},
			},
			{
				Title:   "Error Percentage",
				Type:    "gauge",
				GridPos: GrafanaGridPos{H: 4, W: 8, X: 8, Y: 16},
				Targets: []GrafanaQuery{{
					RefID: "A", Metric: "api.error_rate", Aggregation: "last",
				}},
			},
		},
	}
}

// GrafanaPluginQueryHandler translates Grafana queries into Chronicle queries.
type GrafanaPluginQueryHandler struct {
	db     *DB
	config GrafanaDatasourcePluginConfig
}

// NewGrafanaPluginQueryHandler creates a new handler.
func NewGrafanaPluginQueryHandler(db *DB, config GrafanaDatasourcePluginConfig) *GrafanaPluginQueryHandler {
	return &GrafanaPluginQueryHandler{db: db, config: config}
}

// HandleMetricSearch returns available metrics for variable queries.
func (h *GrafanaPluginQueryHandler) HandleMetricSearch(pattern string) []string {
	if h.db == nil {
		return nil
	}
	metrics := h.db.Metrics()
	if pattern == "" {
		return metrics
	}

	var matched []string
	for _, m := range metrics {
		if grafanaMatchesPattern(m, pattern) {
			matched = append(matched, m)
		}
	}
	return matched
}

func grafanaMatchesPattern(s, pattern string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(s) >= len(prefix) && s[:len(prefix)] == prefix
	}
	return s == pattern
}

// MarshalJSON implements JSON marshaling for GrafanaPluginMeta.
func (m GrafanaPluginMeta) MarshalJSON() ([]byte, error) {
	type alias GrafanaPluginMeta
	return json.Marshal((alias)(m))
}
