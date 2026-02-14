package chronicle

import (
	"testing"
)

func TestDefaultGrafanaPluginMeta(t *testing.T) {
	meta := DefaultGrafanaPluginMeta()
	if meta.ID != "chronicle-datasource" {
		t.Errorf("id = %q", meta.ID)
	}
	if meta.Type != GrafanaPluginDatasource {
		t.Errorf("type = %q", meta.Type)
	}
	if !meta.Backend {
		t.Error("expected backend = true")
	}
	if !meta.Alerting {
		t.Error("expected alerting")
	}
}

func TestDefaultGrafanaDatasourcePluginConfig(t *testing.T) {
	cfg := DefaultGrafanaDatasourcePluginConfig()
	if cfg.URL != "http://localhost:8080" {
		t.Errorf("url = %q", cfg.URL)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("retries = %d", cfg.MaxRetries)
	}
	if cfg.Timeout != 30 {
		t.Errorf("timeout = %d", cfg.Timeout)
	}
}

func TestChronicleOverviewDashboard(t *testing.T) {
	dash := ChronicleOverviewDashboard()
	if dash.Title == "" {
		t.Error("empty title")
	}
	if len(dash.Panels) != 5 {
		t.Errorf("panels = %d, want 5", len(dash.Panels))
	}
	if len(dash.Variables) != 2 {
		t.Errorf("variables = %d, want 2", len(dash.Variables))
	}
	if dash.Refresh != "10s" {
		t.Errorf("refresh = %q", dash.Refresh)
	}

	for _, p := range dash.Panels {
		if p.Title == "" {
			t.Error("panel without title")
		}
		if p.Type == "" {
			t.Error("panel without type")
		}
		if len(p.Targets) == 0 {
			t.Errorf("panel %q has no targets", p.Title)
		}
	}
}

func TestGrafanaPluginQueryHandler_NilDB(t *testing.T) {
	h := NewGrafanaPluginQueryHandler(nil, DefaultGrafanaDatasourcePluginConfig())
	result := h.HandleMetricSearch("")
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestGrafanaMatchesPattern(t *testing.T) {
	tests := []struct {
		s       string
		pattern string
		want    bool
	}{
		{"cpu.usage", "", true},
		{"cpu.usage", "*", true},
		{"cpu.usage", "cpu.*", true},
		{"memory.used", "cpu.*", false},
		{"cpu.usage", "cpu.usage", true},
		{"cpu.usage", "memory.used", false},
	}
	for _, tt := range tests {
		got := grafanaMatchesPattern(tt.s, tt.pattern)
		if got != tt.want {
			t.Errorf("grafanaMatchesPattern(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
		}
	}
}

func TestGrafanaPluginTypes(t *testing.T) {
	if GrafanaPluginDatasource != "datasource" {
		t.Error("wrong datasource type")
	}
	if GrafanaPluginPanel != "panel" {
		t.Error("wrong panel type")
	}
	if GrafanaPluginApp != "app" {
		t.Error("wrong app type")
	}
}

func TestGrafanaPluginMeta_MarshalJSON(t *testing.T) {
	meta := DefaultGrafanaPluginMeta()
	data, err := meta.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty json")
	}
}

func TestGrafanaAnnotationQuery_Fields(t *testing.T) {
	aq := GrafanaAnnotationQuery{
		Name:      "deploys",
		Enable:    true,
		Query:     "tags:deploy",
		IconColor: "green",
	}
	if aq.Name != "deploys" {
		t.Error("wrong name")
	}
	if !aq.Enable {
		t.Error("should be enabled")
	}
}

func TestGrafanaVariable_Fields(t *testing.T) {
	v := GrafanaVariable{
		Name:  "metric",
		Query: "metrics()",
		Type:  "query",
		Multi: true,
	}
	if v.Name != "metric" {
		t.Error("wrong name")
	}
	if !v.Multi {
		t.Error("should be multi")
	}
}
