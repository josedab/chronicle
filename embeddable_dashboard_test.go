package chronicle

import (
	"testing"
	"time"
)

func TestDashboardConfig(t *testing.T) {
	cfg := DefaultDashboardConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.MaxPanels != 20 {
		t.Errorf("expected 20 max panels, got %d", cfg.MaxPanels)
	}
	if cfg.Theme != "light" {
		t.Errorf("expected light theme, got %s", cfg.Theme)
	}
}

func TestDashboardCreateAndLayout(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dashboard := NewEmbeddableDashboard(db, DefaultDashboardConfig())

	// Default layout should exist
	layout, err := dashboard.GetLayout("default")
	if err != nil {
		t.Fatalf("default layout not found: %v", err)
	}
	if layout.Title != "Chronicle Dashboard" {
		t.Errorf("expected default title, got %q", layout.Title)
	}
}

func TestDashboardAddPanel(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dashboard := NewEmbeddableDashboard(db, DefaultDashboardConfig())

	err := dashboard.AddPanel("default", DashboardPanel{
		ID:     "cpu-panel",
		Title:  "CPU Usage",
		Type:   PanelTypeLine,
		Metric: "cpu_usage",
		Width:  6,
		Height: 4,
	})
	if err != nil {
		t.Fatalf("add panel failed: %v", err)
	}

	layout, _ := dashboard.GetLayout("default")
	if len(layout.Panels) != 1 {
		t.Errorf("expected 1 panel, got %d", len(layout.Panels))
	}
	if layout.Panels[0].ID != "cpu-panel" {
		t.Errorf("expected panel ID cpu-panel, got %q", layout.Panels[0].ID)
	}
}

func TestDashboardRemovePanel(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dashboard := NewEmbeddableDashboard(db, DefaultDashboardConfig())
	dashboard.AddPanel("default", DashboardPanel{ID: "panel-1", Title: "P1", Metric: "m1"})
	dashboard.AddPanel("default", DashboardPanel{ID: "panel-2", Title: "P2", Metric: "m2"})

	err := dashboard.RemovePanel("default", "panel-1")
	if err != nil {
		t.Fatalf("remove panel failed: %v", err)
	}

	layout, _ := dashboard.GetLayout("default")
	if len(layout.Panels) != 1 {
		t.Errorf("expected 1 panel after removal, got %d", len(layout.Panels))
	}
}

func TestDashboardMaxPanels(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultDashboardConfig()
	cfg.MaxPanels = 2
	dashboard := NewEmbeddableDashboard(db, cfg)

	dashboard.AddPanel("default", DashboardPanel{Title: "P1", Metric: "m1"})
	dashboard.AddPanel("default", DashboardPanel{Title: "P2", Metric: "m2"})
	err := dashboard.AddPanel("default", DashboardPanel{Title: "P3", Metric: "m3"})
	if err == nil {
		t.Error("expected error when exceeding max panels")
	}
}

func TestDashboardCreateLayout(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dashboard := NewEmbeddableDashboard(db, DefaultDashboardConfig())
	layout := dashboard.CreateLayout("custom", "Custom Dashboard", "A custom view")

	if layout.Title != "Custom Dashboard" {
		t.Errorf("expected Custom Dashboard, got %q", layout.Title)
	}

	layouts := dashboard.ListLayouts()
	if len(layouts) != 2 { // default + custom
		t.Errorf("expected 2 layouts, got %d", len(layouts))
	}
}

func TestDashboardAutoDiscover(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some metrics
	now := time.Now()
	db.Write(Point{Metric: "cpu", Value: 50, Timestamp: now.UnixNano(), Tags: map[string]string{"h": "a"}})
	db.Write(Point{Metric: "mem", Value: 70, Timestamp: now.UnixNano(), Tags: map[string]string{"h": "a"}})
	db.Flush()

	dashboard := NewEmbeddableDashboard(db, DefaultDashboardConfig())
	panels := dashboard.AutoDiscover()

	if len(panels) < 2 {
		t.Errorf("expected at least 2 auto-discovered panels, got %d", len(panels))
	}
}

func TestDashboardPanelTypeString(t *testing.T) {
	tests := []struct {
		pt   DashboardPanelType
		want string
	}{
		{PanelTypeLine, "line"},
		{PanelTypeArea, "area"},
		{PanelTypeStat, "stat"},
		{PanelTypeTable, "table"},
		{PanelTypeHeatmap, "heatmap"},
		{PanelTypeGauge, "gauge"},
	}
	for _, tt := range tests {
		if got := tt.pt.String(); got != tt.want {
			t.Errorf("PanelType(%d).String() = %q, want %q", tt.pt, got, tt.want)
		}
	}
}

func TestDashboardGetLayoutNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	dashboard := NewEmbeddableDashboard(db, DefaultDashboardConfig())
	_, err := dashboard.GetLayout("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent layout")
	}
}
