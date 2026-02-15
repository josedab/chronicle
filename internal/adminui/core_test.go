package adminui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// mockDB implements AdminDB for testing.
type mockDB struct {
	metrics []string
}

func (m *mockDB) Metrics() []string                             { return m.metrics }
func (m *mockDB) Execute(_ *Query) (*Result, error)             { return &Result{}, nil }
func (m *mockDB) WriteBatch(_ []Point) error                    { return nil }
func (m *mockDB) Flush() error                                  { return nil }
func (m *mockDB) GetSchema(_ string) *MetricSchema              { return nil }
func (m *mockDB) Info() DBInfo                                  { return DBInfo{Path: "/tmp/test"} }
func (m *mockDB) IsClosed() bool                                { return false }
func (m *mockDB) ParseQuery(_ string) (*Query, error)           { return &Query{}, nil }

func newTestUI() *AdminUI {
	return NewAdminUI(&mockDB{metrics: []string{"cpu", "mem"}}, AdminConfig{Prefix: "/admin", DevMode: true})
}

func TestNewAdminUI_DefaultPrefix(t *testing.T) {
	ui := NewAdminUI(&mockDB{}, AdminConfig{})
	if ui == nil {
		t.Fatal("expected non-nil AdminUI")
	}
}

func TestNewAdminUI_CustomPrefix(t *testing.T) {
	ui := NewAdminUI(&mockDB{}, AdminConfig{Prefix: "/mgmt"})
	if ui == nil {
		t.Fatal("expected non-nil AdminUI")
	}
}

func TestAdminUI_Handler(t *testing.T) {
	ui := newTestUI()
	h := ui.Handler()
	if h == nil {
		t.Fatal("expected non-nil http.Handler")
	}
}

func TestAdminUI_ServeHTTP_Dashboard(t *testing.T) {
	ui := newTestUI()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	ui.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestAdminUI_APIStats(t *testing.T) {
	ui := newTestUI()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/api/stats", nil)
	ui.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected application/json, got %s", ct)
	}
}

func TestAdminUI_APIHealth(t *testing.T) {
	ui := newTestUI()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/api/health", nil)
	ui.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if _, ok := resp["status"]; !ok {
		t.Error("expected 'status' key in health response")
	}
}

func TestAdminUI_APIMetrics(t *testing.T) {
	ui := newTestUI()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/api/metrics", nil)
	ui.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestLogActivity(t *testing.T) {
	ui := newTestUI()
	ui.logActivity("test_action", "some details")
	ui.mu.RLock()
	defer ui.mu.RUnlock()
	if len(ui.activityLog) != 1 {
		t.Fatalf("expected 1 activity entry, got %d", len(ui.activityLog))
	}
	if ui.activityLog[0].Action != "test_action" {
		t.Errorf("unexpected action: %s", ui.activityLog[0].Action)
	}
}

func TestAddQueryHistory(t *testing.T) {
	ui := newTestUI()
	ui.addQueryHistory("SELECT *", 0, true, "")
	ui.mu.RLock()
	defer ui.mu.RUnlock()
	if len(ui.queryHistory) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(ui.queryHistory))
	}
	if ui.queryHistory[0].Query != "SELECT *" {
		t.Errorf("unexpected query: %s", ui.queryHistory[0].Query)
	}
}

func TestLogActivity_CapAt100(t *testing.T) {
	ui := newTestUI()
	for i := 0; i < 105; i++ {
		ui.logActivity("action", "detail")
	}
	ui.mu.RLock()
	defer ui.mu.RUnlock()
	if len(ui.activityLog) > 100 {
		t.Errorf("expected <= 100 entries, got %d", len(ui.activityLog))
	}
}
