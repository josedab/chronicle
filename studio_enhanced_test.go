package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewStudioEnhancedEngine(t *testing.T) {
	cfg := DefaultStudioEnhancedConfig()
	engine := NewStudioEnhancedEngine(nil, cfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.config.MaxSessions != 100 {
		t.Errorf("expected MaxSessions 100, got %d", engine.config.MaxSessions)
	}
	if engine.config.Theme != "dark" {
		t.Errorf("expected Theme 'dark', got %q", engine.config.Theme)
	}
	if !engine.config.EnableAutocomplete {
		t.Error("expected EnableAutocomplete to be true")
	}
	stats := engine.Stats()
	if stats.ActiveSessions != 0 {
		t.Errorf("expected 0 active sessions, got %d", stats.ActiveSessions)
	}
}

func TestStudioEnhancedCreateSession(t *testing.T) {
	engine := NewStudioEnhancedEngine(nil, DefaultStudioEnhancedConfig())

	s, err := engine.CreateSession("alice")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}
	if s.ID == "" {
		t.Error("expected non-empty session ID")
	}
	if s.UserID != "alice" {
		t.Errorf("expected UserID 'alice', got %q", s.UserID)
	}
	if s.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}

	// Empty user ID should fail
	_, err = engine.CreateSession("")
	if err == nil {
		t.Error("expected error for empty user ID")
	}

	// Session limit
	cfg := DefaultStudioEnhancedConfig()
	cfg.MaxSessions = 1
	limited := NewStudioEnhancedEngine(nil, cfg)
	_, _ = limited.CreateSession("user1")
	_, err = limited.CreateSession("user2")
	if err == nil {
		t.Error("expected error when max sessions reached")
	}
}

func TestStudioEnhancedExecuteQuery(t *testing.T) {
	engine := NewStudioEnhancedEngine(nil, DefaultStudioEnhancedConfig())
	s, err := engine.CreateSession("alice")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	result, err := engine.ExecuteQuery(s.ID, "SELECT * FROM metrics")
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}
	if len(result.Columns) == 0 {
		t.Error("expected non-empty columns")
	}

	// Empty query should fail
	_, err = engine.ExecuteQuery(s.ID, "")
	if err == nil {
		t.Error("expected error for empty query")
	}

	// Invalid session should fail
	_, err = engine.ExecuteQuery("nonexistent", "SELECT 1")
	if err == nil {
		t.Error("expected error for invalid session")
	}

	stats := engine.Stats()
	if stats.QueriesExecuted != 1 {
		t.Errorf("expected 1 query executed, got %d", stats.QueriesExecuted)
	}
}

func TestStudioEnhancedQueryHistory(t *testing.T) {
	cfg := DefaultStudioEnhancedConfig()
	cfg.MaxQueryHistory = 3
	engine := NewStudioEnhancedEngine(nil, cfg)
	s, _ := engine.CreateSession("alice")

	for i := 0; i < 5; i++ {
		_, _ = engine.ExecuteQuery(s.ID, "SELECT "+string(rune('A'+i)))
	}

	history := engine.GetQueryHistory(s.ID)
	if len(history) != 3 {
		t.Errorf("expected 3 history entries (capped), got %d", len(history))
	}
	// Oldest entries should have been evicted
	if history[0].Query != "SELECT C" {
		t.Errorf("expected first entry 'SELECT C', got %q", history[0].Query)
	}

	// Non-existent session returns nil
	nilHistory := engine.GetQueryHistory("nonexistent")
	if nilHistory != nil {
		t.Error("expected nil for non-existent session history")
	}
}

func TestStudioEnhancedLayout(t *testing.T) {
	engine := NewStudioEnhancedEngine(nil, DefaultStudioEnhancedConfig())

	layout, err := engine.CreateLayout("My Dashboard")
	if err != nil {
		t.Fatalf("CreateLayout failed: %v", err)
	}
	if layout.Name != "My Dashboard" {
		t.Errorf("expected name 'My Dashboard', got %q", layout.Name)
	}
	if layout.Columns != 12 {
		t.Errorf("expected 12 columns, got %d", layout.Columns)
	}

	// Empty name should fail
	_, err = engine.CreateLayout("")
	if err == nil {
		t.Error("expected error for empty layout name")
	}

	// Save updated layout
	layout.Rows = 10
	layout.Widgets = append(layout.Widgets, StudioWidget{
		ID: "w1", Type: "chart", Title: "CPU Usage", Size: 4, Position: 0,
	})
	if err := engine.SaveLayout(*layout); err != nil {
		t.Fatalf("SaveLayout failed: %v", err)
	}

	layouts := engine.ListLayouts()
	if len(layouts) != 1 {
		t.Errorf("expected 1 layout, got %d", len(layouts))
	}

	// Save non-existent layout should fail
	err = engine.SaveLayout(StudioLayout{ID: "missing"})
	if err == nil {
		t.Error("expected error for non-existent layout")
	}

	// Save layout without ID should fail
	err = engine.SaveLayout(StudioLayout{})
	if err == nil {
		t.Error("expected error for empty layout ID")
	}
}

func TestStudioEnhancedSharedQueries(t *testing.T) {
	engine := NewStudioEnhancedEngine(nil, DefaultStudioEnhancedConfig())
	s, _ := engine.CreateSession("alice")

	err := engine.ShareQuery(s.ID, "top_cpu", "SELECT * FROM cpu ORDER BY value DESC LIMIT 10")
	if err != nil {
		t.Fatalf("ShareQuery failed: %v", err)
	}

	shared := engine.ListSharedQueries()
	if len(shared) != 1 {
		t.Errorf("expected 1 shared query, got %d", len(shared))
	}
	if shared["top_cpu"] == "" {
		t.Error("expected shared query 'top_cpu' to exist")
	}

	// Invalid session should fail
	err = engine.ShareQuery("nonexistent", "q", "SELECT 1")
	if err == nil {
		t.Error("expected error for invalid session")
	}

	// Empty name should fail
	err = engine.ShareQuery(s.ID, "", "SELECT 1")
	if err == nil {
		t.Error("expected error for empty query name")
	}

	// Empty query should fail
	err = engine.ShareQuery(s.ID, "name", "")
	if err == nil {
		t.Error("expected error for empty query")
	}
}

func TestStudioEnhancedAutocomplete(t *testing.T) {
	engine := NewStudioEnhancedEngine(nil, DefaultStudioEnhancedConfig())

	// All suggestions for empty prefix
	all := engine.GetAutocompleteSuggestions("")
	if len(all) == 0 {
		t.Error("expected non-empty suggestions for empty prefix")
	}

	// Filter by prefix
	cpuSuggestions := engine.GetAutocompleteSuggestions("cpu")
	for _, s := range cpuSuggestions {
		if !strings.HasPrefix(strings.ToLower(s), "cpu") {
			t.Errorf("suggestion %q does not match prefix 'cpu'", s)
		}
	}
	if len(cpuSuggestions) == 0 {
		t.Error("expected at least one cpu suggestion")
	}

	// Disabled autocomplete returns nil
	cfg := DefaultStudioEnhancedConfig()
	cfg.EnableAutocomplete = false
	disabled := NewStudioEnhancedEngine(nil, cfg)
	if got := disabled.GetAutocompleteSuggestions("cpu"); got != nil {
		t.Errorf("expected nil when autocomplete disabled, got %v", got)
	}
}

func TestStudioEnhancedExportResults(t *testing.T) {
	engine := NewStudioEnhancedEngine(nil, DefaultStudioEnhancedConfig())

	// JSON export
	data := map[string]interface{}{"metric": "cpu", "value": 42}
	result, err := engine.ExportResults("json", data)
	if err != nil {
		t.Fatalf("ExportResults json failed: %v", err)
	}
	if len(result) == 0 {
		t.Error("expected non-empty JSON export")
	}

	// CSV export
	csvData := [][]string{{"time", "value"}, {"2024-01-01", "42"}}
	result, err = engine.ExportResults("csv", csvData)
	if err != nil {
		t.Fatalf("ExportResults csv failed: %v", err)
	}
	if !strings.Contains(string(result), "time,value") {
		t.Errorf("expected CSV header in output, got %q", string(result))
	}

	// Unsupported format
	_, err = engine.ExportResults("xml", data)
	if err == nil {
		t.Error("expected error for unsupported format")
	}

	// CSV with invalid data type
	_, err = engine.ExportResults("csv", "not a slice")
	if err == nil {
		t.Error("expected error for invalid csv data type")
	}

	// Disabled export
	cfg := DefaultStudioEnhancedConfig()
	cfg.EnableDataExport = false
	disabled := NewStudioEnhancedEngine(nil, cfg)
	_, err = disabled.ExportResults("json", data)
	if err == nil {
		t.Error("expected error when export is disabled")
	}
}

func TestStudioEnhancedHTTPHandlers(t *testing.T) {
	engine := NewStudioEnhancedEngine(nil, DefaultStudioEnhancedConfig())
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// POST create session
	body := `{"user_id":"alice"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/studio-enhanced/sessions", strings.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 for create session, got %d: %s", rr.Code, rr.Body.String())
	}
	var session StudioSession
	json.NewDecoder(rr.Body).Decode(&session)
	if session.ID == "" {
		t.Error("expected non-empty session ID in response")
	}

	// GET list sessions
	req = httptest.NewRequest(http.MethodGet, "/api/v1/studio-enhanced/sessions", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	// GET session by ID
	req = httptest.NewRequest(http.MethodGet, "/api/v1/studio-enhanced/session/"+session.ID, nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	// POST execute query
	body = `{"session_id":"` + session.ID + `","query":"SELECT 1"}`
	req = httptest.NewRequest(http.MethodPost, "/api/v1/studio-enhanced/query", strings.NewReader(body))
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for execute query, got %d: %s", rr.Code, rr.Body.String())
	}

	// GET stats
	req = httptest.NewRequest(http.MethodGet, "/api/v1/studio-enhanced/stats", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for stats, got %d", rr.Code)
	}

	// GET autocomplete
	req = httptest.NewRequest(http.MethodGet, "/api/v1/studio-enhanced/autocomplete?prefix=cpu", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for autocomplete, got %d", rr.Code)
	}

	// DELETE session
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/studio-enhanced/session/"+session.ID, nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rr.Code)
	}
}
