package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueryConsole_New(t *testing.T) {
	qc, err := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if qc.Handler() == nil {
		t.Error("expected non-nil handler")
	}
}

func TestQueryConsole_Index(t *testing.T) {
	qc, _ := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	qc.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Chronicle Query Console") {
		t.Error("expected console HTML")
	}
}

func TestQueryConsole_Health(t *testing.T) {
	qc, _ := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	qc.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestQueryConsole_QueryNoDB(t *testing.T) {
	qc, _ := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	body := `{"query":"SELECT * FROM cpu"}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	qc.Handler().ServeHTTP(w, req)

	var resp consoleQueryResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error == "" {
		t.Error("expected error when DB is nil")
	}
}

func TestQueryConsole_QueryEmptyBody(t *testing.T) {
	qc, _ := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	body := `{"query":""}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	qc.Handler().ServeHTTP(w, req)

	var resp consoleQueryResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error == "" {
		t.Error("expected error for empty query")
	}
}

func TestQueryConsole_QueryWrongMethod(t *testing.T) {
	qc, _ := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	req := httptest.NewRequest(http.MethodGet, "/api/query", nil)
	w := httptest.NewRecorder()
	qc.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestQueryConsole_MetricsNoDB(t *testing.T) {
	qc, _ := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	req := httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	w := httptest.NewRecorder()
	qc.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestQueryConsole_NotFound(t *testing.T) {
	qc, _ := NewQueryConsole(nil, DefaultQueryConsoleConfig())
	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	w := httptest.NewRecorder()
	qc.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestQueryConsole_CORS(t *testing.T) {
	cfg := DefaultQueryConsoleConfig()
	cfg.EnableCORS = true
	qc, _ := NewQueryConsole(nil, cfg)

	req := httptest.NewRequest(http.MethodOptions, "/api/health", nil)
	w := httptest.NewRecorder()
	qc.corsMiddleware(qc.mux).ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("expected CORS headers")
	}
	if w.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want 204", w.Code)
	}
}

func TestDefaultQueryConsoleConfig(t *testing.T) {
	cfg := DefaultQueryConsoleConfig()
	if cfg.Bind != ":9090" {
		t.Errorf("bind = %q, want :9090", cfg.Bind)
	}
	if cfg.MaxQueryLen != 8192 {
		t.Errorf("max query len = %d, want 8192", cfg.MaxQueryLen)
	}
}
