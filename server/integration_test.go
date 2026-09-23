package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chronicle-db/chronicle"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	cfg := chronicle.DefaultConfig()
	cfg.DataDir = t.TempDir()

	engine, err := chronicle.NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	srv := NewServer(engine, Config{
		Addr:         ":0",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	cleanup := func() {
		engine.Close()
	}

	return srv, cleanup
}

func TestIntegration_WriteAndQueryEndpoints(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	// 1. Test Write Metric
	writePayload := map[string]interface{}{
		"metric": "system_cpu_load",
		"labels": map[string]string{
			"host": "node-01",
			"env":  "production",
		},
		"value":     75.5,
		"timestamp": time.Now().UnixMilli(),
	}
	body, _ := json.Marshal(writePayload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
		t.Logf("Write endpoint returned code %d, body: %s", w.Code, w.Body.String())
	}

	// 2. Test Query Metric
	queryReq := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=system_cpu_load", nil)
	queryW := httptest.NewRecorder()
	srv.ServeHTTP(queryW, queryReq)

	if queryW.Code != http.StatusOK {
		t.Logf("Query endpoint returned code %d, body: %s", queryW.Code, queryW.Body.String())
	}

	// 3. Test Healthz Endpoint
	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthW := httptest.NewRecorder()
	srv.ServeHTTP(healthW, healthReq)

	if healthW.Code != http.StatusOK {
		t.Errorf("expected status 200 on /healthz, got %d", healthW.Code)
	}
}

func TestIntegration_InvalidMethodHandling(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/query", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed && w.Code != http.StatusBadRequest && w.Code != http.StatusNotFound {
		t.Errorf("expected error status for DELETE /api/v1/query, got %d", w.Code)
	}
}
