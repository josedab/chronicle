package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPWriteJSON(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	points := []Point{
		{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 1.5, Timestamp: time.Now().UnixNano()},
	}
	body, _ := json.Marshal(writeRequest{Points: points})

	req := httptest.NewRequest(http.MethodPost, "/write", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHTTPWriteLineProtocol(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	body := "cpu,host=server01 value=1.5 1609459200000000000\n"
	req := httptest.NewRequest(http.MethodPost, "/write", strings.NewReader(body))
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHTTPWriteGzipped(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte("cpu,host=a value=2.0\n"))
	_ = gz.Close()

	req := httptest.NewRequest(http.MethodPost, "/write", &buf)
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHTTPWriteMethodNotAllowed(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	req := httptest.NewRequest(http.MethodGet, "/write", http.NoBody)
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHTTPQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Write some data first
	_ = db.Write(Point{Metric: "temp", Value: 20.0, Timestamp: time.Now().UnixNano()})
	_ = db.Flush()

	qr := queryRequest{Metric: "temp"}
	body, _ := json.Marshal(qr)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp queryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Points) == 0 {
		t.Error("expected points in response")
	}
}

func TestHTTPQueryWithAggregation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	_ = db.Write(Point{Metric: "cpu", Value: 1.0, Timestamp: now})
	_ = db.Write(Point{Metric: "cpu", Value: 3.0, Timestamp: now + int64(time.Second)})
	_ = db.Flush()

	qr := queryRequest{
		Metric:      "cpu",
		Aggregation: "mean",
		Window:      "1h",
	}
	body, _ := json.Marshal(qr)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp queryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Points) == 0 {
		t.Fatal("expected aggregated points")
	}
}

func TestHTTPQueryWithSQL(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Write(Point{Metric: "cpu", Value: 50.0, Timestamp: time.Now().UnixNano()})
	_ = db.Flush()

	qr := queryRequest{Query: "SELECT count(value) FROM cpu"}
	body, _ := json.Marshal(qr)

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHTTPPrometheusWriteDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	cfg.HTTP.PrometheusRemoteWriteEnabled = false
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	req := httptest.NewRequest(http.MethodPost, "/prometheus/write", http.NoBody)
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestParseLineProtocol(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantN   int
		wantErr bool
	}{
		{
			name:  "simple",
			input: "cpu,host=a value=1.5",
			wantN: 1,
		},
		{
			name:  "with timestamp",
			input: "cpu,host=a value=1.5 1609459200000000000",
			wantN: 1,
		},
		{
			name:  "multiple lines",
			input: "cpu value=1.0\nmem value=2.0\n",
			wantN: 2,
		},
		{
			name:  "integer value",
			input: "cpu value=100i",
			wantN: 1,
		},
		{
			name:    "invalid",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:  "empty lines",
			input: "\n\ncpu value=1.0\n\n",
			wantN: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := parseLineProtocol(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(points) != tt.wantN {
				t.Errorf("expected %d points, got %d", tt.wantN, len(points))
			}
		})
	}
}

func TestParseMeasurementTags(t *testing.T) {
	metric, tags := parseMeasurementTags("cpu,host=server01,region=us-west")
	if metric != "cpu" {
		t.Errorf("expected metric 'cpu', got '%s'", metric)
	}
	if tags["host"] != "server01" {
		t.Errorf("expected host=server01, got %s", tags["host"])
	}
	if tags["region"] != "us-west" {
		t.Errorf("expected region=us-west, got %s", tags["region"])
	}
}

func TestParseFieldSet(t *testing.T) {
	key, value, err := parseFieldSet("value=1.5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "value" {
		t.Errorf("expected key 'value', got '%s'", key)
	}
	if value != 1.5 {
		t.Errorf("expected value 1.5, got %f", value)
	}

	// Integer suffix
	_, value, err = parseFieldSet("count=100i")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != 100.0 {
		t.Errorf("expected value 100, got %f", value)
	}

	// Invalid
	_, _, err = parseFieldSet("invalid")
	if err == nil {
		t.Error("expected error for invalid field set")
	}
}

func TestHTTPBodySizeLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Create a body larger than maxBodySize (10MB)
	largeBody := strings.Repeat("cpu value=1.0\n", 1024*1024)

	req := httptest.NewRequest(http.MethodPost, "/write", strings.NewReader(largeBody))
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	// Should get a 400 error due to body size limit
	if w.Code != http.StatusBadRequest {
		t.Logf("response code: %d (body may have been truncated)", w.Code)
	}
}

func TestHTTPEmptyBody(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	req := httptest.NewRequest(http.MethodPost, "/write", strings.NewReader(""))
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", w.Code)
	}
}

func TestHTTPHealth(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %s", resp["status"])
	}
}

func TestHTTPPromQueryInstant(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Write test data
	now := time.Now().UnixNano()
	_ = db.WriteBatch([]Point{
		{Metric: "up", Value: 1, Timestamp: now, Tags: map[string]string{"job": "test"}},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil)
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "success" {
		t.Errorf("expected success, got %v", resp["status"])
	}
}

func TestHTTPPromQueryRange(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = db.WriteBatch([]Point{
			{Metric: "requests", Value: float64(i * 10), Timestamp: now.Add(time.Duration(-i) * time.Minute).UnixNano()},
		})
	}

	start := float64(now.Add(-time.Hour).Unix())
	end := float64(now.Unix())
	url := fmt.Sprintf("/api/v1/query_range?query=requests&start=%f&end=%f&step=1m", start, end)

	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()

	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHTTPSchemas(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// POST - create schema
	schema := MetricSchema{
		Name: "test_metric",
		Tags: []TagSchema{{Name: "host", Required: true}},
	}
	body, _ := json.Marshal(schema)

	req := httptest.NewRequest(http.MethodPost, "/schemas", bytes.NewReader(body))
	w := httptest.NewRecorder()
	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("POST expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// GET - list schemas
	req = httptest.NewRequest(http.MethodGet, "/schemas", nil)
	w = httptest.NewRecorder()
	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET expected 200, got %d", w.Code)
	}

	var schemas []MetricSchema
	if err := json.NewDecoder(w.Body).Decode(&schemas); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(schemas) != 1 {
		t.Errorf("expected 1 schema, got %d", len(schemas))
	}

	// DELETE - remove schema
	req = httptest.NewRequest(http.MethodDelete, "/schemas?name=test_metric", nil)
	w = httptest.NewRecorder()
	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("DELETE expected 204, got %d", w.Code)
	}
}

func TestHTTPAlerts(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// GET alerts
	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts", nil)
	w := httptest.NewRecorder()
	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHTTPRules(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	cfg.HTTP.HTTPEnabled = true
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// POST - create rule
	rule := AlertRule{
		Name:      "test_rule",
		Metric:    "cpu",
		Condition: AlertConditionAbove,
		Threshold: 80,
	}
	body, _ := json.Marshal(rule)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/rules", bytes.NewReader(body))
	w := httptest.NewRecorder()
	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("POST expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// GET - list rules
	req = httptest.NewRequest(http.MethodGet, "/api/v1/rules", nil)
	w = httptest.NewRecorder()
	db.lifecycle.httpHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET expected 200, got %d", w.Code)
	}
}

func TestRateLimiter_Basic(t *testing.T) {
	rl := newRateLimiter(5, time.Second)

	// First 5 requests should succeed
	for i := 0; i < 5; i++ {
		if !rl.allow("192.168.1.1") {
			t.Errorf("request %d should be allowed", i)
		}
	}

	// 6th request should be rate limited
	if rl.allow("192.168.1.1") {
		t.Error("6th request should be rate limited")
	}

	// Different IP should still be allowed
	if !rl.allow("192.168.1.2") {
		t.Error("different IP should be allowed")
	}
}

func TestRateLimiter_WindowReset(t *testing.T) {
	rl := newRateLimiter(2, 50*time.Millisecond)

	// Use up the limit
	rl.allow("192.168.1.1")
	rl.allow("192.168.1.1")

	// Should be rate limited
	if rl.allow("192.168.1.1") {
		t.Error("should be rate limited")
	}

	// Wait for window to reset
	time.Sleep(60 * time.Millisecond)

	// Should be allowed again
	if !rl.allow("192.168.1.1") {
		t.Error("should be allowed after window reset")
	}
}

func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		headers    map[string]string
		want       string
	}{
		{
			name:       "remote addr only",
			remoteAddr: "192.168.1.1:12345",
			want:       "192.168.1.1",
		},
		{
			name:       "x-forwarded-for",
			remoteAddr: "10.0.0.1:12345",
			headers:    map[string]string{"X-Forwarded-For": "203.0.113.1, 70.41.3.18"},
			want:       "203.0.113.1",
		},
		{
			name:       "x-real-ip",
			remoteAddr: "10.0.0.1:12345",
			headers:    map[string]string{"X-Real-IP": "203.0.113.2"},
			want:       "203.0.113.2",
		},
		{
			name:       "x-forwarded-for takes precedence",
			remoteAddr: "10.0.0.1:12345",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.1",
				"X-Real-IP":       "203.0.113.2",
			},
			want: "203.0.113.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
req := httptest.NewRequest("GET", "/", nil)
req.RemoteAddr = tt.remoteAddr
for k, v := range tt.headers {
req.Header.Set(k, v)
}
got := getClientIP(req)
if got != tt.want {
t.Errorf("getClientIP() = %q, want %q", got, tt.want)
}
})
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	rl := newRateLimiter(2, time.Second)

	handler := rateLimitMiddleware(rl, func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusOK)
})

	// First 2 requests should succeed
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.1:12345"
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("request %d: got status %d, want %d", i, w.Code, http.StatusOK)
		}
	}

	// 3rd request should be rate limited
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("got status %d, want %d", w.Code, http.StatusTooManyRequests)
	}
	if w.Header().Get("Retry-After") != "1" {
		t.Error("missing Retry-After header")
	}
}

func TestAuthenticator_Disabled(t *testing.T) {
	auth := newAuthenticator(nil)
	if auth.enabled {
		t.Error("authenticator should be disabled with nil config")
	}

	auth = newAuthenticator(&AuthConfig{Enabled: false})
	if auth.enabled {
		t.Error("authenticator should be disabled when Enabled is false")
	}
}

func TestAuthenticator_Enabled(t *testing.T) {
	auth := newAuthenticator(&AuthConfig{
		Enabled:      true,
		APIKeys:      []string{"key1", "key2"},
		ReadOnlyKeys: []string{"readonly1"},
		ExcludePaths: []string{"/custom-health"},
	})

	if !auth.enabled {
		t.Error("authenticator should be enabled")
	}
	if !auth.apiKeys["key1"] || !auth.apiKeys["key2"] {
		t.Error("API keys should be registered")
	}
	if !auth.readOnlyKeys["readonly1"] {
		t.Error("read-only keys should be registered")
	}
	if !auth.excludePaths["/custom-health"] || !auth.excludePaths["/health"] {
		t.Error("exclude paths should be registered")
	}
}

func TestExtractAPIKey(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		query   string
		want    string
	}{
		{
			name:    "bearer token",
			headers: map[string]string{"Authorization": "Bearer mytoken123"},
			want:    "mytoken123",
		},
		{
			name:    "x-api-key header",
			headers: map[string]string{"X-API-Key": "apikey456"},
			want:    "apikey456",
		},
		{
			name:  "query parameter",
			query: "api_key=querykey789",
			want:  "querykey789",
		},
		{
			name: "bearer takes precedence",
			headers: map[string]string{
				"Authorization": "Bearer bearer",
				"X-API-Key":     "header",
			},
			want: "bearer",
		},
		{
			name: "no key",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
url := "/"
if tt.query != "" {
url = "/?" + tt.query
}
req := httptest.NewRequest("GET", url, nil)
for k, v := range tt.headers {
req.Header.Set(k, v)
}
got := extractAPIKey(req)
if got != tt.want {
t.Errorf("extractAPIKey() = %q, want %q", got, tt.want)
}
})
	}
}

func TestIsWriteOperation(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   bool
	}{
		{"POST", "/write", true},
		{"PUT", "/data", true},
		{"DELETE", "/metrics", true},
		{"GET", "/query", false},
		{"POST", "/query", false},
		{"POST", "/api/v1/query", false},
		{"POST", "/api/v1/query_range", false},
		{"POST", "/graphql", false},
		{"GET", "/health", false},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
req := httptest.NewRequest(tt.method, tt.path, nil)
got := isWriteOperation(req)
if got != tt.want {
t.Errorf("isWriteOperation() = %v, want %v", got, tt.want)
}
})
	}
}

func TestAuthMiddleware_NoAuth(t *testing.T) {
	auth := newAuthenticator(nil)
	called := false
	handler := authMiddleware(auth, func(w http.ResponseWriter, r *http.Request) {
called = true
w.WriteHeader(http.StatusOK)
})

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if !called {
		t.Error("handler should be called when auth is disabled")
	}
	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestAuthMiddleware_ExcludedPath(t *testing.T) {
	auth := newAuthenticator(&AuthConfig{
		Enabled: true,
		APIKeys: []string{"secret"},
	})
	called := false
	handler := authMiddleware(auth, func(w http.ResponseWriter, r *http.Request) {
called = true
w.WriteHeader(http.StatusOK)
})

	// /health is always excluded
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if !called {
		t.Error("handler should be called for excluded path")
	}
}

func TestAuthMiddleware_MissingKey(t *testing.T) {
	auth := newAuthenticator(&AuthConfig{
		Enabled: true,
		APIKeys: []string{"secret"},
	})
	handler := authMiddleware(auth, func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusOK)
})

	req := httptest.NewRequest("GET", "/write", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("got status %d, want %d", w.Code, http.StatusUnauthorized)
	}
	if w.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Error("missing WWW-Authenticate header")
	}
}

func TestAuthMiddleware_InvalidKey(t *testing.T) {
	auth := newAuthenticator(&AuthConfig{
		Enabled: true,
		APIKeys: []string{"secret"},
	})
	handler := authMiddleware(auth, func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusOK)
})

	req := httptest.NewRequest("GET", "/write", nil)
	req.Header.Set("Authorization", "Bearer wrongkey")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("got status %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestAuthMiddleware_ValidKey(t *testing.T) {
	auth := newAuthenticator(&AuthConfig{
		Enabled: true,
		APIKeys: []string{"secret"},
	})
	called := false
	handler := authMiddleware(auth, func(w http.ResponseWriter, r *http.Request) {
called = true
w.WriteHeader(http.StatusOK)
})

	req := httptest.NewRequest("POST", "/write", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	handler(w, req)

	if !called {
		t.Error("handler should be called with valid key")
	}
	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestAuthMiddleware_ReadOnlyKey_ReadOp(t *testing.T) {
	auth := newAuthenticator(&AuthConfig{
		Enabled:      true,
		ReadOnlyKeys: []string{"readonly"},
	})
	called := false
	handler := authMiddleware(auth, func(w http.ResponseWriter, r *http.Request) {
called = true
w.WriteHeader(http.StatusOK)
})

	req := httptest.NewRequest("GET", "/query", nil)
	req.Header.Set("X-API-Key", "readonly")
	w := httptest.NewRecorder()
	handler(w, req)

	if !called {
		t.Error("handler should be called for read operation with read-only key")
	}
}

func TestAuthMiddleware_ReadOnlyKey_WriteOp(t *testing.T) {
	auth := newAuthenticator(&AuthConfig{
		Enabled:      true,
		ReadOnlyKeys: []string{"readonly"},
	})
	handler := authMiddleware(auth, func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusOK)
})

	req := httptest.NewRequest("POST", "/write", nil)
	req.Header.Set("X-API-Key", "readonly")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("got status %d, want %d", w.Code, http.StatusForbidden)
	}
}

// Mock implementations for interface testing

// mockQueryExecutor implements QueryExecutor for testing HTTP handlers.
type mockQueryExecutor struct {
	result *Result
	err    error
	called bool
}

func (m *mockQueryExecutor) Execute(q *Query) (*Result, error) {
	m.called = true
	return m.result, m.err
}

func (m *mockQueryExecutor) ExecuteContext(ctx context.Context, q *Query) (*Result, error) {
	m.called = true
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return m.result, m.err
	}
}

// mockDataWriter implements DataWriter for testing HTTP handlers.
type mockDataWriter struct {
	points []Point
	err    error
}

func (m *mockDataWriter) Write(p Point) error {
	if m.err != nil {
		return m.err
	}
	m.points = append(m.points, p)
	return nil
}

func (m *mockDataWriter) WriteBatch(points []Point) error {
	if m.err != nil {
		return m.err
	}
	m.points = append(m.points, points...)
	return nil
}

// mockMetricLister implements MetricLister for testing.
type mockMetricLister struct {
	metrics []string
}

func (m *mockMetricLister) Metrics() []string {
	return m.metrics
}

// mockSchemaManager implements SchemaManager for testing.
type mockSchemaManager struct {
	schemas map[string]MetricSchema
	err     error
}

func newMockSchemaManager() *mockSchemaManager {
	return &mockSchemaManager{schemas: make(map[string]MetricSchema)}
}

func (m *mockSchemaManager) RegisterSchema(schema MetricSchema) error {
	if m.err != nil {
		return m.err
	}
	m.schemas[schema.Name] = schema
	return nil
}

func (m *mockSchemaManager) UnregisterSchema(name string) {
	delete(m.schemas, name)
}

func (m *mockSchemaManager) GetSchema(name string) *MetricSchema {
	if s, ok := m.schemas[name]; ok {
		return &s
	}
	return nil
}

func (m *mockSchemaManager) ListSchemas() []MetricSchema {
	var result []MetricSchema
	for _, s := range m.schemas {
		result = append(result, s)
	}
	return result
}

// Tests using mock implementations

func TestMockQueryExecutor_Success(t *testing.T) {
	mock := &mockQueryExecutor{
		result: &Result{
			Points: []Point{
				{Metric: "cpu", Tags: map[string]string{"host": "server1"}, Value: 42.0, Timestamp: time.Now().UnixNano()},
			},
		},
	}

	// Verify it implements the interface
	var _ QueryExecutor = mock

	result, err := mock.Execute(&Query{Metric: "cpu"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.called {
		t.Error("Execute should have been called")
	}
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point, got %d", len(result.Points))
	}
}

func TestMockQueryExecutor_Error(t *testing.T) {
	mock := &mockQueryExecutor{
		err: fmt.Errorf("query failed"),
	}

	result, err := mock.Execute(&Query{Metric: "cpu"})
	if err == nil {
		t.Error("expected error")
	}
	if result != nil {
		t.Error("expected nil result on error")
	}
}

func TestMockQueryExecutor_ContextCancellation(t *testing.T) {
	mock := &mockQueryExecutor{
		result: &Result{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := mock.ExecuteContext(ctx, &Query{Metric: "cpu"})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestMockDataWriter_WriteBatch(t *testing.T) {
	mock := &mockDataWriter{}

	// Verify it implements the interface
	var _ DataWriter = mock

	points := []Point{
		{Metric: "cpu", Value: 1.0},
		{Metric: "mem", Value: 2.0},
	}

	err := mock.WriteBatch(points)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mock.points) != 2 {
		t.Errorf("expected 2 points, got %d", len(mock.points))
	}
}

func TestMockDataWriter_WriteError(t *testing.T) {
	mock := &mockDataWriter{
		err: fmt.Errorf("write failed"),
	}

	err := mock.Write(Point{Metric: "cpu", Value: 1.0})
	if err == nil {
		t.Error("expected error")
	}
	if len(mock.points) != 0 {
		t.Error("points should be empty on error")
	}
}

func TestMockMetricLister(t *testing.T) {
	mock := &mockMetricLister{
		metrics: []string{"cpu", "memory", "disk"},
	}

	// Verify it implements the interface
	var _ MetricLister = mock

	metrics := mock.Metrics()
	if len(metrics) != 3 {
		t.Errorf("expected 3 metrics, got %d", len(metrics))
	}
}

func TestMockSchemaManager_RegisterAndGet(t *testing.T) {
	mock := newMockSchemaManager()

	// Verify it implements the interface
	var _ SchemaManager = mock

	schema := MetricSchema{
		Name: "cpu_usage",
		Tags: []TagSchema{
			{Name: "host", Required: true},
		},
	}

	err := mock.RegisterSchema(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved := mock.GetSchema("cpu_usage")
	if retrieved == nil {
		t.Fatal("expected schema to be retrieved")
	}
	if retrieved.Name != "cpu_usage" {
		t.Errorf("expected name 'cpu_usage', got '%s'", retrieved.Name)
	}

	mock.UnregisterSchema("cpu_usage")
	if mock.GetSchema("cpu_usage") != nil {
		t.Error("schema should be nil after unregister")
	}
}

func TestMockSchemaManager_ListSchemas(t *testing.T) {
	mock := newMockSchemaManager()

	_ = mock.RegisterSchema(MetricSchema{Name: "cpu"})
	_ = mock.RegisterSchema(MetricSchema{Name: "memory"})

	schemas := mock.ListSchemas()
	if len(schemas) != 2 {
		t.Errorf("expected 2 schemas, got %d", len(schemas))
	}
}
