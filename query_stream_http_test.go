package chronicle

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPQuery_StreamingJSON(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 1; i <= 5; i++ {
		_ = db.Write(Point{
			Metric:    "http_requests",
			Tags:      map[string]string{"env": "prod"},
			Value:     float64(i * 100),
			Timestamp: now + int64(i)*1000,
		})
	}
	_ = db.Flush()

	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupQueryRoutes(mux, db, wrap)

	// 1. Stream via body field "stream": true
	body := []byte(`{"metric":"http_requests","stream":true}`)
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "application/json") {
		t.Errorf("expected Content-Type application/json, got %s", w.Header().Get("Content-Type"))
	}

	var resp queryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode JSON streamed response: %v, body: %s", err, w.Body.String())
	}
	if len(resp.Points) != 5 {
		t.Fatalf("expected 5 points, got %d", len(resp.Points))
	}

	// 2. Stream via query parameter ?stream=true
	body = []byte(`{"metric":"http_requests"}`)
	req = httptest.NewRequest(http.MethodPost, "/query?stream=true", bytes.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	var respParam queryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &respParam); err != nil {
		t.Fatalf("failed to decode JSON response from ?stream=true: %v", err)
	}
	if len(respParam.Points) != 5 {
		t.Fatalf("expected 5 points, got %d", len(respParam.Points))
	}
}

func TestHTTPQuery_StreamingNDJSON(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 1; i <= 3; i++ {
		_ = db.Write(Point{
			Metric:    "temperature",
			Value:     20.0 + float64(i),
			Timestamp: now + int64(i)*1000,
		})
	}
	_ = db.Flush()

	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupQueryRoutes(mux, db, wrap)

	// Stream via Accept: application/x-ndjson
	body := []byte(`{"metric":"temperature"}`)
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Accept", "application/x-ndjson")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "application/x-ndjson") {
		t.Errorf("expected Content-Type application/x-ndjson, got %s", w.Header().Get("Content-Type"))
	}

	scanner := bufio.NewScanner(w.Body)
	var points []Point
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var p Point
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("failed to decode NDJSON line: %v, line: %s", err, line)
		}
		points = append(points, p)
	}

	if len(points) != 3 {
		t.Fatalf("expected 3 NDJSON points, got %d", len(points))
	}
}

func TestHTTPQuery_StreamingCSV(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UnixNano()
	_ = db.Write(Point{
		Metric:    "disk_free",
		Value:     512.0,
		Timestamp: now,
	})
	_ = db.Flush()

	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupQueryRoutes(mux, db, wrap)

	body := []byte(`{"metric":"disk_free","stream":true,"format":"csv"}`)
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "text/csv") {
		t.Errorf("expected Content-Type text/csv, got %s", w.Header().Get("Content-Type"))
	}

	output := w.Body.String()
	if !strings.Contains(output, "metric,timestamp,value") {
		t.Errorf("expected CSV header, got %s", output)
	}
	if !strings.Contains(output, "disk_free") {
		t.Errorf("expected disk_free data in CSV, got %s", output)
	}
}

func TestHTTPQuery_DedicatedStreamEndpoints(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UnixNano()
	_ = db.Write(Point{
		Metric:    "network_in",
		Value:     1024.0,
		Timestamp: now,
	})
	_ = db.Flush()

	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupQueryRoutes(mux, db, wrap)

	endpoints := []string{"/query/stream", "/api/v1/query/stream"}
	for _, ep := range endpoints {
		body := []byte(`{"metric":"network_in"}`)
		req := httptest.NewRequest(http.MethodPost, ep, bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("endpoint %s returned %d: %s", ep, w.Code, w.Body.String())
		}
		var resp queryResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("endpoint %s invalid JSON: %v", ep, err)
		}
		if len(resp.Points) != 1 {
			t.Fatalf("endpoint %s expected 1 point, got %d", ep, len(resp.Points))
		}
	}
}
