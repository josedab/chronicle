package chronicle

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHttpRoutesWrite(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "setup_write_routes"},
		{name: "setup_query_routes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
			switch tt.name {
			case "setup_write_routes":
				setupWriteRoutes(mux, db, wrap)
			case "setup_query_routes":
				setupQueryRoutes(mux, db, wrap)
			}
		})
	}
}

func TestParseLineProtocolSmoke(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantLen int
	}{
		{name: "single_line", input: "cpu value=1.0 1000000000", wantErr: false, wantLen: 1},
		{name: "empty", input: "", wantErr: false, wantLen: 0},
		{name: "with_tags", input: "cpu,host=a,region=us value=42.0 1000000000", wantErr: false, wantLen: 1},
		{name: "multi_line", input: "cpu value=1.0 1000\nmem value=2.0 2000", wantErr: false, wantLen: 2},
		{name: "missing_field", input: "cpu", wantErr: true, wantLen: 0},
		{name: "bad_value", input: "cpu value=notanumber", wantErr: true, wantLen: 0},
		{name: "whitespace_only", input: "   \n\n   ", wantErr: false, wantLen: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := parseLineProtocol(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseLineProtocol() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(points) != tt.wantLen {
				t.Errorf("got %d points, want %d", len(points), tt.wantLen)
			}
		})
	}
}

// newTestWriteMux creates a mux with write routes and no middleware.
func newTestWriteMux(db *DB) *http.ServeMux {
	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupWriteRoutes(mux, db, wrap)
	setupQueryRoutes(mux, db, wrap)
	return mux
}

func TestWriteHandler_JSONBody(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	body, _ := json.Marshal(writeRequest{Points: []Point{
		{Metric: "cpu", Value: 42.0, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"host": "a"}},
		{Metric: "cpu", Value: 43.0, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"host": "b"}},
	}})

	req := httptest.NewRequest(http.MethodPost, "/write", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

func TestWriteHandler_LineProtocol(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	body := "cpu,host=a value=42.0 1000000000\ncpu,host=b value=43.0 2000000000\n"
	req := httptest.NewRequest(http.MethodPost, "/write", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

func TestWriteHandler_GzipBody(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	payload, _ := json.Marshal(writeRequest{Points: []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()},
	}})

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(payload)
	gz.Close()

	req := httptest.NewRequest(http.MethodPost, "/write", &buf)
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

func TestWriteHandler_EmptyBody(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	req := httptest.NewRequest(http.MethodPost, "/write", bytes.NewReader(nil))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204 for empty body, got %d", w.Code)
	}
}

func TestWriteHandler_MethodNotAllowed(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	req := httptest.NewRequest(http.MethodGet, "/write", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestWriteHandler_RejectsInfValue(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	// JSON with Inf — manually construct since json.Marshal rejects Inf
	body := `{"points":[{"metric":"cpu","value":1e999,"timestamp":1000000000}]}`
	req := httptest.NewRequest(http.MethodPost, "/write", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// 1e999 overflows to +Inf in float64; ValidatePoint should catch it
	// If JSON decode fails on overflow, falls through to line protocol which also fails
	if w.Code == http.StatusAccepted {
		t.Error("expected non-202 for Inf value")
	}
}

func TestWriteHandler_BadGzip(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	req := httptest.NewRequest(http.MethodPost, "/write", bytes.NewBufferString("not-valid-gzip"))
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad gzip, got %d", w.Code)
	}
}

func TestWriteHandler_InvalidJSON(t *testing.T) {
	db := setupTestDB(t)
	mux := newTestWriteMux(db)

	req := httptest.NewRequest(http.MethodPost, "/write", bytes.NewBufferString(`{"not":"valid"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Invalid JSON that doesn't have points falls through to line protocol parsing
	// which will also fail — should return 400
	if w.Code != http.StatusBadRequest && w.Code != http.StatusNoContent {
		t.Errorf("expected 400 or 204 for invalid JSON, got %d", w.Code)
	}
}
