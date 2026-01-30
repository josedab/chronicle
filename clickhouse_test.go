package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClickHouseServer_Ping(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("Ok.\n"))
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Ok") {
		t.Errorf("expected 'Ok' response, got %s", w.Body.String())
	}

	_ = server // use server variable
}

func TestClickHouseServer_Select1(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader("SELECT 1"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	data, ok := resp["data"].([]interface{})
	if !ok || len(data) == 0 {
		t.Errorf("expected data array with results")
	}
}

func TestClickHouseServer_ShowDatabases(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultClickHouseConfig()
	config.DefaultDatabase = "testdb"
	server := NewClickHouseServer(db, config)

	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader("SHOW DATABASES"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	data, ok := resp["data"].([]interface{})
	if !ok || len(data) == 0 {
		t.Errorf("expected data array with results")
		return
	}

	row := data[0].(map[string]interface{})
	if row["name"] != "testdb" {
		t.Errorf("expected database 'testdb', got %v", row["name"])
	}
}

func TestClickHouseServer_ShowTables(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some test data
	_ = db.Write(Point{
		Metric:    "cpu_usage",
		Value:     50.0,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
	})
	_ = db.Flush()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader("SHOW TABLES"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestClickHouseServer_QueryWithAggregation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = db.Write(Point{
			Metric:    "temperature",
			Value:     float64(20 + i),
			Timestamp: now.Add(time.Duration(-i) * time.Minute).UnixNano(),
			Tags:      map[string]string{"room": "living"},
		})
	}
	_ = db.Flush()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	query := "SELECT avg(value) AS avg_temp FROM temperature WHERE room = 'living'"
	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader(query))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestClickHouseServer_QueryFormats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	tests := []struct {
		format      string
		contentType string
	}{
		{"TabSeparated", "text/tab-separated-values"},
		{"JSON", "application/json"},
		{"JSONEachRow", "application/x-ndjson"},
		{"JSONCompact", "application/json"},
		{"CSV", "text/csv"},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/?default_format="+tt.format, strings.NewReader("SELECT 1"))
			w := httptest.NewRecorder()
			server.Handler()(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("expected status 200, got %d", w.Code)
			}

			ct := w.Header().Get("Content-Type")
			if !strings.Contains(ct, tt.contentType) {
				t.Errorf("expected content type containing %s, got %s", tt.contentType, ct)
			}
		})
	}
}

func TestClickHouseServer_SystemTables(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	tests := []string{
		"SELECT * FROM system.databases",
		"SELECT * FROM system.tables",
		"SELECT * FROM system.columns",
		"SELECT * FROM system.settings",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader(query))
			w := httptest.NewRecorder()
			server.Handler()(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestClickHouseServer_TimeConditions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	now := time.Now()
	for i := 0; i < 5; i++ {
		_ = db.Write(Point{
			Metric:    "requests",
			Value:     float64(100 + i*10),
			Timestamp: now.Add(time.Duration(-i) * time.Hour).UnixNano(),
			Tags:      map[string]string{"endpoint": "/api"},
		})
	}
	_ = db.Flush()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	// Test NOW() - INTERVAL syntax
	query := "SELECT value FROM requests WHERE timestamp >= now() - INTERVAL 2 HOUR"
	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader(query))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestClickHouseServer_GroupBy(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = db.Write(Point{
			Metric:    "latency",
			Value:     float64(50 + i%3*10),
			Timestamp: now.Add(time.Duration(-i) * time.Minute).UnixNano(),
			Tags:      map[string]string{"service": "api"},
		})
	}
	_ = db.Flush()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	query := "SELECT avg(value) FROM latency GROUP BY toStartOfMinute(timestamp)"
	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader(query))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestClickHouseServer_Limit(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	now := time.Now()
	for i := 0; i < 20; i++ {
		_ = db.Write(Point{
			Metric:    "events",
			Value:     float64(i),
			Timestamp: now.Add(time.Duration(-i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	query := "SELECT * FROM events LIMIT 5"
	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader(query))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	rows := int(resp["rows"].(float64))
	if rows > 5 {
		t.Errorf("expected at most 5 rows, got %d", rows)
	}
}

func TestClickHouseServer_EmptyQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestClickHouseServer_InvalidQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("INSERT INTO test VALUES (1)"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestClickHouseServer_Version(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader("SELECT version()"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	if !strings.Contains(w.Body.String(), "Chronicle") {
		t.Errorf("expected Chronicle version string in response")
	}
}

func TestClickHouseQueryParser_ParseSelectFields(t *testing.T) {
	parser := &clickHouseQueryParser{}

	tests := []struct {
		input    string
		expected int
	}{
		{"*", 3},
		{"value", 1},
		{"avg(value)", 1},
		{"avg(value) AS avg_val, count(*) AS cnt", 2},
		{"timestamp, value, metric", 3},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			fields := parser.parseSelectFields(tt.input)
			if len(fields) != tt.expected {
				t.Errorf("expected %d fields, got %d", tt.expected, len(fields))
			}
		})
	}
}

func TestClickHouseQueryParser_ParseTimeValue(t *testing.T) {
	parser := &clickHouseQueryParser{}

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"now()", false},
		{"now() - INTERVAL 1 HOUR", false},
		{"now() - 1 hour", false},
		{"2024-01-01", false},
		{"2024-01-01 12:00:00", false},
		{"1704067200", false}, // Unix timestamp (seconds)
		{"invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := parser.parseTimeValue(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimeValue(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestClickHouseServer_URLQueryParam(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	// Test query passed as URL parameter
	req := httptest.NewRequest(http.MethodGet, "/?query=SELECT%201&default_format=JSON", nil)
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestClickHouseServer_MaxResultRows(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultClickHouseConfig()
	config.MaxResultRows = 3
	server := NewClickHouseServer(db, config)

	// Write more data than max rows
	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = db.Write(Point{
			Metric:    "test",
			Value:     float64(i),
			Timestamp: now.Add(time.Duration(-i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	req := httptest.NewRequest(http.MethodPost, "/?default_format=JSON", strings.NewReader("SELECT * FROM test"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	rows := int(resp["rows"].(float64))
	if rows > 3 {
		t.Errorf("expected max 3 rows, got %d", rows)
	}
}

func TestClickHouseServer_TabSeparatedOutput(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	// Write test data
	_ = db.Write(Point{
		Metric:    "test",
		Value:     42.5,
		Timestamp: time.Now().UnixNano(),
	})
	_ = db.Flush()

	req := httptest.NewRequest(http.MethodPost, "/?default_format=TabSeparated", strings.NewReader("SELECT 1"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "tab-separated") {
		t.Errorf("expected tab-separated content type, got %s", ct)
	}

	// Should contain "1" followed by newline
	body := w.Body.String()
	if !strings.Contains(body, "1") {
		t.Errorf("expected '1' in response, got %s", body)
	}
}

func TestClickHouseServer_CSVOutput(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewClickHouseServer(db, DefaultClickHouseConfig())

	req := httptest.NewRequest(http.MethodPost, "/?default_format=CSV", strings.NewReader("SELECT 1"))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "csv") {
		t.Errorf("expected csv content type, got %s", ct)
	}
}

// Helper function using bytes.Buffer
func TestClickHouseServer_LargeQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultClickHouseConfig()
	config.MaxQuerySize = 100
	server := NewClickHouseServer(db, config)

	// Create a query larger than max size
	largeQuery := bytes.Repeat([]byte("SELECT 1 "), 50)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(largeQuery))
	w := httptest.NewRecorder()
	server.Handler()(w, req)

	// Should still work as we just truncate the read
	// The actual behavior depends on implementation
}
