package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestGrafanaBackend(t *testing.T) {
	// Create temp DB
	path := "test_grafana.db"
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, Config{
		PartitionDuration: time.Hour,
		BufferSize:        100,
	})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write test data
	now := time.Now()
	for i := 0; i < 100; i++ {
		p := Point{
			Metric:    "cpu_usage",
			Value:     float64(50 + i%20),
			Timestamp: now.Add(time.Duration(i) * time.Minute).UnixNano(),
			Tags:      map[string]string{"host": "server1"},
		}
		db.Write(p)
	}
	db.Flush()

	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(db, config)

	t.Run("Health", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()

		backend.handleHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}
	})

	t.Run("Metrics", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/metrics", nil)
		w := httptest.NewRecorder()

		backend.handleMetrics(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}

		var metrics []string
		json.NewDecoder(w.Body).Decode(&metrics)

		if len(metrics) == 0 {
			t.Error("Expected at least one metric")
		}
	})

	t.Run("Search", func(t *testing.T) {
		body := bytes.NewBufferString(`{"target":"cpu"}`)
		req := httptest.NewRequest("POST", "/search", body)
		w := httptest.NewRecorder()

		backend.handleSearch(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}

		var results []string
		json.NewDecoder(w.Body).Decode(&results)

		found := false
		for _, r := range results {
			if r == "cpu_usage" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find cpu_usage in search results")
		}
	})

	t.Run("Query", func(t *testing.T) {
		queryReq := GrafanaQueryRequest{
			From: "now-1h",
			To:   "now",
			Queries: []GrafanaQuery{
				{
					RefID:       "A",
					Metric:      "cpu_usage",
					Aggregation: "mean",
					Window:      "5m",
				},
			},
		}

		body, _ := json.Marshal(queryReq)
		req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
		w := httptest.NewRecorder()

		backend.handleQuery(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}

		var resp GrafanaQueryResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if result, ok := resp.Results["A"]; !ok {
			t.Error("Expected result for query A")
		} else if result.Error != "" {
			t.Errorf("Query error: %s", result.Error)
		}
	})

	t.Run("RawQuery", func(t *testing.T) {
		queryReq := GrafanaQueryRequest{
			From: "now-1h",
			To:   "now",
			Queries: []GrafanaQuery{
				{
					RefID:     "B",
					RawQuery:  true,
					QueryText: "SELECT mean(value) FROM cpu_usage GROUP BY time(5m)",
				},
			},
		}

		body, _ := json.Marshal(queryReq)
		req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
		w := httptest.NewRecorder()

		backend.handleQuery(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}
	})

	t.Run("Annotations", func(t *testing.T) {
		// Create annotation
		ann := GrafanaAnnotation{
			Text: "Test annotation",
			Tags: []string{"test"},
		}

		body, _ := json.Marshal(ann)
		req := httptest.NewRequest("POST", "/annotations", bytes.NewReader(body))
		w := httptest.NewRecorder()

		backend.handleAnnotations(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}

		// Get annotations
		req = httptest.NewRequest("GET", "/annotations?from=0&to=9999999999999", nil)
		w = httptest.NewRecorder()

		backend.handleAnnotations(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}

		var annotations []GrafanaAnnotation
		json.NewDecoder(w.Body).Decode(&annotations)

		if len(annotations) == 0 {
			t.Error("Expected at least one annotation")
		}
	})
}

func TestGrafanaBackend_ParseTime(t *testing.T) {
	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(nil, config)

	testCases := []struct {
		input    string
		shouldOK bool
	}{
		{"now", true},
		{"now-1h", true},
		{"now-30m", true},
		{"now-1d", true},
		{"now+1h", true},
		{"1609459200000", true}, // Unix timestamp in ms
		{"invalid", false},
	}

	for _, tc := range testCases {
		_, err := backend.parseTime(tc.input)
		if tc.shouldOK && err != nil {
			t.Errorf("parseTime(%s) should succeed, got error: %v", tc.input, err)
		}
		if !tc.shouldOK && err == nil {
			t.Errorf("parseTime(%s) should fail", tc.input)
		}
	}
}

func TestGrafanaBackend_ParseDuration(t *testing.T) {
	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(nil, config)

	testCases := []struct {
		input    string
		expected time.Duration
	}{
		{"1s", time.Second},
		{"5m", 5 * time.Minute},
		{"2h", 2 * time.Hour},
		{"1d", 24 * time.Hour},
		{"1w", 7 * 24 * time.Hour},
	}

	for _, tc := range testCases {
		result, err := backend.parseDuration(tc.input)
		if err != nil {
			t.Errorf("parseDuration(%s) error: %v", tc.input, err)
			continue
		}
		if result != tc.expected {
			t.Errorf("parseDuration(%s) = %v, expected %v", tc.input, result, tc.expected)
		}
	}
}

func TestGrafanaBackend_ParseAggFunc(t *testing.T) {
	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(nil, config)

	testCases := []struct {
		input    string
		expected AggFunc
	}{
		{"mean", AggMean},
		{"avg", AggMean},
		{"average", AggMean},
		{"sum", AggSum},
		{"count", AggCount},
		{"min", AggMin},
		{"max", AggMax},
		{"first", AggFirst},
		{"last", AggLast},
		{"unknown", AggMean}, // Default
	}

	for _, tc := range testCases {
		result := backend.parseAggFunc(tc.input)
		if result != tc.expected {
			t.Errorf("parseAggFunc(%s) = %v, expected %v", tc.input, result, tc.expected)
		}
	}
}

func TestGrafanaBackend_ParseQueryText(t *testing.T) {
	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(nil, config)

	testCases := []struct {
		query       string
		metric      string
		hasAgg      bool
		shouldError bool
	}{
		{"SELECT mean(value) FROM cpu_usage", "cpu_usage", true, false},
		{"SELECT sum(value) FROM memory GROUP BY time(5m)", "memory", true, false},
		{"SELECT value FROM disk", "disk", false, false},
		{"INVALID QUERY", "", false, true},
	}

	for _, tc := range testCases {
		result, err := backend.parseQueryText(tc.query)

		if tc.shouldError {
			if err == nil {
				t.Errorf("parseQueryText(%s) should error", tc.query)
			}
			continue
		}

		if err != nil {
			t.Errorf("parseQueryText(%s) error: %v", tc.query, err)
			continue
		}

		if result.Metric != tc.metric {
			t.Errorf("parseQueryText(%s) metric = %s, expected %s", tc.query, result.Metric, tc.metric)
		}

		if tc.hasAgg && result.Aggregation == nil {
			t.Errorf("parseQueryText(%s) should have aggregation", tc.query)
		}
	}
}

func TestGrafanaBackend_Downsample(t *testing.T) {
	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(nil, config)

	// Create 1000 points
	points := make([]Point, 1000)
	for i := range points {
		points[i] = Point{
			Metric:    "test",
			Value:     float64(i),
			Timestamp: int64(i * 1000000),
		}
	}

	// Downsample to 100
	result := backend.downsamplePoints(points, 100)

	if len(result) > 100 {
		t.Errorf("Expected at most 100 points, got %d", len(result))
	}

	// Should not downsample if already under limit
	small := points[:50]
	result = backend.downsamplePoints(small, 100)
	if len(result) != 50 {
		t.Errorf("Should not downsample when under limit, got %d", len(result))
	}
}

func TestGrafanaAnnotation(t *testing.T) {
	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(nil, config)

	// Add annotation
	backend.AddAnnotation(GrafanaAnnotation{
		Text: "Test 1",
		Time: 1000,
	})
	backend.AddAnnotation(GrafanaAnnotation{
		Text: "Test 2",
		Time: 2000,
	})
	backend.AddAnnotation(GrafanaAnnotation{
		Text: "Test 3",
		Time: 3000,
	})

	// Get all
	anns := backend.GetAnnotations(0, 5000)
	if len(anns) != 3 {
		t.Errorf("Expected 3 annotations, got %d", len(anns))
	}

	// Get filtered
	anns = backend.GetAnnotations(1500, 2500)
	if len(anns) != 1 {
		t.Errorf("Expected 1 annotation in range, got %d", len(anns))
	}
}

func TestGrafanaBackend_CORS(t *testing.T) {
	config := DefaultGrafanaBackendConfig()
	backend := NewGrafanaBackend(nil, config)

	handler := backend.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Test OPTIONS request
	req := httptest.NewRequest("OPTIONS", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS header not set")
	}

	if w.Code != http.StatusOK {
		t.Errorf("OPTIONS should return 200, got %d", w.Code)
	}
}
