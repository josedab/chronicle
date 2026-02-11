package chronicle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestVisualQueryBuilder(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultVisualQueryBuilderConfig()
	builder := NewVisualQueryBuilder(db, config)

	// Create a visual query
	vq := builder.CreateVisualQuery("test-query")
	
	// Add components
	metricComp := builder.CreateComponent(ComponentMetric, MetricComponent{
		Name: "cpu",
		Tags: map[string]string{"host": "server1"},
	})
	vq.Components = append(vq.Components, metricComp)

	timeComp := builder.CreateComponent(ComponentTimeRange, TimeRangeComponent{
		Relative: "1h",
	})
	vq.Components = append(vq.Components, timeComp)

	aggComp := builder.CreateComponent(ComponentAggregation, AggregationComponent{
		Function: "avg",
		Interval: "5m",
	})
	vq.Components = append(vq.Components, aggComp)

	// Generate query
	result, err := builder.GenerateQuery(context.Background(), vq)
	if err != nil {
		t.Fatalf("failed to generate query: %v", err)
	}

	if !result.Validated {
		t.Errorf("query should be valid, errors: %v", result.Errors)
	}

	if result.SQL == "" {
		t.Error("expected SQL to be generated")
	}

	if !strings.Contains(result.SQL, "cpu") {
		t.Error("SQL should contain metric name 'cpu'")
	}
}

func TestQueryValidation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultVisualQueryBuilderConfig()
	builder := NewVisualQueryBuilder(db, config)

	tests := []struct {
		name       string
		components []*QueryComponent
		expectErr  bool
	}{
		{
			name: "valid query with metric",
			components: []*QueryComponent{
				{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu"}},
			},
			expectErr: false,
		},
		{
			name:       "empty query",
			components: []*QueryComponent{},
			expectErr:  true,
		},
		{
			name: "query without metric",
			components: []*QueryComponent{
				{Type: ComponentTimeRange, Properties: TimeRangeComponent{Relative: "1h"}},
			},
			expectErr: true,
		},
		{
			name: "filter without field",
			components: []*QueryComponent{
				{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu"}},
				{Type: ComponentFilter, Properties: map[string]interface{}{"operator": "=", "value": "test"}},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vq := builder.CreateVisualQuery(tt.name)
			vq.Components = tt.components

			errors, _ := builder.ValidateQuery(vq)
			hasErrors := len(errors) > 0

			if hasErrors != tt.expectErr {
				t.Errorf("expected error: %v, got errors: %v", tt.expectErr, errors)
			}
		})
	}
}

func TestSQLGeneration(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	tests := []struct {
		name     string
		query    *VisualQuerySpec
		contains []string
	}{
		{
			name: "simple select",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu"}},
				},
			},
			contains: []string{"SELECT", "FROM cpu"},
		},
		{
			name: "with filter",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{Name: "memory", Tags: map[string]string{"host": "server1"}}},
					{Type: ComponentFilter, Properties: FilterComponent{Field: "region", Operator: "=", Value: "us-east"}},
				},
			},
			contains: []string{"FROM memory", "WHERE", "host = 'server1'", "region = 'us-east'"},
		},
		{
			name: "with aggregation",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{Name: "requests"}},
					{Type: ComponentAggregation, Properties: AggregationComponent{Function: "sum", Interval: "1m"}},
				},
			},
			contains: []string{"sum(value)", "GROUP BY", "time(1m)"},
		},
		{
			name: "with time range",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu"}},
					{Type: ComponentTimeRange, Properties: TimeRangeComponent{Relative: "24h"}},
				},
			},
			contains: []string{"time >= now() - 24h"},
		},
		{
			name: "with limit",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu"}},
					{Type: ComponentLimit, Properties: 100},
				},
			},
			contains: []string{"LIMIT 100"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := builder.GenerateQuery(context.Background(), tt.query)
			if err != nil {
				t.Fatalf("failed to generate: %v", err)
			}

			for _, substr := range tt.contains {
				if !strings.Contains(result.SQL, substr) {
					t.Errorf("SQL should contain '%s', got: %s", substr, result.SQL)
				}
			}
		})
	}
}

func TestPromQLGeneration(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	tests := []struct {
		name     string
		query    *VisualQuerySpec
		expected string
	}{
		{
			name: "simple metric",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{Name: "http_requests_total"}},
				},
			},
			expected: "http_requests_total",
		},
		{
			name: "metric with labels",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{
						Name: "http_requests_total",
						Tags: map[string]string{"job": "api", "instance": "localhost:9090"},
					}},
				},
			},
			expected: "http_requests_total{",
		},
		{
			name: "with aggregation",
			query: &VisualQuerySpec{
				Components: []*QueryComponent{
					{Type: ComponentMetric, Properties: MetricComponent{Name: "http_requests_total"}},
					{Type: ComponentAggregation, Properties: AggregationComponent{Function: "sum"}},
				},
			},
			expected: "sum(http_requests_total)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := builder.GenerateQuery(context.Background(), tt.query)
			if err != nil {
				t.Fatalf("failed to generate: %v", err)
			}

			if !strings.Contains(result.PromQL, tt.expected) {
				t.Errorf("PromQL should contain '%s', got: %s", tt.expected, result.PromQL)
			}
		})
	}
}

func TestAutocomplete(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	// Test function autocomplete
	resp, err := builder.Autocomplete(context.Background(), &AutocompleteRequest{
		Type:   "function",
		Prefix: "ra",
	})
	if err != nil {
		t.Fatalf("autocomplete failed: %v", err)
	}

	found := false
	for _, s := range resp.Suggestions {
		if s.Value == "rate" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'rate' in function suggestions")
	}

	// Test aggregation autocomplete
	resp, err = builder.Autocomplete(context.Background(), &AutocompleteRequest{
		Type:   "aggregation",
		Prefix: "av",
	})
	if err != nil {
		t.Fatalf("autocomplete failed: %v", err)
	}

	found = false
	for _, s := range resp.Suggestions {
		if s.Value == "avg" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'avg' in aggregation suggestions")
	}
}

func TestGetSchema(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	schema, err := builder.GetSchema(context.Background())
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

	if len(schema.Functions) == 0 {
		t.Error("schema should have functions")
	}

	if len(schema.Aggregations) == 0 {
		t.Error("schema should have aggregations")
	}

	if len(schema.Operators) == 0 {
		t.Error("schema should have operators")
	}

	if len(schema.TimeIntervals) == 0 {
		t.Error("schema should have time intervals")
	}
}

func TestParseFromSQL(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	tests := []struct {
		sql         string
		expectError bool
		metric      string
	}{
		{
			sql:         "SELECT time, value FROM cpu",
			expectError: false,
			metric:      "cpu",
		},
		{
			sql:         "SELECT time, value FROM memory WHERE host = 'server1'",
			expectError: false,
			metric:      "memory",
		},
		{
			sql:         "INVALID QUERY",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			vq, err := builder.ParseFromSQL(tt.sql)
			if tt.expectError {
				if err == nil {
					t.Error("expected error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Find metric component
			for _, comp := range vq.Components {
				if comp.Type == ComponentMetric {
					m := parseMetricComponent(comp.Properties)
					if m != nil && m.Name != tt.metric {
						t.Errorf("expected metric %s, got %s", tt.metric, m.Name)
					}
				}
			}
		})
	}
}

func TestHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	// Test schema endpoint
	t.Run("GetSchema", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/query-builder/schema", nil)
		w := httptest.NewRecorder()

		builder.HandleGetSchema(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var schema QuerySchema
		if err := json.NewDecoder(w.Body).Decode(&schema); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
	})

	// Test autocomplete endpoint
	t.Run("Autocomplete", func(t *testing.T) {
		body := `{"type":"function","prefix":"r"}`
		req := httptest.NewRequest("POST", "/api/query-builder/autocomplete", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		builder.HandleAutocomplete(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	// Test generate endpoint
	t.Run("GenerateQuery", func(t *testing.T) {
		vq := &VisualQuerySpec{
			Components: []*QueryComponent{
				{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu"}},
			},
		}
		body, _ := json.Marshal(vq)
		req := httptest.NewRequest("POST", "/api/query-builder/generate", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		builder.HandleGenerateQuery(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result GeneratedQuery
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result.SQL == "" {
			t.Error("expected SQL in response")
		}
	})

	// Test validate endpoint
	t.Run("ValidateQuery", func(t *testing.T) {
		vq := &VisualQuerySpec{
			Components: []*QueryComponent{}, // Invalid: empty
		}
		body, _ := json.Marshal(vq)
		req := httptest.NewRequest("POST", "/api/query-builder/validate", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		builder.HandleValidateQuery(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result["valid"].(bool) {
			t.Error("empty query should not be valid")
		}
	})

	// Test parse-sql endpoint
	t.Run("ParseSQL", func(t *testing.T) {
		body := `{"sql":"SELECT * FROM cpu WHERE host = 'server1'"}`
		req := httptest.NewRequest("POST", "/api/query-builder/parse-sql", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		builder.HandleParseSQL(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})
}

func TestInternalQueryGeneration(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	now := time.Now()
	start := now.Add(-time.Hour)

	vq := &VisualQuerySpec{
		Components: []*QueryComponent{
			{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu", Tags: map[string]string{"host": "server1"}}},
			{Type: ComponentTimeRange, Properties: TimeRangeComponent{Start: &start, End: &now}},
			{Type: ComponentAggregation, Properties: AggregationComponent{Function: "avg", Interval: "5m"}},
			{Type: ComponentGroupBy, Properties: GroupByComponent{Fields: []string{"host"}}},
			{Type: ComponentLimit, Properties: 1000},
		},
	}

	result, err := builder.GenerateQuery(context.Background(), vq)
	if err != nil {
		t.Fatalf("failed to generate: %v", err)
	}

	if result.Internal == nil {
		t.Fatal("expected internal query")
	}

	if result.Internal.Metric != "cpu" {
		t.Errorf("expected metric 'cpu', got '%s'", result.Internal.Metric)
	}

	if result.Internal.Aggregation == nil || result.Internal.Aggregation.Function != AggMean {
		t.Error("expected aggregation to be set to mean")
	}

	if result.Internal.Limit != 1000 {
		t.Errorf("expected limit 1000, got %d", result.Internal.Limit)
	}
}

func TestRelativeDurationParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"1h", time.Hour},
		{"30m", 30 * time.Minute},
		{"1d", 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"", 0},
		{"invalid", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseRelativeDuration(tt.input)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestComponentCreation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	// Test metric component
	metric := builder.CreateComponent(ComponentMetric, MetricComponent{Name: "cpu"})
	if metric.Type != ComponentMetric {
		t.Error("wrong component type")
	}
	if metric.ID == "" {
		t.Error("component should have an ID")
	}

	// Test filter component
	filter := builder.CreateComponent(ComponentFilter, FilterComponent{
		Field:    "host",
		Operator: "=",
		Value:    "server1",
	})
	if filter.Type != ComponentFilter {
		t.Error("wrong component type")
	}
}

func BenchmarkSQLGeneration(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	vq := &VisualQuerySpec{
		Components: []*QueryComponent{
			{Type: ComponentMetric, Properties: MetricComponent{Name: "cpu", Tags: map[string]string{"host": "server1"}}},
			{Type: ComponentTimeRange, Properties: TimeRangeComponent{Relative: "1h"}},
			{Type: ComponentAggregation, Properties: AggregationComponent{Function: "avg", Interval: "5m"}},
			{Type: ComponentFilter, Properties: FilterComponent{Field: "region", Operator: "=", Value: "us-east"}},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.GenerateQuery(context.Background(), vq)
	}
}

func BenchmarkAutocomplete(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	builder := NewVisualQueryBuilder(db, DefaultVisualQueryBuilderConfig())

	req := &AutocompleteRequest{
		Type:   "function",
		Prefix: "r",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Autocomplete(context.Background(), req)
	}
}
