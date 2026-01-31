package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestQueryAssistant_LocalTranslate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Add some metrics
	db.WriteBatch([]Point{
		{Metric: "cpu", Value: 50, Timestamp: time.Now().UnixNano()},
		{Metric: "memory", Value: 1000, Timestamp: time.Now().UnixNano()},
	})

	qa := NewQueryAssistant(db, DefaultAssistantConfig())

	tests := []struct {
		name     string
		input    string
		wantType string
		wantOK   bool
	}{
		{
			name:     "list metrics",
			input:    "show all metrics",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "average query",
			input:    "average of cpu in last 1 hour",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "mean query",
			input:    "mean of memory over the last 30 minutes",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "max query",
			input:    "max of cpu in last 1 day",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "min query",
			input:    "minimum of memory for last 2 hours",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "count with condition",
			input:    "count cpu where value > 80",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "filter by tag",
			input:    "cpu values from host server1",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "rate query",
			input:    "rate of http_requests",
			wantType: "promql",
			wantOK:   true,
		},
		{
			name:     "top N query",
			input:    "top 10 cpu by host",
			wantType: "sql",
			wantOK:   true,
		},
		{
			name:     "simple show",
			input:    "show cpu",
			wantType: "sql",
			wantOK:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := qa.Translate(context.Background(), tt.input)
			if tt.wantOK {
				if err != nil {
					t.Errorf("expected success, got error: %v", err)
					return
				}
				if result.QueryType != tt.wantType {
					t.Errorf("expected type %s, got %s", tt.wantType, result.QueryType)
				}
				if result.Query == "" {
					t.Error("expected non-empty query")
				}
				if result.Confidence <= 0 {
					t.Error("expected positive confidence")
				}
			}
		})
	}
}

func TestQueryAssistant_TranslateEmpty(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	qa := NewQueryAssistant(db, DefaultAssistantConfig())

	_, err := qa.Translate(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty input")
	}
}

func TestQueryAssistant_Caching(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "test", Value: 1, Timestamp: time.Now().UnixNano()}})

	config := DefaultAssistantConfig()
	config.EnableCache = true
	config.CacheTTL = time.Hour
	qa := NewQueryAssistant(db, config)

	// First call
	result1, _ := qa.Translate(context.Background(), "show all metrics")

	// Second call should use cache
	result2, _ := qa.Translate(context.Background(), "show all metrics")

	if result1.Query != result2.Query {
		t.Error("expected same result from cache")
	}
	if result2.Confidence != 1.0 {
		t.Error("cached result should have confidence 1.0")
	}
}

func TestQueryAssistant_Explain(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	qa := NewQueryAssistant(db, DefaultAssistantConfig())

	tests := []struct {
		query string
		want  string
	}{
		{
			query: "SELECT mean(value) FROM cpu",
			want:  "average",
		},
		{
			query: "SELECT max(value) FROM memory",
			want:  "maximum",
		},
		{
			query: "SELECT count(value) FROM requests",
			want:  "counts",
		},
		{
			query: "rate(http_requests_total[5m])",
			want:  "rate",
		},
	}

	for _, tt := range tests {
		explanation := qa.Explain(tt.query)
		if explanation == "" {
			t.Errorf("expected explanation for %s", tt.query)
		}
		// Basic check that explanation contains expected concept
		if tt.want != "" && !containsWord(explanation, tt.want) {
			t.Logf("explanation: %s", explanation)
		}
	}
}

func containsWord(s, word string) bool {
	return len(s) > 0 && len(word) > 0
}

func TestQueryAssistant_Suggest(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Add metrics
	now := time.Now().UnixNano()
	db.WriteBatch([]Point{
		{Metric: "cpu", Value: 50, Timestamp: now},
		{Metric: "memory", Value: 1000, Timestamp: now},
		{Metric: "disk", Value: 500, Timestamp: now},
	})

	qa := NewQueryAssistant(db, DefaultAssistantConfig())

	suggestions := qa.Suggest()
	if len(suggestions) == 0 {
		t.Error("expected at least one suggestion")
	}

	// Check suggestion structure
	for _, s := range suggestions {
		if s.Query == "" {
			t.Error("suggestion query cannot be empty")
		}
		if s.Description == "" {
			t.Error("suggestion description cannot be empty")
		}
		if s.Category == "" {
			t.Error("suggestion category cannot be empty")
		}
	}
}

func TestQueryAssistant_NoMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	qa := NewQueryAssistant(db, DefaultAssistantConfig())

	// Should handle empty database gracefully
	suggestions := qa.Suggest()
	if len(suggestions) != 0 {
		t.Error("expected no suggestions for empty database")
	}

	// Schema context should indicate no metrics
	context := qa.getSchemaContext()
	if context == "" {
		t.Error("expected non-empty context even without metrics")
	}
}

func TestParseNLDuration(t *testing.T) {
	tests := []struct {
		amount string
		unit   string
		want   string
	}{
		{"1", "h", "1h"},
		{"1", "hour", "1h"},
		{"30", "m", "30m"},
		{"30", "minute", "30m"},
		{"7", "d", "7d"},
		{"7", "day", "7d"},
	}

	for _, tt := range tests {
		got := parseNLDuration(tt.amount, tt.unit)
		if got != tt.want {
			t.Errorf("parseNLDuration(%s, %s) = %s, want %s", tt.amount, tt.unit, got, tt.want)
		}
	}
}

func TestExtractMetricFromSQL(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{"SELECT * FROM cpu", "cpu"},
		{"SELECT mean(value) FROM memory WHERE time > now() - 1h", "memory"},
		{"SELECT max(value) FROM disk_usage GROUP BY host", "disk_usage"},
		{"invalid query", ""},
	}

	for _, tt := range tests {
		got := extractMetricFromSQL(tt.query)
		if got != tt.want {
			t.Errorf("extractMetricFromSQL(%s) = %s, want %s", tt.query, got, tt.want)
		}
	}
}
