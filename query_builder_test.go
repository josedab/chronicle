package chronicle

import (
	"strings"
	"testing"
	"time"
)

func TestQueryBuilder_BuildSQL(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	tests := []struct {
		name     string
		query    *VisualQuery
		contains []string
		wantErr  bool
	}{
		{
			name: "simple select",
			query: &VisualQuery{
				Source: QuerySource{Type: "metric", Name: "cpu_usage"},
				Select: []SelectItem{{Field: "*"}},
			},
			contains: []string{"SELECT *", "FROM cpu_usage"},
		},
		{
			name: "select with aggregation",
			query: &VisualQuery{
				Source: QuerySource{Type: "metric", Name: "cpu_usage"},
				Select: []SelectItem{
					{Field: "value", Aggregation: "avg", Alias: "avg_cpu"},
				},
				GroupBy: []string{"host"},
			},
			contains: []string{"AVG(value) AS avg_cpu", "GROUP BY host"},
		},
		{
			name: "select with filter",
			query: &VisualQuery{
				Source: QuerySource{Type: "metric", Name: "http_requests"},
				Select: []SelectItem{{Field: "count(*)", Alias: "total"}},
				Filters: []QueryFilter{
					{Field: "status", Operator: ">=", Value: 500},
				},
			},
			contains: []string{"WHERE", "status >= 500"},
		},
		{
			name: "select with multiple filters",
			query: &VisualQuery{
				Source: QuerySource{Type: "metric", Name: "requests"},
				Select: []SelectItem{{Field: "*"}},
				Filters: []QueryFilter{
					{Field: "region", Operator: "=", Value: "us-east"},
					{Field: "status", Operator: "<", Value: 400, AndOr: "AND"},
				},
			},
			contains: []string{"region = 'us-east'", "AND", "status < 400"},
		},
		{
			name: "select with order and limit",
			query: &VisualQuery{
				Source:  QuerySource{Type: "metric", Name: "latency"},
				Select:  []SelectItem{{Field: "value"}},
				OrderBy: []OrderItem{{Field: "value", Direction: "desc"}},
				Limit:   10,
			},
			contains: []string{"ORDER BY value DESC", "LIMIT 10"},
		},
		{
			name: "select with time range",
			query: &VisualQuery{
				Source: QuerySource{Type: "metric", Name: "events"},
				Select: []SelectItem{{Field: "*"}},
				TimeRange: &QueryTimeRange{
					Type:     "relative",
					Relative: "1h",
				},
			},
			contains: []string{"timestamp >= NOW() - INTERVAL '1h'"},
		},
		{
			name: "select with join",
			query: &VisualQuery{
				Source: QuerySource{Type: "metric", Name: "orders", Alias: "o"},
				Select: []SelectItem{{Field: "o.id"}, {Field: "c.name"}},
				Joins: []QueryJoin{
					{
						Type:      "left",
						Source:    QuerySource{Name: "customers", Alias: "c"},
						Condition: "o.customer_id = c.id",
					},
				},
			},
			contains: []string{"LEFT JOIN customers AS c ON o.customer_id = c.id"},
		},
		{
			name:    "nil query",
			query:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := qb.BuildSQL(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			for _, expected := range tt.contains {
				if !strings.Contains(sql, expected) {
					t.Errorf("SQL should contain %q, got:\n%s", expected, sql)
				}
			}
		})
	}
}

func TestQueryBuilder_ValidateQuery(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	tests := []struct {
		name     string
		query    *VisualQuery
		wantErrs int
	}{
		{
			name: "valid query",
			query: &VisualQuery{
				Source: QuerySource{Name: "metrics"},
				Select: []SelectItem{{Field: "*"}},
			},
			wantErrs: 0,
		},
		{
			name: "missing source name",
			query: &VisualQuery{
				Source: QuerySource{Type: "metric"},
			},
			wantErrs: 1,
		},
		{
			name: "invalid filter",
			query: &VisualQuery{
				Source: QuerySource{Name: "metrics"},
				Filters: []QueryFilter{
					{Field: "", Operator: "=", Value: "test"},
				},
			},
			wantErrs: 1,
		},
		{
			name: "negative limit",
			query: &VisualQuery{
				Source: QuerySource{Name: "metrics"},
				Limit:  -1,
			},
			wantErrs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := qb.ValidateQuery(tt.query)
			if len(errs) != tt.wantErrs {
				t.Errorf("expected %d errors, got %d: %v", tt.wantErrs, len(errs), errs)
			}
		})
	}
}

func TestQueryBuilder_SavedQueries(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	// Save a query
	sq := &SavedQuery{
		Name:        "My Query",
		Description: "Test query",
		Query: &VisualQuery{
			Source: QuerySource{Name: "metrics"},
		},
		Owner:    "user1",
		IsPublic: true,
	}

	err := qb.SaveQuery(sq)
	if err != nil {
		t.Fatalf("SaveQuery error: %v", err)
	}

	if sq.ID == "" {
		t.Error("expected ID to be generated")
	}

	// Retrieve query
	retrieved, ok := qb.GetSavedQuery(sq.ID)
	if !ok {
		t.Fatal("expected query to exist")
	}

	if retrieved.Name != "My Query" {
		t.Errorf("expected name 'My Query', got %s", retrieved.Name)
	}

	// List queries
	queries := qb.ListSavedQueries("user1", false)
	if len(queries) != 1 {
		t.Errorf("expected 1 query, got %d", len(queries))
	}

	// Delete query
	deleted := qb.DeleteSavedQuery(sq.ID)
	if !deleted {
		t.Error("expected delete to succeed")
	}

	_, ok = qb.GetSavedQuery(sq.ID)
	if ok {
		t.Error("expected query to be deleted")
	}
}

func TestQueryBuilder_SaveQueryRequiresName(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	err := qb.SaveQuery(&SavedQuery{
		Query: &VisualQuery{Source: QuerySource{Name: "test"}},
	})
	if err == nil {
		t.Error("expected error for missing name")
	}
}

func TestQueryBuilder_History(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	// Record history
	for i := 0; i < 5; i++ {
		qb.RecordHistory(&QueryHistoryEntry{
			Query:      &VisualQuery{Source: QuerySource{Name: "metrics"}},
			SQL:        "SELECT * FROM metrics",
			ExecutedAt: time.Now(),
			Duration:   100,
			User:       "user1",
			Success:    true,
		})
	}

	history := qb.GetHistory("user1", 10)
	if len(history) != 5 {
		t.Errorf("expected 5 entries, got %d", len(history))
	}

	// Test limit
	history = qb.GetHistory("", 3)
	if len(history) != 3 {
		t.Errorf("expected 3 entries with limit, got %d", len(history))
	}
}

func TestQueryBuilder_Templates(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	templates := qb.GetTemplates()
	if len(templates) == 0 {
		t.Error("expected built-in templates")
	}

	// Check specific template
	template, ok := qb.GetTemplate("avg-by-time")
	if !ok {
		t.Fatal("expected avg-by-time template")
	}

	if template.Category != "aggregation" {
		t.Errorf("expected category 'aggregation', got %s", template.Category)
	}
}

func TestQueryBuilder_ApplyTemplate(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	// Apply template with parameters
	query, err := qb.ApplyTemplate("avg-by-time", map[string]any{
		"metric": "cpu_usage",
	})
	if err != nil {
		t.Fatalf("ApplyTemplate error: %v", err)
	}

	if query.Source.Name != "cpu_usage" {
		t.Errorf("expected source name 'cpu_usage', got %s", query.Source.Name)
	}
}

func TestQueryBuilder_ApplyTemplateInvalid(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	// Non-existent template
	_, err := qb.ApplyTemplate("nonexistent", nil)
	if err == nil {
		t.Error("expected error for non-existent template")
	}

	// Missing required parameter
	_, err = qb.ApplyTemplate("avg-by-time", map[string]any{})
	if err == nil {
		t.Error("expected error for missing required parameter")
	}
}

func TestQueryBuilder_Autocomplete(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	tests := []struct {
		context  AutocompleteContext
		minCount int
	}{
		{AutocompleteContext{Type: "metric", Prefix: ""}, 1},
		{AutocompleteContext{Type: "field", Prefix: ""}, 1},
		{AutocompleteContext{Type: "operator", Prefix: ""}, 5},
		{AutocompleteContext{Type: "aggregation", Prefix: ""}, 5},
		{AutocompleteContext{Type: "function", Prefix: ""}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.context.Type, func(t *testing.T) {
			suggestions := qb.GetAutocomplete(tt.context)
			if len(suggestions) < tt.minCount {
				t.Errorf("expected at least %d suggestions for %s, got %d",
					tt.minCount, tt.context.Type, len(suggestions))
			}
		})
	}
}

func TestQueryBuilder_AutocompleteWithPrefix(t *testing.T) {
	db := &DB{}
	qb := NewQueryBuilder(db, DefaultQueryBuilderConfig())

	// Test metric prefix
	suggestions := qb.GetAutocomplete(AutocompleteContext{
		Type:   "metric",
		Prefix: "cpu",
	})

	for _, s := range suggestions {
		if !strings.HasPrefix(s.Value, "cpu") {
			t.Errorf("suggestion %s should start with 'cpu'", s.Value)
		}
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		value    any
		expected string
	}{
		{"test", "'test'"},
		{123, "123"},
		{45.67, "45.67"},
		{[]string{"a", "b"}, "'a', 'b'"},
	}

	for _, tt := range tests {
		result := formatValue(tt.value)
		if result != tt.expected {
			t.Errorf("formatValue(%v) = %s, want %s", tt.value, result, tt.expected)
		}
	}
}

func TestDefaultQueryBuilderConfig(t *testing.T) {
	config := DefaultQueryBuilderConfig()

	if !config.Enabled {
		t.Error("expected Enabled to be true")
	}
	if config.MaxSavedQueries != 100 {
		t.Errorf("expected MaxSavedQueries 100, got %d", config.MaxSavedQueries)
	}
	if config.AutocompleteLimit != 50 {
		t.Errorf("expected AutocompleteLimit 50, got %d", config.AutocompleteLimit)
	}
}
