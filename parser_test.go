package chronicle

import (
	"testing"
	"time"
)

func TestQueryParserBasic(t *testing.T) {
	parser := &QueryParser{}

	tests := []struct {
		name    string
		query   string
		wantErr bool
		check   func(*Query) error
	}{
		{
			name:  "simple select with count",
			query: "SELECT count(value) FROM cpu",
			check: func(q *Query) error {
				if q.Metric != "cpu" {
					t.Errorf("expected metric 'cpu', got '%s'", q.Metric)
				}
				return nil
			},
		},
		{
			name:  "select with aggregation",
			query: "SELECT mean(value) FROM temperature",
			check: func(q *Query) error {
				if q.Aggregation == nil {
					t.Error("expected aggregation")
					return nil
				}
				if q.Aggregation.Function != AggMean {
					t.Errorf("expected AggMean, got %v", q.Aggregation.Function)
				}
				return nil
			},
		},
		{
			name:  "select with where equals",
			query: "SELECT count(value) FROM cpu WHERE host = 'server1'",
			check: func(q *Query) error {
				if len(q.TagFilters) != 1 {
					t.Errorf("expected 1 tag filter, got %d", len(q.TagFilters))
					return nil
				}
				if q.TagFilters[0].Key != "host" {
					t.Errorf("expected key 'host', got '%s'", q.TagFilters[0].Key)
				}
				if q.TagFilters[0].Op != TagOpEq {
					t.Errorf("expected TagOpEq, got %v", q.TagFilters[0].Op)
				}
				return nil
			},
		},
		{
			name:  "select with where not equals",
			query: "SELECT sum(value) FROM cpu WHERE host != 'server1'",
			check: func(q *Query) error {
				if len(q.TagFilters) != 1 {
					t.Errorf("expected 1 tag filter, got %d", len(q.TagFilters))
					return nil
				}
				if q.TagFilters[0].Op != TagOpNotEq {
					t.Errorf("expected TagOpNotEq, got %v", q.TagFilters[0].Op)
				}
				return nil
			},
		},
		{
			name:  "select with IN clause",
			query: "SELECT max(value) FROM cpu WHERE region IN ('us', 'eu')",
			check: func(q *Query) error {
				if len(q.TagFilters) != 1 {
					t.Errorf("expected 1 tag filter, got %d", len(q.TagFilters))
					return nil
				}
				if q.TagFilters[0].Op != TagOpIn {
					t.Errorf("expected TagOpIn, got %v", q.TagFilters[0].Op)
				}
				if len(q.TagFilters[0].Values) != 2 {
					t.Errorf("expected 2 values, got %d", len(q.TagFilters[0].Values))
				}
				return nil
			},
		},
		{
			name:  "select with group by",
			query: "SELECT mean(value) FROM cpu GROUP BY host",
			check: func(q *Query) error {
				if len(q.GroupBy) != 1 {
					t.Errorf("expected 1 group by, got %d", len(q.GroupBy))
					return nil
				}
				if q.GroupBy[0] != "host" {
					t.Errorf("expected 'host', got '%s'", q.GroupBy[0])
				}
				return nil
			},
		},
		{
			name:  "select with time group by",
			query: "SELECT mean(value) FROM cpu GROUP BY time(5m)",
			check: func(q *Query) error {
				if q.Aggregation == nil {
					t.Error("expected aggregation")
					return nil
				}
				if q.Aggregation.Window != 5*time.Minute {
					t.Errorf("expected 5m window, got %v", q.Aggregation.Window)
				}
				return nil
			},
		},
		{
			name:  "select with limit",
			query: "SELECT min(value) FROM cpu LIMIT 100",
			check: func(q *Query) error {
				if q.Limit != 100 {
					t.Errorf("expected limit 100, got %d", q.Limit)
				}
				return nil
			},
		},
		{
			name:    "empty query",
			query:   "",
			wantErr: true,
		},
		{
			name:    "missing FROM",
			query:   "SELECT",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
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
			if tt.check != nil {
				_ = tt.check(q)
			}
		})
	}
}

func TestQueryParserAggFunctions(t *testing.T) {
	parser := &QueryParser{}

	funcs := []struct {
		name string
		want AggFunc
	}{
		{"count(value)", AggCount},
		{"sum(value)", AggSum},
		{"mean(value)", AggMean},
		{"min(value)", AggMin},
		{"max(value)", AggMax},
		{"first(value)", AggFirst},
		{"last(value)", AggLast},
		{"stddev(value)", AggStddev},
	}

	for _, f := range funcs {
		t.Run(f.name, func(t *testing.T) {
			q, err := parser.Parse("SELECT " + f.name + " FROM cpu")
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if q.Aggregation == nil {
				t.Fatal("expected aggregation")
			}
			if q.Aggregation.Function != f.want {
				t.Errorf("expected %v, got %v", f.want, q.Aggregation.Function)
			}
		})
	}
}

func TestQueryParserTimeRange(t *testing.T) {
	parser := &QueryParser{}

	q, err := parser.Parse("SELECT count(value) FROM cpu WHERE time >= now() - 1h AND time <= now()")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if q.Start == 0 {
		t.Error("expected start time")
	}
	if q.End == 0 {
		t.Error("expected end time")
	}
	if q.Start >= q.End {
		t.Error("start should be before end")
	}
}

func TestQueryParserMultipleFilters(t *testing.T) {
	parser := &QueryParser{}

	q, err := parser.Parse("SELECT sum(value) FROM cpu WHERE host = 'a' AND region = 'us'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if len(q.TagFilters) != 2 {
		t.Errorf("expected 2 tag filters, got %d", len(q.TagFilters))
	}
}

func TestQueryParserComplexQuery(t *testing.T) {
	parser := &QueryParser{}

	q, err := parser.Parse("SELECT mean(value) FROM cpu WHERE host != 'a' AND region IN ('us','eu') GROUP BY time(5m), host LIMIT 1000")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if q.Metric != "cpu" {
		t.Errorf("expected metric 'cpu', got '%s'", q.Metric)
	}
	if q.Aggregation == nil || q.Aggregation.Function != AggMean {
		t.Error("expected mean aggregation")
	}
	if q.Aggregation.Window != 5*time.Minute {
		t.Errorf("expected 5m window, got %v", q.Aggregation.Window)
	}
	if len(q.TagFilters) != 2 {
		t.Errorf("expected 2 tag filters, got %d", len(q.TagFilters))
	}
	if len(q.GroupBy) != 1 || q.GroupBy[0] != "host" {
		t.Errorf("expected group by host, got %v", q.GroupBy)
	}
	if q.Limit != 1000 {
		t.Errorf("expected limit 1000, got %d", q.Limit)
	}
}
