package chronicle

import (
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	db := setupTestDB(t)

	// Write a point so we can query.
	if err := db.Write(Point{
		Metric:    "query.test",
		Value:     1.0,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "a"},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	tests := []struct {
		name    string
		query   *Query
		wantErr bool
	}{
		{
			name:    "basic_query",
			query:   &Query{Metric: "query.test"},
			wantErr: false,
		},
		{
			name:    "query_with_tag_filter",
			query:   &Query{Metric: "query.test", Tags: map[string]string{"host": "a"}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.Execute(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
			if result == nil {
				t.Error("expected non-nil result")
			}
		})
	}
}
