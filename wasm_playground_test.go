package chronicle

import (
	"testing"
)

func TestPlaygroundExecuteQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultPlaygroundConfig()
	pg := NewPlayground(db, config)

	resp := pg.ExecuteQuery(PlaygroundQueryRequest{Query: "", Language: "sql"})
	if resp.Error == "" {
		t.Error("expected error for empty query")
	}

	longQuery := make([]byte, config.MaxQueryLength+1)
	for i := range longQuery {
		longQuery[i] = 'a'
	}
	resp = pg.ExecuteQuery(PlaygroundQueryRequest{Query: string(longQuery)})
	if resp.Error == "" {
		t.Error("expected error for query exceeding max length")
	}
}

func TestPlaygroundSuggestChart(t *testing.T) {
	pg := NewPlayground(nil, DefaultPlaygroundConfig())

	resp := PlaygroundQueryResponse{
		Columns: []string{"time", "value"},
		Rows:    [][]interface{}{{"2024-01-01", 42.0}},
	}
	suggestion := pg.SuggestChart(resp)
	if suggestion == nil {
		t.Fatal("expected chart suggestion")
	}
	if suggestion.Type != "line" {
		t.Errorf("expected line chart, got %s", suggestion.Type)
	}

	resp2 := PlaygroundQueryResponse{
		Columns: []string{"host", "count"},
		Rows:    [][]interface{}{{"web-1", 100}},
	}
	suggestion2 := pg.SuggestChart(resp2)
	if suggestion2 == nil {
		t.Fatal("expected bar chart suggestion")
	}
	if suggestion2.Type != "bar" {
		t.Errorf("expected bar chart, got %s", suggestion2.Type)
	}

	errResp := PlaygroundQueryResponse{Error: "some error"}
	if pg.SuggestChart(errResp) != nil {
		t.Error("expected no suggestion for error response")
	}
}

func TestPlaygroundListSessions(t *testing.T) {
	pg := NewPlayground(nil, DefaultPlaygroundConfig())
	sessions := pg.ListSessions()
	if len(sessions) != 0 {
		t.Errorf("expected 0 sessions, got %d", len(sessions))
	}
}

func TestPlaygroundDefaultConfig(t *testing.T) {
	config := DefaultPlaygroundConfig()
	if config.Theme != "dark" {
		t.Errorf("expected dark theme, got %s", config.Theme)
	}
	if config.MaxQueryLength != 10000 {
		t.Errorf("expected max query length 10000, got %d", config.MaxQueryLength)
	}
	if !config.EnableCharts {
		t.Error("expected charts enabled by default")
	}
}

func TestPlaygroundCQLFallback(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	pg := NewPlayground(db, DefaultPlaygroundConfig())

	// CQL engine may or may not be available; just ensure no panic
	resp := pg.ExecuteQuery(PlaygroundQueryRequest{Query: "SELECT * FROM test", Language: "cql"})
	_ = resp
}
