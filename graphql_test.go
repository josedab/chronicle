package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGraphQLServer_NewGraphQLServer(t *testing.T) {
	server := NewGraphQLServer(nil)
	if server == nil {
		t.Fatal("NewGraphQLServer returned nil")
	}
	if server.subscriptions == nil {
		t.Error("subscriptions map should be initialized")
	}
}

func TestGraphQLServer_Execute_EmptyQuery(t *testing.T) {
	server := NewGraphQLServer(nil)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: "",
	})

	if len(resp.Errors) == 0 {
		t.Error("expected error for empty query")
	}
}

func TestGraphQLServer_Execute_InvalidSelectionSet(t *testing.T) {
	server := NewGraphQLServer(nil)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: "query { invalid",
	})

	if len(resp.Errors) == 0 {
		t.Error("expected error for invalid query")
	}
}

func TestGraphQLServer_Execute_MetricsQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some data
	db.WriteBatch([]Point{
		{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "memory", Value: 2.0, Timestamp: time.Now().UnixNano()},
	})

	server := NewGraphQLServer(db)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: `{ metrics }`,
	})

	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", resp.Errors)
	}

	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map data, got %T", resp.Data)
	}

	metrics, ok := data["metrics"].([]string)
	if !ok {
		t.Fatalf("expected []string for metrics")
	}

	if len(metrics) < 2 {
		t.Errorf("expected at least 2 metrics, got %d", len(metrics))
	}
}

func TestGraphQLServer_Execute_StatsQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewGraphQLServer(db)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: `{ stats }`,
	})

	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", resp.Errors)
	}

	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map data")
	}

	stats, ok := data["stats"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected stats map")
	}

	if _, ok := stats["version"]; !ok {
		t.Error("expected version in stats")
	}
}

func TestGraphQLServer_Execute_UnknownField(t *testing.T) {
	server := NewGraphQLServer(nil)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: `{ unknownField }`,
	})

	if len(resp.Errors) == 0 {
		t.Error("expected error for unknown field")
	}
}

func TestGraphQLServer_Execute_Mutation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewGraphQLServer(db)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: `mutation { write(metric: "test", value: "1.5") }`,
	})

	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", resp.Errors)
	}
}

func TestGraphQLServer_Execute_UnknownMutation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewGraphQLServer(db)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: `mutation { unknownMutation }`,
	})

	if len(resp.Errors) == 0 {
		t.Error("expected error for unknown mutation")
	}
}

func TestGraphQLServer_Handler_POST(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewGraphQLServer(db)
	handler := server.Handler()

	reqBody := `{"query": "{ metrics }"}`
	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp GraphQLResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestGraphQLServer_Handler_GET(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewGraphQLServer(db)
	handler := server.Handler()

	req := httptest.NewRequest(http.MethodGet, "/graphql?query={metrics}", nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestGraphQLServer_Handler_InvalidMethod(t *testing.T) {
	server := NewGraphQLServer(nil)
	handler := server.Handler()

	req := httptest.NewRequest(http.MethodPut, "/graphql", nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", rec.Code)
	}
}

func TestGraphQLServer_Handler_InvalidJSON(t *testing.T) {
	server := NewGraphQLServer(nil)
	handler := server.Handler()

	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString("invalid json"))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestGraphQLServer_Schema(t *testing.T) {
	server := NewGraphQLServer(nil)

	schema := server.Schema()
	if schema == "" {
		t.Error("schema should not be empty")
	}

	if !containsString(schema, "type Query") {
		t.Error("schema should contain Query type")
	}
	if !containsString(schema, "type Mutation") {
		t.Error("schema should contain Mutation type")
	}
}

func TestGraphQLServer_ServePlayground(t *testing.T) {
	server := NewGraphQLServer(nil)
	handler := server.ServePlayground()

	req := httptest.NewRequest(http.MethodGet, "/playground", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	if contentType := rec.Header().Get("Content-Type"); contentType != "text/html" {
		t.Errorf("expected text/html, got %s", contentType)
	}
}

func TestGraphQLServer_Subscribe(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewGraphQLServer(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, id, err := server.Subscribe(ctx, "{ metrics }", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if id == "" {
		t.Error("subscription id should not be empty")
	}

	// Wait for at least one result
	select {
	case result := <-ch:
		if result == nil {
			t.Error("result should not be nil")
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for subscription result")
	}

	// Unsubscribe
	server.Unsubscribe(id)
}

func TestGraphQLServer_Unsubscribe(t *testing.T) {
	server := NewGraphQLServer(nil)

	ctx := context.Background()
	_, id, _ := server.Subscribe(ctx, "{ metrics }", time.Hour)

	server.Unsubscribe(id)

	server.subscriptionsMu.RLock()
	_, exists := server.subscriptions[id]
	server.subscriptionsMu.RUnlock()

	if exists {
		t.Error("subscription should have been removed")
	}
}

func TestGraphQLServer_Unsubscribe_NotFound(t *testing.T) {
	server := NewGraphQLServer(nil)

	// Should not panic
	server.Unsubscribe("nonexistent")
}

func TestParseGraphQLQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantType string
		wantErr  bool
	}{
		{
			name:     "simple query",
			query:    "{ metrics }",
			wantType: "query",
		},
		{
			name:     "explicit query",
			query:    "query { metrics }",
			wantType: "query",
		},
		{
			name:     "mutation",
			query:    "mutation { write(metric: \"test\") }",
			wantType: "mutation",
		},
		{
			name:     "subscription",
			query:    "subscription { points(metric: \"cpu\") }",
			wantType: "subscription",
		},
		{
			name:     "named query",
			query:    "query GetMetrics { metrics }",
			wantType: "query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := parseGraphQLQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseGraphQLQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if op != nil && op.Type != tt.wantType {
				t.Errorf("parseGraphQLQuery() type = %v, want %v", op.Type, tt.wantType)
			}
		})
	}
}

func TestParseGraphQLArguments(t *testing.T) {
	args := parseGraphQLArguments(`metric: "cpu", start: "1h"`)

	if args["metric"] != "cpu" {
		t.Errorf("expected metric=cpu, got %v", args["metric"])
	}
	if args["start"] != "1h" {
		t.Errorf("expected start=1h, got %v", args["start"])
	}
}

func TestParseTimeArg(t *testing.T) {
	tests := []struct {
		input string
		check func(int64) bool
	}{
		{"", func(v int64) bool { return v == 0 }},
		{"1h", func(v int64) bool { return v > 0 }},
		{"2024-01-01T00:00:00Z", func(v int64) bool { return v > 0 }},
		{"1000000000", func(v int64) bool { return v == 1000000000 }},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseTimeArg(tt.input)
			if !tt.check(result) {
				t.Errorf("parseTimeArg(%q) = %d, unexpected", tt.input, result)
			}
		})
	}
}

func TestTagsToOutput(t *testing.T) {
	tags := map[string]string{
		"host": "server1",
		"env":  "prod",
	}

	output := tagsToOutput(tags)

	if len(output) != 2 {
		t.Errorf("expected 2 tags, got %d", len(output))
	}

	// Should be sorted by key
	if output[0].Key != "env" {
		t.Error("tags should be sorted by key")
	}
}

func TestTagsToOutput_Empty(t *testing.T) {
	output := tagsToOutput(nil)
	if output != nil {
		t.Error("nil input should return nil")
	}

	output = tagsToOutput(map[string]string{})
	if output != nil {
		t.Error("empty input should return nil")
	}
}

func TestGraphQLServer_Execute_SubscriptionError(t *testing.T) {
	server := NewGraphQLServer(nil)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: "subscription { points(metric: \"cpu\") }",
	})

	// Subscriptions via HTTP should return error
	if len(resp.Errors) == 0 {
		t.Error("expected error for subscription over HTTP")
	}
}

func TestGraphQLServer_Execute_ConfigQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	server := NewGraphQLServer(db)

	resp := server.Execute(context.Background(), GraphQLRequest{
		Query: `{ config }`,
	})

	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", resp.Errors)
	}

	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map data")
	}

	config, ok := data["config"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected config map")
	}

	if _, ok := config["bufferSize"]; !ok {
		t.Error("expected bufferSize in config")
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
