package chronicle

import (
	"testing"
)

func TestNLQueryConfig(t *testing.T) {
	config := DefaultNLQueryConfig()

	if config.Provider == "" {
		t.Error("Provider should be set by default")
	}
	if !config.EnableConversation {
		t.Error("EnableConversation should be true by default")
	}
}

func TestNLQueryEngine(t *testing.T) {
	config := DefaultNLQueryConfig()
	engine := NewNLQueryEngine(nil, config)

	if engine == nil {
		t.Fatal("Failed to create NLQueryEngine")
	}
}

func TestNLQueryResponse(t *testing.T) {
	response := NLQueryResponse{
		Query:       "SELECT AVG(value) FROM cpu WHERE timestamp > NOW() - 1h",
		Explanation: "Computing the average CPU usage over the last hour",
		Confidence:  0.95,
	}

	if response.Query == "" {
		t.Error("Query should not be empty")
	}
	if response.Confidence != 0.95 {
		t.Errorf("Expected confidence 0.95, got %f", response.Confidence)
	}
}

func TestConversation(t *testing.T) {
	conv := Conversation{
		ID:    "conv-001",
		Turns: []ConversationTurn{},
	}

	// Add turns
	conv.Turns = append(conv.Turns, ConversationTurn{
		Role:    "user",
		Content: "Show me CPU usage",
	})
	conv.Turns = append(conv.Turns, ConversationTurn{
		Role:    "assistant",
		Content: "Here is your query",
	})

	if len(conv.Turns) != 2 {
		t.Errorf("Expected 2 turns, got %d", len(conv.Turns))
	}
}

func TestConversationTurn(t *testing.T) {
	turn := ConversationTurn{
		Role:    "user",
		Content: "Show average CPU usage",
		Query:   "SELECT AVG(value) FROM cpu",
	}

	if turn.Role != "user" {
		t.Error("Turn role should be user")
	}
	if turn.Content == "" {
		t.Error("Turn content should not be empty")
	}
}

func TestConversationContext(t *testing.T) {
	ctx := ConversationContext{
		CurrentMetric:     "cpu",
		CurrentFilters:    map[string]string{"host": "server1"},
		CurrentTimeRange:  "1h",
		ReferencedMetrics: []string{"cpu", "memory"},
	}

	if ctx.CurrentMetric != "cpu" {
		t.Errorf("Expected metric 'cpu', got %s", ctx.CurrentMetric)
	}
	if ctx.CurrentFilters["host"] != "server1" {
		t.Error("Filters should contain host=server1")
	}
}

func TestIntentPattern(t *testing.T) {
	pattern := IntentPattern{
		Name:     "aggregation",
		Priority: 10,
	}

	if pattern.Name == "" {
		t.Error("Pattern name should not be empty")
	}
	if pattern.Priority != 10 {
		t.Errorf("Expected priority 10, got %d", pattern.Priority)
	}
}

func TestVisualizationSuggestion(t *testing.T) {
	viz := VisualizationSuggestion{
		Type:  "line_chart",
		Title: "CPU Usage Over Time",
	}

	if viz.Type == "" {
		t.Error("Viz type should not be empty")
	}
}

func TestNLQueryEngineWithConversation(t *testing.T) {
	// Skip - requires a real DB instance
	t.Skip("Requires a real DB instance")
}

func TestNLQueryEngineGetConversation(t *testing.T) {
	config := DefaultNLQueryConfig()
	engine := NewNLQueryEngine(nil, config)

	// Manually add a conversation (doesn't require DB)
	engine.getOrCreateConversation("test-conv-123")

	// Then get it
	conv, found := engine.GetConversation("test-conv-123")
	if !found {
		t.Error("Conversation should be found")
	}
	if conv == nil {
		t.Error("Conversation should not be nil")
	}
	if conv.ID != "test-conv-123" {
		t.Errorf("Expected ID test-conv-123, got %s", conv.ID)
	}
}

func TestNLQueryEngineClearConversation(t *testing.T) {
	config := DefaultNLQueryConfig()
	engine := NewNLQueryEngine(nil, config)

	// Manually create a conversation
	conv := engine.getOrCreateConversation("clear-conv")
	conv.Turns = append(conv.Turns, ConversationTurn{Role: "user", Content: "test"})

	// Clear it
	engine.ClearConversation("clear-conv")

	// Should still exist but be empty
	conv, found := engine.GetConversation("clear-conv")
	if !found {
		t.Error("Conversation should still exist after clear")
	}
	if len(conv.Turns) != 0 {
		t.Errorf("Expected 0 turns after clear, got %d", len(conv.Turns))
	}
}

func TestNLQueryEngineDeleteConversation(t *testing.T) {
	config := DefaultNLQueryConfig()
	engine := NewNLQueryEngine(nil, config)

	// Manually create a conversation (doesn't require DB)
	engine.getOrCreateConversation("delete-conv")

	// Delete it
	engine.DeleteConversation("delete-conv")

	// Should not be found
	_, found := engine.GetConversation("delete-conv")
	if found {
		t.Error("Conversation should be deleted")
	}
}

func TestNLQueryEngineQuery(t *testing.T) {
	// Skip - requires a real DB instance
	t.Skip("Requires a real DB instance")
}

func TestQueryOptimizer(t *testing.T) {
	optimizer := QueryOptimizer{}

	// Just test that the struct exists
	_ = optimizer
}

func TestNLAnalytics(t *testing.T) {
	analytics := NewNLAnalytics()

	// Just test that the struct exists
	_ = analytics
}

func TestNLQueryRecord(t *testing.T) {
	record := NLQueryRecord{
		Input:   "show me cpu usage",
		Success: true,
	}

	if record.Input == "" {
		t.Error("Record input should not be empty")
	}
}

func TestNLAnalyticsReport(t *testing.T) {
	report := NLAnalyticsReport{
		TotalQueries: 100,
		SuccessRate:  0.90,
	}

	if report.TotalQueries != 100 {
		t.Errorf("Expected 100 total queries, got %d", report.TotalQueries)
	}
}

func TestGetAnalytics(t *testing.T) {
	config := DefaultNLQueryConfig()
	engine := NewNLQueryEngine(nil, config)

	report := engine.GetAnalytics()

	// New engine should have zero queries
	if report.TotalQueries != 0 {
		t.Errorf("Expected 0 queries for new engine, got %d", report.TotalQueries)
	}
}

func TestNLQueryHandler(t *testing.T) {
	config := DefaultNLQueryConfig()
	engine := NewNLQueryEngine(nil, config)

	handler := engine.NLQueryHandler()
	if handler == nil {
		t.Error("Handler should not be nil")
	}
}
