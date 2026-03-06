package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOpenAIProviderInterface(t *testing.T) {
	cfg := DefaultOpenAIConfig()
	provider := NewOpenAIProvider(cfg)

	if provider.ModelName() != "openai:gpt-4o-mini" {
		t.Fatalf("expected model name 'openai:gpt-4o-mini', got %s", provider.ModelName())
	}
	if provider.MaxTokens() != 2048 {
		t.Fatalf("expected max tokens 2048, got %d", provider.MaxTokens())
	}

	// Verify interface compliance
	var _ LLMProvider = provider
}

func TestOllamaProviderInterface(t *testing.T) {
	cfg := DefaultOllamaConfig()
	provider := NewOllamaProvider(cfg)

	if provider.ModelName() != "ollama:llama3.2" {
		t.Fatalf("expected model name 'ollama:llama3.2', got %s", provider.ModelName())
	}
	if provider.MaxTokens() != 2048 {
		t.Fatalf("expected max tokens 2048, got %d", provider.MaxTokens())
	}

	var _ LLMProvider = provider
}

func TestNLToPromQLTranslator_RuleBased(t *testing.T) {
	// Use TemplatedLLM as a fallback provider
	llm := NewTemplatedLLM(DefaultLLMConfig())
	translator := NewNLToPromQLTranslator(llm)

	tests := []struct {
		query    string
		contains string
	}{
		{"show me the rate of http_requests", "rate(http_requests"},
		{"what is the average cpu_usage", "avg_over_time(cpu_usage"},
		{"find the maximum memory_used", "max_over_time(memory_used"},
		{"top 10 metrics for disk_io", "topk(10, disk_io)"},
		{"predict future cpu_load", "predict_linear(cpu_load"},
		{"show total of request_count", "sum(request_count)"},
	}

	for _, tt := range tests {
		result := translator.ruleBasedTranslate(tt.query)
		if result.PromQL == "" {
			t.Errorf("empty PromQL for query: %s", tt.query)
			continue
		}
		if !strings.Contains(result.PromQL, tt.contains) {
			t.Errorf("query '%s': expected PromQL containing '%s', got '%s'", tt.query, tt.contains, result.PromQL)
		}
		if !result.Valid {
			t.Errorf("query '%s': expected valid result", tt.query)
		}
	}
}

func TestExtractPromQL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"rate(http_requests[5m])", "rate(http_requests[5m])"},
		{"```promql\nrate(http_requests[5m])\n```", "rate(http_requests[5m])"},
		{"```\nrate(http_requests[5m])\n```", "rate(http_requests[5m])"},
		{"# comment\nrate(http_requests[5m])", "rate(http_requests[5m])"},
	}

	for _, tt := range tests {
		got := extractPromQL(tt.input)
		if got != tt.expected {
			t.Errorf("extractPromQL(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestSchemaContext(t *testing.T) {
	llm := NewTemplatedLLM(DefaultLLMConfig())
	translator := NewNLToPromQLTranslator(llm)

	schema := SchemaContext{
		AvailableMetrics: []string{"cpu_usage", "memory_used", "disk_io"},
		LabelNames:       []string{"instance", "job", "region"},
	}
	translator.SetSchema(schema)

	// Verify schema is stored
	translator.mu.RLock()
	if len(translator.schema.AvailableMetrics) != 3 {
		t.Fatalf("expected 3 metrics in schema, got %d", len(translator.schema.AvailableMetrics))
	}
	translator.mu.RUnlock()
}

func TestAskEndpoint_Creation(t *testing.T) {
	llmConfig := DefaultLLMConfig()
	llm := NewTemplatedLLM(llmConfig)

	ragConfig := DefaultTSRAGConfig()
	ragEngine := NewTSRAGEngine(nil, ragConfig)

	pipeline := NewRAGPipeline(ragEngine, llm)
	translator := NewNLToPromQLTranslator(llm)

	endpoint := NewAskEndpoint(pipeline, translator)
	if endpoint == nil {
		t.Fatal("expected non-nil endpoint")
	}
}

func TestRagLLMCalculateConfidence(t *testing.T) {
	// No evidence -> low confidence
	conf := ragLLMCalculateConfidence(nil)
	if conf < 0 || conf > 1 {
		t.Fatalf("expected confidence in [0,1], got %f", conf)
	}

	// With evidence
	evidence := []Evidence{
		{Metric: "cpu", Similarity: 0.9, Pattern: PatternEmbedding{Features: PatternFeatures{Mean: 50}}},
		{Metric: "memory", Similarity: 0.8, Pattern: PatternEmbedding{Features: PatternFeatures{Mean: 70}}},
	}
	conf = ragLLMCalculateConfidence(evidence)
	if math.IsNaN(conf) || conf < 0 || conf > 1 {
		t.Fatalf("invalid confidence: %f", conf)
	}
}

func TestTranslate_WithContext(t *testing.T) {
	llm := NewTemplatedLLM(DefaultLLMConfig())
	translator := NewNLToPromQLTranslator(llm)

	ctx := context.Background()
	result, err := translator.Translate(ctx, "show me the rate of http_requests_total")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.PromQL == "" {
		t.Fatal("expected non-empty PromQL")
	}
}

func TestLLMAnomalyExplainer(t *testing.T) {
	llm := NewTemplatedLLM(DefaultLLMConfig())
	explainer := NewLLMAnomalyExplainer(llm)

	result := &AnomalyExplanationResult{
		Metric:         "cpu_usage",
		Value:          95.0,
		BaselineMean:   50.0,
		BaselineStddev: 10.0,
		Deviation:      4.5,
		RootCauses:     []string{"memory_pressure"},
		NLExplanation:  "fallback explanation",
	}

	ctx := context.Background()
	explanation, err := explainer.ExplainWithLLM(ctx, result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if explanation == "" {
		t.Fatal("expected non-empty explanation")
	}
}

func TestLLMAnomalyExplainer_NilResult(t *testing.T) {
	llm := NewTemplatedLLM(DefaultLLMConfig())
	explainer := NewLLMAnomalyExplainer(llm)

	_, err := explainer.ExplainWithLLM(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil result")
	}
}

func TestConversationMemory(t *testing.T) {
	cm := NewConversationMemory(5, time.Hour)

	cm.AddTurn("conv-1", ConversationEntry{Role: "user", Content: "what is cpu usage?"})
	cm.AddTurn("conv-1", ConversationEntry{Role: "assistant", Content: "cpu is at 50%"})
	cm.AddTurn("conv-1", ConversationEntry{Role: "user", Content: "show me the trend", QueryUsed: "rate(cpu[5m])"})

	conv := cm.GetOrCreate("conv-1")
	if len(conv.Turns) != 3 {
		t.Fatalf("expected 3 turns, got %d", len(conv.Turns))
	}
	if len(conv.QueryHistory) != 1 {
		t.Fatalf("expected 1 query in history, got %d", len(conv.QueryHistory))
	}

	ctx := cm.GetContext("conv-1")
	if len(ctx) != 3 {
		t.Fatalf("expected 3 context entries, got %d", len(ctx))
	}

	if cm.Count() != 1 {
		t.Fatalf("expected 1 conversation, got %d", cm.Count())
	}
}

func TestConversationMemory_TrimOld(t *testing.T) {
	cm := NewConversationMemory(3, time.Hour)

	for i := 0; i < 10; i++ {
		cm.AddTurn("conv-1", ConversationEntry{Role: "user", Content: "msg"})
	}

	conv := cm.GetOrCreate("conv-1")
	if len(conv.Turns) > 3 {
		t.Fatalf("expected max 3 turns after trim, got %d", len(conv.Turns))
	}
}

func TestConversationMemory_Cleanup(t *testing.T) {
	cm := NewConversationMemory(10, time.Millisecond)

	cm.AddTurn("conv-1", ConversationEntry{Role: "user", Content: "hello"})
	time.Sleep(5 * time.Millisecond)

	removed := cm.Cleanup()
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if cm.Count() != 0 {
		t.Fatal("expected 0 conversations after cleanup")
	}
}

func TestQueryValidator(t *testing.T) {
	v := NewQueryValidator()

	valid, _ := v.ValidatePromQL("rate(http_requests[5m])")
	if !valid {
		t.Error("expected valid PromQL")
	}

	valid, errMsg := v.ValidatePromQL("")
	if valid {
		t.Error("expected invalid for empty query")
	}
	if errMsg == "" {
		t.Error("expected error message")
	}
}

func TestQueryValidator_Sanitize(t *testing.T) {
	v := NewQueryValidator()

	cleaned, err := v.ValidateAndSanitize("  rate(http_requests[5m])  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cleaned != "rate(http_requests[5m])" {
		t.Errorf("expected trimmed query, got %q", cleaned)
	}

	_, err = v.ValidateAndSanitize("")
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestAskEndpoint_HTTP(t *testing.T) {
	llm := NewTemplatedLLM(DefaultLLMConfig())
	ragEngine := NewTSRAGEngine(nil, DefaultTSRAGConfig())
	pipeline := NewRAGPipeline(ragEngine, llm)
	translator := NewNLToPromQLTranslator(llm)

	endpoint := NewAskEndpoint(pipeline, translator)
	mux := http.NewServeMux()
	endpoint.RegisterHTTPHandlers(mux)

	t.Run("POST returns JSON response", func(t *testing.T) {
		body, _ := json.Marshal(AskRequest{
			Question:  "what is the average cpu usage?",
			Translate: true,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ask", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp AskResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp.ConversationID == "" {
			t.Error("expected conversation ID")
		}
		// Translate was requested, so query should be populated
		if resp.Query == nil {
			t.Error("expected translated query")
		} else if resp.Query.PromQL == "" {
			t.Error("expected non-empty PromQL")
		}
	})

	t.Run("GET returns method not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/ask", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("empty question returns 400", func(t *testing.T) {
		body, _ := json.Marshal(AskRequest{Question: ""})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ask", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("invalid JSON returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ask", bytes.NewReader([]byte("not json")))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("streaming returns SSE", func(t *testing.T) {
		body, _ := json.Marshal(AskRequest{
			Question: "show me cpu metrics",
			Stream:   true,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ask", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		ct := w.Header().Get("Content-Type")
		if ct != "text/event-stream" {
			t.Errorf("expected text/event-stream, got %s", ct)
		}
		if !strings.Contains(w.Body.String(), "data: ") {
			t.Error("expected SSE data prefix")
		}
		if !strings.Contains(w.Body.String(), "[DONE]") {
			t.Error("expected [DONE] terminator")
		}
	})

	t.Run("conversation memory persists", func(t *testing.T) {
		convID := "test-conv-persist"
		body1, _ := json.Marshal(AskRequest{Question: "what is cpu?", ConversationID: convID})
		req1 := httptest.NewRequest(http.MethodPost, "/api/v1/ask", bytes.NewReader(body1))
		req1.Header.Set("Content-Type", "application/json")
		w1 := httptest.NewRecorder()
		mux.ServeHTTP(w1, req1)

		body2, _ := json.Marshal(AskRequest{Question: "show me the trend", ConversationID: convID})
		req2 := httptest.NewRequest(http.MethodPost, "/api/v1/ask", bytes.NewReader(body2))
		req2.Header.Set("Content-Type", "application/json")
		w2 := httptest.NewRecorder()
		mux.ServeHTTP(w2, req2)

		// Memory should have turns from both requests
		conv := endpoint.memory.GetOrCreate(convID)
		if len(conv.Turns) < 2 {
			t.Errorf("expected at least 2 turns in memory, got %d", len(conv.Turns))
		}
	})
}
