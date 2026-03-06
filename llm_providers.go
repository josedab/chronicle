package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// --- OpenAI API LLM Provider ---

// OpenAIProvider implements LLMProvider using the OpenAI-compatible API.
type OpenAIProvider struct {
	config   OpenAIConfig
	client   *http.Client
	mu       sync.RWMutex
	reqCount int
}

// OpenAIConfig configures the OpenAI API provider.
type OpenAIConfig struct {
	APIKey      string  `json:"api_key"`
	BaseURL     string  `json:"base_url"`
	Model       string  `json:"model"`
	MaxTokens   int     `json:"max_tokens"`
	Temperature float64 `json:"temperature"`
	TopP        float64 `json:"top_p"`
	Timeout     time.Duration `json:"timeout"`
}

// DefaultOpenAIConfig returns sensible defaults for OpenAI configuration.
func DefaultOpenAIConfig() OpenAIConfig {
	return OpenAIConfig{
		BaseURL:     "https://api.openai.com/v1",
		Model:       "gpt-4o-mini",
		MaxTokens:   2048,
		Temperature: 0.3,
		TopP:        0.9,
		Timeout:     30 * time.Second,
	}
}

// NewOpenAIProvider creates a new OpenAI API provider.
func NewOpenAIProvider(config OpenAIConfig) *OpenAIProvider {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.openai.com/v1"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	return &OpenAIProvider{
		config: config,
		client: &http.Client{Timeout: config.Timeout},
	}
}

type openAIChatRequest struct {
	Model       string              `json:"model"`
	Messages    []openAIChatMessage `json:"messages"`
	MaxTokens   int                 `json:"max_tokens,omitempty"`
	Temperature float64             `json:"temperature"`
	TopP        float64             `json:"top_p,omitempty"`
	Stream      bool                `json:"stream,omitempty"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Generate produces a response from the given prompt.
func (p *OpenAIProvider) Generate(ctx context.Context, prompt string) (string, error) {
	return p.GenerateWithContext(ctx, prompt, nil)
}

// GenerateWithContext produces a response using the prompt and retrieved context evidence.
func (p *OpenAIProvider) GenerateWithContext(ctx context.Context, prompt string, evidence []string) (string, error) {
	messages := []openAIChatMessage{
		{Role: "system", Content: "You are a time-series data analysis assistant specialized in PromQL and SQL queries for observability data. Answer questions about metrics, trends, anomalies, and forecasts based on provided evidence."},
	}

	if len(evidence) > 0 {
		contextMsg := "Context evidence:\n" + strings.Join(evidence, "\n")
		messages = append(messages, openAIChatMessage{Role: "system", Content: contextMsg})
	}

	messages = append(messages, openAIChatMessage{Role: "user", Content: prompt})

	reqBody := openAIChatRequest{
		Model:       p.config.Model,
		Messages:    messages,
		MaxTokens:   p.config.MaxTokens,
		Temperature: p.config.Temperature,
		TopP:        p.config.TopP,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("openai: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.config.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("openai: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if p.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.config.APIKey)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("openai: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("openai: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("openai: API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var chatResp openAIChatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return "", fmt.Errorf("openai: decode response: %w", err)
	}

	if chatResp.Error != nil {
		return "", fmt.Errorf("openai: %s", chatResp.Error.Message)
	}

	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("openai: no response choices")
	}

	p.mu.Lock()
	p.reqCount++
	p.mu.Unlock()

	return chatResp.Choices[0].Message.Content, nil
}

func (p *OpenAIProvider) ModelName() string { return "openai:" + p.config.Model }
func (p *OpenAIProvider) MaxTokens() int   { return p.config.MaxTokens }

// --- Ollama Local Model Provider ---

// OllamaProvider implements LLMProvider using a local Ollama server.
type OllamaProvider struct {
	config   OllamaConfig
	client   *http.Client
	mu       sync.RWMutex
	reqCount int
}

// OllamaConfig configures the Ollama local model provider.
type OllamaConfig struct {
	BaseURL   string        `json:"base_url"`
	Model     string        `json:"model"`
	MaxTokens int           `json:"max_tokens"`
	Timeout   time.Duration `json:"timeout"`
}

// DefaultOllamaConfig returns sensible defaults for Ollama configuration.
func DefaultOllamaConfig() OllamaConfig {
	return OllamaConfig{
		BaseURL:   "http://localhost:11434",
		Model:     "llama3.2",
		MaxTokens: 2048,
		Timeout:   60 * time.Second,
	}
}

// NewOllamaProvider creates a new Ollama local model provider.
func NewOllamaProvider(config OllamaConfig) *OllamaProvider {
	if config.BaseURL == "" {
		config.BaseURL = "http://localhost:11434"
	}
	if config.Timeout == 0 {
		config.Timeout = 60 * time.Second
	}
	return &OllamaProvider{
		config: config,
		client: &http.Client{Timeout: config.Timeout},
	}
}

type ollamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
	Error    string `json:"error,omitempty"`
}

// Generate produces a response from the given prompt via Ollama.
func (p *OllamaProvider) Generate(ctx context.Context, prompt string) (string, error) {
	return p.GenerateWithContext(ctx, prompt, nil)
}

// GenerateWithContext produces a response using the prompt and evidence via Ollama.
func (p *OllamaProvider) GenerateWithContext(ctx context.Context, prompt string, evidence []string) (string, error) {
	fullPrompt := prompt
	if len(evidence) > 0 {
		fullPrompt = "Context:\n" + strings.Join(evidence, "\n") + "\n\nQuestion: " + prompt
	}

	reqBody := ollamaGenerateRequest{
		Model:  p.config.Model,
		Prompt: fullPrompt,
		Stream: false,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("ollama: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.config.BaseURL+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("ollama: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("ollama: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama: API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var genResp ollamaGenerateResponse
	if err := json.Unmarshal(respBody, &genResp); err != nil {
		return "", fmt.Errorf("ollama: decode response: %w", err)
	}

	if genResp.Error != "" {
		return "", fmt.Errorf("ollama: %s", genResp.Error)
	}

	p.mu.Lock()
	p.reqCount++
	p.mu.Unlock()

	return genResp.Response, nil
}

func (p *OllamaProvider) ModelName() string { return "ollama:" + p.config.Model }
func (p *OllamaProvider) MaxTokens() int   { return p.config.MaxTokens }

// --- NL-to-PromQL Translation ---

// NLToPromQLTranslator translates natural language queries to PromQL using schema-aware prompting.
type NLToPromQLTranslator struct {
	llm       LLMProvider
	parser    *PromQLCompleteParser
	schema    SchemaContext
	mu        sync.RWMutex
}

// SchemaContext provides schema information for context-aware query generation.
type SchemaContext struct {
	AvailableMetrics []string          `json:"available_metrics"`
	LabelNames       []string          `json:"label_names"`
	LabelValues      map[string][]string `json:"label_values"`
	RetentionPeriod  string            `json:"retention_period"`
}

// NewNLToPromQLTranslator creates a new translator with schema awareness.
func NewNLToPromQLTranslator(llm LLMProvider) *NLToPromQLTranslator {
	return &NLToPromQLTranslator{
		llm:    llm,
		parser: NewPromQLCompleteParser(),
	}
}

// SetSchema updates the schema context used for query generation.
func (t *NLToPromQLTranslator) SetSchema(schema SchemaContext) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.schema = schema
}

// TranslateResult holds the result of a NL-to-PromQL translation.
type TranslateResult struct {
	PromQL     string  `json:"promql"`
	SQL        string  `json:"sql,omitempty"`
	Confidence float64 `json:"confidence"`
	Valid      bool    `json:"valid"`
	Error      string  `json:"error,omitempty"`
}

// Translate converts a natural language query to PromQL.
func (t *NLToPromQLTranslator) Translate(ctx context.Context, query string) (*TranslateResult, error) {
	if query == "" {
		return nil, fmt.Errorf("nl_translate: empty query")
	}

	t.mu.RLock()
	schema := t.schema
	t.mu.RUnlock()

	// Build schema-aware prompt
	prompt := t.buildSchemaPrompt(query, schema)

	// Generate PromQL via LLM
	generated, err := t.llm.Generate(ctx, prompt)
	if err != nil {
		// Fall back to rule-based translation
		return t.ruleBasedTranslate(query), nil
	}

	// Extract PromQL from LLM response
	promql := extractPromQL(generated)

	// Validate generated query against parser
	result := &TranslateResult{
		PromQL:     promql,
		Confidence: 0.8,
	}

	_, parseErr := t.parser.ParseComplete(promql)
	if parseErr != nil {
		result.Valid = false
		result.Error = parseErr.Error()
		result.Confidence = 0.3

		// Try rule-based fallback
		fallback := t.ruleBasedTranslate(query)
		if fallback.Valid {
			return fallback, nil
		}
	} else {
		result.Valid = true
	}

	return result, nil
}

func (t *NLToPromQLTranslator) buildSchemaPrompt(query string, schema SchemaContext) string {
	var sb strings.Builder
	sb.WriteString("Translate the following natural language query into a valid PromQL expression.\n")
	sb.WriteString("Respond with ONLY the PromQL query, no explanation.\n\n")

	if len(schema.AvailableMetrics) > 0 {
		sb.WriteString("Available metrics: ")
		maxMetrics := 50
		if len(schema.AvailableMetrics) < maxMetrics {
			maxMetrics = len(schema.AvailableMetrics)
		}
		sb.WriteString(strings.Join(schema.AvailableMetrics[:maxMetrics], ", "))
		sb.WriteString("\n")
	}

	if len(schema.LabelNames) > 0 {
		sb.WriteString("Available labels: ")
		sb.WriteString(strings.Join(schema.LabelNames, ", "))
		sb.WriteString("\n")
	}

	sb.WriteString("\nQuery: ")
	sb.WriteString(query)
	return sb.String()
}

// ruleBasedTranslate provides a fallback rule-based NL-to-PromQL translation.
func (t *NLToPromQLTranslator) ruleBasedTranslate(query string) *TranslateResult {
	lower := strings.ToLower(query)
	result := &TranslateResult{Confidence: 0.6, Valid: true}

	metric := extractMetricFromText(query)
	if metric == "" {
		metric = "up"
	}

	switch {
	case strings.Contains(lower, "rate") || strings.Contains(lower, "per second"):
		result.PromQL = fmt.Sprintf("rate(%s[5m])", metric)
	case strings.Contains(lower, "average") || strings.Contains(lower, "avg") || strings.Contains(lower, "mean"):
		result.PromQL = fmt.Sprintf("avg_over_time(%s[1h])", metric)
	case strings.Contains(lower, "max") || strings.Contains(lower, "maximum") || strings.Contains(lower, "highest"):
		result.PromQL = fmt.Sprintf("max_over_time(%s[1h])", metric)
	case strings.Contains(lower, "min") || strings.Contains(lower, "minimum") || strings.Contains(lower, "lowest"):
		result.PromQL = fmt.Sprintf("min_over_time(%s[1h])", metric)
	case strings.Contains(lower, "top") || strings.Contains(lower, "highest"):
		result.PromQL = fmt.Sprintf("topk(10, %s)", metric)
	case strings.Contains(lower, "bottom") || strings.Contains(lower, "lowest"):
		result.PromQL = fmt.Sprintf("bottomk(10, %s)", metric)
	case strings.Contains(lower, "increase") || strings.Contains(lower, "growth"):
		result.PromQL = fmt.Sprintf("increase(%s[1h])", metric)
	case strings.Contains(lower, "sum") || strings.Contains(lower, "total"):
		result.PromQL = fmt.Sprintf("sum(%s)", metric)
	case strings.Contains(lower, "count"):
		result.PromQL = fmt.Sprintf("count(%s)", metric)
	case strings.Contains(lower, "predict") || strings.Contains(lower, "forecast"):
		result.PromQL = fmt.Sprintf("predict_linear(%s[1h], 3600)", metric)
	case strings.Contains(lower, "absent") || strings.Contains(lower, "missing"):
		result.PromQL = fmt.Sprintf("absent(%s)", metric)
	default:
		result.PromQL = metric
		result.Confidence = 0.4
	}

	return result
}

// extractPromQL extracts a PromQL expression from an LLM response.
func extractPromQL(response string) string {
	response = strings.TrimSpace(response)

	// Check for code block
	if idx := strings.Index(response, "```"); idx >= 0 {
		end := strings.Index(response[idx+3:], "```")
		if end >= 0 {
			block := response[idx+3 : idx+3+end]
			block = strings.TrimPrefix(block, "promql\n")
			block = strings.TrimPrefix(block, "promql")
			return strings.TrimSpace(block)
		}
	}

	// Take the first non-empty line
	lines := strings.Split(response, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") && !strings.HasPrefix(line, "//") {
			return line
		}
	}

	return response
}

// --- /api/v1/ask Endpoint ---

// AskRequest represents a request to the /api/v1/ask endpoint.
type AskRequest struct {
	Question       string `json:"question"`
	ConversationID string `json:"conversation_id,omitempty"`
	Stream         bool   `json:"stream,omitempty"`
	Translate      bool   `json:"translate,omitempty"`
}

// AskResponse represents a response from the /api/v1/ask endpoint.
type AskResponse struct {
	Answer         string           `json:"answer"`
	Query          *TranslateResult `json:"query,omitempty"`
	Evidence       []string         `json:"evidence,omitempty"`
	Confidence     float64          `json:"confidence"`
	Intent         string           `json:"intent"`
	FollowUps      []string         `json:"follow_ups,omitempty"`
	ConversationID string           `json:"conversation_id"`
}

// AskEndpoint provides the /api/v1/ask HTTP handler.
type AskEndpoint struct {
	pipeline        *RAGPipeline
	translator      *NLToPromQLTranslator
	anomalyExplainer *LLMAnomalyExplainer
	memory          *ConversationMemory
	mu              sync.RWMutex
}

// NewAskEndpoint creates a new ask endpoint.
func NewAskEndpoint(pipeline *RAGPipeline, translator *NLToPromQLTranslator) *AskEndpoint {
	return &AskEndpoint{
		pipeline:   pipeline,
		translator: translator,
		memory:     NewConversationMemory(20, 24*time.Hour),
	}
}

// SetAnomalyExplainer sets the LLM-enhanced anomaly explainer.
func (e *AskEndpoint) SetAnomalyExplainer(explainer *LLMAnomalyExplainer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.anomalyExplainer = explainer
}

// RegisterHTTPHandlers registers the /api/v1/ask endpoint.
func (e *AskEndpoint) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/ask", e.handleAsk)
}

func (e *AskEndpoint) handleAsk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Question == "" {
		http.Error(w, "question is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Track conversation if ID provided
	convID := req.ConversationID
	if convID == "" {
		convID = fmt.Sprintf("conv-%d", time.Now().UnixNano())
	}

	if e.memory != nil {
		e.memory.AddTurn(convID, ConversationEntry{Role: "user", Content: req.Question})
	}

	response := &AskResponse{
		ConversationID: convID,
		Intent:         "general",
	}

	// Ask RAG pipeline
	if e.pipeline != nil {
		ragAnswer, err := e.pipeline.Ask(ctx, req.Question)
		if err == nil && ragAnswer != nil {
			response.Answer = ragAnswer.Answer
			response.Evidence = ragAnswer.Evidence
			response.Confidence = ragAnswer.Confidence
			response.Intent = ragAnswer.Intent
			response.FollowUps = ragAnswer.FollowUps
		}
	}

	// Translate to PromQL if requested
	if req.Translate && e.translator != nil {
		translated, err := e.translator.Translate(ctx, req.Question)
		if err == nil {
			response.Query = translated
		}
	}

	// Track assistant response
	if e.memory != nil && response.Answer != "" {
		e.memory.AddTurn(convID, ConversationEntry{
			Role:    "assistant",
			Content: response.Answer,
			QueryUsed: func() string {
				if response.Query != nil {
					return response.Query.PromQL
				}
				return ""
			}(),
		})
	}

	// Stream response if requested
	if req.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		data, _ := json.Marshal(response)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// --- LLM-Enhanced Anomaly Explanation ---

// LLMAnomalyExplainer uses an LLM to generate richer anomaly explanations
// by combining causal analysis evidence with natural language generation.
type LLMAnomalyExplainer struct {
	llm     LLMProvider
	parser  *PromQLCompleteParser
	mu      sync.RWMutex
}

// NewLLMAnomalyExplainer creates a new LLM-enhanced anomaly explainer.
func NewLLMAnomalyExplainer(llm LLMProvider) *LLMAnomalyExplainer {
	return &LLMAnomalyExplainer{
		llm:    llm,
		parser: NewPromQLCompleteParser(),
	}
}

// ExplainWithLLM generates an LLM-enhanced explanation given causal analysis results.
func (e *LLMAnomalyExplainer) ExplainWithLLM(ctx context.Context, result *AnomalyExplanationResult) (string, error) {
	if result == nil {
		return "", fmt.Errorf("llm_explainer: nil result")
	}

	// Build evidence chain from causal analysis
	var evidence []string
	evidence = append(evidence, fmt.Sprintf("Metric: %s, Value: %.4f, Baseline Mean: %.4f, Baseline StdDev: %.4f",
		result.Metric, result.Value, result.BaselineMean, result.BaselineStddev))
	evidence = append(evidence, fmt.Sprintf("Deviation: %.2f standard deviations", result.Deviation))

	if len(result.RootCauses) > 0 {
		evidence = append(evidence, fmt.Sprintf("Root causes identified: %s", strings.Join(result.RootCauses, ", ")))
	}

	for _, pred := range result.CausalPredecessors {
		evidence = append(evidence, fmt.Sprintf("Causal predecessor: %s -> %s (F=%.2f, p=%.4f)",
			pred.Cause, pred.Effect, pred.FStatistic, pred.PValue))
	}

	for _, dim := range result.DimensionAttrs {
		evidence = append(evidence, fmt.Sprintf("Dimension %s=%s: attribution=%.4f",
			dim.Dimension, dim.Value, dim.ShapleyValue))
	}

	prompt := fmt.Sprintf(
		"Explain the following metric anomaly in plain language suitable for an SRE or DevOps engineer.\n"+
			"Focus on root causes, impact, and recommended actions.\n\n"+
			"Evidence:\n%s",
		strings.Join(evidence, "\n"))

	explanation, err := e.llm.GenerateWithContext(ctx, prompt, evidence)
	if err != nil {
		// Fall back to the template-generated explanation
		return result.NLExplanation, nil
	}

	return explanation, nil
}

// --- Conversation Memory Manager ---

// ConversationMemory manages multi-turn conversation context with sliding window.
type ConversationMemory struct {
	conversations map[string]*ConversationState
	mu            sync.RWMutex
	maxTurns      int
	maxAge        time.Duration
}

// ConversationState holds the state for a single conversation.
type ConversationState struct {
	ID            string              `json:"id"`
	Turns         []ConversationEntry `json:"turns"`
	LastActivity  time.Time           `json:"last_activity"`
	MetricContext []string            `json:"metric_context"`
	QueryHistory  []string            `json:"query_history"`
}

// ConversationEntry represents a single turn in a conversation.
type ConversationEntry struct {
	Role      string    `json:"role"` // user, assistant
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
	QueryUsed string    `json:"query_used,omitempty"`
}

// NewConversationMemory creates a new conversation memory manager.
func NewConversationMemory(maxTurns int, maxAge time.Duration) *ConversationMemory {
	if maxTurns <= 0 {
		maxTurns = 20
	}
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	return &ConversationMemory{
		conversations: make(map[string]*ConversationState),
		maxTurns:      maxTurns,
		maxAge:        maxAge,
	}
}

// GetOrCreate returns an existing conversation or creates a new one.
func (cm *ConversationMemory) GetOrCreate(id string) *ConversationState {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conv, ok := cm.conversations[id]
	if !ok || time.Since(conv.LastActivity) > cm.maxAge {
		conv = &ConversationState{
			ID:           id,
			Turns:        make([]ConversationEntry, 0),
			LastActivity: time.Now(),
		}
		cm.conversations[id] = conv
	}
	return conv
}

// AddTurn adds a turn to a conversation and trims old entries.
func (cm *ConversationMemory) AddTurn(id string, entry ConversationEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conv, ok := cm.conversations[id]
	if !ok {
		conv = &ConversationState{ID: id, Turns: make([]ConversationEntry, 0)}
		cm.conversations[id] = conv
	}

	entry.Timestamp = time.Now()
	conv.Turns = append(conv.Turns, entry)
	conv.LastActivity = time.Now()

	// Track queries
	if entry.QueryUsed != "" {
		conv.QueryHistory = append(conv.QueryHistory, entry.QueryUsed)
	}

	// Trim to max turns
	if len(conv.Turns) > cm.maxTurns {
		conv.Turns = conv.Turns[len(conv.Turns)-cm.maxTurns:]
	}
}

// GetContext returns the conversation context as strings for LLM prompting.
func (cm *ConversationMemory) GetContext(id string) []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	conv, ok := cm.conversations[id]
	if !ok {
		return nil
	}

	context := make([]string, 0, len(conv.Turns))
	for _, turn := range conv.Turns {
		context = append(context, fmt.Sprintf("[%s] %s", turn.Role, turn.Content))
	}
	return context
}

// Cleanup removes expired conversations.
func (cm *ConversationMemory) Cleanup() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	removed := 0
	for id, conv := range cm.conversations {
		if time.Since(conv.LastActivity) > cm.maxAge {
			delete(cm.conversations, id)
			removed++
		}
	}
	return removed
}

// Count returns the number of active conversations.
func (cm *ConversationMemory) Count() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.conversations)
}

// --- Query Validator ---

// QueryValidator validates generated queries before execution.
type QueryValidator struct {
	parser *PromQLCompleteParser
}

// NewQueryValidator creates a new query validator.
func NewQueryValidator() *QueryValidator {
	return &QueryValidator{parser: NewPromQLCompleteParser()}
}

// ValidatePromQL checks if a PromQL query is syntactically valid.
func (v *QueryValidator) ValidatePromQL(query string) (bool, string) {
	if query == "" {
		return false, "empty query"
	}
	_, err := v.parser.ParseComplete(query)
	if err != nil {
		return false, err.Error()
	}
	return true, ""
}

// ValidateAndSanitize validates and sanitizes a query for safe execution.
func (v *QueryValidator) ValidateAndSanitize(query string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", fmt.Errorf("empty query")
	}

	// Remove potential injection patterns
	query = strings.ReplaceAll(query, ";", "")
	query = strings.ReplaceAll(query, "--", "")

	valid, errMsg := v.ValidatePromQL(query)
	if !valid {
		return "", fmt.Errorf("invalid PromQL: %s", errMsg)
	}

	return query, nil
}
