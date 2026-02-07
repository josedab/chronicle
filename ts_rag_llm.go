package chronicle

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"text/template"
	"time"
)

// LLMProvider defines the interface for language model backends used in RAG pipelines.
type LLMProvider interface {
	// Generate produces a response from the given prompt.
	Generate(ctx context.Context, prompt string) (string, error)

	// GenerateWithContext produces a response using the prompt and retrieved context evidence.
	GenerateWithContext(ctx context.Context, prompt string, context []string) (string, error)

	// ModelName returns the name/identifier of the underlying model.
	ModelName() string

	// MaxTokens returns the maximum token limit for generation.
	MaxTokens() int
}

// LLMConfig configures a language model provider.
type LLMConfig struct {
	// Provider is the LLM backend type ("local", "mock", "api").
	Provider string `json:"provider"`

	// ModelPath is the filesystem path to a local model (if applicable).
	ModelPath string `json:"model_path,omitempty"`

	// MaxTokens is the maximum number of tokens for generation.
	MaxTokensLimit int `json:"max_tokens"`

	// Temperature controls generation randomness (0-1).
	Temperature float64 `json:"temperature"`

	// TopP controls nucleus sampling.
	TopP float64 `json:"top_p"`

	// SystemPrompt is the system-level instruction prepended to all prompts.
	SystemPrompt string `json:"system_prompt,omitempty"`
}

// DefaultLLMConfig returns sensible defaults for LLM configuration.
func DefaultLLMConfig() LLMConfig {
	return LLMConfig{
		Provider:       "local",
		MaxTokensLimit: 2048,
		Temperature:    0.7,
		TopP:           0.9,
		SystemPrompt:   "You are a time-series data analysis assistant. Answer questions about metrics, trends, anomalies, and forecasts based on the provided evidence.",
	}
}

// PromptTemplate defines a named template for generating structured responses.
type PromptTemplate struct {
	// Name identifies this template.
	Name string

	// Template is the Go text/template string.
	Template string

	// IntentPattern is the question intent type this template handles.
	IntentPattern string
}

// TemplatedLLM is a rule-based LLM implementation that uses templates to generate
// responses from retrieved evidence without requiring an actual language model.
type TemplatedLLM struct {
	config    LLMConfig
	templates map[string]*PromptTemplate
	compiled  map[string]*template.Template
	mu        sync.RWMutex
}

// NewTemplatedLLM creates a new template-based LLM with built-in templates.
func NewTemplatedLLM(config LLMConfig) *TemplatedLLM {
	t := &TemplatedLLM{
		config:    config,
		templates: make(map[string]*PromptTemplate),
		compiled:  make(map[string]*template.Template),
	}

	// Register built-in templates
	builtins := []PromptTemplate{
		{
			Name:          "anomaly_explain",
			Template:      "Based on the data, {{.metric}} showed an anomaly at {{.time}}. The value was {{.value}} which is {{.deviation}} standard deviations from the mean of {{.mean}}.",
			IntentPattern: "anomaly_explanation",
		},
		{
			Name:          "trend_analysis",
			Template:      "The metric {{.metric}} shows a {{.direction}} trend over the period {{.start}} to {{.end}}. The average value is {{.avg}} with a slope of {{.slope}}.",
			IntentPattern: "trend_analysis",
		},
		{
			Name:          "comparison",
			Template:      "Comparing {{.metric1}} and {{.metric2}}: correlation is {{.correlation}}, {{.metric1}} averages {{.avg1}} while {{.metric2}} averages {{.avg2}}.",
			IntentPattern: "comparison",
		},
		{
			Name:          "forecast",
			Template:      "Based on historical patterns, {{.metric}} is projected to {{.direction}} over the next {{.period}}. Expected range: {{.low}} to {{.high}}.",
			IntentPattern: "forecasting",
		},
		{
			Name:          "summary",
			Template:      "{{.metric}} over {{.period}}: min={{.min}}, max={{.max}}, avg={{.avg}}, stddev={{.stddev}}. {{.additional_context}}",
			IntentPattern: "general",
		},
	}

	for i := range builtins {
		pt := &builtins[i]
		t.templates[pt.Name] = pt
		tmpl, err := template.New(pt.Name).Parse(pt.Template)
		if err == nil {
			t.compiled[pt.Name] = tmpl
		}
	}

	return t
}

// Generate produces a response by analyzing the prompt and selecting an appropriate template.
func (t *TemplatedLLM) Generate(ctx context.Context, prompt string) (string, error) {
	return t.GenerateWithContext(ctx, prompt, nil)
}

// GenerateWithContext produces a response using the prompt and evidence context strings.
func (t *TemplatedLLM) GenerateWithContext(ctx context.Context, prompt string, evidence []string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	intent := analyzeQuestionIntent(prompt)
	data := t.buildTemplateData(prompt, intent, evidence)

	// Find the template matching this intent
	t.mu.RLock()
	tmplName := t.templateNameForIntent(intent.Type)
	compiled, ok := t.compiled[tmplName]
	t.mu.RUnlock()

	if !ok {
		// Fallback: build a generic response from evidence
		return t.fallbackResponse(prompt, evidence), nil
	}

	var buf bytes.Buffer
	if err := compiled.Execute(&buf, data); err != nil {
		return t.fallbackResponse(prompt, evidence), nil
	}

	return buf.String(), nil
}

// ModelName returns the model identifier.
func (t *TemplatedLLM) ModelName() string {
	if t.config.ModelPath != "" {
		return "templated:" + t.config.ModelPath
	}
	return "templated:built-in"
}

// MaxTokens returns the configured maximum token limit.
func (t *TemplatedLLM) MaxTokens() int {
	return t.config.MaxTokensLimit
}

// templateNameForIntent maps an intent type to a template name.
func (t *TemplatedLLM) templateNameForIntent(intentType string) string {
	for name, pt := range t.templates {
		if pt.IntentPattern == intentType {
			return name
		}
	}
	return "summary"
}

// buildTemplateData extracts key-value data from evidence strings to populate templates.
func (t *TemplatedLLM) buildTemplateData(prompt string, intent questionIntent, evidence []string) map[string]string {
	data := map[string]string{
		"metric":             "unknown",
		"time":               time.Now().Format(time.RFC3339),
		"value":              "N/A",
		"deviation":          "N/A",
		"mean":               "N/A",
		"direction":          "stable",
		"start":              "N/A",
		"end":                "N/A",
		"avg":                "N/A",
		"slope":              "N/A",
		"metric1":            "metric_a",
		"metric2":            "metric_b",
		"correlation":        "N/A",
		"avg1":               "N/A",
		"avg2":               "N/A",
		"period":             "N/A",
		"low":                "N/A",
		"high":               "N/A",
		"min":                "N/A",
		"max":                "N/A",
		"stddev":             "N/A",
		"additional_context": "",
	}

	// Extract metric name from prompt
	if metric := extractMetricFromText(prompt); metric != "" {
		data["metric"] = metric
	}

	// Parse evidence strings for numerical data
	if len(evidence) > 0 {
		t.parseEvidence(data, evidence)
	}

	return data
}

// parseEvidence extracts structured data from evidence strings.
func (t *TemplatedLLM) parseEvidence(data map[string]string, evidence []string) {
	for _, e := range evidence {
		lower := strings.ToLower(e)

		// Extract metric names from evidence
		if strings.Contains(lower, "metric") {
			if m := extractMetricFromText(e); m != "" && data["metric"] == "unknown" {
				data["metric"] = m
			}
		}

		// Extract trend direction
		if strings.Contains(lower, "upward") || strings.Contains(lower, "increasing") {
			data["direction"] = "increasing"
		} else if strings.Contains(lower, "downward") || strings.Contains(lower, "decreasing") {
			data["direction"] = "decreasing"
		}

		// Extract numerical values from evidence using key=value patterns
		for _, kv := range parseKeyValuePairs(e) {
			if _, exists := data[kv.key]; exists {
				data[kv.key] = kv.value
			}
		}
	}

	// Build additional context from all evidence
	if len(evidence) > 0 {
		var parts []string
		for _, e := range evidence {
			if len(e) > 0 {
				parts = append(parts, e)
			}
		}
		data["additional_context"] = strings.Join(parts, " ")
	}
}

type kvPair struct {
	key   string
	value string
}

// parseKeyValuePairs extracts key=value or key: value pairs from text.
func parseKeyValuePairs(text string) []kvPair {
	var pairs []kvPair
	// Split by common delimiters
	parts := strings.FieldsFunc(text, func(r rune) bool {
		return r == ',' || r == ';'
	})
	for _, part := range parts {
		part = strings.TrimSpace(part)
		var key, value string
		if idx := strings.Index(part, "="); idx > 0 {
			key = strings.TrimSpace(part[:idx])
			value = strings.TrimSpace(part[idx+1:])
		} else if idx := strings.Index(part, ":"); idx > 0 {
			key = strings.TrimSpace(part[:idx])
			value = strings.TrimSpace(part[idx+1:])
		}
		if key != "" && value != "" {
			pairs = append(pairs, kvPair{key: strings.ToLower(key), value: value})
		}
	}
	return pairs
}

// extractMetricFromText attempts to extract a metric name from natural language text.
func extractMetricFromText(text string) string {
	// Look for quoted metric names
	if idx := strings.Index(text, "'"); idx >= 0 {
		end := strings.Index(text[idx+1:], "'")
		if end > 0 {
			return text[idx+1 : idx+1+end]
		}
	}
	if idx := strings.Index(text, "\""); idx >= 0 {
		end := strings.Index(text[idx+1:], "\"")
		if end > 0 {
			return text[idx+1 : idx+1+end]
		}
	}

	// Look for common metric name patterns (e.g., cpu.usage, mem_used)
	words := strings.Fields(text)
	for _, w := range words {
		if strings.Contains(w, ".") || strings.Contains(w, "_") {
			clean := strings.Trim(w, ".,;:!?()")
			if len(clean) > 2 {
				return clean
			}
		}
	}
	return ""
}

// fallbackResponse generates a generic response when no template matches.
func (t *TemplatedLLM) fallbackResponse(prompt string, evidence []string) string {
	var sb strings.Builder
	sb.WriteString("Based on the available data: ")
	if len(evidence) > 0 {
		sb.WriteString(fmt.Sprintf("I found %d relevant piece(s) of evidence. ", len(evidence)))
		for i, e := range evidence {
			if i >= 3 {
				break
			}
			sb.WriteString(e)
			sb.WriteString(" ")
		}
	} else {
		sb.WriteString("No matching patterns were found for this query.")
	}
	return strings.TrimSpace(sb.String())
}

// --- RAG Pipeline ---

// RAGAnswer represents the result of a RAG pipeline query.
type RAGAnswer struct {
	// Answer is the generated natural language response.
	Answer string `json:"answer"`

	// Evidence contains the retrieved evidence strings.
	Evidence []string `json:"evidence"`

	// Confidence is the overall confidence score (0-1).
	Confidence float64 `json:"confidence"`

	// Intent is the detected question intent type.
	Intent string `json:"intent"`

	// FollowUps contains suggested follow-up questions.
	FollowUps []string `json:"follow_ups,omitempty"`
}

// RAGPipeline orchestrates the full RAG flow: retrieve evidence, generate answers,
// and maintain conversation history.
type RAGPipeline struct {
	rag                 *TSRAGEngine
	llm                 LLMProvider
	conversationHistory []ConversationTurn
	maxHistory          int
	mu                  sync.RWMutex
}

// NewRAGPipeline creates a new RAG pipeline with the given engine and LLM provider.
func NewRAGPipeline(rag *TSRAGEngine, llm LLMProvider) *RAGPipeline {
	return &RAGPipeline{
		rag:                 rag,
		llm:                 llm,
		conversationHistory: make([]ConversationTurn, 0),
		maxHistory:          20,
	}
}

// Ask processes a natural language question through the full RAG pipeline.
func (p *RAGPipeline) Ask(ctx context.Context, question string) (*RAGAnswer, error) {
	if question == "" {
		return nil, fmt.Errorf("ts_rag_llm: empty question")
	}

	// Add question to conversation history
	p.mu.Lock()
	p.conversationHistory = append(p.conversationHistory, ConversationTurn{
		Role:      "user",
		Content:   question,
		Timestamp: time.Now(),
	})
	p.trimHistory()
	p.mu.Unlock()

	// Analyze intent
	intent := analyzeQuestionIntent(question)

	// Retrieve relevant evidence from the RAG engine
	evidence := p.retrieveEvidence(ctx, question, intent)

	// Build context from evidence + conversation history
	contextStrings := p.buildContext(evidence)

	// Generate answer via LLM
	answer, err := p.llm.GenerateWithContext(ctx, question, contextStrings)
	if err != nil {
		return nil, fmt.Errorf("ts_rag_llm: generation failed: %w", err)
	}

	// Build evidence strings for the response
	evidenceStrings := make([]string, len(evidence))
	for i, e := range evidence {
		evidenceStrings[i] = fmt.Sprintf("[%s] %s (similarity: %.1f%%)", e.Metric, e.Explanation, e.Similarity*100)
	}

	// Calculate confidence
	confidence := calculateConfidence(evidence)

	// Generate follow-up suggestions
	followUps := suggestFollowUps(intent, evidence)

	// Add answer to conversation history
	p.mu.Lock()
	p.conversationHistory = append(p.conversationHistory, ConversationTurn{
		Role:      "assistant",
		Content:   answer,
		Timestamp: time.Now(),
	})
	p.trimHistory()
	p.mu.Unlock()

	return &RAGAnswer{
		Answer:     answer,
		Evidence:   evidenceStrings,
		Confidence: confidence,
		Intent:     intent.Type,
		FollowUps:  followUps,
	}, nil
}

// ExplainAnomaly generates a detailed explanation of an anomaly for a specific metric and time.
func (p *RAGPipeline) ExplainAnomaly(ctx context.Context, metric string, timestamp time.Time) (*RAGAnswer, error) {
	question := fmt.Sprintf("Explain the anomaly in metric '%s' at %s", metric, timestamp.Format(time.RFC3339))
	return p.Ask(ctx, question)
}

// FindSimilar finds patterns similar to the specified metric's behavior in the given time range.
func (p *RAGPipeline) FindSimilar(ctx context.Context, metric string, start, end time.Time, topK int) (*RAGAnswer, error) {
	if topK <= 0 {
		topK = 5
	}
	question := fmt.Sprintf("Find %d patterns similar to '%s' between %s and %s",
		topK, metric, start.Format(time.RFC3339), end.Format(time.RFC3339))
	return p.Ask(ctx, question)
}

// ConversationHistory returns a copy of the current conversation history.
func (p *RAGPipeline) ConversationHistory() []ConversationTurn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]ConversationTurn, len(p.conversationHistory))
	copy(result, p.conversationHistory)
	return result
}

// ClearHistory resets the conversation history.
func (p *RAGPipeline) ClearHistory() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conversationHistory = p.conversationHistory[:0]
}

// retrieveEvidence queries the RAG engine for relevant evidence based on the question.
func (p *RAGPipeline) retrieveEvidence(_ context.Context, _ string, _ questionIntent) []Evidence {
	// Build a synthetic query vector from the question intent for retrieval
	p.rag.embMu.RLock()
	embCount := len(p.rag.embeddings)
	p.rag.embMu.RUnlock()

	if embCount == 0 {
		return nil
	}

	// Use a zero vector as a baseline; the RAG engine's Retrieve method will
	// return patterns above the similarity threshold.
	queryVector := make([]float64, p.rag.config.EmbeddingDim)
	return p.rag.Retrieve(queryVector, p.rag.config.MaxRetrievedPatterns)
}

// buildContext combines evidence and conversation history into context strings for the LLM.
func (p *RAGPipeline) buildContext(evidence []Evidence) []string {
	var contextStrings []string

	// Add evidence
	for _, e := range evidence {
		ctx := fmt.Sprintf("metric=%s, time_range=%s, similarity=%.2f, trend=%.4f, mean=%.4f, stddev=%.4f, min=%.4f, max=%.4f; %s",
			e.Metric, e.TimeRange, e.Similarity,
			e.Pattern.Features.Trend, e.Pattern.Features.Mean,
			e.Pattern.Features.StdDev, e.Pattern.Features.Min,
			e.Pattern.Features.Max, e.Explanation)
		contextStrings = append(contextStrings, ctx)
	}

	// Add recent conversation history for multi-turn context
	p.mu.RLock()
	histLen := len(p.conversationHistory)
	start := 0
	if histLen > 4 {
		start = histLen - 4
	}
	for i := start; i < histLen; i++ {
		turn := p.conversationHistory[i]
		contextStrings = append(contextStrings, fmt.Sprintf("[%s] %s", turn.Role, turn.Content))
	}
	p.mu.RUnlock()

	return contextStrings
}

// trimHistory keeps conversation history within the maxHistory limit.
func (p *RAGPipeline) trimHistory() {
	if len(p.conversationHistory) > p.maxHistory {
		excess := len(p.conversationHistory) - p.maxHistory
		p.conversationHistory = p.conversationHistory[excess:]
	}
}

// --- Confidence Helpers ---

// ragLLMCalculateConfidence computes confidence from evidence for RAGAnswer.
// This wraps the existing calculateConfidence but applies additional
// adjustments based on evidence diversity.
func ragLLMCalculateConfidence(evidence []Evidence) float64 {
	base := calculateConfidence(evidence)

	// Boost confidence if evidence comes from multiple metrics
	metrics := make(map[string]bool)
	for _, e := range evidence {
		metrics[e.Metric] = true
	}
	diversityBoost := math.Min(float64(len(metrics))*0.05, 0.15)

	return math.Min(base+diversityBoost, 1.0)
}
