package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// QueryAssistant provides AI-powered natural language to query translation.
type QueryAssistant struct {
	db     *DB
	config AssistantConfig
	client *http.Client
	cache  map[string]cachedQuery
}

type cachedQuery struct {
	query     string
	timestamp time.Time
}

// AssistantConfig configures the query assistant.
type AssistantConfig struct {
	// Provider is the LLM provider (openai, anthropic, local).
	Provider string

	// APIKey for the LLM provider.
	APIKey string

	// Endpoint is the API endpoint (for custom providers).
	Endpoint string

	// Model is the model to use (e.g., gpt-4, claude-3).
	Model string

	// Temperature controls randomness (0-1).
	Temperature float64

	// MaxTokens limits response length.
	MaxTokens int

	// Timeout for API requests.
	Timeout time.Duration

	// EnableCache caches query translations.
	EnableCache bool

	// CacheTTL is the cache time-to-live.
	CacheTTL time.Duration
}

// DefaultAssistantConfig returns default configuration.
func DefaultAssistantConfig() AssistantConfig {
	return AssistantConfig{
		Provider:    "local",
		Model:       "default",
		Temperature: 0.1,
		MaxTokens:   500,
		Timeout:     30 * time.Second,
		EnableCache: true,
		CacheTTL:    time.Hour,
	}
}

// NewQueryAssistant creates a new query assistant.
func NewQueryAssistant(db *DB, config AssistantConfig) *QueryAssistant {
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	return &QueryAssistant{
		db:     db,
		config: config,
		client: &http.Client{Timeout: config.Timeout},
		cache:  make(map[string]cachedQuery),
	}
}

// TranslateResponse contains the translation result.
type TranslateResponse struct {
	Query       string   `json:"query"`
	QueryType   string   `json:"query_type"` // sql, promql
	Explanation string   `json:"explanation"`
	Confidence  float64  `json:"confidence"`
	Suggestions []string `json:"suggestions,omitempty"`
}

// Translate converts natural language to a query.
func (qa *QueryAssistant) Translate(ctx context.Context, naturalLanguage string) (*TranslateResponse, error) {
	if naturalLanguage == "" {
		return nil, errors.New("input cannot be empty")
	}

	// Check cache
	if qa.config.EnableCache {
		if cached, ok := qa.cache[naturalLanguage]; ok {
			if time.Since(cached.timestamp) < qa.config.CacheTTL {
				return &TranslateResponse{
					Query:      cached.query,
					QueryType:  "sql",
					Confidence: 1.0,
				}, nil
			}
		}
	}

	// Get schema context for better translation
	schemaContext := qa.getSchemaContext()

	// Try local rule-based translation first
	if result := qa.localTranslate(naturalLanguage, schemaContext); result != nil {
		if qa.config.EnableCache {
			qa.cache[naturalLanguage] = cachedQuery{
				query:     result.Query,
				timestamp: time.Now(),
			}
		}
		return result, nil
	}

	// Fall back to LLM if configured
	if qa.config.APIKey != "" {
		return qa.llmTranslate(ctx, naturalLanguage, schemaContext)
	}

	return nil, errors.New("unable to translate query; no LLM configured and local translation failed")
}

// getSchemaContext builds context about available metrics and tags.
func (qa *QueryAssistant) getSchemaContext() string {
	metrics := qa.db.Metrics()
	if len(metrics) == 0 {
		return "No metrics available."
	}

	var sb strings.Builder
	sb.WriteString("Available metrics:\n")
	for i, m := range metrics {
		if i >= 20 {
			sb.WriteString(fmt.Sprintf("... and %d more\n", len(metrics)-20))
			break
		}
		sb.WriteString(fmt.Sprintf("- %s\n", m))
	}
	return sb.String()
}

// localTranslate uses rule-based patterns for common queries.
func (qa *QueryAssistant) localTranslate(input string, _ string) *TranslateResponse {
	input = strings.ToLower(strings.TrimSpace(input))

	// Pattern: "show/list/get all metrics"
	if matched, _ := regexp.MatchString(`^(show|list|get)\s+(all\s+)?metrics?`, input); matched {
		return &TranslateResponse{
			Query:       "SELECT DISTINCT metric FROM *",
			QueryType:   "sql",
			Explanation: "Lists all available metric names",
			Confidence:  0.95,
		}
	}

	// Pattern: "average/mean of <metric> in last <duration>"
	avgPattern := regexp.MustCompile(`(average|mean|avg)\s+(?:of\s+)?(\w+)\s+(?:in|over|for)\s+(?:the\s+)?last\s+(\d+)\s*(hour|minute|day|h|m|d)s?`)
	if matches := avgPattern.FindStringSubmatch(input); matches != nil {
		metric := matches[2]
		amount := matches[3]
		unit := matches[4]
		duration := parseNLDuration(amount, unit)
		return &TranslateResponse{
			Query:       fmt.Sprintf("SELECT mean(value) FROM %s WHERE time > now() - %s GROUP BY time(5m)", metric, duration),
			QueryType:   "sql",
			Explanation: fmt.Sprintf("Calculates the average of %s over the last %s%s", metric, amount, unit),
			Confidence:  0.9,
		}
	}

	// Pattern: "max/min of <metric>"
	maxMinPattern := regexp.MustCompile(`(max|min|maximum|minimum)\s+(?:of\s+)?(\w+)\s+(?:in|over|for)\s+(?:the\s+)?last\s+(\d+)\s*(hour|minute|day|h|m|d)s?`)
	if matches := maxMinPattern.FindStringSubmatch(input); matches != nil {
		fn := matches[1]
		if fn == "maximum" {
			fn = "max"
		} else if fn == "minimum" {
			fn = "min"
		}
		metric := matches[2]
		amount := matches[3]
		unit := matches[4]
		duration := parseNLDuration(amount, unit)
		return &TranslateResponse{
			Query:       fmt.Sprintf("SELECT %s(value) FROM %s WHERE time > now() - %s", fn, metric, duration),
			QueryType:   "sql",
			Explanation: fmt.Sprintf("Finds the %s value of %s over the last %s%s", fn, metric, amount, unit),
			Confidence:  0.9,
		}
	}

	// Pattern: "count <metric> where/when <condition>"
	countPattern := regexp.MustCompile(`count\s+(\w+)\s+(?:where|when|if)\s+(\w+)\s*(>|<|=|>=|<=)\s*(\d+(?:\.\d+)?)`)
	if matches := countPattern.FindStringSubmatch(input); matches != nil {
		metric := matches[1]
		_ = matches[2] // field (usually "value")
		op := matches[3]
		threshold := matches[4]
		return &TranslateResponse{
			Query:       fmt.Sprintf("SELECT count(value) FROM %s WHERE value %s %s", metric, op, threshold),
			QueryType:   "sql",
			Explanation: fmt.Sprintf("Counts %s points where value %s %s", metric, op, threshold),
			Confidence:  0.85,
		}
	}

	// Pattern: "<metric> values from <host/tag>"
	filterPattern := regexp.MustCompile(`(\w+)\s+(?:values?\s+)?(?:from|for|on)\s+(\w+)\s*=?\s*['\"]?(\w+)['\"]?`)
	if matches := filterPattern.FindStringSubmatch(input); matches != nil {
		metric := matches[1]
		tag := matches[2]
		value := matches[3]
		return &TranslateResponse{
			Query:       fmt.Sprintf("SELECT * FROM %s WHERE %s = '%s'", metric, tag, value),
			QueryType:   "sql",
			Explanation: fmt.Sprintf("Retrieves %s values where %s is %s", metric, tag, value),
			Confidence:  0.85,
		}
	}

	// Pattern: "rate of <metric>"
	if matched := regexp.MustCompile(`rate\s+(?:of\s+)?(\w+)`).FindStringSubmatch(input); matched != nil {
		metric := matched[1]
		return &TranslateResponse{
			Query:       fmt.Sprintf("rate(%s[5m])", metric),
			QueryType:   "promql",
			Explanation: fmt.Sprintf("Calculates the per-second rate of change of %s", metric),
			Confidence:  0.9,
		}
	}

	// Pattern: "top 10 <metric> by <tag>"
	topPattern := regexp.MustCompile(`top\s+(\d+)\s+(\w+)\s+by\s+(\w+)`)
	if matches := topPattern.FindStringSubmatch(input); matches != nil {
		limit := matches[1]
		metric := matches[2]
		groupBy := matches[3]
		return &TranslateResponse{
			Query:       fmt.Sprintf("SELECT mean(value) FROM %s GROUP BY %s ORDER BY mean DESC LIMIT %s", metric, groupBy, limit),
			QueryType:   "sql",
			Explanation: fmt.Sprintf("Shows top %s %s values grouped by %s", limit, metric, groupBy),
			Confidence:  0.85,
		}
	}

	// Pattern: simple metric query "show <metric>"
	simplePattern := regexp.MustCompile(`^(?:show|get|query|select)\s+(\w+)$`)
	if matches := simplePattern.FindStringSubmatch(input); matches != nil {
		metric := matches[1]
		return &TranslateResponse{
			Query:       fmt.Sprintf("SELECT * FROM %s LIMIT 100", metric),
			QueryType:   "sql",
			Explanation: fmt.Sprintf("Retrieves recent values from %s", metric),
			Confidence:  0.8,
		}
	}

	return nil
}

func parseNLDuration(amount, unit string) string {
	switch unit {
	case "h", "hour":
		return amount + "h"
	case "m", "minute":
		return amount + "m"
	case "d", "day":
		return amount + "d"
	default:
		return amount + unit
	}
}

// llmTranslate uses an LLM API for translation.
func (qa *QueryAssistant) llmTranslate(ctx context.Context, input, schemaContext string) (*TranslateResponse, error) {
	prompt := fmt.Sprintf(`You are a time-series database query assistant. Convert the following natural language request into either SQL or PromQL query.

Database context:
%s

User request: %s

Respond with JSON only:
{"query": "your query here", "query_type": "sql or promql", "explanation": "brief explanation", "confidence": 0.0-1.0}`, schemaContext, input)

	switch qa.config.Provider {
	case "openai":
		return qa.callOpenAI(ctx, prompt)
	case "anthropic":
		return qa.callAnthropic(ctx, prompt)
	default:
		return nil, fmt.Errorf("unsupported LLM provider: %s", qa.config.Provider)
	}
}

// callOpenAI calls the OpenAI API.
func (qa *QueryAssistant) callOpenAI(ctx context.Context, prompt string) (*TranslateResponse, error) {
	endpoint := qa.config.Endpoint
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1/chat/completions"
	}

	model := qa.config.Model
	if model == "" {
		model = "gpt-3.5-turbo"
	}

	reqBody := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"temperature": qa.config.Temperature,
		"max_tokens":  qa.config.MaxTokens,
	}

	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+qa.config.APIKey)

	resp, err := qa.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenAI API error: %s", resp.Status)
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if len(result.Choices) == 0 {
		return nil, errors.New("no response from OpenAI")
	}

	// Parse the JSON response
	var response TranslateResponse
	if err := json.Unmarshal([]byte(result.Choices[0].Message.Content), &response); err != nil {
		// If JSON parsing fails, use the raw response as the query
		response = TranslateResponse{
			Query:      result.Choices[0].Message.Content,
			QueryType:  "sql",
			Confidence: 0.7,
		}
	}

	return &response, nil
}

// callAnthropic calls the Anthropic API.
func (qa *QueryAssistant) callAnthropic(ctx context.Context, prompt string) (*TranslateResponse, error) {
	endpoint := qa.config.Endpoint
	if endpoint == "" {
		endpoint = "https://api.anthropic.com/v1/messages"
	}

	model := qa.config.Model
	if model == "" {
		model = "claude-3-haiku-20240307"
	}

	reqBody := map[string]any{
		"model":      model,
		"max_tokens": qa.config.MaxTokens,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", qa.config.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := qa.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Anthropic API error: %s", resp.Status)
	}

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if len(result.Content) == 0 {
		return nil, errors.New("no response from Anthropic")
	}

	var response TranslateResponse
	if err := json.Unmarshal([]byte(result.Content[0].Text), &response); err != nil {
		response = TranslateResponse{
			Query:      result.Content[0].Text,
			QueryType:  "sql",
			Confidence: 0.7,
		}
	}

	return &response, nil
}

// Explain converts a query to natural language explanation.
func (qa *QueryAssistant) Explain(query string) string {
	query = strings.TrimSpace(query)
	lower := strings.ToLower(query)

	// Simple SQL patterns
	if strings.HasPrefix(lower, "select") {
		if strings.Contains(lower, "mean(") || strings.Contains(lower, "avg(") {
			if metric := extractMetricFromSQL(query); metric != "" {
				return fmt.Sprintf("This query calculates the average value of the %s metric.", metric)
			}
		}
		if strings.Contains(lower, "max(") {
			if metric := extractMetricFromSQL(query); metric != "" {
				return fmt.Sprintf("This query finds the maximum value of the %s metric.", metric)
			}
		}
		if strings.Contains(lower, "count(") {
			if metric := extractMetricFromSQL(query); metric != "" {
				return fmt.Sprintf("This query counts the number of data points in the %s metric.", metric)
			}
		}
		if strings.Contains(lower, "group by") {
			return "This query aggregates data by grouping it according to specified dimensions."
		}
		return "This query selects data from the time-series database."
	}

	// PromQL patterns
	if strings.Contains(lower, "rate(") {
		return "This PromQL query calculates the per-second rate of change."
	}
	if strings.Contains(lower, "sum(") {
		return "This PromQL query sums values across selected dimensions."
	}
	if strings.Contains(lower, "histogram_quantile") {
		return "This PromQL query calculates percentiles from histogram data."
	}

	return "Unable to explain this query."
}

func extractMetricFromSQL(query string) string {
	pattern := regexp.MustCompile(`FROM\s+(\w+)`)
	if matches := pattern.FindStringSubmatch(query); matches != nil {
		return matches[1]
	}
	return ""
}

// Suggest provides query suggestions based on available data.
func (qa *QueryAssistant) Suggest() []QuerySuggestion {
	metrics := qa.db.Metrics()
	var suggestions []QuerySuggestion

	if len(metrics) == 0 {
		return suggestions
	}

	// Generate suggestions for first few metrics
	for i, metric := range metrics {
		if i >= 5 {
			break
		}
		suggestions = append(suggestions, QuerySuggestion{
			Query:       fmt.Sprintf("SELECT mean(value) FROM %s WHERE time > now() - 1h GROUP BY time(5m)", metric),
			Description: fmt.Sprintf("Average %s over the last hour", metric),
			Category:    "aggregation",
		})
		suggestions = append(suggestions, QuerySuggestion{
			Query:       fmt.Sprintf("SELECT max(value), min(value) FROM %s WHERE time > now() - 24h", metric),
			Description: fmt.Sprintf("Min/max %s over the last 24 hours", metric),
			Category:    "aggregation",
		})
	}

	return suggestions
}

// QuerySuggestion represents a suggested query.
type QuerySuggestion struct {
	Query       string `json:"query"`
	Description string `json:"description"`
	Category    string `json:"category"`
}
