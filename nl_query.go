package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// NLQueryConfig configures the enhanced natural language query interface.
type NLQueryConfig struct {
	// Provider is the LLM provider.
	Provider string

	// APIKey for the LLM provider.
	APIKey string

	// Endpoint is the API endpoint.
	Endpoint string

	// Model is the model to use.
	Model string

	// EnableConversation enables conversational context.
	EnableConversation bool

	// MaxConversationTurns limits conversation history.
	MaxConversationTurns int

	// EnableOptimization enables query optimization suggestions.
	EnableOptimization bool

	// EnableExplanation enables automatic query explanations.
	EnableExplanation bool

	// EnableVisualization suggests appropriate visualizations.
	EnableVisualization bool

	// EnableAlerts allows creating alerts from NL.
	EnableAlerts bool

	// Timeout for processing.
	Timeout time.Duration

	// FallbackToLocal uses local parsing when LLM unavailable.
	FallbackToLocal bool
}

// DefaultNLQueryConfig returns default configuration.
func DefaultNLQueryConfig() NLQueryConfig {
	return NLQueryConfig{
		Provider:             "local",
		EnableConversation:   true,
		MaxConversationTurns: 10,
		EnableOptimization:   true,
		EnableExplanation:    true,
		EnableVisualization:  true,
		EnableAlerts:         true,
		Timeout:              30 * time.Second,
		FallbackToLocal:      true,
	}
}

// NLQueryEngine provides an enhanced natural language query interface.
type NLQueryEngine struct {
	config    NLQueryConfig
	db        *DB
	assistant *QueryAssistant

	// Conversation context
	conversations   map[string]*Conversation
	conversationsMu sync.RWMutex

	// Intent recognition
	intents   []IntentPattern
	intentsMu sync.RWMutex

	// Query optimization
	optimizer *QueryOptimizer

	// Usage analytics
	analytics   *NLAnalytics
	analyticsMu sync.RWMutex
}

// Conversation represents a conversation session.
type Conversation struct {
	ID          string         `json:"id"`
	Turns       []ConversationTurn `json:"turns"`
	Context     ConversationContext `json:"context"`
	CreatedAt   time.Time      `json:"created_at"`
	LastActiveAt time.Time     `json:"last_active_at"`
}

// ConversationTurn represents a single turn in a conversation.
type ConversationTurn struct {
	Role      string    `json:"role"` // user, assistant
	Content   string    `json:"content"`
	Query     string    `json:"query,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// ConversationContext holds context for a conversation.
type ConversationContext struct {
	CurrentMetric     string            `json:"current_metric,omitempty"`
	CurrentTimeRange  string            `json:"current_time_range,omitempty"`
	CurrentFilters    map[string]string `json:"current_filters,omitempty"`
	LastQueryResult   *Result           `json:"-"`
	ReferencedMetrics []string          `json:"referenced_metrics,omitempty"`
}

// IntentPattern defines a pattern for intent recognition.
type IntentPattern struct {
	Name        string
	Patterns    []*regexp.Regexp
	Handler     func(matches []string, ctx *ConversationContext) *NLQueryResponse
	Priority    int
	RequiresLLM bool
}

// NLQueryResponse represents the response from an NL query.
type NLQueryResponse struct {
	// Original input
	Input string `json:"input"`

	// Recognized intent
	Intent string `json:"intent"`

	// Generated query
	Query     string `json:"query"`
	QueryType string `json:"query_type"` // sql, promql, action

	// Query explanation
	Explanation string `json:"explanation,omitempty"`

	// Confidence score (0-1)
	Confidence float64 `json:"confidence"`

	// Query result (if executed)
	Result *Result `json:"result,omitempty"`

	// Suggested visualization
	Visualization *VisualizationSuggestion `json:"visualization,omitempty"`

	// Optimization suggestions
	Optimizations []string `json:"optimizations,omitempty"`

	// Follow-up suggestions
	FollowUps []string `json:"follow_ups,omitempty"`

	// Errors
	Error string `json:"error,omitempty"`
}

// VisualizationSuggestion suggests how to visualize query results.
type VisualizationSuggestion struct {
	Type       string            `json:"type"` // line, bar, gauge, table, heatmap
	Title      string            `json:"title"`
	XAxis      string            `json:"x_axis,omitempty"`
	YAxis      string            `json:"y_axis,omitempty"`
	GroupBy    string            `json:"group_by,omitempty"`
	Options    map[string]any    `json:"options,omitempty"`
}

// NewNLQueryEngine creates a new NL query engine.
func NewNLQueryEngine(db *DB, config NLQueryConfig) *NLQueryEngine {
	assistantConfig := AssistantConfig{
		Provider:    config.Provider,
		APIKey:      config.APIKey,
		Endpoint:    config.Endpoint,
		Model:       config.Model,
		Timeout:     config.Timeout,
		EnableCache: true,
	}

	engine := &NLQueryEngine{
		config:        config,
		db:            db,
		assistant:     NewQueryAssistant(db, assistantConfig),
		conversations: make(map[string]*Conversation),
		optimizer:     NewQueryOptimizer(db),
		analytics:     NewNLAnalytics(),
	}

	// Register built-in intents
	engine.registerBuiltinIntents()

	return engine
}

func (e *NLQueryEngine) registerBuiltinIntents() {
	e.intents = []IntentPattern{
		// Greeting
		{
			Name:     "greeting",
			Patterns: []*regexp.Regexp{regexp.MustCompile(`(?i)^(hi|hello|hey|good\s+(morning|afternoon|evening))`)},
			Handler:  e.handleGreeting,
			Priority: 100,
		},
		// Help
		{
			Name:     "help",
			Patterns: []*regexp.Regexp{regexp.MustCompile(`(?i)^(help|what\s+can\s+you\s+do|\?)$`)},
			Handler:  e.handleHelp,
			Priority: 100,
		},
		// List metrics
		{
			Name:     "list_metrics",
			Patterns: []*regexp.Regexp{regexp.MustCompile(`(?i)(show|list|get)\s+(all\s+)?(metrics?|measurements?)`)},
			Handler:  e.handleListMetrics,
			Priority: 90,
		},
		// Aggregation queries
		{
			Name: "aggregation",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)(average|mean|avg|sum|count|max|min|median|p\d+)\s+(?:of\s+)?(\w+)`),
			},
			Handler:  e.handleAggregation,
			Priority: 80,
		},
		// Time range queries
		{
			Name: "time_range",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)(last|past|previous)\s+(\d+)\s*(second|minute|hour|day|week|month|year|s|m|h|d|w)s?`),
			},
			Handler:  e.handleTimeRange,
			Priority: 70,
		},
		// Comparison queries
		{
			Name: "comparison",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)compare\s+(\w+)\s+(?:to|with|and)\s+(\w+)`),
			},
			Handler:  e.handleComparison,
			Priority: 75,
		},
		// Alert creation
		{
			Name: "create_alert",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)(alert|notify|warn)\s+(?:me\s+)?(?:when|if)\s+(\w+)\s*(>|<|=|>=|<=)\s*(\d+(?:\.\d+)?)`),
			},
			Handler:  e.handleCreateAlert,
			Priority: 85,
		},
		// Rate/trend queries
		{
			Name: "rate",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)(rate|trend|change|growth)\s+(?:of\s+)?(\w+)`),
			},
			Handler:  e.handleRate,
			Priority: 75,
		},
		// Top N queries
		{
			Name: "top_n",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)top\s+(\d+)\s+(\w+)(?:\s+by\s+(\w+))?`),
			},
			Handler:  e.handleTopN,
			Priority: 75,
		},
		// Anomaly detection
		{
			Name: "anomaly",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)(anomal|unusual|abnormal|outlier).*(\w+)`),
			},
			Handler:  e.handleAnomaly,
			Priority: 70,
		},
		// Forecast
		{
			Name: "forecast",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)(forecast|predict|project).*(\w+).*(?:next|for)\s+(\d+)\s*(hour|day|week|h|d|w)s?`),
			},
			Handler:  e.handleForecast,
			Priority: 70,
		},
		// Context reference (this, it, that)
		{
			Name: "context_reference",
			Patterns: []*regexp.Regexp{
				regexp.MustCompile(`(?i)(show|get|query)\s+(this|it|that|the\s+same)`),
			},
			Handler:  e.handleContextReference,
			Priority: 60,
		},
	}

	// Sort by priority
	sort.Slice(e.intents, func(i, j int) bool {
		return e.intents[i].Priority > e.intents[j].Priority
	})
}

// Query processes a natural language query.
func (e *NLQueryEngine) Query(ctx context.Context, input string) (*NLQueryResponse, error) {
	return e.QueryWithConversation(ctx, "", input)
}

// QueryWithConversation processes a query within a conversation context.
func (e *NLQueryEngine) QueryWithConversation(ctx context.Context, conversationID, input string) (*NLQueryResponse, error) {
	if input == "" {
		return nil, errors.New("input cannot be empty")
	}

	start := time.Now()

	// Get or create conversation
	var conv *Conversation
	if e.config.EnableConversation && conversationID != "" {
		conv = e.getOrCreateConversation(conversationID)
	}

	// Recognize intent
	response := e.recognizeAndHandle(input, conv)

	// Execute query if generated
	if response.Query != "" && response.QueryType != "action" {
		parser := QueryParser{}
		query, parseErr := parser.Parse(response.Query)
		if parseErr != nil {
			response.Error = parseErr.Error()
		} else {
			result, err := e.db.Execute(query)
			if err != nil {
				response.Error = err.Error()
			} else {
				response.Result = result

				// Update conversation context
				if conv != nil {
					e.updateConversationContext(conv, input, response)
				}
			}
		}
	}

	// Add optimizations
	if e.config.EnableOptimization && response.Query != "" {
		response.Optimizations = e.optimizer.Suggest(response.Query)
	}

	// Add visualization suggestion
	if e.config.EnableVisualization && response.Result != nil {
		response.Visualization = e.suggestVisualization(response)
	}

	// Generate follow-ups
	response.FollowUps = e.generateFollowUps(response)

	// Record analytics
	e.recordAnalytics(input, response, time.Since(start))

	return response, nil
}

func (e *NLQueryEngine) getOrCreateConversation(id string) *Conversation {
	e.conversationsMu.Lock()
	defer e.conversationsMu.Unlock()

	if conv, ok := e.conversations[id]; ok {
		conv.LastActiveAt = time.Now()
		return conv
	}

	conv := &Conversation{
		ID:           id,
		Turns:        make([]ConversationTurn, 0),
		Context:      ConversationContext{CurrentFilters: make(map[string]string)},
		CreatedAt:    time.Now(),
		LastActiveAt: time.Now(),
	}
	e.conversations[id] = conv
	return conv
}

func (e *NLQueryEngine) recognizeAndHandle(input string, conv *Conversation) *NLQueryResponse {
	// Try pattern-based intent recognition first
	for _, intent := range e.intents {
		for _, pattern := range intent.Patterns {
			if matches := pattern.FindStringSubmatch(input); matches != nil {
				var ctx *ConversationContext
				if conv != nil {
					ctx = &conv.Context
				}
				response := intent.Handler(matches, ctx)
				response.Input = input
				response.Intent = intent.Name
				return response
			}
		}
	}

	// Fall back to assistant
	if e.config.FallbackToLocal || e.config.APIKey != "" {
		translated, err := e.assistant.Translate(context.Background(), input)
		if err == nil && translated != nil {
			return &NLQueryResponse{
				Input:       input,
				Intent:      "translated",
				Query:       translated.Query,
				QueryType:   translated.QueryType,
				Explanation: translated.Explanation,
				Confidence:  translated.Confidence,
			}
		}
	}

	// Unable to understand
	return &NLQueryResponse{
		Input:      input,
		Intent:     "unknown",
		Confidence: 0,
		Error:      "I couldn't understand that query. Try asking about a specific metric or use 'help' for examples.",
		FollowUps: []string{
			"What metrics are available?",
			"Show me the last hour of cpu_usage",
			"What is the average temperature today?",
		},
	}
}

func (e *NLQueryEngine) updateConversationContext(conv *Conversation, input string, response *NLQueryResponse) {
	// Add turn to conversation
	conv.Turns = append(conv.Turns, ConversationTurn{
		Role:      "user",
		Content:   input,
		Timestamp: time.Now(),
	})
	conv.Turns = append(conv.Turns, ConversationTurn{
		Role:      "assistant",
		Content:   response.Explanation,
		Query:     response.Query,
		Timestamp: time.Now(),
	})

	// Trim conversation if too long
	if len(conv.Turns) > e.config.MaxConversationTurns*2 {
		conv.Turns = conv.Turns[len(conv.Turns)-e.config.MaxConversationTurns*2:]
	}

	// Extract context from query
	metricPattern := regexp.MustCompile(`FROM\s+(\w+)`)
	if matches := metricPattern.FindStringSubmatch(response.Query); matches != nil {
		conv.Context.CurrentMetric = matches[1]
		if !contains(conv.Context.ReferencedMetrics, matches[1]) {
			conv.Context.ReferencedMetrics = append(conv.Context.ReferencedMetrics, matches[1])
		}
	}

	// Extract time range
	timePattern := regexp.MustCompile(`time\s*>\s*now\(\)\s*-\s*(\d+[smhd])`)
	if matches := timePattern.FindStringSubmatch(response.Query); matches != nil {
		conv.Context.CurrentTimeRange = matches[1]
	}

	// Store last result
	conv.Context.LastQueryResult = response.Result
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// ========== Intent Handlers ==========

func (e *NLQueryEngine) handleGreeting(_ []string, _ *ConversationContext) *NLQueryResponse {
	metrics := e.db.Metrics()
	var greeting string
	if len(metrics) > 0 {
		greeting = fmt.Sprintf("Hello! I can help you query your time-series data. You have %d metrics available. What would you like to know?", len(metrics))
	} else {
		greeting = "Hello! I can help you query your time-series data. It looks like there's no data yet. Would you like to know how to ingest data?"
	}

	return &NLQueryResponse{
		Intent:      "greeting",
		Explanation: greeting,
		Confidence:  1.0,
		QueryType:   "action",
		FollowUps: []string{
			"Show me all metrics",
			"What data do you have?",
		},
	}
}

func (e *NLQueryEngine) handleHelp(_ []string, _ *ConversationContext) *NLQueryResponse {
	help := `I can help you with:

**Querying Data:**
- "Show me the average cpu_usage in the last hour"
- "What's the maximum temperature today?"
- "Get the sum of requests by host"

**Analysis:**
- "Are there any anomalies in memory_usage?"
- "Forecast disk_usage for the next 24 hours"
- "Compare cpu_usage to memory_usage"

**Alerts:**
- "Alert me when cpu_usage > 80"

**Visualization:**
- "Graph temperature over time"

Try asking about your specific metrics!`

	return &NLQueryResponse{
		Intent:      "help",
		Explanation: help,
		Confidence:  1.0,
		QueryType:   "action",
		FollowUps: []string{
			"What metrics are available?",
			"Show me an example query",
		},
	}
}

func (e *NLQueryEngine) handleListMetrics(_ []string, _ *ConversationContext) *NLQueryResponse {
	metrics := e.db.Metrics()

	var explanation string
	if len(metrics) == 0 {
		explanation = "No metrics are currently available."
	} else if len(metrics) <= 10 {
		explanation = fmt.Sprintf("Available metrics: %s", strings.Join(metrics, ", "))
	} else {
		explanation = fmt.Sprintf("Found %d metrics. First 10: %s...", len(metrics), strings.Join(metrics[:10], ", "))
	}

	return &NLQueryResponse{
		Intent:      "list_metrics",
		Query:       "SHOW MEASUREMENTS",
		QueryType:   "sql",
		Explanation: explanation,
		Confidence:  1.0,
	}
}

func (e *NLQueryEngine) handleAggregation(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if len(matches) < 3 {
		return nil
	}

	fn := strings.ToLower(matches[1])
	metric := matches[2]

	// Normalize function name
	switch fn {
	case "average", "avg":
		fn = "mean"
	case "maximum":
		fn = "max"
	case "minimum":
		fn = "min"
	}

	// Get time range from context or use default
	timeRange := "1h"
	if ctx != nil && ctx.CurrentTimeRange != "" {
		timeRange = ctx.CurrentTimeRange
	}

	query := fmt.Sprintf("SELECT %s(value) FROM %s WHERE time > now() - %s", fn, metric, timeRange)

	return &NLQueryResponse{
		Intent:      "aggregation",
		Query:       query,
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Calculating %s of %s over the last %s", fn, metric, timeRange),
		Confidence:  0.9,
	}
}

func (e *NLQueryEngine) handleTimeRange(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if len(matches) < 4 {
		return nil
	}

	amount := matches[2]
	unit := normalizeTimeUnit(matches[3])
	timeRange := amount + unit

	// Use current metric from context
	metric := "value"
	if ctx != nil && ctx.CurrentMetric != "" {
		metric = ctx.CurrentMetric
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE time > now() - %s", metric, timeRange)

	return &NLQueryResponse{
		Intent:      "time_range",
		Query:       query,
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Showing %s for the last %s%s", metric, amount, unit),
		Confidence:  0.85,
	}
}

func (e *NLQueryEngine) handleComparison(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if len(matches) < 3 {
		return nil
	}

	metric1 := matches[1]
	metric2 := matches[2]

	timeRange := "1h"
	if ctx != nil && ctx.CurrentTimeRange != "" {
		timeRange = ctx.CurrentTimeRange
	}

	// Generate a comparison query
	query := fmt.Sprintf(`SELECT mean(value) as "%s", mean(value) as "%s" FROM %s, %s WHERE time > now() - %s GROUP BY time(5m)`,
		metric1, metric2, metric1, metric2, timeRange)

	return &NLQueryResponse{
		Intent:      "comparison",
		Query:       query,
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Comparing %s and %s over the last %s", metric1, metric2, timeRange),
		Confidence:  0.8,
		Visualization: &VisualizationSuggestion{
			Type:  "line",
			Title: fmt.Sprintf("Comparison: %s vs %s", metric1, metric2),
		},
	}
}

func (e *NLQueryEngine) handleCreateAlert(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if !e.config.EnableAlerts {
		return &NLQueryResponse{
			Intent:      "create_alert",
			Error:       "Alert creation is disabled",
			Confidence:  0.9,
			QueryType:   "action",
		}
	}

	if len(matches) < 5 {
		return nil
	}

	metric := matches[2]
	operator := matches[3]
	threshold := matches[4]

	return &NLQueryResponse{
		Intent:      "create_alert",
		Query:       fmt.Sprintf("CREATE ALERT ON %s WHEN value %s %s", metric, operator, threshold),
		QueryType:   "action",
		Explanation: fmt.Sprintf("Creating alert: notify when %s %s %s", metric, operator, threshold),
		Confidence:  0.9,
	}
}

func (e *NLQueryEngine) handleRate(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if len(matches) < 3 {
		return nil
	}

	metric := matches[2]

	return &NLQueryResponse{
		Intent:      "rate",
		Query:       fmt.Sprintf("rate(%s[5m])", metric),
		QueryType:   "promql",
		Explanation: fmt.Sprintf("Calculating per-second rate of change for %s", metric),
		Confidence:  0.9,
	}
}

func (e *NLQueryEngine) handleTopN(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if len(matches) < 3 {
		return nil
	}

	n := matches[1]
	metric := matches[2]
	groupBy := "host"
	if len(matches) > 3 && matches[3] != "" {
		groupBy = matches[3]
	}

	query := fmt.Sprintf("SELECT mean(value) FROM %s WHERE time > now() - 1h GROUP BY %s ORDER BY mean DESC LIMIT %s",
		metric, groupBy, n)

	return &NLQueryResponse{
		Intent:      "top_n",
		Query:       query,
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Finding top %s %s by %s", n, metric, groupBy),
		Confidence:  0.85,
		Visualization: &VisualizationSuggestion{
			Type:  "bar",
			Title: fmt.Sprintf("Top %s %s", n, metric),
		},
	}
}

func (e *NLQueryEngine) handleAnomaly(matches []string, ctx *ConversationContext) *NLQueryResponse {
	metric := ""
	if len(matches) > 2 {
		metric = matches[2]
	} else if ctx != nil && ctx.CurrentMetric != "" {
		metric = ctx.CurrentMetric
	}

	if metric == "" {
		return &NLQueryResponse{
			Intent:     "anomaly",
			Error:      "Please specify which metric to check for anomalies",
			Confidence: 0.7,
			FollowUps:  []string{"Check anomalies in cpu_usage"},
		}
	}

	// This would integrate with the anomaly detection engine
	query := fmt.Sprintf("SELECT * FROM %s WHERE time > now() - 24h AND anomaly = true", metric)

	return &NLQueryResponse{
		Intent:      "anomaly",
		Query:       query,
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Detecting anomalies in %s over the last 24 hours", metric),
		Confidence:  0.8,
	}
}

func (e *NLQueryEngine) handleForecast(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if len(matches) < 5 {
		return nil
	}

	metric := matches[2]
	amount := matches[3]
	unit := normalizeTimeUnit(matches[4])

	return &NLQueryResponse{
		Intent:      "forecast",
		Query:       fmt.Sprintf("FORECAST %s FOR %s%s", metric, amount, unit),
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Forecasting %s for the next %s%s", metric, amount, unit),
		Confidence:  0.8,
		Visualization: &VisualizationSuggestion{
			Type:  "line",
			Title: fmt.Sprintf("%s Forecast", metric),
			Options: map[string]any{
				"show_confidence_band": true,
			},
		},
	}
}

func (e *NLQueryEngine) handleContextReference(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if ctx == nil || ctx.CurrentMetric == "" {
		return &NLQueryResponse{
			Intent:     "context_reference",
			Error:      "I don't have enough context. Please specify what you'd like to see.",
			Confidence: 0.5,
		}
	}

	timeRange := "1h"
	if ctx.CurrentTimeRange != "" {
		timeRange = ctx.CurrentTimeRange
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE time > now() - %s", ctx.CurrentMetric, timeRange)

	return &NLQueryResponse{
		Intent:      "context_reference",
		Query:       query,
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Showing %s (from previous context)", ctx.CurrentMetric),
		Confidence:  0.75,
	}
}

// ========== Visualization Suggestions ==========

func (e *NLQueryEngine) suggestVisualization(response *NLQueryResponse) *VisualizationSuggestion {
	if response.Result == nil || len(response.Result.Points) == 0 {
		return nil
	}

	// Determine best visualization based on query type and data
	switch response.Intent {
	case "aggregation":
		if len(response.Result.Points) == 1 {
			return &VisualizationSuggestion{
				Type:  "gauge",
				Title: "Current Value",
			}
		}
		return &VisualizationSuggestion{
			Type:  "line",
			Title: "Time Series",
			XAxis: "time",
			YAxis: "value",
		}

	case "comparison":
		return &VisualizationSuggestion{
			Type:  "line",
			Title: "Comparison",
		}

	case "top_n":
		return &VisualizationSuggestion{
			Type:  "bar",
			Title: "Top Values",
		}

	default:
		// Default to line chart for time-series data
		return &VisualizationSuggestion{
			Type:  "line",
			Title: "Time Series Data",
			XAxis: "time",
			YAxis: "value",
		}
	}
}

// ========== Follow-up Generation ==========

func (e *NLQueryEngine) generateFollowUps(response *NLQueryResponse) []string {
	followUps := make([]string, 0, 3)

	// Extract metric from response
	var metric string
	metricPattern := regexp.MustCompile(`FROM\s+(\w+)`)
	if matches := metricPattern.FindStringSubmatch(response.Query); matches != nil {
		metric = matches[1]
	}

	switch response.Intent {
	case "aggregation":
		if metric != "" {
			followUps = append(followUps, fmt.Sprintf("Show me the trend of %s", metric))
			followUps = append(followUps, fmt.Sprintf("Are there anomalies in %s?", metric))
		}

	case "time_range":
		followUps = append(followUps, "Show me a longer time range")
		followUps = append(followUps, "What's the average?")

	case "anomaly":
		if metric != "" {
			followUps = append(followUps, fmt.Sprintf("What caused the anomaly in %s?", metric))
			followUps = append(followUps, fmt.Sprintf("Create an alert for %s", metric))
		}

	case "forecast":
		followUps = append(followUps, "Show historical data for comparison")
		followUps = append(followUps, "What's the confidence interval?")

	default:
		followUps = append(followUps,
			"Show me more details",
			"Compare with yesterday",
			"Create an alert",
		)
	}

	return followUps[:min(len(followUps), 3)]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ========== Query Optimizer ==========

// QueryOptimizer provides query optimization suggestions.
type QueryOptimizer struct {
	db *DB
}

// NewQueryOptimizer creates a new query optimizer.
func NewQueryOptimizer(db *DB) *QueryOptimizer {
	return &QueryOptimizer{db: db}
}

// Suggest returns optimization suggestions for a query.
func (o *QueryOptimizer) Suggest(query string) []string {
	suggestions := make([]string, 0)
	lower := strings.ToLower(query)

	// Check for SELECT * without LIMIT
	if strings.Contains(lower, "select *") && !strings.Contains(lower, "limit") {
		suggestions = append(suggestions, "Consider adding a LIMIT clause to prevent large result sets")
	}

	// Check for missing time filter
	if !strings.Contains(lower, "time >") && !strings.Contains(lower, "time <") {
		suggestions = append(suggestions, "Consider adding a time range filter for better performance")
	}

	// Check for inefficient GROUP BY
	if strings.Contains(lower, "group by") {
		if strings.Contains(lower, "group by time(1s)") {
			suggestions = append(suggestions, "Consider using a larger GROUP BY time interval for faster queries")
		}
	}

	// Check for missing index usage hints
	if strings.Contains(lower, "where") && !strings.Contains(lower, "time") {
		suggestions = append(suggestions, "Queries with time filters are generally faster")
	}

	return suggestions
}

// ========== Analytics ==========

// NLAnalytics tracks usage analytics for the NL interface.
type NLAnalytics struct {
	queries        []NLQueryRecord
	queriesMu      sync.RWMutex
	intentCounts   map[string]int
	avgLatency     time.Duration
	totalQueries   int
}

// NLQueryRecord records a single NL query.
type NLQueryRecord struct {
	Input      string        `json:"input"`
	Intent     string        `json:"intent"`
	Confidence float64       `json:"confidence"`
	Latency    time.Duration `json:"latency"`
	Success    bool          `json:"success"`
	Timestamp  time.Time     `json:"timestamp"`
}

// NewNLAnalytics creates a new analytics tracker.
func NewNLAnalytics() *NLAnalytics {
	return &NLAnalytics{
		queries:      make([]NLQueryRecord, 0),
		intentCounts: make(map[string]int),
	}
}

func (e *NLQueryEngine) recordAnalytics(input string, response *NLQueryResponse, latency time.Duration) {
	record := NLQueryRecord{
		Input:      input,
		Intent:     response.Intent,
		Confidence: response.Confidence,
		Latency:    latency,
		Success:    response.Error == "",
		Timestamp:  time.Now(),
	}

	e.analyticsMu.Lock()
	defer e.analyticsMu.Unlock()

	e.analytics.queries = append(e.analytics.queries, record)
	e.analytics.intentCounts[response.Intent]++
	e.analytics.totalQueries++

	// Update average latency
	e.analytics.avgLatency = time.Duration(
		(int64(e.analytics.avgLatency)*int64(e.analytics.totalQueries-1) + int64(latency)) /
			int64(e.analytics.totalQueries))

	// Trim history
	if len(e.analytics.queries) > 10000 {
		e.analytics.queries = e.analytics.queries[len(e.analytics.queries)-10000:]
	}
}

// GetAnalytics returns usage analytics.
func (e *NLQueryEngine) GetAnalytics() NLAnalyticsReport {
	e.analyticsMu.RLock()
	defer e.analyticsMu.RUnlock()

	successCount := 0
	for _, q := range e.analytics.queries {
		if q.Success {
			successCount++
		}
	}

	successRate := 0.0
	if len(e.analytics.queries) > 0 {
		successRate = float64(successCount) / float64(len(e.analytics.queries))
	}

	return NLAnalyticsReport{
		TotalQueries: e.analytics.totalQueries,
		SuccessRate:  successRate,
		AvgLatency:   e.analytics.avgLatency,
		IntentCounts: e.analytics.intentCounts,
	}
}

// NLAnalyticsReport contains analytics report data.
type NLAnalyticsReport struct {
	TotalQueries int               `json:"total_queries"`
	SuccessRate  float64           `json:"success_rate"`
	AvgLatency   time.Duration     `json:"avg_latency"`
	IntentCounts map[string]int    `json:"intent_counts"`
}

// ========== Conversation Management ==========

// GetConversation retrieves a conversation by ID.
func (e *NLQueryEngine) GetConversation(id string) (*Conversation, bool) {
	e.conversationsMu.RLock()
	defer e.conversationsMu.RUnlock()

	conv, ok := e.conversations[id]
	return conv, ok
}

// ClearConversation clears a conversation's history.
func (e *NLQueryEngine) ClearConversation(id string) {
	e.conversationsMu.Lock()
	defer e.conversationsMu.Unlock()

	if conv, ok := e.conversations[id]; ok {
		conv.Turns = conv.Turns[:0]
		conv.Context = ConversationContext{CurrentFilters: make(map[string]string)}
	}
}

// DeleteConversation deletes a conversation.
func (e *NLQueryEngine) DeleteConversation(id string) {
	e.conversationsMu.Lock()
	defer e.conversationsMu.Unlock()

	delete(e.conversations, id)
}

// ========== Utility Functions ==========

func normalizeTimeUnit(unit string) string {
	unit = strings.ToLower(unit)
	switch unit {
	case "second", "seconds":
		return "s"
	case "minute", "minutes":
		return "m"
	case "hour", "hours":
		return "h"
	case "day", "days":
		return "d"
	case "week", "weeks":
		return "w"
	default:
		return unit
	}
}

// ========== HTTP Handler ==========

// NLQueryHandler returns an HTTP handler for NL queries.
func (e *NLQueryEngine) NLQueryHandler() func(w ResponseWriter, r *Request) {
	return func(w ResponseWriter, r *Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}

		var req struct {
			Query          string `json:"query"`
			ConversationID string `json:"conversation_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		response, err := e.QueryWithConversation(r.Context(), req.ConversationID, req.Query)
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// ResponseWriter interface for HTTP responses
type ResponseWriter interface {
	Header() Header
	Write([]byte) (int, error)
	WriteHeader(statusCode int)
}

// Header interface for HTTP headers
type Header interface {
	Set(key, value string)
}

// Request represents an HTTP request (simplified)
type Request struct {
	Method string
	Body   io.ReadCloser
}

// Context returns the request context
func (r *Request) Context() context.Context {
	return context.Background()
}

// io.ReadCloser is already defined in io package
