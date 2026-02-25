package chronicle

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	allowedAggFunctions = map[string]bool{
		"mean": true, "max": true, "min": true, "count": true, "sum": true,
	}
	validTimeRangeRe = regexp.MustCompile(`^\d+[smhd]$`)
	allowedOperators = map[string]bool{
		">": true, "<": true, ">=": true, "<=": true, "=": true, "!=": true,
		"above": true, "below": true, "exceeds": true,
	}
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
	ID           string              `json:"id"`
	Turns        []ConversationTurn  `json:"turns"`
	Context      ConversationContext `json:"context"`
	CreatedAt    time.Time           `json:"created_at"`
	LastActiveAt time.Time           `json:"last_active_at"`
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
	Type    string         `json:"type"` // line, bar, gauge, table, heatmap
	Title   string         `json:"title"`
	XAxis   string         `json:"x_axis,omitempty"`
	YAxis   string         `json:"y_axis,omitempty"`
	GroupBy string         `json:"group_by,omitempty"`
	Options map[string]any `json:"options,omitempty"`
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

	if !allowedAggFunctions[fn] {
		return &NLQueryResponse{
			Intent:     "aggregation",
			Error:      fmt.Sprintf("unsupported aggregation function: %s", fn),
			Confidence: 0.0,
			QueryType:  "sql",
		}
	}

	// Get time range from context or use default
	timeRange := "1h"
	if ctx != nil && ctx.CurrentTimeRange != "" {
		timeRange = ctx.CurrentTimeRange
	}

	if !validTimeRangeRe.MatchString(timeRange) {
		timeRange = "1h"
	}

	query := fmt.Sprintf("SELECT %s(value) FROM %s WHERE time > now() - %s", fn, sanitizeIdentifier(metric), timeRange)

	return &NLQueryResponse{
		Intent:      "aggregation",
		Query:       query,
		QueryType:   "sql",
		Explanation: fmt.Sprintf("Calculating %s of %s over the last %s", fn, metric, timeRange),
		Confidence:  0.9,
	}
}
