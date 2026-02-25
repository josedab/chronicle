package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Intent handlers (time range, comparison, alerts, anomaly, forecast), analytics, and HTTP handler for natural language queries.

func (e *NLQueryEngine) handleTimeRange(matches []string, ctx *ConversationContext) *NLQueryResponse {
	if len(matches) < 4 {
		return nil
	}

	amount := matches[2]
	unit := normalizeTimeUnit(matches[3])
	timeRange := amount + unit

	if !validTimeRangeRe.MatchString(timeRange) {
		timeRange = "1h"
	}

	// Use current metric from context
	metric := "value"
	if ctx != nil && ctx.CurrentMetric != "" {
		metric = ctx.CurrentMetric
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE time > now() - %s", sanitizeIdentifier(metric), timeRange)

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

	if !validTimeRangeRe.MatchString(timeRange) {
		timeRange = "1h"
	}
	query := fmt.Sprintf(`SELECT mean(value) as "%s", mean(value) as "%s" FROM %s, %s WHERE time > now() - %s GROUP BY time(5m)`,
		sanitizeIdentifier(metric1), sanitizeIdentifier(metric2), sanitizeIdentifier(metric1), sanitizeIdentifier(metric2), timeRange)

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
			Intent:     "create_alert",
			Error:      "Alert creation is disabled",
			Confidence: 0.9,
			QueryType:  "action",
		}
	}

	if len(matches) < 5 {
		return nil
	}

	metric := matches[2]
	operator := matches[3]
	threshold := matches[4]

	if !allowedOperators[operator] {
		return &NLQueryResponse{
			Intent:     "create_alert",
			Error:      fmt.Sprintf("unsupported operator: %s", operator),
			Confidence: 0.0,
			QueryType:  "action",
		}
	}

	if _, err := strconv.ParseFloat(threshold, 64); err != nil {
		return &NLQueryResponse{
			Intent:     "create_alert",
			Error:      fmt.Sprintf("invalid threshold value: %s", threshold),
			Confidence: 0.0,
			QueryType:  "action",
		}
	}

	return &NLQueryResponse{
		Intent:      "create_alert",
		Query:       fmt.Sprintf("CREATE ALERT ON %s WHEN value %s %s", sanitizeIdentifier(metric), operator, threshold),
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
		Query:       fmt.Sprintf("rate(%s[5m])", sanitizeIdentifier(metric)),
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
	if _, err := strconv.Atoi(n); err != nil {
		return &NLQueryResponse{
			Intent:     "top_n",
			Error:      fmt.Sprintf("invalid limit value: %s", n),
			Confidence: 0.0,
			QueryType:  "sql",
		}
	}
	metric := matches[2]
	groupBy := "host"
	if len(matches) > 3 && matches[3] != "" {
		groupBy = matches[3]
	}

	query := fmt.Sprintf("SELECT mean(value) FROM %s WHERE time > now() - 1h GROUP BY %s ORDER BY mean DESC LIMIT %s",
		sanitizeIdentifier(metric), sanitizeIdentifier(groupBy), n)

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
	query := fmt.Sprintf("SELECT * FROM %s WHERE time > now() - 24h AND anomaly = true", sanitizeIdentifier(metric))

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

	query := fmt.Sprintf("SELECT * FROM %s WHERE time > now() - %s", sanitizeIdentifier(ctx.CurrentMetric), timeRange)

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
	queries      []NLQueryRecord
	queriesMu    sync.RWMutex
	intentCounts map[string]int
	avgLatency   time.Duration
	totalQueries int
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
	TotalQueries int            `json:"total_queries"`
	SuccessRate  float64        `json:"success_rate"`
	AvgLatency   time.Duration  `json:"avg_latency"`
	IntentCounts map[string]int `json:"intent_counts"`
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
