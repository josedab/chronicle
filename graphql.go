package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// GraphQLServer provides a GraphQL API for Chronicle.
type GraphQLServer struct {
	db              *DB
	subscriptions   map[string]*graphQLSubscription
	subscriptionsMu sync.RWMutex
}

type graphQLSubscription struct {
	id       string
	query    string
	ch       chan *GraphQLResult
	doneCh   chan struct{}
	interval time.Duration
}

// GraphQLRequest represents a GraphQL request.
type GraphQLRequest struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName,omitempty"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
}

// GraphQLResponse represents a GraphQL response.
type GraphQLResponse struct {
	Data   interface{}     `json:"data,omitempty"`
	Errors []GraphQLError  `json:"errors,omitempty"`
}

// GraphQLError represents a GraphQL error.
type GraphQLError struct {
	Message   string   `json:"message"`
	Path      []string `json:"path,omitempty"`
	Locations []struct {
		Line   int `json:"line"`
		Column int `json:"column"`
	} `json:"locations,omitempty"`
}

// GraphQLResult is a query result for subscriptions.
type GraphQLResult struct {
	Data  interface{}
	Error error
}

// NewGraphQLServer creates a GraphQL server.
func NewGraphQLServer(db *DB) *GraphQLServer {
	return &GraphQLServer{
		db:            db,
		subscriptions: make(map[string]*graphQLSubscription),
	}
}

// Handler returns an HTTP handler for GraphQL requests.
func (s *GraphQLServer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest

		switch r.Method {
		case http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeGraphQLError(w, "invalid request body: "+err.Error())
				return
			}
		case http.MethodGet:
			req.Query = r.URL.Query().Get("query")
			req.OperationName = r.URL.Query().Get("operationName")
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		result := s.Execute(r.Context(), req)
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
}

// Execute executes a GraphQL query.
func (s *GraphQLServer) Execute(ctx context.Context, req GraphQLRequest) *GraphQLResponse {
	query := strings.TrimSpace(req.Query)

	if query == "" {
		return &GraphQLResponse{
			Errors: []GraphQLError{{Message: "query is required"}},
		}
	}

	// Parse the GraphQL query
	op, err := parseGraphQLQuery(query)
	if err != nil {
		return &GraphQLResponse{
			Errors: []GraphQLError{{Message: err.Error()}},
		}
	}

	// Execute based on operation
	switch op.Type {
	case "query":
		return s.executeQuery(ctx, op, req.Variables)
	case "mutation":
		return s.executeMutation(ctx, op, req.Variables)
	case "subscription":
		return &GraphQLResponse{
			Errors: []GraphQLError{{Message: "subscriptions should use WebSocket"}},
		}
	default:
		return &GraphQLResponse{
			Errors: []GraphQLError{{Message: "unknown operation type: " + op.Type}},
		}
	}
}

type graphQLOperation struct {
	Type       string
	Name       string
	Selections []graphQLSelection
}

type graphQLSelection struct {
	Name      string
	Arguments map[string]interface{}
	Fields    []string
}

func parseGraphQLQuery(query string) (*graphQLOperation, error) {
	query = strings.TrimSpace(query)

	op := &graphQLOperation{
		Type: "query", // Default
	}

	// Determine operation type
	if strings.HasPrefix(query, "query") {
		op.Type = "query"
		query = strings.TrimPrefix(query, "query")
	} else if strings.HasPrefix(query, "mutation") {
		op.Type = "mutation"
		query = strings.TrimPrefix(query, "mutation")
	} else if strings.HasPrefix(query, "subscription") {
		op.Type = "subscription"
		query = strings.TrimPrefix(query, "subscription")
	}

	// Extract operation name (if present)
	query = strings.TrimSpace(query)
	if !strings.HasPrefix(query, "{") {
		// Has operation name
		parts := strings.SplitN(query, "{", 2)
		if len(parts) == 2 {
			op.Name = strings.TrimSpace(parts[0])
			query = "{" + parts[1]
		}
	}

	// Parse selections
	selections, err := parseGraphQLSelections(query)
	if err != nil {
		return nil, err
	}
	op.Selections = selections

	return op, nil
}

func parseGraphQLSelections(body string) ([]graphQLSelection, error) {
	body = strings.TrimSpace(body)
	if !strings.HasPrefix(body, "{") || !strings.HasSuffix(body, "}") {
		return nil, fmt.Errorf("invalid selection set")
	}

	body = body[1 : len(body)-1]
	body = strings.TrimSpace(body)

	var selections []graphQLSelection

	// Simple parser for field selections
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		sel := graphQLSelection{
			Arguments: make(map[string]interface{}),
		}

		// Parse field name and arguments
		if idx := strings.Index(line, "("); idx != -1 {
			sel.Name = strings.TrimSpace(line[:idx])
			// Parse arguments
			endIdx := strings.Index(line, ")")
			if endIdx > idx {
				argsStr := line[idx+1 : endIdx]
				sel.Arguments = parseGraphQLArguments(argsStr)
			}
		} else if idx := strings.Index(line, "{"); idx != -1 {
			sel.Name = strings.TrimSpace(line[:idx])
		} else {
			sel.Name = strings.TrimSpace(line)
		}

		if sel.Name != "" {
			selections = append(selections, sel)
		}
	}

	return selections, nil
}

func parseGraphQLArguments(argsStr string) map[string]interface{} {
	args := make(map[string]interface{})
	
	parts := strings.Split(argsStr, ",")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			
			// Remove quotes from string values
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				value = value[1 : len(value)-1]
			}
			
			args[key] = value
		}
	}
	
	return args
}

func (s *GraphQLServer) executeQuery(ctx context.Context, op *graphQLOperation, variables map[string]interface{}) *GraphQLResponse {
	data := make(map[string]interface{})

	for _, sel := range op.Selections {
		switch sel.Name {
		case "metrics":
			data["metrics"] = s.queryMetrics(ctx, sel)
		case "series":
			data["series"] = s.querySeries(ctx, sel)
		case "points":
			data["points"] = s.queryPoints(ctx, sel, variables)
		case "stats":
			data["stats"] = s.queryStats(ctx)
		case "config":
			data["config"] = s.queryConfig(ctx)
		default:
			return &GraphQLResponse{
				Errors: []GraphQLError{{Message: "unknown field: " + sel.Name}},
			}
		}
	}

	return &GraphQLResponse{Data: data}
}

func (s *GraphQLServer) executeMutation(ctx context.Context, op *graphQLOperation, variables map[string]interface{}) *GraphQLResponse {
	data := make(map[string]interface{})

	for _, sel := range op.Selections {
		switch sel.Name {
		case "write":
			result, err := s.mutationWrite(ctx, sel, variables)
			if err != nil {
				return &GraphQLResponse{
					Errors: []GraphQLError{{Message: err.Error()}},
				}
			}
			data["write"] = result
		case "delete":
			result, err := s.mutationDelete(ctx, sel, variables)
			if err != nil {
				return &GraphQLResponse{
					Errors: []GraphQLError{{Message: err.Error()}},
				}
			}
			data["delete"] = result
		default:
			return &GraphQLResponse{
				Errors: []GraphQLError{{Message: "unknown mutation: " + sel.Name}},
			}
		}
	}

	return &GraphQLResponse{Data: data}
}

func (s *GraphQLServer) queryMetrics(_ context.Context, _ graphQLSelection) []string {
	return s.db.Metrics()
}

func (s *GraphQLServer) querySeries(_ context.Context, sel graphQLSelection) []map[string]interface{} {
	metric, _ := sel.Arguments["metric"].(string)
	
	if metric == "" {
		// Return all metrics with their series
		var result []map[string]interface{}
		for _, m := range s.db.Metrics() {
			result = append(result, map[string]interface{}{
				"metric": m,
			})
		}
		return result
	}

	return []map[string]interface{}{
		{"metric": metric},
	}
}

func (s *GraphQLServer) queryPoints(ctx context.Context, sel graphQLSelection, variables map[string]interface{}) []map[string]interface{} {
	metric, _ := sel.Arguments["metric"].(string)
	if metric == "" {
		if m, ok := variables["metric"].(string); ok {
			metric = m
		}
	}

	startStr, _ := sel.Arguments["start"].(string)
	endStr, _ := sel.Arguments["end"].(string)

	start := parseTimeArg(startStr)
	end := parseTimeArg(endStr)
	if end == 0 {
		end = time.Now().UnixNano()
	}

	query := &Query{
		Metric: metric,
		Start:  start,
		End:    end,
	}

	result, err := s.db.Execute(query)
	if err != nil {
		return nil
	}

	var points []map[string]interface{}
	for _, p := range result.Points {
		points = append(points, map[string]interface{}{
			"timestamp": p.Timestamp,
			"value":     p.Value,
			"tags":      p.Tags,
		})
	}

	return points
}

func (s *GraphQLServer) queryStats(_ context.Context) map[string]interface{} {
	return map[string]interface{}{
		"metrics":      len(s.db.Metrics()),
		"uptime":       time.Since(time.Now()).String(),
		"version":      "1.0.0",
	}
}

func (s *GraphQLServer) queryConfig(_ context.Context) map[string]interface{} {
	return map[string]interface{}{
		"partitionDuration": s.db.config.PartitionDuration.String(),
		"bufferSize":        s.db.config.BufferSize,
		"retention":         s.db.config.RetentionDuration.String(),
	}
}

func (s *GraphQLServer) mutationWrite(ctx context.Context, sel graphQLSelection, variables map[string]interface{}) (bool, error) {
	metric, _ := sel.Arguments["metric"].(string)
	if metric == "" {
		if m, ok := variables["metric"].(string); ok {
			metric = m
		}
	}

	valueStr, _ := sel.Arguments["value"].(string)
	var value float64
	fmt.Sscanf(valueStr, "%f", &value)

	point := Point{
		Metric:    metric,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	if err := s.db.Write(point); err != nil {
		return false, err
	}

	return true, nil
}

func (s *GraphQLServer) mutationDelete(ctx context.Context, sel graphQLSelection, variables map[string]interface{}) (int, error) {
	metric, _ := sel.Arguments["metric"].(string)
	if metric == "" {
		if m, ok := variables["metric"].(string); ok {
			metric = m
		}
	}

	if metric == "" {
		return 0, fmt.Errorf("metric is required")
	}

	// Delete is not directly supported, return 0
	return 0, nil
}

// Subscribe creates a subscription for real-time updates.
func (s *GraphQLServer) Subscribe(ctx context.Context, query string, interval time.Duration) (<-chan *GraphQLResult, string, error) {
	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()

	id := fmt.Sprintf("sub_%d", time.Now().UnixNano())
	
	sub := &graphQLSubscription{
		id:       id,
		query:    query,
		ch:       make(chan *GraphQLResult, 10),
		doneCh:   make(chan struct{}),
		interval: interval,
	}

	s.subscriptions[id] = sub

	// Start subscription goroutine
	go s.runSubscription(ctx, sub)

	return sub.ch, id, nil
}

// Unsubscribe cancels a subscription.
func (s *GraphQLServer) Unsubscribe(id string) {
	s.subscriptionsMu.Lock()
	sub, ok := s.subscriptions[id]
	if ok {
		delete(s.subscriptions, id)
	}
	s.subscriptionsMu.Unlock()

	if ok && sub.doneCh != nil {
		close(sub.doneCh)
	}
}

func (s *GraphQLServer) runSubscription(ctx context.Context, sub *graphQLSubscription) {
	defer close(sub.ch)

	ticker := time.NewTicker(sub.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.doneCh:
			return
		case <-ticker.C:
			req := GraphQLRequest{Query: sub.query}
			resp := s.Execute(ctx, req)
			
			result := &GraphQLResult{
				Data: resp.Data,
			}
			if len(resp.Errors) > 0 {
				result.Error = fmt.Errorf("%s", resp.Errors[0].Message)
			}

			select {
			case sub.ch <- result:
			default:
				// Channel full, skip
			}
		}
	}
}

// Schema returns the GraphQL schema definition.
func (s *GraphQLServer) Schema() string {
	return `
type Query {
  metrics: [String!]!
  series(metric: String): [Series!]!
  points(metric: String!, start: String, end: String): [Point!]!
  stats: Stats!
  config: Config!
}

type Mutation {
  write(metric: String!, value: Float!, tags: TagInput): Boolean!
  delete(metric: String!): Int!
}

type Subscription {
  points(metric: String!): Point!
}

type Series {
  metric: String!
  tags: [Tag!]!
}

type Point {
  timestamp: Int!
  value: Float!
  tags: [Tag!]!
}

type Tag {
  key: String!
  value: String!
}

input TagInput {
  key: String!
  value: String!
}

type Stats {
  metrics: Int!
  uptime: String!
  version: String!
}

type Config {
  partitionDuration: String!
  bufferSize: Int!
  retention: String!
}
`
}

// ServePlayground serves a GraphQL playground HTML page.
func (s *GraphQLServer) ServePlayground() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(graphqlPlaygroundHTML))
	})
}

func writeGraphQLError(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(GraphQLResponse{
		Errors: []GraphQLError{{Message: message}},
	})
}

func parseTimeArg(s string) int64 {
	if s == "" {
		return 0
	}

	// Try parsing as duration from now
	if d, err := time.ParseDuration(s); err == nil {
		return time.Now().Add(-d).UnixNano()
	}

	// Try parsing as RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixNano()
	}

	// Try parsing as Unix timestamp
	var ts int64
	if _, err := fmt.Sscanf(s, "%d", &ts); err == nil {
		return ts
	}

	return 0
}

const graphqlPlaygroundHTML = `<!DOCTYPE html>
<html>
<head>
  <title>Chronicle GraphQL Playground</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/css/index.css" />
  <script src="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/js/middleware.js"></script>
</head>
<body>
  <div id="root"></div>
  <script>
    window.addEventListener('load', function() {
      GraphQLPlayground.init(document.getElementById('root'), {
        endpoint: '/graphql',
        settings: {
          'editor.theme': 'dark'
        }
      });
    });
  </script>
</body>
</html>`

// Types for the GraphQL schema

// TagInput represents a tag input.
type TagInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SeriesOutput represents a series in GraphQL output.
type SeriesOutput struct {
	Metric string           `json:"metric"`
	Tags   []TagOutput      `json:"tags"`
}

// TagOutput represents a tag in GraphQL output.
type TagOutput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// PointOutput represents a point in GraphQL output.
type PointOutput struct {
	Timestamp int64       `json:"timestamp"`
	Value     float64     `json:"value"`
	Tags      []TagOutput `json:"tags"`
}

// StatsOutput represents stats in GraphQL output.
type StatsOutput struct {
	Metrics int    `json:"metrics"`
	Uptime  string `json:"uptime"`
	Version string `json:"version"`
}

// ConfigOutput represents config in GraphQL output.
type ConfigOutput struct {
	PartitionDuration string `json:"partitionDuration"`
	BufferSize        int    `json:"bufferSize"`
	Retention         string `json:"retention"`
}

// Helper to convert tags map to sorted slice
func tagsToOutput(tags map[string]string) []TagOutput {
	if len(tags) == 0 {
		return nil
	}
	
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	out := make([]TagOutput, len(keys))
	for i, k := range keys {
		out[i] = TagOutput{Key: k, Value: tags[k]}
	}
	return out
}
