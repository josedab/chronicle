package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Saved query management, history tracking, and query templates.

// QueryValidationError represents a query validation error.
type QueryValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// SaveQuery saves a query.
func (qb *QueryBuilder) SaveQuery(sq *SavedQuery) error {
	if sq.Name == "" {
		return errors.New("query name required")
	}

	qb.mu.Lock()
	defer qb.mu.Unlock()

	if len(qb.savedQueries) >= qb.config.MaxSavedQueries {
		return errors.New("maximum saved queries reached")
	}

	if sq.ID == "" {
		sq.ID = fmt.Sprintf("sq-%d", time.Now().UnixNano())
	}

	sq.Created = time.Now()
	sq.Updated = time.Now()
	qb.savedQueries[sq.ID] = sq

	return nil
}

// GetSavedQuery retrieves a saved query.
func (qb *QueryBuilder) GetSavedQuery(id string) (*SavedQuery, bool) {
	qb.mu.RLock()
	defer qb.mu.RUnlock()

	sq, ok := qb.savedQueries[id]
	return sq, ok
}

// ListSavedQueries returns saved queries.
func (qb *QueryBuilder) ListSavedQueries(user string, includePublic bool) []*SavedQuery {
	qb.mu.RLock()
	defer qb.mu.RUnlock()

	var queries []*SavedQuery
	for _, sq := range qb.savedQueries {
		if sq.Owner == user || (includePublic && sq.IsPublic) {
			queries = append(queries, sq)
		}
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Updated.After(queries[j].Updated)
	})

	return queries
}

// DeleteSavedQuery removes a saved query.
func (qb *QueryBuilder) DeleteSavedQuery(id string) bool {
	qb.mu.Lock()
	defer qb.mu.Unlock()

	if _, ok := qb.savedQueries[id]; ok {
		delete(qb.savedQueries, id)
		return true
	}
	return false
}

// RecordHistory records query execution.
func (qb *QueryBuilder) RecordHistory(entry *QueryHistoryEntry) {
	qb.mu.Lock()
	defer qb.mu.Unlock()

	if entry.ID == "" {
		entry.ID = fmt.Sprintf("qh-%d", time.Now().UnixNano())
	}

	qb.history = append(qb.history, entry)

	// Trim history
	if len(qb.history) > qb.config.QueryHistorySize {
		qb.history = qb.history[len(qb.history)-qb.config.QueryHistorySize:]
	}
}

// GetHistory returns query history.
func (qb *QueryBuilder) GetHistory(user string, limit int) []*QueryHistoryEntry {
	qb.mu.RLock()
	defer qb.mu.RUnlock()

	var entries []*QueryHistoryEntry
	for _, e := range qb.history {
		if user == "" || e.User == user {
			entries = append(entries, e)
		}
	}

	// Sort by time descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ExecutedAt.After(entries[j].ExecutedAt)
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries
}

// GetTemplates returns available templates.
func (qb *QueryBuilder) GetTemplates() []*QueryTemplate {
	qb.mu.RLock()
	defer qb.mu.RUnlock()

	templates := make([]*QueryTemplate, 0, len(qb.templates))
	for _, t := range qb.templates {
		templates = append(templates, t)
	}

	sort.Slice(templates, func(i, j int) bool {
		return templates[i].Name < templates[j].Name
	})

	return templates
}

// GetTemplate returns a template by ID.
func (qb *QueryBuilder) GetTemplate(id string) (*QueryTemplate, bool) {
	qb.mu.RLock()
	defer qb.mu.RUnlock()

	t, ok := qb.templates[id]
	return t, ok
}

// ApplyTemplate applies a template with parameters.
func (qb *QueryBuilder) ApplyTemplate(templateID string, params map[string]any) (*VisualQuery, error) {
	template, ok := qb.GetTemplate(templateID)
	if !ok {
		return nil, fmt.Errorf("template not found: %s", templateID)
	}

	// Validate required parameters
	for _, p := range template.Parameters {
		if p.Required {
			if _, ok := params[p.Name]; !ok {
				return nil, fmt.Errorf("missing required parameter: %s", p.Name)
			}
		}
	}

	// Clone and apply parameters
	queryJSON, _ := json.Marshal(template.Query)
	queryStr := string(queryJSON)

	// Replace placeholders
	for name, value := range params {
		placeholder := fmt.Sprintf("{{%s}}", name)
		queryStr = strings.ReplaceAll(queryStr, placeholder, fmt.Sprintf("%v", value))
	}

	// Apply defaults
	for _, p := range template.Parameters {
		if _, ok := params[p.Name]; !ok && p.DefaultValue != nil {
			placeholder := fmt.Sprintf("{{%s}}", p.Name)
			queryStr = strings.ReplaceAll(queryStr, placeholder, fmt.Sprintf("%v", p.DefaultValue))
		}
	}

	var result VisualQuery
	if err := json.Unmarshal([]byte(queryStr), &result); err != nil {
		return nil, fmt.Errorf("template application failed: %w", err)
	}

	return &result, nil
}

// GetAutocomplete returns autocomplete suggestions.
func (qb *QueryBuilder) GetAutocomplete(context AutocompleteContext) []AutocompleteSuggestion {
	var suggestions []AutocompleteSuggestion

	switch context.Type {
	case "metric":
		suggestions = qb.getMetricSuggestions(context.Prefix)
	case "field":
		suggestions = qb.getFieldSuggestions(context.Source, context.Prefix)
	case "operator":
		suggestions = qb.getOperatorSuggestions()
	case "aggregation":
		suggestions = qb.getAggregationSuggestions()
	case "function":
		suggestions = qb.getFunctionSuggestions(context.Prefix)
	}

	if len(suggestions) > qb.config.AutocompleteLimit {
		suggestions = suggestions[:qb.config.AutocompleteLimit]
	}

	return suggestions
}

// AutocompleteContext provides context for autocomplete.
type AutocompleteContext struct {
	Type   string `json:"type"`   // "metric", "field", "operator", "aggregation", "function"
	Prefix string `json:"prefix"` // Text typed so far
	Source string `json:"source"` // Source metric for field suggestions
}

// AutocompleteSuggestion represents an autocomplete suggestion.
type AutocompleteSuggestion struct {
	Value       string `json:"value"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type"`
}

func (qb *QueryBuilder) getMetricSuggestions(prefix string) []AutocompleteSuggestion {
	// In production, would query actual metrics from DB
	metrics := []string{
		"cpu_usage", "memory_usage", "disk_io", "network_bytes",
		"http_requests", "http_latency", "error_count", "queue_depth",
	}

	var suggestions []AutocompleteSuggestion
	for _, m := range metrics {
		if prefix == "" || strings.HasPrefix(m, prefix) {
			suggestions = append(suggestions, AutocompleteSuggestion{
				Value: m,
				Label: m,
				Type:  "metric",
			})
		}
	}
	return suggestions
}

func (qb *QueryBuilder) getFieldSuggestions(source, prefix string) []AutocompleteSuggestion {
	// Common fields
	fields := []string{
		"timestamp", "value", "host", "region", "environment",
		"service", "instance", "status", "method", "path",
	}

	var suggestions []AutocompleteSuggestion
	for _, f := range fields {
		if prefix == "" || strings.HasPrefix(f, prefix) {
			suggestions = append(suggestions, AutocompleteSuggestion{
				Value: f,
				Label: f,
				Type:  "field",
			})
		}
	}
	return suggestions
}

func (qb *QueryBuilder) getOperatorSuggestions() []AutocompleteSuggestion {
	operators := []struct {
		value string
		desc  string
	}{
		{"=", "Equals"},
		{"!=", "Not equals"},
		{">", "Greater than"},
		{">=", "Greater than or equal"},
		{"<", "Less than"},
		{"<=", "Less than or equal"},
		{"IN", "In list"},
		{"NOT IN", "Not in list"},
		{"LIKE", "Pattern match"},
		{"REGEX", "Regular expression"},
		{"IS NULL", "Is null"},
		{"IS NOT NULL", "Is not null"},
	}

	suggestions := make([]AutocompleteSuggestion, len(operators))
	for i, op := range operators {
		suggestions[i] = AutocompleteSuggestion{
			Value:       op.value,
			Label:       op.value,
			Description: op.desc,
			Type:        "operator",
		}
	}
	return suggestions
}

func (qb *QueryBuilder) getAggregationSuggestions() []AutocompleteSuggestion {
	aggregations := []struct {
		value string
		desc  string
	}{
		{"SUM", "Sum of values"},
		{"AVG", "Average of values"},
		{"COUNT", "Count of rows"},
		{"MIN", "Minimum value"},
		{"MAX", "Maximum value"},
		{"P50", "50th percentile"},
		{"P90", "90th percentile"},
		{"P99", "99th percentile"},
		{"STDDEV", "Standard deviation"},
		{"RATE", "Rate of change"},
	}

	suggestions := make([]AutocompleteSuggestion, len(aggregations))
	for i, agg := range aggregations {
		suggestions[i] = AutocompleteSuggestion{
			Value:       agg.value,
			Label:       agg.value,
			Description: agg.desc,
			Type:        "aggregation",
		}
	}
	return suggestions
}

func (qb *QueryBuilder) getFunctionSuggestions(prefix string) []AutocompleteSuggestion {
	functions := []struct {
		value string
		desc  string
	}{
		{"time_bucket", "Group by time intervals"},
		{"derivative", "Rate of change"},
		{"histogram", "Distribution histogram"},
		{"moving_avg", "Moving average"},
		{"lag", "Previous value"},
		{"lead", "Next value"},
		{"cumsum", "Cumulative sum"},
		{"fill", "Fill missing values"},
	}

	var suggestions []AutocompleteSuggestion
	for _, f := range functions {
		if prefix == "" || strings.HasPrefix(f.value, prefix) {
			suggestions = append(suggestions, AutocompleteSuggestion{
				Value:       f.value,
				Label:       f.value,
				Description: f.desc,
				Type:        "function",
			})
		}
	}
	return suggestions
}

// HTTPHandler returns an HTTP handler for the query builder API.
func (qb *QueryBuilder) HTTPHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/query-builder/build", qb.handleBuild)
	mux.HandleFunc("/api/v1/query-builder/validate", qb.handleValidate)
	mux.HandleFunc("/api/query-builder/saved", qb.handleSavedQueries)
	mux.HandleFunc("/api/query-builder/templates", qb.handleTemplates)
	mux.HandleFunc("/api/v1/query-builder/autocomplete", qb.handleAutocomplete)
	mux.HandleFunc("/api/query-builder/history", qb.handleHistory)

	return mux
}

func (qb *QueryBuilder) handleBuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var vq VisualQuery
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&vq); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	sql, err := qb.BuildSQL(&vq)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"sql": sql})
}

func (qb *QueryBuilder) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var vq VisualQuery
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&vq); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	errs := qb.ValidateQuery(&vq)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"valid":  len(errs) == 0,
		"errors": errs,
	})
}

func (qb *QueryBuilder) handleSavedQueries(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		queries := qb.ListSavedQueries("", true)
		json.NewEncoder(w).Encode(queries)

	case http.MethodPost:
		var sq SavedQuery
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&sq); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := qb.SaveQuery(&sq); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(sq)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (qb *QueryBuilder) handleTemplates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	templates := qb.GetTemplates()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(templates)
}

func (qb *QueryBuilder) handleAutocomplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ctx AutocompleteContext
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&ctx); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	suggestions := qb.GetAutocomplete(ctx)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(suggestions)
}

func (qb *QueryBuilder) handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	history := qb.GetHistory("", 50)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
}
