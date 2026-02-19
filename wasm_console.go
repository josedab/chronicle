package chronicle

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// WASMConsoleConfig configures the interactive browser query console.
type WASMConsoleConfig struct {
	Title                  string   `json:"title"`
	MaxQueryHistory        int      `json:"max_query_history"`
	EnableAutoComplete     bool     `json:"enable_auto_complete"`
	EnableChartSuggestions bool     `json:"enable_chart_suggestions"`
	Theme                  string   `json:"theme"`
	MaxResultRows          int      `json:"max_result_rows"`
	SupportedLanguages     []string `json:"supported_languages"`
}

// DefaultWASMConsoleConfig returns sensible defaults for the WASM query console.
func DefaultWASMConsoleConfig() WASMConsoleConfig {
	return WASMConsoleConfig{
		Title:                  "Chronicle Query Console",
		MaxQueryHistory:        100,
		EnableAutoComplete:     true,
		EnableChartSuggestions: true,
		Theme:                  "dark",
		MaxResultRows:          1000,
		SupportedLanguages:     []string{"sql", "promql", "cql"},
	}
}

// WASMQueryConsole provides an interactive browser-based query console experience.
type WASMQueryConsole struct {
	config       WASMConsoleConfig
	db           *DB
	history      []WASMConsoleQuery
	savedQueries map[string]WASMConsoleSavedQuery
	mu           sync.RWMutex
}

// WASMConsoleQuery represents a single executed query with its result.
type WASMConsoleQuery struct {
	ID        string             `json:"id"`
	Query     string             `json:"query"`
	Language  string             `json:"language"`
	Result    *WASMConsoleResult `json:"result,omitempty"`
	Duration  time.Duration      `json:"duration"`
	Timestamp time.Time          `json:"timestamp"`
}

// WASMConsoleResult holds the output of a console query execution.
type WASMConsoleResult struct {
	Columns   []string         `json:"columns"`
	Rows      [][]any          `json:"rows"`
	RowCount  int              `json:"row_count"`
	Truncated bool             `json:"truncated"`
	Chart     *ChartSuggestion `json:"chart,omitempty"`
	QueryPlan string           `json:"query_plan,omitempty"`
	Error     string           `json:"error,omitempty"`
}

// WASMConsoleSavedQuery represents a named query stored for reuse.
type WASMConsoleSavedQuery struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Query       string    `json:"query"`
	Language    string    `json:"language"`
	CreatedAt   time.Time `json:"created_at"`
}

// WASMConsoleCompletion represents a single autocomplete suggestion.
type WASMConsoleCompletion struct {
	Text        string `json:"text"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// NewWASMQueryConsole creates a new interactive WASM query console.
func NewWASMQueryConsole(db *DB, config WASMConsoleConfig) *WASMQueryConsole {
	if config.MaxQueryHistory <= 0 {
		config.MaxQueryHistory = 100
	}
	if config.MaxResultRows <= 0 {
		config.MaxResultRows = 1000
	}
	if config.Title == "" {
		config.Title = "Chronicle Query Console"
	}
	if config.Theme == "" {
		config.Theme = "dark"
	}
	if len(config.SupportedLanguages) == 0 {
		config.SupportedLanguages = []string{"sql", "promql", "cql"}
	}
	return &WASMQueryConsole{
		config:       config,
		db:           db,
		history:      make([]WASMConsoleQuery, 0),
		savedQueries: make(map[string]WASMConsoleSavedQuery),
	}
}

// Execute runs a query in the specified language and returns the result.
func (qc *WASMQueryConsole) Execute(query, language string) (*WASMConsoleResult, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("empty query")
	}

	if language == "" {
		language = DetectQueryLanguage(query)
	}

	start := time.Now()
	result := qc.executeInternal(query, language)
	duration := time.Since(start)

	if qc.config.EnableChartSuggestions && result.Error == "" {
		result.Chart = qc.suggestChart(result)
	}

	entry := WASMConsoleQuery{
		ID:        fmt.Sprintf("q-%d", time.Now().UnixNano()),
		Query:     query,
		Language:  language,
		Result:    result,
		Duration:  duration,
		Timestamp: time.Now(),
	}

	qc.mu.Lock()
	qc.history = append(qc.history, entry)
	if len(qc.history) > qc.config.MaxQueryHistory {
		qc.history = qc.history[len(qc.history)-qc.config.MaxQueryHistory:]
	}
	qc.mu.Unlock()

	if result.Error != "" {
		return result, fmt.Errorf("%s", result.Error)
	}
	return result, nil
}

func (qc *WASMQueryConsole) executeInternal(query, language string) *WASMConsoleResult {
	switch language {
	case "cql":
		return qc.executeCQL(query)
	case "promql":
		return qc.executePromQL(query)
	default:
		return qc.executeSQL(query)
	}
}

func (qc *WASMQueryConsole) executeSQL(query string) *WASMConsoleResult {
	q, err := parsePlaygroundQuery(query)
	if err != nil {
		return &WASMConsoleResult{Error: fmt.Sprintf("parse error: %v", err)}
	}

	result, err := qc.db.Execute(q)
	if err != nil {
		return &WASMConsoleResult{Error: err.Error()}
	}

	return qc.formatResult(result)
}

func (qc *WASMQueryConsole) executeCQL(query string) *WASMConsoleResult {
	engine := qc.db.CQLEngine()
	if engine == nil {
		return &WASMConsoleResult{Error: "CQL engine not available"}
	}

	cqlResult, err := engine.Execute(context.Background(), query)
	if err != nil {
		return &WASMConsoleResult{Error: err.Error()}
	}

	data, err := json.Marshal(cqlResult)
	if err != nil {
		return &WASMConsoleResult{Error: err.Error()}
	}

	var generic any
	json.Unmarshal(data, &generic)

	return &WASMConsoleResult{
		Columns:  []string{"result"},
		Rows:     [][]any{{generic}},
		RowCount: 1,
	}
}

func (qc *WASMQueryConsole) executePromQL(query string) *WASMConsoleResult {
	q, err := parsePlaygroundQuery(query)
	if err != nil {
		return &WASMConsoleResult{Error: fmt.Sprintf("parse error: %v", err)}
	}

	result, err := qc.db.Execute(q)
	if err != nil {
		return &WASMConsoleResult{Error: err.Error()}
	}

	return qc.formatResult(result)
}

func (qc *WASMQueryConsole) formatResult(result *Result) *WASMConsoleResult {
	if result == nil {
		return &WASMConsoleResult{Columns: []string{}, Rows: [][]any{}}
	}

	columns := []string{"time", "metric", "value"}
	rows := make([][]any, 0, len(result.Points))
	truncated := false

	for i, pt := range result.Points {
		if i >= qc.config.MaxResultRows {
			truncated = true
			break
		}
		rows = append(rows, []any{
			time.Unix(0, pt.Timestamp).UTC().Format(time.RFC3339Nano),
			pt.Metric,
			pt.Value,
		})
	}

	return &WASMConsoleResult{
		Columns:   columns,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
	}
}

func (qc *WASMQueryConsole) suggestChart(result *WASMConsoleResult) *ChartSuggestion {
	if len(result.Columns) == 0 || len(result.Rows) == 0 {
		return nil
	}

	hasTime := false
	for _, col := range result.Columns {
		lower := strings.ToLower(col)
		if lower == "time" || lower == "timestamp" || lower == "ts" {
			hasTime = true
			break
		}
	}

	if hasTime && len(result.Columns) >= 2 {
		yField := "value"
		for _, col := range result.Columns {
			lower := strings.ToLower(col)
			if lower != "time" && lower != "timestamp" && lower != "ts" {
				yField = col
				break
			}
		}
		return &ChartSuggestion{
			Type:   "line",
			XField: "time",
			YField: yField,
			Title:  fmt.Sprintf("%s over time", yField),
		}
	}

	if len(result.Columns) >= 2 && len(result.Rows) <= 20 {
		return &ChartSuggestion{
			Type:   "bar",
			XField: result.Columns[0],
			YField: result.Columns[len(result.Columns)-1],
			Title:  "Query Results",
		}
	}

	return nil
}

// AutoComplete returns completion suggestions for the given prefix and language.
func (qc *WASMQueryConsole) AutoComplete(prefix, language string) []WASMConsoleCompletion {
	if !qc.config.EnableAutoComplete {
		return nil
	}

	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil
	}

	lower := strings.ToLower(prefix)
	var completions []WASMConsoleCompletion

	keywords := wasmConsoleKeywords(language)
	for _, kw := range keywords {
		if strings.HasPrefix(strings.ToLower(kw), lower) {
			completions = append(completions, WASMConsoleCompletion{
				Text:        kw,
				Type:        "keyword",
				Description: fmt.Sprintf("%s keyword", language),
			})
		}
	}

	if qc.db != nil {
		for _, m := range qc.db.Metrics() {
			if strings.HasPrefix(strings.ToLower(m), lower) {
				completions = append(completions, WASMConsoleCompletion{
					Text:        m,
					Type:        "metric",
					Description: "metric name",
				})
			}
		}

		for _, tk := range qc.db.TagKeys() {
			if strings.HasPrefix(strings.ToLower(tk), lower) {
				completions = append(completions, WASMConsoleCompletion{
					Text:        tk,
					Type:        "tag",
					Description: "tag key",
				})
			}
		}
	}

	functions := wasmConsoleFunctions(language)
	for _, fn := range functions {
		if strings.HasPrefix(strings.ToLower(fn), lower) {
			completions = append(completions, WASMConsoleCompletion{
				Text:        fn,
				Type:        "function",
				Description: fmt.Sprintf("%s function", language),
			})
		}
	}

	return completions
}

func wasmConsoleKeywords(language string) []string {
	switch language {
	case "promql":
		return []string{
			"by", "without", "on", "ignoring", "group_left", "group_right",
			"offset", "bool", "and", "or", "unless",
		}
	case "cql":
		return []string{
			"SELECT", "FROM", "WHERE", "GROUP BY", "WINDOW", "GAP_FILL",
			"ALIGN", "ORDER BY", "LIMIT", "OFFSET", "BETWEEN", "AND", "OR",
			"ASOF JOIN", "INTERPOLATE", "LAST", "NOW",
		}
	default:
		return []string{
			"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT",
			"OFFSET", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE",
			"AS", "JOIN", "ON", "HAVING", "DISTINCT", "COUNT", "SUM",
		}
	}
}

func wasmConsoleFunctions(language string) []string {
	switch language {
	case "promql":
		return []string{
			"rate", "irate", "increase", "sum", "avg", "min", "max",
			"count", "stddev", "stdvar", "topk", "bottomk",
			"histogram_quantile", "label_replace", "label_join",
			"abs", "ceil", "floor", "round", "clamp", "clamp_min", "clamp_max",
			"delta", "deriv", "predict_linear", "time", "timestamp",
		}
	case "cql":
		return []string{
			"rate", "delta", "increase", "sum", "avg", "min", "max",
			"count", "stddev", "first", "last", "mean", "percentile",
			"gap_fill", "interpolate", "now",
		}
	default:
		return []string{
			"count", "sum", "avg", "min", "max", "mean", "stddev",
			"first", "last", "now", "abs", "ceil", "floor", "round",
		}
	}
}

// History returns the query history in chronological order.
func (qc *WASMQueryConsole) History() []WASMConsoleQuery {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	out := make([]WASMConsoleQuery, len(qc.history))
	copy(out, qc.history)
	return out
}

// ClearHistory removes all entries from the query history.
func (qc *WASMQueryConsole) ClearHistory() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.history = qc.history[:0]
}

// SaveQuery stores a named query for later reuse.
func (qc *WASMQueryConsole) SaveQuery(name, description, query, language string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("query name is required")
	}
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query is required")
	}
	if language == "" {
		language = DetectQueryLanguage(query)
	}

	id := fmt.Sprintf("sq-%d", time.Now().UnixNano())

	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.savedQueries[id] = WASMConsoleSavedQuery{
		ID:          id,
		Name:        name,
		Description: description,
		Query:       query,
		Language:    language,
		CreatedAt:   time.Now(),
	}
	return nil
}

// ListSavedQueries returns all saved queries.
func (qc *WASMQueryConsole) ListSavedQueries() []WASMConsoleSavedQuery {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	saved := make([]WASMConsoleSavedQuery, 0, len(qc.savedQueries))
	for _, sq := range qc.savedQueries {
		saved = append(saved, sq)
	}
	return saved
}

// DetectQueryLanguage uses heuristics to determine the query language.
func DetectQueryLanguage(query string) string {
	lower := strings.ToLower(strings.TrimSpace(query))

	// PromQL indicators
	promqlFuncs := []string{"rate(", "irate(", "increase(", "histogram_quantile(", "label_replace(", "label_join(", "delta(", "deriv(", "predict_linear("}
	for _, fn := range promqlFuncs {
		if strings.Contains(lower, fn) {
			return "promql"
		}
	}
	if (strings.Contains(lower, "sum(") || strings.Contains(lower, "avg(") || strings.Contains(lower, "count(")) &&
		(strings.Contains(lower, ") by") || strings.Contains(lower, ") without")) {
		return "promql"
	}

	// CQL indicators
	cqlKeywords := []string{"window", "gap_fill", "interpolate", "asof join", "align"}
	for _, kw := range cqlKeywords {
		if strings.Contains(lower, kw) {
			return "cql"
		}
	}

	return "sql"
}

// ExportConsoleResults exports query results in the specified format (csv or json).
func ExportConsoleResults(result *WASMConsoleResult, format string) ([]byte, error) {
	if result == nil {
		return nil, fmt.Errorf("no result to export")
	}

	switch strings.ToLower(format) {
	case "csv":
		return exportWASMConsoleCSV(result)
	case "json":
		return exportWASMConsoleJSON(result)
	default:
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}
}

func exportWASMConsoleCSV(result *WASMConsoleResult) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if err := w.Write(result.Columns); err != nil {
		return nil, fmt.Errorf("csv write header: %w", err)
	}

	for _, row := range result.Rows {
		record := make([]string, len(row))
		for i, v := range row {
			record[i] = fmt.Sprintf("%v", v)
		}
		if err := w.Write(record); err != nil {
			return nil, fmt.Errorf("csv write row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("csv flush: %w", err)
	}
	return buf.Bytes(), nil
}

func exportWASMConsoleJSON(result *WASMConsoleResult) ([]byte, error) {
	rows := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		m := make(map[string]any, len(result.Columns))
		for i, col := range result.Columns {
			if i < len(row) {
				m[col] = row[i]
			}
		}
		rows = append(rows, m)
	}
	return json.Marshal(rows)
}

// RegisterWASMConsoleHTTPHandlers registers WASM console HTTP endpoints on the given mux.
func (qc *WASMQueryConsole) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/console/query", qc.handleConsoleQuery)
	mux.HandleFunc("/console/history", qc.handleConsoleHistory)
	mux.HandleFunc("/console/autocomplete", qc.handleConsoleAutoComplete)
	mux.HandleFunc("/console/save", qc.handleConsoleSave)
	mux.HandleFunc("/console/saved", qc.handleConsoleSaved)
	mux.HandleFunc("/console/export", qc.handleConsoleExport)
}

func (qc *WASMQueryConsole) handleConsoleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Query    string `json:"query"`
		Language string `json:"language"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	result, _ := qc.Execute(req.Query, req.Language)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (qc *WASMQueryConsole) handleConsoleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(qc.History())
}

func (qc *WASMQueryConsole) handleConsoleAutoComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	lang := r.URL.Query().Get("lang")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(qc.AutoComplete(prefix, lang))
}

func (qc *WASMQueryConsole) handleConsoleSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Query       string `json:"query"`
		Language    string `json:"language"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := qc.SaveQuery(req.Name, req.Description, req.Query, req.Language); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "saved"})
}

func (qc *WASMQueryConsole) handleConsoleSaved(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(qc.ListSavedQueries())
}

func (qc *WASMQueryConsole) handleConsoleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	qc.mu.RLock()
	var lastResult *WASMConsoleResult
	if len(qc.history) > 0 {
		lastResult = qc.history[len(qc.history)-1].Result
	}
	qc.mu.RUnlock()

	if lastResult == nil {
		http.Error(w, "no results to export", http.StatusNotFound)
		return
	}

	data, err := ExportConsoleResults(lastResult, format)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch strings.ToLower(format) {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=results.csv")
	default:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=results.json")
	}
	w.Write(data)
}
