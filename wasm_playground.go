package chronicle

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"sync"
	"time"
)

// PlaygroundConfig configures the embeddable WASM query playground.
type PlaygroundConfig struct {
	Enabled         bool   `json:"enabled"`
	Title           string `json:"title"`
	Theme           string `json:"theme"`
	MaxQueryLength  int    `json:"max_query_length"`
	DefaultQuery    string `json:"default_query"`
	EnableCharts    bool   `json:"enable_charts"`
	EnableExport    bool   `json:"enable_export"`
	WASMPath        string `json:"wasm_path"`
	CDNBaseURL      string `json:"cdn_base_url,omitempty"`
	AllowedOrigins  string `json:"allowed_origins,omitempty"`
	AutocompleteOn  bool   `json:"autocomplete_on"`
	MaxResultRows   int    `json:"max_result_rows"`
	SessionTimeout  time.Duration `json:"session_timeout"`
}

// DefaultPlaygroundConfig returns sensible defaults for the playground.
func DefaultPlaygroundConfig() PlaygroundConfig {
	return PlaygroundConfig{
		Enabled:        false,
		Title:          "Chronicle Query Playground",
		Theme:          "dark",
		MaxQueryLength: 10000,
		DefaultQuery:   "SELECT * FROM temperature WHERE time > now() - 1h LIMIT 100",
		EnableCharts:   true,
		EnableExport:   true,
		WASMPath:       "/wasm/chronicle.wasm",
		AutocompleteOn: true,
		MaxResultRows:  10000,
		SessionTimeout: 30 * time.Minute,
	}
}

// PlaygroundSession tracks an active playground session.
type PlaygroundSession struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	LastQuery string    `json:"last_query"`
	QueryCount int      `json:"query_count"`
}

// PlaygroundQueryRequest represents a query from the playground.
type PlaygroundQueryRequest struct {
	Query    string `json:"query"`
	Format   string `json:"format"`
	Language string `json:"language"`
}

// PlaygroundQueryResponse holds the result of a playground query.
type PlaygroundQueryResponse struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	RowCount  int             `json:"row_count"`
	Duration  string          `json:"duration"`
	QueryPlan string          `json:"query_plan,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// ChartSuggestion describes a recommended visualization for query results.
type ChartSuggestion struct {
	Type   string `json:"type"`
	XField string `json:"x_field"`
	YField string `json:"y_field"`
	Title  string `json:"title"`
}

// Playground provides an embeddable WASM-based query interface.
type Playground struct {
	config   PlaygroundConfig
	db       *DB
	sessions map[string]*PlaygroundSession
	mu       sync.RWMutex
}

// NewPlayground creates a new playground instance.
func NewPlayground(db *DB, config PlaygroundConfig) *Playground {
	return &Playground{
		config:   config,
		db:       db,
		sessions: make(map[string]*PlaygroundSession),
	}
}

// ExecuteQuery runs a query from the playground and returns formatted results.
func (p *Playground) ExecuteQuery(req PlaygroundQueryRequest) PlaygroundQueryResponse {
	if len(req.Query) > p.config.MaxQueryLength {
		return PlaygroundQueryResponse{Error: "query exceeds maximum length"}
	}
	if strings.TrimSpace(req.Query) == "" {
		return PlaygroundQueryResponse{Error: "empty query"}
	}

	start := time.Now()

	// Try CQL first, then fall back to SQL-like query
	lang := req.Language
	if lang == "" {
		lang = "sql"
	}

	var result *Result
	var err error

	switch lang {
	case "cql":
		if p.db.CQLEngine() != nil {
			var cqlResult interface{}
			cqlResult, err = p.db.CQLEngine().Execute(nil, req.Query)
			if err == nil {
				return p.formatCQLResult(cqlResult, time.Since(start))
			}
		}
		return PlaygroundQueryResponse{Error: "CQL engine not available"}
	default:
		q, parseErr := parsePlaygroundQuery(req.Query)
		if parseErr != nil {
			return PlaygroundQueryResponse{Error: fmt.Sprintf("parse error: %v", parseErr)}
		}
		result, err = p.db.Execute(q)
	}

	if err != nil {
		return PlaygroundQueryResponse{Error: err.Error()}
	}

	return p.formatResult(result, time.Since(start))
}

// SuggestChart recommends a visualization type based on query results.
func (p *Playground) SuggestChart(resp PlaygroundQueryResponse) *ChartSuggestion {
	if resp.Error != "" || len(resp.Columns) == 0 || len(resp.Rows) == 0 {
		return nil
	}

	hasTime := false
	for _, col := range resp.Columns {
		lower := strings.ToLower(col)
		if lower == "time" || lower == "timestamp" || lower == "ts" {
			hasTime = true
			break
		}
	}

	if hasTime && len(resp.Columns) >= 2 {
		yField := "value"
		for _, col := range resp.Columns {
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

	if len(resp.Columns) >= 2 && len(resp.Rows) <= 20 {
		return &ChartSuggestion{
			Type:   "bar",
			XField: resp.Columns[0],
			YField: resp.Columns[len(resp.Columns)-1],
			Title:  "Query Results",
		}
	}

	return nil
}

// ListSessions returns active playground sessions.
func (p *Playground) ListSessions() []*PlaygroundSession {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sessions := make([]*PlaygroundSession, 0, len(p.sessions))
	for _, s := range p.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}

// RegisterHTTPHandlers registers playground HTTP endpoints.
func (p *Playground) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/playground", p.handlePlaygroundPage)
	mux.HandleFunc("/playground/query", p.handlePlaygroundQuery)
	mux.HandleFunc("/playground/config", p.handlePlaygroundConfig)
	mux.HandleFunc("/playground/embed.js", p.handleEmbedScript)
}

func (p *Playground) handlePlaygroundPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl := template.Must(template.New("playground").Parse(playgroundHTML))
	tmpl.Execute(w, map[string]interface{}{
		"Title":        p.config.Title,
		"Theme":        p.config.Theme,
		"DefaultQuery": p.config.DefaultQuery,
		"EnableCharts": p.config.EnableCharts,
		"WASMPath":     p.config.WASMPath,
	})
}

func (p *Playground) handlePlaygroundQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PlaygroundQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	resp := p.ExecuteQuery(req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (p *Playground) handlePlaygroundConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"title":          p.config.Title,
		"theme":          p.config.Theme,
		"enable_charts":  p.config.EnableCharts,
		"enable_export":  p.config.EnableExport,
		"autocomplete":   p.config.AutocompleteOn,
		"max_result_rows": p.config.MaxResultRows,
		"default_query":  p.config.DefaultQuery,
	})
}

func (p *Playground) handleEmbedScript(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript")
	fmt.Fprintf(w, embedJS, p.config.Title)
}

func (p *Playground) formatResult(result *Result, duration time.Duration) PlaygroundQueryResponse {
	if result == nil {
		return PlaygroundQueryResponse{Duration: duration.String()}
	}

	columns := []string{"time", "metric", "value"}
	rows := make([][]interface{}, 0)

	for i, pt := range result.Points {
		if i >= p.config.MaxResultRows {
			break
		}
		rows = append(rows, []interface{}{
			time.Unix(0, pt.Timestamp).UTC().Format(time.RFC3339Nano),
			pt.Metric,
			pt.Value,
		})
	}

	return PlaygroundQueryResponse{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
		Duration: duration.String(),
	}
}

func (p *Playground) formatCQLResult(result interface{}, duration time.Duration) PlaygroundQueryResponse {
	// CQL results are generic; serialize to JSON and re-parse
	data, err := json.Marshal(result)
	if err != nil {
		return PlaygroundQueryResponse{Error: err.Error(), Duration: duration.String()}
	}

	var generic interface{}
	json.Unmarshal(data, &generic)

	return PlaygroundQueryResponse{
		Columns:  []string{"result"},
		Rows:     [][]interface{}{{generic}},
		RowCount: 1,
		Duration: duration.String(),
	}
}

const playgroundHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{.Title}}</title>
<style>
:root { --bg: {{if eq .Theme "dark"}}#1e1e1e{{else}}#ffffff{{end}}; --fg: {{if eq .Theme "dark"}}#d4d4d4{{else}}#333333{{end}}; }
body { font-family: 'Consolas', monospace; background: var(--bg); color: var(--fg); margin: 0; padding: 20px; }
.editor { width: 100%; min-height: 120px; background: {{if eq .Theme "dark"}}#2d2d2d{{else}}#f5f5f5{{end}}; color: var(--fg); border: 1px solid #555; padding: 10px; font-family: inherit; font-size: 14px; resize: vertical; }
.btn { background: #0078d4; color: white; border: none; padding: 8px 16px; cursor: pointer; margin: 5px 0; }
.btn:hover { background: #005a9e; }
.results { margin-top: 10px; overflow-x: auto; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #555; padding: 6px 10px; text-align: left; }
th { background: {{if eq .Theme "dark"}}#333{{else}}#eee{{end}}; }
h1 { font-size: 1.4em; }
</style>
</head>
<body>
<h1>{{.Title}}</h1>
<textarea class="editor" id="query">{{.DefaultQuery}}</textarea><br>
<button class="btn" onclick="runQuery()">Run Query (Ctrl+Enter)</button>
<span id="status"></span>
<div class="results" id="results"></div>
<script>
async function runQuery() {
  const q = document.getElementById('query').value;
  document.getElementById('status').textContent = 'Running...';
  try {
    const resp = await fetch('/playground/query', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({query: q}) });
    const data = await resp.json();
    if (data.error) { document.getElementById('results').innerHTML = '<pre style="color:red">' + data.error + '</pre>'; }
    else { renderTable(data); }
    document.getElementById('status').textContent = data.duration || '';
  } catch(e) { document.getElementById('results').innerHTML = '<pre style="color:red">' + e + '</pre>'; }
}
function renderTable(data) {
  let html = '<table><thead><tr>' + data.columns.map(c => '<th>'+c+'</th>').join('') + '</tr></thead><tbody>';
  (data.rows||[]).forEach(r => { html += '<tr>' + r.map(v => '<td>'+v+'</td>').join('') + '</tr>'; });
  html += '</tbody></table><p>' + data.row_count + ' rows</p>';
  document.getElementById('results').innerHTML = html;
}
document.getElementById('query').addEventListener('keydown', e => { if (e.ctrlKey && e.key === 'Enter') runQuery(); });
</script>
</body>
</html>`

const embedJS = `(function(){
  var frame = document.createElement('iframe');
  frame.src = window.ChroniclePlaygroundURL || '/playground';
  frame.style.cssText = 'width:100%%;height:500px;border:1px solid #ccc;border-radius:4px;';
  frame.title = '%s';
  var target = document.getElementById('chronicle-playground') || document.currentScript.parentElement;
  target.appendChild(frame);
})();`

// parsePlaygroundQuery converts a simple SQL-like string into a Query struct.
func parsePlaygroundQuery(queryStr string) (*Query, error) {
	q := &Query{}
	// Use the existing SQL parser if available; otherwise build a minimal metric query
	lower := strings.ToLower(strings.TrimSpace(queryStr))
	if strings.HasPrefix(lower, "select") {
		// Try to extract metric from "FROM <metric>"
		parts := strings.Fields(queryStr)
		for i, p := range parts {
			if strings.EqualFold(p, "from") && i+1 < len(parts) {
				q.Metric = parts[i+1]
				break
			}
		}
		// Extract LIMIT
		for i, p := range parts {
			if strings.EqualFold(p, "limit") && i+1 < len(parts) {
				fmt.Sscanf(parts[i+1], "%d", &q.Limit)
				break
			}
		}
		if q.Metric == "" {
			return nil, fmt.Errorf("could not parse metric from query")
		}
		return q, nil
	}
	// Treat as metric name directly
	q.Metric = strings.TrimSpace(queryStr)
	return q, nil
}
