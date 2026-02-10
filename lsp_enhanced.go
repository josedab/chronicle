package chronicle

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// LSPLanguage identifies a query language for LSP support.
type LSPLanguage int

const (
	LSPLanguageCQL LSPLanguage = iota
	LSPLanguagePromQL
	LSPLanguageSQL
)

// String returns the string representation of the LSP language.
func (l LSPLanguage) String() string {
	switch l {
	case LSPLanguageCQL:
		return "cql"
	case LSPLanguagePromQL:
		return "promql"
	case LSPLanguageSQL:
		return "sql"
	default:
		return "unknown"
	}
}

// LSPMetricInfo provides metadata about a metric for LSP features.
type LSPMetricInfo struct {
	Name        string              `json:"name"`
	Type        string              `json:"type"`
	Description string              `json:"description"`
	Tags        map[string][]string `json:"tags"`
	Cardinality int64               `json:"cardinality"`
	LastSeen    time.Time           `json:"last_seen"`
	SampleRate  float64             `json:"sample_rate"`
}

// LSPEnhancedConfig extends LSP configuration with multi-language support.
type LSPEnhancedConfig struct {
	EnableCQL           bool `json:"enable_cql"`
	EnablePromQL        bool `json:"enable_promql"`
	EnableSQL           bool `json:"enable_sql"`
	MetricCacheSize     int  `json:"metric_cache_size"`
	TagCacheSize        int  `json:"tag_cache_size"`
	EnableSignatureHelp bool `json:"enable_signature_help"`
	EnableCodeActions   bool `json:"enable_code_actions"`
}

// DefaultLSPEnhancedConfig returns defaults for enhanced LSP.
func DefaultLSPEnhancedConfig() LSPEnhancedConfig {
	return LSPEnhancedConfig{
		EnableCQL:           true,
		EnablePromQL:        true,
		EnableSQL:           true,
		MetricCacheSize:     10000,
		TagCacheSize:        50000,
		EnableSignatureHelp: true,
		EnableCodeActions:   true,
	}
}

// SignatureHelp provides function signature information.
type SignatureHelp struct {
	Signatures      []SignatureInfo `json:"signatures"`
	ActiveSignature int             `json:"activeSignature"`
	ActiveParameter int             `json:"activeParameter"`
}

// SignatureInfo describes a callable signature.
type SignatureInfo struct {
	Label         string          `json:"label"`
	Documentation string          `json:"documentation"`
	Parameters    []ParameterInfo `json:"parameters"`
}

// ParameterInfo describes a parameter of a callable.
type ParameterInfo struct {
	Label         string `json:"label"`
	Documentation string `json:"documentation"`
}

// CodeAction represents a code action suggestion.
type CodeAction struct {
	Title       string `json:"title"`
	Kind        string `json:"kind"`
	Description string `json:"description"`
	NewText     string `json:"new_text"`
}

// LSPMetricCatalog provides metric metadata for LSP features.
type LSPMetricCatalog struct {
	db      *DB
	metrics map[string]*LSPMetricInfo
	mu      sync.RWMutex
}

// NewLSPMetricCatalog creates a new metric catalog.
func NewLSPMetricCatalog(db *DB) *LSPMetricCatalog {
	return &LSPMetricCatalog{
		db:      db,
		metrics: make(map[string]*LSPMetricInfo),
	}
}

// Refresh refreshes the metric catalog from the database.
func (c *LSPMetricCatalog) Refresh() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, name := range c.db.Metrics() {
		if _, exists := c.metrics[name]; !exists {
			c.metrics[name] = &LSPMetricInfo{
				Name:     name,
				Type:     "gauge",
				LastSeen: time.Now(),
			}
		}
	}
}

// Lookup returns metadata for a metric.
func (c *LSPMetricCatalog) Lookup(name string) (*LSPMetricInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.metrics[name]
	return info, ok
}

// Search searches the catalog for metrics matching a prefix.
func (c *LSPMetricCatalog) Search(prefix string) []*LSPMetricInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var results []*LSPMetricInfo
	lowerPrefix := strings.ToLower(prefix)
	for _, info := range c.metrics {
		if strings.HasPrefix(strings.ToLower(info.Name), lowerPrefix) {
			results = append(results, info)
		}
	}
	return results
}

// List returns all known metrics.
func (c *LSPMetricCatalog) List() []*LSPMetricInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*LSPMetricInfo, 0, len(c.metrics))
	for _, info := range c.metrics {
		out = append(out, info)
	}
	return out
}

// LSPEnhancedServer extends the LSP server with multi-language support.
type LSPEnhancedServer struct {
	db      *DB
	config  LSPEnhancedConfig
	catalog *LSPMetricCatalog
	mu      sync.RWMutex
}

// NewLSPEnhancedServer creates an enhanced LSP server.
func NewLSPEnhancedServer(db *DB, cfg LSPEnhancedConfig) *LSPEnhancedServer {
	return &LSPEnhancedServer{
		db:      db,
		config:  cfg,
		catalog: NewLSPMetricCatalog(db),
	}
}

// DetectLanguage detects the query language from content.
func (s *LSPEnhancedServer) DetectLanguage(content string) LSPLanguage {
	lower := strings.ToLower(strings.TrimSpace(content))

	// PromQL indicators
	promqlIndicators := []string{
		"rate(", "sum(", "avg(", "histogram_quantile(",
		"label_replace(", "irate(", "increase(",
	}
	for _, indicator := range promqlIndicators {
		if strings.Contains(lower, indicator) {
			// Check if it looks more like PromQL than SQL
			if !strings.Contains(lower, "select") {
				return LSPLanguagePromQL
			}
		}
	}

	// SQL indicators
	sqlIndicators := []string{"select ", "from ", "where ", "group by ", "order by ", "insert "}
	for _, indicator := range sqlIndicators {
		if strings.Contains(lower, indicator) {
			return LSPLanguageSQL
		}
	}

	// Default to CQL
	return LSPLanguageCQL
}

// GetCompletions returns completions for the given content and position.
func (s *LSPEnhancedServer) GetCompletions(content string, pos Position, lang LSPLanguage) []CompletionItem {
	var items []CompletionItem

	switch lang {
	case LSPLanguagePromQL:
		items = append(items, s.getPromQLCompletions(content, pos)...)
	case LSPLanguageSQL:
		items = append(items, s.getSQLCompletions(content, pos)...)
	default:
		items = append(items, s.getCQLCompletions(content, pos)...)
	}

	// Always include metric name completions
	items = append(items, s.getMetricCompletions(content, pos)...)
	return items
}

func (s *LSPEnhancedServer) getPromQLCompletions(content string, pos Position) []CompletionItem {
	promqlFunctions := []struct {
		name string
		doc  string
	}{
		{"rate", "Calculate per-second rate of increase"},
		{"irate", "Calculate instant rate of increase"},
		{"increase", "Calculate increase over time range"},
		{"sum", "Sum over dimensions"},
		{"avg", "Average over dimensions"},
		{"min", "Minimum over dimensions"},
		{"max", "Maximum over dimensions"},
		{"count", "Count number of elements"},
		{"stddev", "Standard deviation over dimensions"},
		{"histogram_quantile", "Calculate quantile from histogram"},
		{"label_replace", "Replace label values using regex"},
		{"label_join", "Join label values into new label"},
		{"absent", "Check if metric exists"},
		{"delta", "Calculate delta for gauges"},
		{"deriv", "Calculate derivative"},
		{"topk", "Get top K elements by value"},
		{"bottomk", "Get bottom K elements by value"},
		{"sort", "Sort elements ascending"},
		{"sort_desc", "Sort elements descending"},
		{"abs", "Absolute value"},
		{"ceil", "Ceiling function"},
		{"floor", "Floor function"},
		{"round", "Round to nearest integer"},
		{"clamp", "Clamp values to range"},
		{"clamp_max", "Clamp values to upper bound"},
		{"clamp_min", "Clamp values to lower bound"},
		{"vector", "Create constant vector"},
		{"scalar", "Convert single-element vector to scalar"},
	}

	var items []CompletionItem
	for _, fn := range promqlFunctions {
		items = append(items, CompletionItem{
			Label:         fn.name,
			Kind:          CompletionItemKindFunction,
			Detail:        "PromQL function",
			Documentation: fn.doc,
			InsertText:    fn.name + "(",
		})
	}
	return items
}

func (s *LSPEnhancedServer) getSQLCompletions(content string, pos Position) []CompletionItem {
	sqlKeywords := []string{
		"SELECT", "FROM", "WHERE", "AND", "OR", "NOT",
		"GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
		"AS", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN",
		"ON", "IN", "BETWEEN", "LIKE", "IS NULL", "IS NOT NULL",
		"ASC", "DESC", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",
	}

	var items []CompletionItem
	for _, kw := range sqlKeywords {
		items = append(items, CompletionItem{
			Label:      kw,
			Kind:       CompletionItemKindKeyword,
			Detail:     "SQL keyword",
			InsertText: kw + " ",
		})
	}
	return items
}

func (s *LSPEnhancedServer) getCQLCompletions(content string, pos Position) []CompletionItem {
	cqlKeywords := []string{
		"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY",
		"LIMIT", "BETWEEN", "AND", "WINDOW", "FILL",
	}

	aggFunctions := []string{
		"mean", "sum", "count", "min", "max", "first", "last",
		"stddev", "median", "percentile", "rate", "derivative",
	}

	var items []CompletionItem
	for _, kw := range cqlKeywords {
		items = append(items, CompletionItem{
			Label:      kw,
			Kind:       CompletionItemKindKeyword,
			Detail:     "CQL keyword",
			InsertText: kw + " ",
		})
	}
	for _, fn := range aggFunctions {
		items = append(items, CompletionItem{
			Label:         fn,
			Kind:          CompletionItemKindFunction,
			Detail:        "CQL aggregation",
			InsertText:    fn + "(",
			Documentation: fmt.Sprintf("Compute %s of values", fn),
		})
	}
	return items
}

func (s *LSPEnhancedServer) getMetricCompletions(content string, pos Position) []CompletionItem {
	s.catalog.Refresh()
	metrics := s.catalog.List()

	var items []CompletionItem
	for _, m := range metrics {
		items = append(items, CompletionItem{
			Label:      m.Name,
			Kind:       CompletionItemKindVariable,
			Detail:     fmt.Sprintf("metric (%s)", m.Type),
			InsertText: m.Name,
		})
	}
	return items
}

// GetSignatureHelp returns signature help for the given context.
func (s *LSPEnhancedServer) GetSignatureHelp(fnName string) *SignatureHelp {
	signatures := map[string]SignatureInfo{
		"rate": {
			Label:         "rate(v range-vector)",
			Documentation: "Calculates the per-second average rate of increase of the time series.",
			Parameters: []ParameterInfo{
				{Label: "v", Documentation: "Range vector to calculate rate for"},
			},
		},
		"histogram_quantile": {
			Label:         "histogram_quantile(φ float, b instant-vector)",
			Documentation: "Calculates the φ-quantile from histogram buckets.",
			Parameters: []ParameterInfo{
				{Label: "φ", Documentation: "Quantile value (0-1)"},
				{Label: "b", Documentation: "Histogram bucket metric"},
			},
		},
		"label_replace": {
			Label:         "label_replace(v, dst_label, replacement, src_label, regex)",
			Documentation: "Matches regex against src_label, replaces dst_label with replacement.",
			Parameters: []ParameterInfo{
				{Label: "v", Documentation: "Input vector"},
				{Label: "dst_label", Documentation: "Destination label name"},
				{Label: "replacement", Documentation: "Replacement string ($1 for groups)"},
				{Label: "src_label", Documentation: "Source label to match"},
				{Label: "regex", Documentation: "Regular expression to match"},
			},
		},
		"label_join": {
			Label:         "label_join(v, dst_label, separator, src_labels...)",
			Documentation: "Joins the values of src_labels with separator into dst_label.",
			Parameters: []ParameterInfo{
				{Label: "v", Documentation: "Input vector"},
				{Label: "dst_label", Documentation: "Destination label name"},
				{Label: "separator", Documentation: "Separator string"},
				{Label: "src_labels", Documentation: "Source labels to join"},
			},
		},
	}

	sig, exists := signatures[strings.ToLower(fnName)]
	if !exists {
		return nil
	}

	return &SignatureHelp{
		Signatures:      []SignatureInfo{sig},
		ActiveSignature: 0,
		ActiveParameter: 0,
	}
}

// GetCodeActions returns suggested code actions for a query.
func (s *LSPEnhancedServer) GetCodeActions(content string, lang LSPLanguage) []CodeAction {
	var actions []CodeAction

	lower := strings.ToLower(content)

	// Suggest adding time range
	if lang == LSPLanguagePromQL && !strings.Contains(lower, "[") && strings.Contains(lower, "rate(") {
		actions = append(actions, CodeAction{
			Title:       "Add time range selector",
			Kind:        "quickfix",
			Description: "rate() requires a range selector like [5m]",
		})
	}

	// Suggest using aggregation
	if !strings.Contains(lower, "sum(") && !strings.Contains(lower, "avg(") &&
		!strings.Contains(lower, "group by") {
		actions = append(actions, CodeAction{
			Title:       "Wrap with sum() aggregation",
			Kind:        "refactor",
			Description: "Add sum() aggregation to reduce cardinality",
		})
	}

	return actions
}

// GetHoverInfo returns hover information for a word in context.
func (s *LSPEnhancedServer) GetHoverInfo(word string) *Hover {
	// Check metric catalog
	if info, ok := s.catalog.Lookup(word); ok {
		return &Hover{
			Contents: MarkupContent{
				Kind:  "markdown",
				Value: fmt.Sprintf("**%s** (%s)\n\nCardinality: %d\nLast seen: %s", info.Name, info.Type, info.Cardinality, info.LastSeen.Format(time.RFC3339)),
			},
		}
	}

	// Check built-in functions
	builtins := map[string]string{
		"rate":               "Calculate per-second rate of increase of a counter.",
		"irate":              "Calculate instant rate based on last two data points.",
		"increase":           "Calculate increase of a counter over a time range.",
		"sum":                "Sum values across dimensions.",
		"avg":                "Average values across dimensions.",
		"histogram_quantile": "Calculate φ-quantile from histogram buckets.",
		"label_replace":      "Replace label values using regex capture groups.",
		"label_join":         "Join multiple label values into a new label.",
		"absent":             "Returns 1 if the metric does not exist.",
		"vector":             "Returns a constant vector with no labels.",
		"scalar":             "Converts a single-element vector to a scalar.",
	}

	if doc, ok := builtins[strings.ToLower(word)]; ok {
		return &Hover{
			Contents: MarkupContent{
				Kind:  "markdown",
				Value: fmt.Sprintf("**%s**\n\n%s", word, doc),
			},
		}
	}

	return nil
}
