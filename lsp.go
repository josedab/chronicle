package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// LSPServer implements a Language Server Protocol server for Chronicle queries.
type LSPServer struct {
	db          *DB
	config      LSPConfig
	conn        net.Conn
	mu          sync.RWMutex
	initialized bool
	shutdown    bool

	// Document management
	documents  map[string]*TextDocument
	documentMu sync.RWMutex

	// Schema cache for completions
	schemaCache   *SchemaCache
	schemaCacheMu sync.RWMutex

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// LSPConfig configures the LSP server.
type LSPConfig struct {
	// Port for TCP connections (default: 9257)
	Port int `json:"port"`

	// EnableDiagnostics enables real-time diagnostics
	EnableDiagnostics bool `json:"enable_diagnostics"`

	// EnableCompletion enables auto-completion
	EnableCompletion bool `json:"enable_completion"`

	// EnableHover enables hover information
	EnableHover bool `json:"enable_hover"`

	// EnableFormatting enables query formatting
	EnableFormatting bool `json:"enable_formatting"`

	// DiagnosticDelay before sending diagnostics
	DiagnosticDelay time.Duration `json:"diagnostic_delay"`
}

// DefaultLSPConfig returns default LSP configuration.
func DefaultLSPConfig() LSPConfig {
	return LSPConfig{
		Port:              9257,
		EnableDiagnostics: true,
		EnableCompletion:  true,
		EnableHover:       true,
		EnableFormatting:  true,
		DiagnosticDelay:   300 * time.Millisecond,
	}
}

// TextDocument represents an open document.
type TextDocument struct {
	URI        string `json:"uri"`
	LanguageID string `json:"languageId"`
	Version    int    `json:"version"`
	Content    string `json:"text"`
}

// Position represents a position in a document.
type Position struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

// Range represents a range in a document.
type Range struct {
	Start Position `json:"start"`
	End   Position `json:"end"`
}

// Location represents a location in a document.
type Location struct {
	URI   string `json:"uri"`
	Range Range  `json:"range"`
}

// Diagnostic represents a diagnostic message.
type Diagnostic struct {
	Range    Range              `json:"range"`
	Severity DiagnosticSeverity `json:"severity"`
	Code     string             `json:"code,omitempty"`
	Source   string             `json:"source,omitempty"`
	Message  string             `json:"message"`
}

// DiagnosticSeverity indicates the severity level.
type DiagnosticSeverity int

const (
	DiagnosticSeverityError       DiagnosticSeverity = 1
	DiagnosticSeverityWarning     DiagnosticSeverity = 2
	DiagnosticSeverityInformation DiagnosticSeverity = 3
	DiagnosticSeverityHint        DiagnosticSeverity = 4
)

// CompletionItem represents a completion suggestion.
type CompletionItem struct {
	Label            string             `json:"label"`
	Kind             CompletionItemKind `json:"kind"`
	Detail           string             `json:"detail,omitempty"`
	Documentation    string             `json:"documentation,omitempty"`
	InsertText       string             `json:"insertText,omitempty"`
	InsertTextFormat int                `json:"insertTextFormat,omitempty"`
	SortText         string             `json:"sortText,omitempty"`
}

// CompletionItemKind indicates the type of completion.
type CompletionItemKind int

const (
	CompletionItemKindText     CompletionItemKind = 1
	CompletionItemKindMethod   CompletionItemKind = 2
	CompletionItemKindFunction CompletionItemKind = 3
	CompletionItemKindField    CompletionItemKind = 4
	CompletionItemKindVariable CompletionItemKind = 6
	CompletionItemKindKeyword  CompletionItemKind = 14
	CompletionItemKindSnippet  CompletionItemKind = 15
	CompletionItemKindValue    CompletionItemKind = 12
	CompletionItemKindOperator CompletionItemKind = 24
)

// Hover represents hover information.
type Hover struct {
	Contents MarkupContent `json:"contents"`
	Range    *Range        `json:"range,omitempty"`
}

// MarkupContent represents formatted content.
type MarkupContent struct {
	Kind  string `json:"kind"` // "plaintext" or "markdown"
	Value string `json:"value"`
}

// SchemaCache caches metric schemas for completions.
type SchemaCache struct {
	Metrics    []string            `json:"metrics"`
	Tags       map[string][]string `json:"tags"`       // metric -> tag keys
	TagValues  map[string][]string `json:"tag_values"` // tag key -> values
	Functions  []FunctionInfo      `json:"functions"`
	LastUpdate time.Time           `json:"last_update"`
}

// FunctionInfo describes an aggregation function.
type FunctionInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Signature   string `json:"signature"`
	Example     string `json:"example"`
}

// LSP Message types
type lspMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *lspError       `json:"error,omitempty"`
}

type lspError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// NewLSPServer creates a new LSP server.
func NewLSPServer(db *DB, config LSPConfig) *LSPServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &LSPServer{
		db:        db,
		config:    config,
		documents: make(map[string]*TextDocument),
		schemaCache: &SchemaCache{
			Tags:      make(map[string][]string),
			TagValues: make(map[string][]string),
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the LSP server.
func (s *LSPServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to start LSP server: %w", err)
	}

	go s.refreshSchemaCache()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-s.ctx.Done():
					return
				default:
					slog.Error("LSP accept error", "err", err)
					continue
				}
			}
			go s.handleConnection(conn)
		}
	}()

	slog.Info("LSP server started", "port", s.config.Port)
	return nil
}

// Stop stops the LSP server.
func (s *LSPServer) Stop() {
	s.cancel()
}

func (s *LSPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	decoder := json.NewDecoder(conn)
	for {
		var msg lspMessage
		if err := decoder.Decode(&msg); err != nil {
			return
		}

		response := s.handleMessage(&msg)
		if response != nil {
			s.sendResponse(response)
		}
	}
}

func (s *LSPServer) handleMessage(msg *lspMessage) *lspMessage {
	switch msg.Method {
	case "initialize":
		return s.handleInitialize(msg)
	case "initialized":
		return nil
	case "shutdown":
		return s.handleShutdown(msg)
	case "exit":
		return nil
	case "textDocument/didOpen":
		s.handleDidOpen(msg)
		return nil
	case "textDocument/didChange":
		s.handleDidChange(msg)
		return nil
	case "textDocument/didClose":
		s.handleDidClose(msg)
		return nil
	case "textDocument/completion":
		return s.handleCompletion(msg)
	case "textDocument/hover":
		return s.handleHover(msg)
	case "textDocument/formatting":
		return s.handleFormatting(msg)
	case "textDocument/diagnostic":
		return s.handleDiagnostic(msg)
	default:
		return nil
	}
}

func (s *LSPServer) handleInitialize(msg *lspMessage) *lspMessage {
	s.mu.Lock()
	s.initialized = true
	s.mu.Unlock()

	capabilities := map[string]any{
		"capabilities": map[string]any{
			"textDocumentSync": 1, // Full sync
			"completionProvider": map[string]any{
				"triggerCharacters": []string{".", " ", "(", ","},
				"resolveProvider":   false,
			},
			"hoverProvider":              s.config.EnableHover,
			"documentFormattingProvider": s.config.EnableFormatting,
			"diagnosticProvider": map[string]any{
				"interFileDependencies": false,
				"workspaceDiagnostics":  false,
			},
		},
		"serverInfo": map[string]string{
			"name":    "chronicle-lsp",
			"version": "1.0.0",
		},
	}

	return &lspMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  capabilities,
	}
}

func (s *LSPServer) handleShutdown(msg *lspMessage) *lspMessage {
	s.mu.Lock()
	s.shutdown = true
	s.mu.Unlock()

	return &lspMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  nil,
	}
}

func (s *LSPServer) handleDidOpen(msg *lspMessage) {
	var params struct {
		TextDocument TextDocument `json:"textDocument"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return
	}

	s.documentMu.Lock()
	s.documents[params.TextDocument.URI] = &params.TextDocument
	s.documentMu.Unlock()

	// Run diagnostics
	if s.config.EnableDiagnostics {
		go s.publishDiagnostics(params.TextDocument.URI)
	}
}

func (s *LSPServer) handleDidChange(msg *lspMessage) {
	var params struct {
		TextDocument struct {
			URI     string `json:"uri"`
			Version int    `json:"version"`
		} `json:"textDocument"`
		ContentChanges []struct {
			Text string `json:"text"`
		} `json:"contentChanges"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return
	}

	s.documentMu.Lock()
	if doc, ok := s.documents[params.TextDocument.URI]; ok {
		if len(params.ContentChanges) > 0 {
			doc.Content = params.ContentChanges[0].Text
			doc.Version = params.TextDocument.Version
		}
	}
	s.documentMu.Unlock()

	// Debounced diagnostics
	if s.config.EnableDiagnostics {
		go func() {
			time.Sleep(s.config.DiagnosticDelay)
			s.publishDiagnostics(params.TextDocument.URI)
		}()
	}
}

func (s *LSPServer) handleDidClose(msg *lspMessage) {
	var params struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return
	}

	s.documentMu.Lock()
	delete(s.documents, params.TextDocument.URI)
	s.documentMu.Unlock()
}

func (s *LSPServer) handleCompletion(msg *lspMessage) *lspMessage {
	if !s.config.EnableCompletion {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: []CompletionItem{}}
	}

	var params struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
		Position Position `json:"position"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: []CompletionItem{}}
	}

	s.documentMu.RLock()
	doc, ok := s.documents[params.TextDocument.URI]
	s.documentMu.RUnlock()

	if !ok {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: []CompletionItem{}}
	}

	completions := s.getCompletions(doc.Content, params.Position)

	return &lspMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  completions,
	}
}

func (s *LSPServer) getCompletions(content string, pos Position) []CompletionItem {
	var items []CompletionItem

	// Get context
	lines := strings.Split(content, "\n")
	if pos.Line >= len(lines) {
		return items
	}

	line := lines[pos.Line]
	if pos.Character > len(line) {
		pos.Character = len(line)
	}

	prefix := strings.ToLower(strings.TrimLeft(line[:pos.Character], " \t"))
	context := s.analyzeContext(content, pos)

	// Keywords
	if context == "start" || context == "keyword" {
		items = append(items, s.getKeywordCompletions(prefix)...)
	}

	// Functions
	if context == "function" || context == "select" {
		items = append(items, s.getFunctionCompletions(prefix)...)
	}

	// Metrics
	if context == "metric" || context == "from" {
		items = append(items, s.getMetricCompletions(prefix)...)
	}

	// Tags
	if context == "where" || context == "tag" {
		items = append(items, s.getTagCompletions(prefix)...)
	}

	// Sort by relevance
	sort.Slice(items, func(i, j int) bool {
		return items[i].SortText < items[j].SortText
	})

	return items
}

func (s *LSPServer) analyzeContext(content string, pos Position) string {
	lines := strings.Split(content, "\n")
	if pos.Line >= len(lines) {
		return "start"
	}

	fullContent := strings.Join(lines[:pos.Line+1], "\n")
	if pos.Line < len(lines) && pos.Character < len(lines[pos.Line]) {
		fullContent = strings.Join(lines[:pos.Line], "\n") + "\n" + lines[pos.Line][:pos.Character]
	}

	upper := strings.ToUpper(fullContent)

	if strings.Contains(upper, "WHERE") && !strings.Contains(upper, "GROUP BY") {
		return "where"
	}
	if strings.Contains(upper, "FROM") && !strings.Contains(upper, "WHERE") {
		return "metric"
	}
	if strings.Contains(upper, "SELECT") && !strings.Contains(upper, "FROM") {
		return "select"
	}
	if strings.Contains(upper, "GROUP BY") {
		return "groupby"
	}

	return "start"
}

func (s *LSPServer) getKeywordCompletions(prefix string) []CompletionItem {
	keywords := []struct {
		word string
		doc  string
	}{
		{"SELECT", "Start a query to select data"},
		{"FROM", "Specify the metric to query"},
		{"WHERE", "Filter results by conditions"},
		{"AND", "Combine multiple conditions"},
		{"OR", "Alternative conditions"},
		{"GROUP BY", "Group results by tag or time"},
		{"ORDER BY", "Sort results"},
		{"LIMIT", "Limit number of results"},
		{"OFFSET", "Skip results"},
		{"ASC", "Ascending order"},
		{"DESC", "Descending order"},
	}

	var items []CompletionItem
	for _, kw := range keywords {
		if prefix == "" || strings.HasPrefix(strings.ToLower(kw.word), prefix) {
			items = append(items, CompletionItem{
				Label:         kw.word,
				Kind:          CompletionItemKindKeyword,
				Detail:        "keyword",
				Documentation: kw.doc,
				InsertText:    kw.word + " ",
				SortText:      "0" + kw.word,
			})
		}
	}
	return items
}

func (s *LSPServer) getFunctionCompletions(prefix string) []CompletionItem {
	functions := s.getBuiltinFunctions()

	var items []CompletionItem
	for _, fn := range functions {
		if prefix == "" || strings.HasPrefix(strings.ToLower(fn.Name), prefix) {
			items = append(items, CompletionItem{
				Label:            fn.Name,
				Kind:             CompletionItemKindFunction,
				Detail:           fn.Signature,
				Documentation:    fn.Description + "\n\nExample: " + fn.Example,
				InsertText:       fn.Name + "($1)",
				InsertTextFormat: 2, // Snippet
				SortText:         "1" + fn.Name,
			})
		}
	}
	return items
}

func (s *LSPServer) getBuiltinFunctions() []FunctionInfo {
	return []FunctionInfo{
		{Name: "count", Signature: "count(field)", Description: "Count number of values", Example: "SELECT count(value) FROM cpu"},
		{Name: "sum", Signature: "sum(field)", Description: "Sum of values", Example: "SELECT sum(value) FROM requests"},
		{Name: "mean", Signature: "mean(field)", Description: "Average of values", Example: "SELECT mean(value) FROM temperature"},
		{Name: "min", Signature: "min(field)", Description: "Minimum value", Example: "SELECT min(value) FROM pressure"},
		{Name: "max", Signature: "max(field)", Description: "Maximum value", Example: "SELECT max(value) FROM throughput"},
		{Name: "stddev", Signature: "stddev(field)", Description: "Standard deviation", Example: "SELECT stddev(value) FROM latency"},
		{Name: "percentile", Signature: "percentile(field, n)", Description: "Nth percentile", Example: "SELECT percentile(value, 95) FROM response_time"},
		{Name: "rate", Signature: "rate(field)", Description: "Rate of change", Example: "SELECT rate(value) FROM counter"},
		{Name: "first", Signature: "first(field)", Description: "First value in time range", Example: "SELECT first(value) FROM metric"},
		{Name: "last", Signature: "last(field)", Description: "Last value in time range", Example: "SELECT last(value) FROM metric"},
	}
}

func (s *LSPServer) getMetricCompletions(prefix string) []CompletionItem {
	s.schemaCacheMu.RLock()
	metrics := s.schemaCache.Metrics
	s.schemaCacheMu.RUnlock()

	var items []CompletionItem
	for _, metric := range metrics {
		if prefix == "" || strings.HasPrefix(strings.ToLower(metric), prefix) {
			items = append(items, CompletionItem{
				Label:      metric,
				Kind:       CompletionItemKindVariable,
				Detail:     "metric",
				InsertText: metric + " ",
				SortText:   "2" + metric,
			})
		}
	}
	return items
}

func (s *LSPServer) getTagCompletions(prefix string) []CompletionItem {
	s.schemaCacheMu.RLock()
	tags := s.schemaCache.Tags
	tagValues := s.schemaCache.TagValues
	s.schemaCacheMu.RUnlock()

	var items []CompletionItem

	// Tag keys
	seen := make(map[string]bool)
	for _, tagKeys := range tags {
		for _, key := range tagKeys {
			if seen[key] {
				continue
			}
			seen[key] = true
			if prefix == "" || strings.HasPrefix(strings.ToLower(key), prefix) {
				items = append(items, CompletionItem{
					Label:      key,
					Kind:       CompletionItemKindField,
					Detail:     "tag key",
					InsertText: key + " = ",
					SortText:   "3" + key,
				})
			}
		}
	}

	// Tag values (if prefix contains =)
	if strings.Contains(prefix, "=") {
		parts := strings.SplitN(prefix, "=", 2)
		tagKey := strings.TrimSpace(parts[0])
		valuePrefix := ""
		if len(parts) > 1 {
			valuePrefix = strings.TrimSpace(parts[1])
			valuePrefix = strings.Trim(valuePrefix, "'\"")
		}

		if values, ok := tagValues[tagKey]; ok {
			for _, val := range values {
				if valuePrefix == "" || strings.HasPrefix(strings.ToLower(val), valuePrefix) {
					items = append(items, CompletionItem{
						Label:      val,
						Kind:       CompletionItemKindValue,
						Detail:     "tag value for " + tagKey,
						InsertText: "'" + val + "'",
						SortText:   "4" + val,
					})
				}
			}
		}
	}

	return items
}

func (s *LSPServer) handleHover(msg *lspMessage) *lspMessage {
	if !s.config.EnableHover {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: nil}
	}

	var params struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
		Position Position `json:"position"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: nil}
	}

	s.documentMu.RLock()
	doc, ok := s.documents[params.TextDocument.URI]
	s.documentMu.RUnlock()

	if !ok {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: nil}
	}

	hover := s.getHover(doc.Content, params.Position)

	return &lspMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  hover,
	}
}

func (s *LSPServer) getHover(content string, pos Position) *Hover {
	word := s.getWordAtPosition(content, pos)
	if word == "" {
		return nil
	}

	// Check if it's a function
	for _, fn := range s.getBuiltinFunctions() {
		if strings.EqualFold(fn.Name, word) {
			return &Hover{
				Contents: MarkupContent{
					Kind: "markdown",
					Value: fmt.Sprintf("**%s**\n\n%s\n\n```chronicle\n%s\n```\n\nExample:\n```chronicle\n%s\n```",
						fn.Name, fn.Description, fn.Signature, fn.Example),
				},
			}
		}
	}

	// Check if it's a keyword
	keywords := map[string]string{
		"SELECT":   "Selects fields or aggregations from metrics",
		"FROM":     "Specifies the metric to query",
		"WHERE":    "Filters results based on conditions",
		"GROUP BY": "Groups results by tag keys or time intervals",
		"LIMIT":    "Limits the number of returned results",
		"ORDER BY": "Sorts results by a field",
	}

	if desc, ok := keywords[strings.ToUpper(word)]; ok {
		return &Hover{
			Contents: MarkupContent{
				Kind:  "markdown",
				Value: fmt.Sprintf("**%s** (keyword)\n\n%s", strings.ToUpper(word), desc),
			},
		}
	}

	// Check if it's a metric
	s.schemaCacheMu.RLock()
	for _, metric := range s.schemaCache.Metrics {
		if strings.EqualFold(metric, word) {
			s.schemaCacheMu.RUnlock()
			return &Hover{
				Contents: MarkupContent{
					Kind:  "markdown",
					Value: fmt.Sprintf("**%s** (metric)\n\nTime-series metric available in the database", metric),
				},
			}
		}
	}
	s.schemaCacheMu.RUnlock()

	return nil
}

func (s *LSPServer) getWordAtPosition(content string, pos Position) string {
	lines := strings.Split(content, "\n")
	if pos.Line >= len(lines) {
		return ""
	}

	line := lines[pos.Line]
	if pos.Character >= len(line) {
		return ""
	}

	// Find word boundaries
	start := pos.Character
	end := pos.Character

	for start > 0 && isWordChar(line[start-1]) {
		start--
	}
	for end < len(line) && isWordChar(line[end]) {
		end++
	}

	return line[start:end]
}

func isWordChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

func (s *LSPServer) handleFormatting(msg *lspMessage) *lspMessage {
	if !s.config.EnableFormatting {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: []any{}}
	}

	var params struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: []any{}}
	}

	s.documentMu.RLock()
	doc, ok := s.documents[params.TextDocument.URI]
	s.documentMu.RUnlock()

	if !ok {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: []any{}}
	}

	formatted := s.formatQuery(doc.Content)

	edits := []map[string]any{
		{
			"range": Range{
				Start: Position{Line: 0, Character: 0},
				End:   s.getEndPosition(doc.Content),
			},
			"newText": formatted,
		},
	}

	return &lspMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  edits,
	}
}

func (s *LSPServer) formatQuery(query string) string {
	// Simple query formatter
	query = strings.TrimSpace(query)

	// Keywords to uppercase
	keywords := []string{"SELECT", "FROM", "WHERE", "AND", "OR", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "ASC", "DESC"}
	for _, kw := range keywords {
		// Case-insensitive replace
		query = replaceWordIgnoreCase(query, kw, kw)
	}

	// Add newlines before major clauses
	clauses := []string{"FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"}
	for _, clause := range clauses {
		query = strings.Replace(query, " "+clause+" ", "\n"+clause+" ", -1)
	}

	return query
}

func replaceWordIgnoreCase(s, old, new string) string {
	lower := strings.ToLower(s)
	oldLower := strings.ToLower(old)

	result := s
	offset := 0
	for {
		idx := strings.Index(lower[offset:], oldLower)
		if idx == -1 {
			break
		}
		idx += offset

		// Check word boundaries
		before := idx == 0 || !isWordChar(s[idx-1])
		after := idx+len(old) >= len(s) || !isWordChar(s[idx+len(old)])

		if before && after {
			result = result[:idx] + new + result[idx+len(old):]
			lower = strings.ToLower(result)
		}
		offset = idx + len(new)
		if offset >= len(lower) {
			break
		}
	}
	return result
}

func (s *LSPServer) getEndPosition(content string) Position {
	lines := strings.Split(content, "\n")
	if len(lines) == 0 {
		return Position{Line: 0, Character: 0}
	}
	return Position{
		Line:      len(lines) - 1,
		Character: len(lines[len(lines)-1]),
	}
}

func (s *LSPServer) handleDiagnostic(msg *lspMessage) *lspMessage {
	var params struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{"items": []Diagnostic{}}}
	}

	s.documentMu.RLock()
	doc, ok := s.documents[params.TextDocument.URI]
	s.documentMu.RUnlock()

	if !ok {
		return &lspMessage{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{"items": []Diagnostic{}}}
	}

	diagnostics := s.validateQuery(doc.Content)

	return &lspMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  map[string]any{"items": diagnostics},
	}
}

func (s *LSPServer) validateQuery(query string) []Diagnostic {
	var diagnostics []Diagnostic

	// Parse query
	parser := &QueryParser{}
	_, err := parser.Parse(query)
	if err != nil {
		// Find error position (simplified)
		diagnostics = append(diagnostics, Diagnostic{
			Range: Range{
				Start: Position{Line: 0, Character: 0},
				End:   s.getEndPosition(query),
			},
			Severity: DiagnosticSeverityError,
			Code:     "parse-error",
			Source:   "chronicle",
			Message:  err.Error(),
		})
	}

	// Additional validations
	diagnostics = append(diagnostics, s.checkQueryOptimizations(query)...)

	return diagnostics
}

func (s *LSPServer) checkQueryOptimizations(query string) []Diagnostic {
	var diagnostics []Diagnostic
	upper := strings.ToUpper(query)

	// Warn about missing LIMIT
	if !strings.Contains(upper, "LIMIT") && strings.Contains(upper, "SELECT") {
		diagnostics = append(diagnostics, Diagnostic{
			Range: Range{
				Start: Position{Line: 0, Character: 0},
				End:   Position{Line: 0, Character: 6},
			},
			Severity: DiagnosticSeverityHint,
			Code:     "missing-limit",
			Source:   "chronicle",
			Message:  "Consider adding LIMIT to prevent returning too many results",
		})
	}

	// Warn about SELECT * (if supported)
	if strings.Contains(upper, "SELECT *") {
		idx := strings.Index(upper, "SELECT *")
		diagnostics = append(diagnostics, Diagnostic{
			Range: Range{
				Start: Position{Line: 0, Character: idx},
				End:   Position{Line: 0, Character: idx + 8},
			},
			Severity: DiagnosticSeverityWarning,
			Code:     "select-all",
			Source:   "chronicle",
			Message:  "SELECT * may return more data than needed. Consider specifying fields.",
		})
	}

	return diagnostics
}

func (s *LSPServer) publishDiagnostics(uri string) {
	s.documentMu.RLock()
	doc, ok := s.documents[uri]
	s.documentMu.RUnlock()

	if !ok {
		return
	}

	diagnostics := s.validateQuery(doc.Content)

	notification := &lspMessage{
		JSONRPC: "2.0",
		Method:  "textDocument/publishDiagnostics",
		Params: json.RawMessage(mustMarshal(map[string]any{
			"uri":         uri,
			"version":     doc.Version,
			"diagnostics": diagnostics,
		})),
	}

	s.sendResponse(notification)
}

func (s *LSPServer) sendResponse(msg *lspMessage) {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn == nil {
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	header := fmt.Sprintf("Content-Length: %d\r\n\r\n", len(data))
	conn.Write([]byte(header))
	conn.Write(data)
}

func (s *LSPServer) refreshSchemaCache() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	s.updateSchemaCache()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.updateSchemaCache()
		}
	}
}

func (s *LSPServer) updateSchemaCache() {
	if s.db == nil {
		return
	}

	s.schemaCacheMu.Lock()
	defer s.schemaCacheMu.Unlock()

	s.schemaCache.Metrics = s.db.Metrics()
	s.schemaCache.LastUpdate = time.Now()

	// Populate tag information from schema registry
	if s.db.schemaRegistry != nil {
		schemas := s.db.schemaRegistry.List()
		for _, schema := range schemas {
			var tagKeys []string
			for _, tag := range schema.Tags {
				tagKeys = append(tagKeys, tag.Name)
				if len(tag.AllowedVals) > 0 {
					s.schemaCache.TagValues[tag.Name] = tag.AllowedVals
				}
			}
			s.schemaCache.Tags[schema.Name] = tagKeys
		}
	}
}

func mustMarshal(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

// QueryValidation provides query validation utilities.
type QueryValidation struct {
	parser *QueryParser
}

// NewQueryValidation creates a new query validator.
func NewQueryValidation() *QueryValidation {
	return &QueryValidation{
		parser: &QueryParser{},
	}
}

// Validate validates a query and returns diagnostics.
func (v *QueryValidation) Validate(query string) []Diagnostic {
	var diagnostics []Diagnostic

	_, err := v.parser.Parse(query)
	if err != nil {
		diagnostics = append(diagnostics, Diagnostic{
			Range: Range{
				Start: Position{Line: 0, Character: 0},
				End:   Position{Line: 0, Character: len(query)},
			},
			Severity: DiagnosticSeverityError,
			Code:     "parse-error",
			Source:   "chronicle",
			Message:  err.Error(),
		})
	}

	return diagnostics
}

// FormatQuery formats a query string.
func FormatQuery(query string) string {
	server := &LSPServer{}
	return server.formatQuery(query)
}
