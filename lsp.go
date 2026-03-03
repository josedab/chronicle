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

	// WaitGroup for background goroutines
	wg sync.WaitGroup

	// Debounce timer for diagnostics
	diagnosticTimer   *time.Timer
	diagnosticTimerMu sync.Mutex
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

	s.wg.Add(2)
	go func(ctx context.Context) {
		defer s.wg.Done()
		s.refreshSchemaCache()
	}(s.ctx)

	go func(ctx context.Context) {
		defer s.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					slog.Error("LSP accept error", "err", err)
					continue
				}
			}
			go s.handleConnection(conn)
		}
	}(s.ctx)

	slog.Info("LSP server started", "port", s.config.Port)
	return nil
}

// Stop stops the LSP server.
func (s *LSPServer) Stop() {
	s.cancel()
	s.wg.Wait()
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
		s.diagnosticTimerMu.Lock()
		if s.diagnosticTimer != nil {
			s.diagnosticTimer.Stop()
		}
		uri := params.TextDocument.URI
		s.diagnosticTimer = time.AfterFunc(s.config.DiagnosticDelay, func() {
			s.publishDiagnostics(uri)
		})
		s.diagnosticTimerMu.Unlock()
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
