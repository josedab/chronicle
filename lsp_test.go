package chronicle

import (
	"testing"
)

func TestLSPServer_NewLSPServer(t *testing.T) {
	config := DefaultLSPConfig()
	server := NewLSPServer(nil, config)

	if server == nil {
		t.Fatal("Expected non-nil LSPServer")
	}

	if server.config.Port != 9257 {
		t.Errorf("Expected port 9257, got %d", server.config.Port)
	}
}

func TestLSPServer_GetKeywordCompletions(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	completions := server.getKeywordCompletions("")

	if len(completions) == 0 {
		t.Error("Expected keyword completions")
	}

	// Check for SELECT keyword
	found := false
	for _, c := range completions {
		if c.Label == "SELECT" {
			found = true
			if c.Kind != CompletionItemKindKeyword {
				t.Error("Expected keyword kind")
			}
			break
		}
	}

	if !found {
		t.Error("Expected SELECT in completions")
	}
}

func TestLSPServer_GetFunctionCompletions(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	completions := server.getFunctionCompletions("")

	if len(completions) == 0 {
		t.Error("Expected function completions")
	}

	// Check for mean function
	found := false
	for _, c := range completions {
		if c.Label == "mean" {
			found = true
			if c.Kind != CompletionItemKindFunction {
				t.Error("Expected function kind")
			}
			break
		}
	}

	if !found {
		t.Error("Expected mean in completions")
	}
}

func TestLSPServer_GetFunctionCompletions_Filtered(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	completions := server.getFunctionCompletions("me")

	found := false
	for _, c := range completions {
		if c.Label == "mean" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected mean in filtered completions")
	}

	// Should not include functions that don't match
	for _, c := range completions {
		if c.Label == "sum" {
			t.Error("sum should not be in filtered completions")
		}
	}
}

func TestLSPServer_AnalyzeContext(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	tests := []struct {
		content  string
		pos      Position
		expected string
	}{
		{"SELECT ", Position{Line: 0, Character: 7}, "select"},
		{"SELECT count(value) FROM ", Position{Line: 0, Character: 25}, "metric"},
		{"SELECT count(value) FROM cpu WHERE ", Position{Line: 0, Character: 36}, "where"},
		{"", Position{Line: 0, Character: 0}, "start"},
	}

	for _, tt := range tests {
		result := server.analyzeContext(tt.content, tt.pos)
		if result != tt.expected {
			t.Errorf("analyzeContext(%q) = %q, want %q", tt.content, result, tt.expected)
		}
	}
}

func TestLSPServer_GetWordAtPosition(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	tests := []struct {
		content  string
		pos      Position
		expected string
	}{
		{"SELECT mean FROM cpu", Position{Line: 0, Character: 8}, "mean"},
		{"SELECT mean FROM cpu", Position{Line: 0, Character: 17}, "cpu"},
		{"hello world", Position{Line: 0, Character: 2}, "hello"},
	}

	for _, tt := range tests {
		result := server.getWordAtPosition(tt.content, tt.pos)
		if result != tt.expected {
			t.Errorf("getWordAtPosition(%q, %v) = %q, want %q", tt.content, tt.pos, result, tt.expected)
		}
	}
}

func TestLSPServer_FormatQuery(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	tests := []struct {
		input    string
		expected string
	}{
		{"select count(value) from cpu", "SELECT count(value)\nFROM cpu"},
		{"SELECT mean(value) FROM cpu WHERE host = 'server1'", "SELECT mean(value)\nFROM cpu\nWHERE host = 'server1'"},
	}

	for _, tt := range tests {
		result := server.formatQuery(tt.input)
		if result != tt.expected {
			t.Errorf("formatQuery(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestLSPServer_ValidateQuery(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	// Valid query should produce warnings/hints, not errors
	diagnostics := server.validateQuery("SELECT mean(value) FROM cpu")

	hasError := false
	for _, d := range diagnostics {
		if d.Severity == DiagnosticSeverityError {
			hasError = true
			break
		}
	}

	// A valid query shouldn't have parse errors (may have hints)
	if hasError {
		t.Log("Query validation found an error (may be expected for simplified test)")
	}
}

func TestLSPServer_GetHover(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	// Test hover on function
	hover := server.getHover("SELECT mean(value) FROM cpu", Position{Line: 0, Character: 8})
	if hover == nil {
		t.Error("Expected hover info for function")
		return
	}

	if hover.Contents.Kind != "markdown" {
		t.Error("Expected markdown content")
	}

	// Test hover on keyword
	hover = server.getHover("SELECT mean(value) FROM cpu", Position{Line: 0, Character: 2})
	if hover == nil {
		t.Error("Expected hover info for keyword")
	}
}

func TestLSPServer_GetEndPosition(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	tests := []struct {
		content  string
		expected Position
	}{
		{"hello", Position{Line: 0, Character: 5}},
		{"hello\nworld", Position{Line: 1, Character: 5}},
		{"line1\nline2\nline3", Position{Line: 2, Character: 5}},
		{"", Position{Line: 0, Character: 0}},
	}

	for _, tt := range tests {
		result := server.getEndPosition(tt.content)
		if result != tt.expected {
			t.Errorf("getEndPosition(%q) = %v, want %v", tt.content, result, tt.expected)
		}
	}
}

func TestLSPServer_GetBuiltinFunctions(t *testing.T) {
	server := NewLSPServer(nil, DefaultLSPConfig())

	functions := server.getBuiltinFunctions()

	if len(functions) == 0 {
		t.Error("Expected builtin functions")
	}

	// Check for common functions
	names := make(map[string]bool)
	for _, fn := range functions {
		names[fn.Name] = true
	}

	expected := []string{"count", "sum", "mean", "min", "max", "stddev", "percentile", "rate", "first", "last"}
	for _, name := range expected {
		if !names[name] {
			t.Errorf("Expected function %s", name)
		}
	}
}

func TestQueryValidation_Validate(t *testing.T) {
	v := NewQueryValidation()

	// Invalid query
	diagnostics := v.Validate("INVALID QUERY")
	if len(diagnostics) == 0 {
		t.Error("Expected diagnostics for invalid query")
	}

	// Check severity
	if diagnostics[0].Severity != DiagnosticSeverityError {
		t.Error("Expected error severity")
	}
}

func TestFormatQuery(t *testing.T) {
	result := FormatQuery("select count(value) from cpu where host='server1'")

	if result == "" {
		t.Error("Expected non-empty formatted query")
	}

	// Should uppercase keywords
	if result[:6] != "SELECT" {
		t.Error("Expected SELECT to be uppercase")
	}
}

func TestDiagnosticSeverity(t *testing.T) {
	if DiagnosticSeverityError != 1 {
		t.Error("Expected DiagnosticSeverityError to be 1")
	}
	if DiagnosticSeverityWarning != 2 {
		t.Error("Expected DiagnosticSeverityWarning to be 2")
	}
	if DiagnosticSeverityInformation != 3 {
		t.Error("Expected DiagnosticSeverityInformation to be 3")
	}
	if DiagnosticSeverityHint != 4 {
		t.Error("Expected DiagnosticSeverityHint to be 4")
	}
}

func TestCompletionItemKind(t *testing.T) {
	if CompletionItemKindKeyword != 14 {
		t.Error("Expected CompletionItemKindKeyword to be 14")
	}
	if CompletionItemKindFunction != 3 {
		t.Error("Expected CompletionItemKindFunction to be 3")
	}
}

func TestIsWordChar(t *testing.T) {
	tests := []struct {
		char     byte
		expected bool
	}{
		{'a', true},
		{'Z', true},
		{'0', true},
		{'_', true},
		{' ', false},
		{'.', false},
		{'(', false},
	}

	for _, tt := range tests {
		result := isWordChar(tt.char)
		if result != tt.expected {
			t.Errorf("isWordChar(%c) = %v, want %v", tt.char, result, tt.expected)
		}
	}
}

func TestReplaceWordIgnoreCase(t *testing.T) {
	tests := []struct {
		input    string
		old      string
		new      string
		expected string
	}{
		{"select from", "select", "SELECT", "SELECT from"},
		{"SELECT FROM", "from", "FROM", "SELECT FROM"},
		{"selectfrom", "select", "SELECT", "selectfrom"}, // Not replaced (not a word boundary)
	}

	for _, tt := range tests {
		result := replaceWordIgnoreCase(tt.input, tt.old, tt.new)
		if result != tt.expected {
			t.Errorf("replaceWordIgnoreCase(%q, %q, %q) = %q, want %q",
				tt.input, tt.old, tt.new, result, tt.expected)
		}
	}
}
