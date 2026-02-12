package cql

import (
	"testing"
)

func TestCQLLexer_BasicTokens(t *testing.T) {
	lexer := NewCQLLexer("SELECT * FROM cpu")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	expected := []CQLTokenType{TokenSelect, TokenStar, TokenFrom, TokenIdent, TokenEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tt := range expected {
		if tokens[i].Type != tt {
			t.Errorf("token[%d]: expected type %d, got %d (%q)", i, tt, tokens[i].Type, tokens[i].Value)
		}
	}
}

func TestCQLLexer_ComplexQuery(t *testing.T) {
	query := "SELECT avg(value) FROM cpu WHERE host = 'server1' GROUP BY host WINDOW 5m"
	lexer := NewCQLLexer(query)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	// Should contain SELECT, function name, parens, FROM, WHERE, operators, GROUP BY, WINDOW, etc.
	if len(tokens) < 10 {
		t.Errorf("expected at least 10 tokens for complex query, got %d", len(tokens))
	}

	// Check last token is EOF.
	if tokens[len(tokens)-1].Type != TokenEOF {
		t.Errorf("expected last token to be EOF")
	}
}

func TestCQLParser_SimpleSelect(t *testing.T) {
	lexer := NewCQLLexer("SELECT value FROM cpu")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	parser := NewCQLParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if stmt.From == nil {
		t.Fatal("expected non-nil From clause")
	}
	if stmt.From.Metric != "cpu" {
		t.Errorf("expected metric 'cpu', got %q", stmt.From.Metric)
	}
	if len(stmt.Columns) == 0 {
		t.Errorf("expected at least one column")
	}
}

func TestCQLParser_WindowQuery(t *testing.T) {
	lexer := NewCQLLexer("SELECT avg(value) FROM cpu WINDOW 5m")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	parser := NewCQLParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if stmt.Window == nil {
		t.Fatal("expected non-nil Window clause")
	}
	if stmt.Window.Duration.Minutes() != 5 {
		t.Errorf("expected 5m window, got %v", stmt.Window.Duration)
	}
}

func TestCQLParser_GapFill(t *testing.T) {
	lexer := NewCQLLexer("SELECT value FROM cpu GAP_FILL linear")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	parser := NewCQLParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if stmt.GapFill == nil {
		t.Fatal("expected non-nil GapFill clause")
	}
	if stmt.GapFill.Method != "linear" {
		t.Errorf("expected gap fill method 'linear', got %q", stmt.GapFill.Method)
	}
}

func TestCQLParser_WhereClause(t *testing.T) {
	lexer := NewCQLLexer("SELECT value FROM cpu WHERE host = 'server1'")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	parser := NewCQLParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if stmt.Where == nil {
		t.Fatal("expected non-nil Where clause")
	}
	if stmt.Where.Condition == nil {
		t.Fatal("expected non-nil Where condition")
	}
}

func TestCQLTranslator_ToQuery(t *testing.T) {
	lexer := NewCQLLexer("SELECT avg(value) FROM cpu WHERE host = 'server1'")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	parser := NewCQLParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	translator := NewCQLTranslator()
	query, err := translator.Translate(stmt)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}

	if query.Metric != "cpu" {
		t.Errorf("expected metric 'cpu', got %q", query.Metric)
	}
	if query.Aggregation == nil {
		t.Fatal("expected non-nil aggregation")
	}
	if query.Aggregation.Function != AggMean {
		t.Errorf("expected AggMean, got %v", query.Aggregation.Function)
	}
}

func TestCQLEngine_Validate(t *testing.T) {
	engine := NewCQLEngine(nil, DefaultCQLConfig())

	t.Run("ValidQuery", func(t *testing.T) {
		if err := engine.Validate("SELECT value FROM cpu"); err != nil {
			t.Errorf("expected valid query, got error: %v", err)
		}
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		if err := engine.Validate("INVALID STUFF"); err == nil {
			t.Errorf("expected error for invalid query")
		}
	})
}

func TestCQLEngine_Explain(t *testing.T) {
	engine := NewCQLEngine(nil, DefaultCQLConfig())
	result, err := engine.Explain("SELECT avg(value) FROM cpu")
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if result.OriginalQuery != "SELECT avg(value) FROM cpu" {
		t.Errorf("expected original query to match")
	}
	if result.ParsedAST == nil {
		t.Errorf("expected non-nil ParsedAST")
	}
	if result.TranslatedQuery == nil {
		t.Errorf("expected non-nil TranslatedQuery")
	}
}
