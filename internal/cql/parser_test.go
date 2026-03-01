package cql

import (
	"testing"
	"time"
)

func parseQuery(t *testing.T, query string) *CQLSelectStmt {
	t.Helper()
	lexer := NewCQLLexer(query)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize(%q): %v", query, err)
	}
	parser := NewCQLParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(%q): %v", query, err)
	}
	return stmt
}

func TestSelectLimitOffset(t *testing.T) {
	stmt := parseQuery(t, "SELECT value FROM cpu LIMIT 10 OFFSET 5")
	if stmt.Limit != 10 {
		t.Errorf("limit = %d, want 10", stmt.Limit)
	}
	if stmt.Offset != 5 {
		t.Errorf("offset = %d, want 5", stmt.Offset)
	}
}

func TestSelectLimitZero(t *testing.T) {
	stmt := parseQuery(t, "SELECT value FROM cpu LIMIT 0")
	if stmt.Limit != 0 {
		t.Errorf("limit = %d, want 0", stmt.Limit)
	}
}

func TestWhereWithNOT(t *testing.T) {
	// NOT is defined as a token but the parser doesn't have a unary handler;
	// verify that using NOT as an identifier parses without panic.
	lexer := NewCQLLexer("SELECT value FROM cpu WHERE NOT host = 'server1'")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	parser := NewCQLParser(tokens)
	// NOT may cause parse error since it's not handled in parsePrimary
	_, _ = parser.Parse()
}

func TestNestedFunctionCalls(t *testing.T) {
	stmt := parseQuery(t, "SELECT avg(sum(value)) FROM cpu")
	if len(stmt.Columns) == 0 {
		t.Fatal("expected columns")
	}
	fn, ok := stmt.Columns[0].(*CQLFunctionExpr)
	if !ok {
		t.Fatalf("expected CQLFunctionExpr, got %T", stmt.Columns[0])
	}
	if fn.Name != "avg" {
		t.Errorf("outer function = %q, want avg", fn.Name)
	}
	if len(fn.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(fn.Args))
	}
	inner, ok := fn.Args[0].(*CQLFunctionExpr)
	if !ok {
		t.Fatalf("expected inner CQLFunctionExpr, got %T", fn.Args[0])
	}
	if inner.Name != "sum" {
		t.Errorf("inner function = %q, want sum", inner.Name)
	}
}

func TestUnterminatedString(t *testing.T) {
	lexer := NewCQLLexer("SELECT value FROM cpu WHERE host = 'unterminated")
	_, err := lexer.Tokenize()
	if err == nil {
		t.Error("expected error for unterminated string")
	}
}

func TestInvalidEscapeInString(t *testing.T) {
	// Escape sequences: backslash followed by character should be consumed
	lexer := NewCQLLexer(`SELECT value FROM cpu WHERE host = 'test\'s'`)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	// Find the string token
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenString {
			found = true
			// The escaped quote should be consumed as a literal character
			if tok.Value != "test's" {
				t.Errorf("string value = %q, want \"test's\"", tok.Value)
			}
		}
	}
	if !found {
		t.Error("expected string token")
	}
}

func TestDurationWithoutUnit(t *testing.T) {
	_, err := parseDurationString("5")
	if err == nil {
		t.Error("expected error for duration without unit")
	}
}

func TestDurationUnknownUnit(t *testing.T) {
	_, err := parseDurationString("5x")
	if err == nil {
		t.Error("expected error for unknown duration unit")
	}
}

func TestDurationValidUnits(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"5s", 5 * time.Second},
		{"10m", 10 * time.Minute},
		{"2h", 2 * time.Hour},
		{"1d", 24 * time.Hour},
	}
	for _, tc := range tests {
		dur, err := parseDurationString(tc.input)
		if err != nil {
			t.Errorf("parseDurationString(%q): %v", tc.input, err)
			continue
		}
		if dur != tc.expected {
			t.Errorf("parseDurationString(%q) = %v, want %v", tc.input, dur, tc.expected)
		}
	}
}

func TestAsOfJoinWithTolerance(t *testing.T) {
	stmt := parseQuery(t, "SELECT value FROM cpu ASOF JOIN memory TOLERANCE 5s")
	if stmt.From == nil {
		t.Fatal("expected From clause")
	}
	if stmt.From.AsOfJoin == nil {
		t.Fatal("expected AsOfJoin")
	}
	if stmt.From.AsOfJoin.RightMetric != "memory" {
		t.Errorf("right metric = %q, want memory", stmt.From.AsOfJoin.RightMetric)
	}
	if stmt.From.AsOfJoin.Tolerance != 5*time.Second {
		t.Errorf("tolerance = %v, want 5s", stmt.From.AsOfJoin.Tolerance)
	}
}

func TestAsOfJoinWithoutTolerance(t *testing.T) {
	stmt := parseQuery(t, "SELECT value FROM cpu ASOF JOIN memory")
	if stmt.From.AsOfJoin == nil {
		t.Fatal("expected AsOfJoin")
	}
	if stmt.From.AsOfJoin.Tolerance != 0 {
		t.Errorf("tolerance = %v, want 0", stmt.From.AsOfJoin.Tolerance)
	}
}

func TestLexer_UnexpectedCharacter(t *testing.T) {
	lexer := NewCQLLexer("SELECT @ FROM cpu")
	_, err := lexer.Tokenize()
	if err == nil {
		t.Error("expected error for unexpected character '@'")
	}
}

func TestParser_GroupBy(t *testing.T) {
	stmt := parseQuery(t, "SELECT avg(value) FROM cpu GROUP BY host")
	if len(stmt.GroupBy) == 0 {
		t.Error("expected group by fields")
	}
}

func TestParser_OrderByDesc(t *testing.T) {
	stmt := parseQuery(t, "SELECT value FROM cpu ORDER BY value DESC")
	if len(stmt.OrderBy) == 0 {
		t.Fatal("expected order by fields")
	}
	if !stmt.OrderBy[0].Desc {
		t.Error("expected DESC order")
	}
}

func TestParser_Align(t *testing.T) {
	stmt := parseQuery(t, "SELECT value FROM cpu ALIGN calendar")
	if stmt.Align == nil {
		t.Fatal("expected align clause")
	}
	if stmt.Align.Boundary != "calendar" {
		t.Errorf("align boundary = %q, want calendar", stmt.Align.Boundary)
	}
}

func TestParser_RateFunction(t *testing.T) {
	stmt := parseQuery(t, "SELECT rate(value) FROM cpu")
	if len(stmt.Columns) == 0 {
		t.Fatal("expected columns")
	}
	fn, ok := stmt.Columns[0].(*CQLFunctionExpr)
	if !ok {
		t.Fatalf("expected CQLFunctionExpr, got %T", stmt.Columns[0])
	}
	if fn.Name != "rate" {
		t.Errorf("function name = %q, want rate", fn.Name)
	}
}

func TestParser_StarSelect(t *testing.T) {
	stmt := parseQuery(t, "SELECT * FROM cpu")
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
	}
	col, ok := stmt.Columns[0].(*CQLColumnExpr)
	if !ok {
		t.Fatalf("expected CQLColumnExpr, got %T", stmt.Columns[0])
	}
	if col.Name != "*" {
		t.Errorf("column name = %q, want *", col.Name)
	}
}

func TestParser_ColumnAlias(t *testing.T) {
	stmt := parseQuery(t, "SELECT value AS val FROM cpu")
	if len(stmt.Columns) == 0 {
		t.Fatal("expected columns")
	}
}

func TestParser_DottedColumn(t *testing.T) {
	stmt := parseQuery(t, "SELECT cpu.value FROM cpu")
	if len(stmt.Columns) == 0 {
		t.Fatal("expected columns")
	}
	col, ok := stmt.Columns[0].(*CQLColumnExpr)
	if !ok {
		t.Fatalf("expected CQLColumnExpr, got %T", stmt.Columns[0])
	}
	if col.Name != "cpu.value" {
		t.Errorf("column name = %q, want cpu.value", col.Name)
	}
}

func TestParser_MultipleColumns(t *testing.T) {
	stmt := parseQuery(t, "SELECT value, avg(value), host FROM cpu")
	if len(stmt.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(stmt.Columns))
	}
}

func TestParser_WhereAndOr(t *testing.T) {
	stmt := parseQuery(t, "SELECT value FROM cpu WHERE host = 'a' AND region = 'us' OR dc = 'east'")
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParser_ParenthesizedExpr(t *testing.T) {
	stmt := parseQuery(t, "SELECT (value) FROM cpu")
	if len(stmt.Columns) == 0 {
		t.Fatal("expected columns")
	}
}

func TestParser_NowFunction(t *testing.T) {
	stmt := parseQuery(t, "SELECT now() FROM cpu")
	if len(stmt.Columns) == 0 {
		t.Fatal("expected columns")
	}
}

func TestLexer_ComparisonOperators(t *testing.T) {
	lexer := NewCQLLexer("a = b != c < d > e <= f >= g")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	ops := []CQLTokenType{TokenEQ, TokenNEQ, TokenLT, TokenGT, TokenLTE, TokenGTE}
	opIdx := 0
	for _, tok := range tokens {
		if tok.Type == TokenEQ || tok.Type == TokenNEQ || tok.Type == TokenLT ||
			tok.Type == TokenGT || tok.Type == TokenLTE || tok.Type == TokenGTE {
			if opIdx >= len(ops) {
				t.Fatalf("too many operators")
			}
			if tok.Type != ops[opIdx] {
				t.Errorf("operator[%d] = %d, want %d", opIdx, tok.Type, ops[opIdx])
			}
			opIdx++
		}
	}
	if opIdx != len(ops) {
		t.Errorf("found %d operators, want %d", opIdx, len(ops))
	}
}

func TestLexer_BETWEENToken(t *testing.T) {
	lexer := NewCQLLexer("BETWEEN")
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Type != TokenBetween {
		t.Errorf("expected BETWEEN token type, got %d", tokens[0].Type)
	}
}

func TestEngine_DisabledEngine(t *testing.T) {
	config := DefaultCQLConfig()
	config.Enabled = false
	engine := NewCQLEngine(nil, config)

	_, err := engine.Execute(nil, "SELECT value FROM cpu")
	if err == nil {
		t.Error("expected error for disabled engine")
	}
}

func TestEngine_ExceedsMaxLength(t *testing.T) {
	config := DefaultCQLConfig()
	config.MaxQueryLength = 10
	engine := NewCQLEngine(nil, config)

	_, err := engine.Execute(nil, "SELECT value FROM cpu WHERE host = 'server1'")
	if err == nil {
		t.Error("expected error for query exceeding max length")
	}
}

func FuzzCQLParser(f *testing.F) {
	seeds := []string{
		"SELECT value FROM cpu",
		"SELECT avg(value) FROM cpu WHERE host = 'server1' GROUP BY host WINDOW 5m",
		"SELECT * FROM memory LIMIT 100 OFFSET 50",
		"SELECT rate(value) FROM cpu ASOF JOIN memory TOLERANCE 5s",
		"SELECT value FROM cpu ORDER BY value DESC",
		"SELECT avg(sum(value)) FROM cpu",
		"SELECT value FROM cpu GAP_FILL linear ALIGN calendar",
		"SELECT value FROM cpu LAST 1h",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		lexer := NewCQLLexer(input)
		tokens, err := lexer.Tokenize()
		if err != nil {
			return // lexer errors are expected for random input
		}
		parser := NewCQLParser(tokens)
		// Should not panic
		_, _ = parser.Parse()
	})
}
