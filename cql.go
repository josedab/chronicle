package chronicle

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// CQL Token Types & Lexer
// ---------------------------------------------------------------------------

// CQLTokenType represents the type of a CQL token.
type CQLTokenType int

const (
	TokenSelect CQLTokenType = iota
	TokenFrom
	TokenWhere
	TokenGroupBy
	TokenWindow
	TokenGapFill
	TokenInterpolate
	TokenAsOfJoin
	TokenAlign
	TokenRate
	TokenDelta
	TokenIncrease
	TokenTimestamp
	TokenDuration
	TokenString
	TokenNumber
	TokenIdent
	TokenStar
	TokenComma
	TokenLParen
	TokenRParen
	TokenDot
	TokenEQ
	TokenNEQ
	TokenLT
	TokenGT
	TokenLTE
	TokenGTE
	TokenAnd
	TokenOr
	TokenNot
	TokenBetween
	TokenLast
	TokenNow
	TokenAS
	TokenTolerance
	TokenOrderBy
	TokenLimit
	TokenOffset
	TokenDesc
	TokenAsc
	TokenEOF
)

// CQLToken represents a single lexical token.
type CQLToken struct {
	Type  CQLTokenType
	Value string
	Pos   int
}

var cqlKeywords = map[string]CQLTokenType{
	"SELECT":      TokenSelect,
	"FROM":        TokenFrom,
	"WHERE":       TokenWhere,
	"GROUP":       TokenGroupBy, // consumed with BY
	"WINDOW":      TokenWindow,
	"GAP_FILL":    TokenGapFill,
	"INTERPOLATE": TokenInterpolate,
	"ASOF":        TokenAsOfJoin, // consumed with JOIN
	"ALIGN":       TokenAlign,
	"RATE":        TokenRate,
	"DELTA":       TokenDelta,
	"INCREASE":    TokenIncrease,
	"AND":         TokenAnd,
	"OR":          TokenOr,
	"NOT":         TokenNot,
	"BETWEEN":     TokenBetween,
	"LAST":        TokenLast,
	"NOW":         TokenNow,
	"AS":          TokenAS,
	"TOLERANCE":   TokenTolerance,
	"ORDER":       TokenOrderBy, // consumed with BY
	"LIMIT":       TokenLimit,
	"OFFSET":      TokenOffset,
	"DESC":        TokenDesc,
	"ASC":         TokenAsc,
	"JOIN":        TokenAsOfJoin,
}

// CQLLexer tokenizes a CQL query string.
type CQLLexer struct {
	input  string
	pos    int
	tokens []CQLToken
}

// NewCQLLexer creates a new CQL lexer.
func NewCQLLexer(input string) *CQLLexer {
	return &CQLLexer{input: input}
}

// Tokenize performs full tokenization of the input.
func (l *CQLLexer) Tokenize() ([]CQLToken, error) {
	l.tokens = nil
	l.pos = 0
	for l.pos < len(l.input) {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			break
		}
		ch := l.input[l.pos]
		switch {
		case ch == '\'':
			tok, err := l.readString()
			if err != nil {
				return nil, err
			}
			l.tokens = append(l.tokens, tok)
		case ch == '*':
			l.tokens = append(l.tokens, CQLToken{Type: TokenStar, Value: "*", Pos: l.pos})
			l.pos++
		case ch == ',':
			l.tokens = append(l.tokens, CQLToken{Type: TokenComma, Value: ",", Pos: l.pos})
			l.pos++
		case ch == '(':
			l.tokens = append(l.tokens, CQLToken{Type: TokenLParen, Value: "(", Pos: l.pos})
			l.pos++
		case ch == ')':
			l.tokens = append(l.tokens, CQLToken{Type: TokenRParen, Value: ")", Pos: l.pos})
			l.pos++
		case ch == '.':
			l.tokens = append(l.tokens, CQLToken{Type: TokenDot, Value: ".", Pos: l.pos})
			l.pos++
		case ch == '=':
			l.tokens = append(l.tokens, CQLToken{Type: TokenEQ, Value: "=", Pos: l.pos})
			l.pos++
		case ch == '!' && l.peek(1) == '=':
			l.tokens = append(l.tokens, CQLToken{Type: TokenNEQ, Value: "!=", Pos: l.pos})
			l.pos += 2
		case ch == '<' && l.peek(1) == '=':
			l.tokens = append(l.tokens, CQLToken{Type: TokenLTE, Value: "<=", Pos: l.pos})
			l.pos += 2
		case ch == '>' && l.peek(1) == '=':
			l.tokens = append(l.tokens, CQLToken{Type: TokenGTE, Value: ">=", Pos: l.pos})
			l.pos += 2
		case ch == '<':
			l.tokens = append(l.tokens, CQLToken{Type: TokenLT, Value: "<", Pos: l.pos})
			l.pos++
		case ch == '>':
			l.tokens = append(l.tokens, CQLToken{Type: TokenGT, Value: ">", Pos: l.pos})
			l.pos++
		case isDigit(ch):
			l.tokens = append(l.tokens, l.readNumber())
		case isIdentStart(ch):
			l.tokens = append(l.tokens, l.readIdentOrKeyword())
		default:
			return nil, fmt.Errorf("cql: unexpected character %q at position %d", ch, l.pos)
		}
	}
	l.tokens = append(l.tokens, CQLToken{Type: TokenEOF, Pos: l.pos})
	return l.tokens, nil
}

func (l *CQLLexer) skipWhitespace() {
	for l.pos < len(l.input) && (l.input[l.pos] == ' ' || l.input[l.pos] == '\t' || l.input[l.pos] == '\n' || l.input[l.pos] == '\r') {
		l.pos++
	}
}

func (l *CQLLexer) peek(offset int) byte {
	idx := l.pos + offset
	if idx < len(l.input) {
		return l.input[idx]
	}
	return 0
}

func (l *CQLLexer) readString() (CQLToken, error) {
	start := l.pos
	l.pos++ // skip opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		if l.input[l.pos] == '\'' {
			l.pos++
			return CQLToken{Type: TokenString, Value: sb.String(), Pos: start}, nil
		}
		if l.input[l.pos] == '\\' && l.pos+1 < len(l.input) {
			l.pos++
		}
		sb.WriteByte(l.input[l.pos])
		l.pos++
	}
	return CQLToken{}, fmt.Errorf("cql: unterminated string at position %d", start)
}

func (l *CQLLexer) readNumber() CQLToken {
	start := l.pos
	for l.pos < len(l.input) && (isDigit(l.input[l.pos]) || l.input[l.pos] == '.') {
		l.pos++
	}
	return CQLToken{Type: TokenNumber, Value: l.input[start:l.pos], Pos: start}
}

func (l *CQLLexer) readIdentOrKeyword() CQLToken {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	word := l.input[start:l.pos]
	upper := strings.ToUpper(word)

	// Handle two-word keywords: GROUP BY, ORDER BY, ASOF JOIN, GAP_FILL
	if upper == "GROUP" || upper == "ORDER" {
		saved := l.pos
		l.skipWhitespace()
		if l.pos < len(l.input) {
			ns := l.pos
			for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
				l.pos++
			}
			next := strings.ToUpper(l.input[ns:l.pos])
			if next == "BY" {
				if upper == "GROUP" {
					return CQLToken{Type: TokenGroupBy, Value: "GROUP BY", Pos: start}
				}
				return CQLToken{Type: TokenOrderBy, Value: "ORDER BY", Pos: start}
			}
			l.pos = saved // rewind
		}
	}
	if upper == "ASOF" {
		saved := l.pos
		l.skipWhitespace()
		if l.pos < len(l.input) {
			ns := l.pos
			for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
				l.pos++
			}
			next := strings.ToUpper(l.input[ns:l.pos])
			if next == "JOIN" {
				return CQLToken{Type: TokenAsOfJoin, Value: "ASOF JOIN", Pos: start}
			}
			l.pos = saved
		}
	}

	if tt, ok := cqlKeywords[upper]; ok {
		return CQLToken{Type: tt, Value: upper, Pos: start}
	}
	return CQLToken{Type: TokenIdent, Value: word, Pos: start}
}

func isDigit(ch byte) bool   { return ch >= '0' && ch <= '9' }
func isIdentStart(ch byte) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' }
func isIdentPart(ch byte) bool  { return isIdentStart(ch) || isDigit(ch) }

// ---------------------------------------------------------------------------
// CQL AST Nodes
// ---------------------------------------------------------------------------

// CQLNode is the interface implemented by all AST nodes.
type CQLNode interface {
	nodeType() string
}

// CQLExpr is the interface implemented by all expression nodes.
type CQLExpr interface {
	CQLNode
	exprType() string
}

// CQLSelectStmt represents a parsed CQL SELECT statement.
type CQLSelectStmt struct {
	Columns []CQLExpr
	From    *CQLFromClause
	Where   *CQLWhereClause
	GroupBy []CQLExpr
	Window  *CQLWindowClause
	GapFill *CQLGapFillClause
	Align   *CQLAlignClause
	OrderBy []CQLOrderExpr
	Limit   int
	Offset  int
}

func (s *CQLSelectStmt) nodeType() string { return "SelectStmt" }

// CQLFromClause represents the FROM clause.
type CQLFromClause struct {
	Metric   string
	Alias    string
	AsOfJoin *CQLAsOfJoin
}

func (f *CQLFromClause) nodeType() string { return "FromClause" }

// CQLWhereClause represents the WHERE clause.
type CQLWhereClause struct {
	Condition CQLExpr
}

func (w *CQLWhereClause) nodeType() string { return "WhereClause" }

// CQLWindowClause represents the WINDOW clause.
type CQLWindowClause struct {
	Duration time.Duration
	Slide    time.Duration
}

func (w *CQLWindowClause) nodeType() string { return "WindowClause" }

// CQLGapFillClause represents the GAP_FILL clause.
type CQLGapFillClause struct {
	Method string // linear, previous, zero, null
	MaxGap time.Duration
}

func (g *CQLGapFillClause) nodeType() string { return "GapFillClause" }

// CQLAlignClause represents the ALIGN clause.
type CQLAlignClause struct {
	Boundary string // calendar, epoch
	Timezone string
}

func (a *CQLAlignClause) nodeType() string { return "AlignClause" }

// CQLAsOfJoin represents an ASOF JOIN clause.
type CQLAsOfJoin struct {
	RightMetric string
	Tolerance   time.Duration
}

func (j *CQLAsOfJoin) nodeType() string { return "AsOfJoin" }

// CQLColumnExpr represents a column expression with optional function and alias.
type CQLColumnExpr struct {
	Name     string
	Alias    string
	Function string
	Args     []CQLExpr
}

func (c *CQLColumnExpr) nodeType() string { return "ColumnExpr" }
func (c *CQLColumnExpr) exprType() string { return "column" }

// CQLBinaryExpr represents a binary expression.
type CQLBinaryExpr struct {
	Left  CQLExpr
	Op    string
	Right CQLExpr
}

func (b *CQLBinaryExpr) nodeType() string { return "BinaryExpr" }
func (b *CQLBinaryExpr) exprType() string { return "binary" }

// CQLLiteralExpr represents a literal value.
type CQLLiteralExpr struct {
	Value       interface{}
	LiteralType string // string, number
}

func (l *CQLLiteralExpr) nodeType() string { return "LiteralExpr" }
func (l *CQLLiteralExpr) exprType() string { return "literal" }

// CQLIdentExpr represents an identifier expression.
type CQLIdentExpr struct {
	Name string
}

func (i *CQLIdentExpr) nodeType() string { return "IdentExpr" }
func (i *CQLIdentExpr) exprType() string { return "ident" }

// CQLFunctionExpr represents a function call expression.
type CQLFunctionExpr struct {
	Name     string
	Args     []CQLExpr
	Distinct bool
}

func (f *CQLFunctionExpr) nodeType() string { return "FunctionExpr" }
func (f *CQLFunctionExpr) exprType() string { return "function" }

// CQLDurationExpr represents a duration literal (e.g., 5m, 1h).
type CQLDurationExpr struct {
	Duration time.Duration
}

func (d *CQLDurationExpr) nodeType() string { return "DurationExpr" }
func (d *CQLDurationExpr) exprType() string { return "duration" }

// CQLOrderExpr represents an ORDER BY expression.
type CQLOrderExpr struct {
	Expr CQLExpr
	Desc bool
}

func (o *CQLOrderExpr) nodeType() string { return "OrderExpr" }

// ---------------------------------------------------------------------------
// CQL Parser
// ---------------------------------------------------------------------------

// CQLParser is a Pratt-style recursive descent parser for CQL.
type CQLParser struct {
	tokens []CQLToken
	pos    int
}

// NewCQLParser creates a new CQL parser.
func NewCQLParser(tokens []CQLToken) *CQLParser {
	return &CQLParser{tokens: tokens}
}

// Parse parses the token stream into a CQL AST.
func (p *CQLParser) Parse() (*CQLSelectStmt, error) {
	return p.parseSelect()
}

func (p *CQLParser) current() CQLToken {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return CQLToken{Type: TokenEOF}
}

func (p *CQLParser) advance() CQLToken {
	tok := p.current()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *CQLParser) expect(tt CQLTokenType) (CQLToken, error) {
	tok := p.current()
	if tok.Type != tt {
		return tok, fmt.Errorf("cql: expected token type %d but got %d (%q) at pos %d", tt, tok.Type, tok.Value, tok.Pos)
	}
	p.pos++
	return tok, nil
}

func (p *CQLParser) parseSelect() (*CQLSelectStmt, error) {
	if _, err := p.expect(TokenSelect); err != nil {
		return nil, err
	}
	stmt := &CQLSelectStmt{}

	// Parse column list
	cols, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// Parse FROM
	if p.current().Type == TokenFrom {
		from, err := p.parseFrom()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	// Parse WHERE
	if p.current().Type == TokenWhere {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse LAST (shorthand for time range)
	if p.current().Type == TokenLast {
		p.advance()
		dur, err := p.parseDuration()
		if err != nil {
			return nil, fmt.Errorf("cql: invalid duration in LAST clause: %w", err)
		}
		now := time.Now()
		stmt.Where = mergeTimeRange(stmt.Where, now.Add(-dur), now)
	}

	// Parse GROUP BY
	if p.current().Type == TokenGroupBy {
		gb, err := p.parseGroupBy()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = gb
	}

	// Parse WINDOW
	if p.current().Type == TokenWindow {
		win, err := p.parseWindow()
		if err != nil {
			return nil, err
		}
		stmt.Window = win
	}

	// Parse GAP_FILL
	if p.current().Type == TokenGapFill {
		gf, err := p.parseGapFill()
		if err != nil {
			return nil, err
		}
		stmt.GapFill = gf
	}

	// Parse ALIGN
	if p.current().Type == TokenAlign {
		al, err := p.parseAlign()
		if err != nil {
			return nil, err
		}
		stmt.Align = al
	}

	// Parse ORDER BY
	if p.current().Type == TokenOrderBy {
		ob, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = ob
	}

	// Parse LIMIT
	if p.current().Type == TokenLimit {
		lim, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		stmt.Limit = lim
	}

	// Parse OFFSET
	if p.current().Type == TokenOffset {
		p.advance()
		tok, err := p.expect(TokenNumber)
		if err != nil {
			return nil, fmt.Errorf("cql: expected number after OFFSET: %w", err)
		}
		n, _ := strconv.Atoi(tok.Value)
		stmt.Offset = n
	}

	return stmt, nil
}

func (p *CQLParser) parseColumnList() ([]CQLExpr, error) {
	var cols []CQLExpr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		// Check for alias with AS
		if p.current().Type == TokenAS {
			p.advance()
			alias, err := p.expect(TokenIdent)
			if err != nil {
				return nil, fmt.Errorf("cql: expected alias after AS: %w", err)
			}
			if col, ok := expr.(*CQLColumnExpr); ok {
				col.Alias = alias.Value
			}
		}
		cols = append(cols, expr)
		if p.current().Type != TokenComma {
			break
		}
		p.advance() // consume comma
	}
	return cols, nil
}

func (p *CQLParser) parseFrom() (*CQLFromClause, error) {
	p.advance() // consume FROM
	tok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, fmt.Errorf("cql: expected metric name after FROM: %w", err)
	}
	from := &CQLFromClause{Metric: tok.Value}

	// Check for alias
	if p.current().Type == TokenAS {
		p.advance()
		alias, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("cql: expected alias after AS: %w", err)
		}
		from.Alias = alias.Value
	}

	// Check for ASOF JOIN
	if p.current().Type == TokenAsOfJoin {
		p.advance()
		rightTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("cql: expected metric name after ASOF JOIN: %w", err)
		}
		join := &CQLAsOfJoin{RightMetric: rightTok.Value}

		// Optional alias for right metric (AS b)
		if p.current().Type == TokenAS {
			p.advance()
			if _, err := p.expect(TokenIdent); err != nil {
				return nil, fmt.Errorf("cql: expected alias after AS: %w", err)
			}
		}

		// Parse TOLERANCE
		if p.current().Type == TokenTolerance {
			p.advance()
			dur, err := p.parseDuration()
			if err != nil {
				return nil, fmt.Errorf("cql: invalid tolerance duration: %w", err)
			}
			join.Tolerance = dur
		}
		from.AsOfJoin = join
	}

	return from, nil
}

func (p *CQLParser) parseWhere() (*CQLWhereClause, error) {
	p.advance() // consume WHERE
	expr, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("cql: invalid WHERE condition: %w", err)
	}
	return &CQLWhereClause{Condition: expr}, nil
}

func (p *CQLParser) parseGroupBy() ([]CQLExpr, error) {
	p.advance() // consume GROUP BY
	var exprs []CQLExpr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.current().Type != TokenComma {
			break
		}
		p.advance()
	}
	return exprs, nil
}

func (p *CQLParser) parseWindow() (*CQLWindowClause, error) {
	p.advance() // consume WINDOW
	dur, err := p.parseDuration()
	if err != nil {
		return nil, fmt.Errorf("cql: invalid window duration: %w", err)
	}
	win := &CQLWindowClause{Duration: dur}
	return win, nil
}

func (p *CQLParser) parseGapFill() (*CQLGapFillClause, error) {
	p.advance() // consume GAP_FILL
	tok := p.current()
	method := "linear"
	if tok.Type == TokenIdent {
		method = strings.ToLower(tok.Value)
		p.advance()
	}
	return &CQLGapFillClause{Method: method}, nil
}

func (p *CQLParser) parseAlign() (*CQLAlignClause, error) {
	p.advance() // consume ALIGN
	tok := p.current()
	boundary := "calendar"
	if tok.Type == TokenIdent {
		boundary = strings.ToLower(tok.Value)
		p.advance()
	}
	return &CQLAlignClause{Boundary: boundary}, nil
}

func (p *CQLParser) parseOrderBy() ([]CQLOrderExpr, error) {
	p.advance() // consume ORDER BY
	var orders []CQLOrderExpr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		oe := CQLOrderExpr{Expr: expr}
		if p.current().Type == TokenDesc {
			oe.Desc = true
			p.advance()
		} else if p.current().Type == TokenAsc {
			p.advance()
		}
		orders = append(orders, oe)
		if p.current().Type != TokenComma {
			break
		}
		p.advance()
	}
	return orders, nil
}

func (p *CQLParser) parseLimit() (int, error) {
	p.advance() // consume LIMIT
	tok, err := p.expect(TokenNumber)
	if err != nil {
		return 0, fmt.Errorf("cql: expected number after LIMIT: %w", err)
	}
	n, _ := strconv.Atoi(tok.Value)
	return n, nil
}

func (p *CQLParser) parseExpr() (CQLExpr, error) {
	return p.parseOr()
}

func (p *CQLParser) parseOr() (CQLExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.current().Type == TokenOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &CQLBinaryExpr{Left: left, Op: "OR", Right: right}
	}
	return left, nil
}

func (p *CQLParser) parseAnd() (CQLExpr, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for p.current().Type == TokenAnd {
		p.advance()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &CQLBinaryExpr{Left: left, Op: "AND", Right: right}
	}
	return left, nil
}

func (p *CQLParser) parseComparison() (CQLExpr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	switch p.current().Type {
	case TokenEQ, TokenNEQ, TokenLT, TokenGT, TokenLTE, TokenGTE:
		op := p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &CQLBinaryExpr{Left: left, Op: op.Value, Right: right}, nil
	}
	return left, nil
}

func (p *CQLParser) parsePrimary() (CQLExpr, error) {
	tok := p.current()
	switch tok.Type {
	case TokenNumber:
		p.advance()
		if isDurationSuffix(tok.Value, p.current()) {
			dur, err := parseDurationString(tok.Value + p.current().Value)
			if err == nil {
				p.advance()
				return &CQLDurationExpr{Duration: dur}, nil
			}
		}
		v, _ := strconv.ParseFloat(tok.Value, 64)
		return &CQLLiteralExpr{Value: v, LiteralType: "number"}, nil
	case TokenString:
		p.advance()
		return &CQLLiteralExpr{Value: tok.Value, LiteralType: "string"}, nil
	case TokenStar:
		p.advance()
		return &CQLColumnExpr{Name: "*"}, nil
	case TokenLParen:
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("cql: missing closing parenthesis: %w", err)
		}
		return expr, nil
	case TokenNow:
		p.advance()
		return &CQLFunctionExpr{Name: "now"}, nil
	case TokenIdent:
		return p.parseIdentOrFunction()
	case TokenRate, TokenDelta, TokenIncrease:
		return p.parseFunctionCall(tok.Value)
	default:
		return nil, fmt.Errorf("cql: unexpected token %q at pos %d", tok.Value, tok.Pos)
	}
}

func (p *CQLParser) parseIdentOrFunction() (CQLExpr, error) {
	tok := p.advance()
	// Check for dot notation (a.value)
	if p.current().Type == TokenDot {
		p.advance()
		field, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("cql: expected field after dot: %w", err)
		}
		return &CQLColumnExpr{Name: tok.Value + "." + field.Value}, nil
	}
	// Check for function call
	if p.current().Type == TokenLParen {
		p.pos-- // rewind to re-read name
		return p.parseFunctionCall(tok.Value)
	}
	return &CQLIdentExpr{Name: tok.Value}, nil
}

func (p *CQLParser) parseFunctionCall(name string) (CQLExpr, error) {
	if p.current().Type == TokenIdent || p.current().Type == TokenRate || p.current().Type == TokenDelta || p.current().Type == TokenIncrease {
		p.advance()
	}
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("cql: expected '(' after function %s: %w", name, err)
	}
	var args []CQLExpr
	if p.current().Type != TokenRParen {
		for {
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			if p.current().Type != TokenComma {
				break
			}
			p.advance()
		}
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("cql: expected ')' after function args: %w", err)
	}
	return &CQLFunctionExpr{Name: strings.ToLower(name), Args: args}, nil
}

func (p *CQLParser) parseDuration() (time.Duration, error) {
	tok := p.current()
	// Duration can be a number followed by a unit identifier (e.g., 5 m)
	// or a combined token like "5m"
	if tok.Type == TokenNumber {
		p.advance()
		next := p.current()
		if next.Type == TokenIdent {
			combined := tok.Value + next.Value
			dur, err := parseDurationString(combined)
			if err != nil {
				return 0, err
			}
			p.advance()
			return dur, nil
		}
		return 0, fmt.Errorf("cql: expected duration unit after %s", tok.Value)
	}
	if tok.Type == TokenIdent {
		dur, err := parseDurationString(tok.Value)
		if err != nil {
			return 0, err
		}
		p.advance()
		return dur, nil
	}
	return 0, fmt.Errorf("cql: expected duration, got %q", tok.Value)
}

func parseDurationString(s string) (time.Duration, error) {
	// Find where digits end and unit begins
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9') {
		i++
	}
	if i == 0 || i == len(s) {
		return 0, fmt.Errorf("cql: invalid duration %q", s)
	}
	num, err := strconv.Atoi(s[:i])
	if err != nil {
		return 0, fmt.Errorf("cql: invalid duration number %q: %w", s[:i], err)
	}
	unit := s[i:]
	switch strings.ToLower(unit) {
	case "s":
		return time.Duration(num) * time.Second, nil
	case "m":
		return time.Duration(num) * time.Minute, nil
	case "h":
		return time.Duration(num) * time.Hour, nil
	case "d":
		return time.Duration(num) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("cql: unknown duration unit %q", unit)
	}
}

func isDurationSuffix(numVal string, next CQLToken) bool {
	if next.Type != TokenIdent {
		return false
	}
	switch strings.ToLower(next.Value) {
	case "s", "m", "h", "d":
		return true
	}
	return false
}

func mergeTimeRange(existing *CQLWhereClause, start, end time.Time) *CQLWhereClause {
	cond := &CQLBinaryExpr{
		Left: &CQLBinaryExpr{
			Left:  &CQLIdentExpr{Name: "timestamp"},
			Op:    ">=",
			Right: &CQLLiteralExpr{Value: start.UnixNano(), LiteralType: "number"},
		},
		Op: "AND",
		Right: &CQLBinaryExpr{
			Left:  &CQLIdentExpr{Name: "timestamp"},
			Op:    "<=",
			Right: &CQLLiteralExpr{Value: end.UnixNano(), LiteralType: "number"},
		},
	}
	if existing != nil && existing.Condition != nil {
		return &CQLWhereClause{
			Condition: &CQLBinaryExpr{Left: existing.Condition, Op: "AND", Right: cond},
		}
	}
	return &CQLWhereClause{Condition: cond}
}

// ---------------------------------------------------------------------------
// CQL-to-Query Translator
// ---------------------------------------------------------------------------

// CQLTranslator converts CQL AST to internal Query objects.
type CQLTranslator struct{}

// NewCQLTranslator creates a new translator.
func NewCQLTranslator() *CQLTranslator {
	return &CQLTranslator{}
}

// Translate converts a CQL AST into an internal Query.
func (t *CQLTranslator) Translate(stmt *CQLSelectStmt) (*Query, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cql: nil statement")
	}
	q := &Query{}

	// Extract metric from FROM clause
	if stmt.From != nil {
		q.Metric = stmt.From.Metric
	}

	// Extract tags and time range from WHERE clause
	if stmt.Where != nil && stmt.Where.Condition != nil {
		tags, start, end := t.extractConditions(stmt.Where.Condition)
		q.Tags = tags
		q.Start = start
		q.End = end
	}

	// Extract aggregation from column functions
	aggFunc, found := t.extractAggregation(stmt.Columns)
	if found {
		agg := &Aggregation{Function: aggFunc}
		if stmt.Window != nil {
			agg.Window = stmt.Window.Duration
		}
		q.Aggregation = agg
	}

	// Extract GROUP BY
	for _, gb := range stmt.GroupBy {
		switch e := gb.(type) {
		case *CQLIdentExpr:
			q.GroupBy = append(q.GroupBy, e.Name)
		case *CQLColumnExpr:
			q.GroupBy = append(q.GroupBy, e.Name)
		}
	}

	// Set limit
	if stmt.Limit > 0 {
		q.Limit = stmt.Limit
	}

	return q, nil
}

func (t *CQLTranslator) extractConditions(expr CQLExpr) (map[string]string, int64, int64) {
	tags := make(map[string]string)
	var start, end int64

	switch e := expr.(type) {
	case *CQLBinaryExpr:
		if e.Op == "AND" || e.Op == "OR" {
			lt, ls, le := t.extractConditions(e.Left)
			rt, rs, re := t.extractConditions(e.Right)
			for k, v := range lt {
				tags[k] = v
			}
			for k, v := range rt {
				tags[k] = v
			}
			if ls != 0 {
				start = ls
			}
			if rs != 0 {
				start = rs
			}
			if le != 0 {
				end = le
			}
			if re != 0 {
				end = re
			}
		} else if e.Op == "=" {
			if ident, ok := e.Left.(*CQLIdentExpr); ok {
				if ident.Name == "timestamp" {
					if lit, ok := e.Right.(*CQLLiteralExpr); ok {
						if v, ok := lit.Value.(int64); ok {
							start = v
							end = v
						}
					}
				} else if lit, ok := e.Right.(*CQLLiteralExpr); ok {
					if s, ok := lit.Value.(string); ok {
						tags[ident.Name] = s
					}
				}
			}
		} else if e.Op == ">=" {
			if ident, ok := e.Left.(*CQLIdentExpr); ok && ident.Name == "timestamp" {
				if lit, ok := e.Right.(*CQLLiteralExpr); ok {
					if v, ok := lit.Value.(int64); ok {
						start = v
					}
				}
			}
		} else if e.Op == "<=" {
			if ident, ok := e.Left.(*CQLIdentExpr); ok && ident.Name == "timestamp" {
				if lit, ok := e.Right.(*CQLLiteralExpr); ok {
					if v, ok := lit.Value.(int64); ok {
						end = v
					}
				}
			}
		}
	}
	return tags, start, end
}

var cqlAggFuncMap = map[string]AggFunc{
	"count":  AggCount,
	"sum":    AggSum,
	"avg":    AggMean,
	"mean":   AggMean,
	"min":    AggMin,
	"max":    AggMax,
	"first":  AggFirst,
	"last":   AggLast,
	"stddev": AggStddev,
	"rate":   AggRate,
}

func (t *CQLTranslator) extractAggregation(cols []CQLExpr) (AggFunc, bool) {
	for _, col := range cols {
		if fn, ok := col.(*CQLFunctionExpr); ok {
			if af, found := cqlAggFuncMap[fn.Name]; found {
				return af, true
			}
		}
	}
	return AggNone, false
}

// TranslateToPromQL converts a CQL AST to a PromQL string.
func (t *CQLTranslator) TranslateToPromQL(stmt *CQLSelectStmt) (string, error) {
	if stmt == nil {
		return "", fmt.Errorf("cql: nil statement")
	}
	var sb strings.Builder
	metric := ""
	if stmt.From != nil {
		metric = stmt.From.Metric
	}

	// Extract label matchers from WHERE
	var matchers []string
	if stmt.Where != nil && stmt.Where.Condition != nil {
		matchers = t.extractPromQLMatchers(stmt.Where.Condition)
	}

	// Build the inner selector
	selector := metric
	if len(matchers) > 0 {
		selector = metric + "{" + strings.Join(matchers, ",") + "}"
	}

	// Wrap with function if present
	funcName := ""
	for _, col := range stmt.Columns {
		if fn, ok := col.(*CQLFunctionExpr); ok {
			funcName = fn.Name
			break
		}
	}

	window := ""
	if stmt.Window != nil {
		window = "[" + formatPromDuration(stmt.Window.Duration) + "]"
	}

	switch funcName {
	case "rate":
		sb.WriteString("rate(")
		sb.WriteString(selector)
		sb.WriteString(window)
		sb.WriteString(")")
	case "sum", "avg", "min", "max", "count":
		if window != "" {
			sb.WriteString(funcName)
			sb.WriteString("(")
			sb.WriteString(funcName)
			sb.WriteString("_over_time(")
			sb.WriteString(selector)
			sb.WriteString(window)
			sb.WriteString("))")
		} else {
			sb.WriteString(funcName)
			sb.WriteString("(")
			sb.WriteString(selector)
			sb.WriteString(")")
		}
	default:
		sb.WriteString(selector)
		if window != "" {
			sb.WriteString(window)
		}
	}

	// Add group by
	if len(stmt.GroupBy) > 0 {
		var groups []string
		for _, g := range stmt.GroupBy {
			switch e := g.(type) {
			case *CQLIdentExpr:
				groups = append(groups, e.Name)
			}
		}
		if len(groups) > 0 {
			sb.WriteString(" by (")
			sb.WriteString(strings.Join(groups, ","))
			sb.WriteString(")")
		}
	}

	return sb.String(), nil
}

func (t *CQLTranslator) extractPromQLMatchers(expr CQLExpr) []string {
	var matchers []string
	switch e := expr.(type) {
	case *CQLBinaryExpr:
		if e.Op == "AND" || e.Op == "OR" {
			matchers = append(matchers, t.extractPromQLMatchers(e.Left)...)
			matchers = append(matchers, t.extractPromQLMatchers(e.Right)...)
		} else if e.Op == "=" || e.Op == "!=" {
			if ident, ok := e.Left.(*CQLIdentExpr); ok {
				if ident.Name == "timestamp" {
					return nil
				}
				if lit, ok := e.Right.(*CQLLiteralExpr); ok {
					op := e.Op
					if op == "=" {
						op = "="
					}
					matchers = append(matchers, fmt.Sprintf(`%s%s"%v"`, ident.Name, op, lit.Value))
				}
			}
		}
	}
	return matchers
}

func formatPromDuration(d time.Duration) string {
	if d >= time.Hour {
		h := int(d.Hours())
		return fmt.Sprintf("%dh", h)
	}
	if d >= time.Minute {
		m := int(d.Minutes())
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

// ---------------------------------------------------------------------------
// CQL Engine
// ---------------------------------------------------------------------------

// CQLConfig configures the CQL query engine.
type CQLConfig struct {
	Enabled        bool
	MaxQueryLength int
	DefaultTimeout time.Duration
	CacheEnabled   bool
	CacheSize      int
}

// DefaultCQLConfig returns the default CQL engine configuration.
func DefaultCQLConfig() CQLConfig {
	return CQLConfig{
		Enabled:        true,
		MaxQueryLength: 4096,
		DefaultTimeout: 30 * time.Second,
		CacheEnabled:   true,
		CacheSize:      1000,
	}
}

type cqlCacheEntry struct {
	result    *Result
	createdAt time.Time
}

// CQLExplainResult describes the query execution plan.
type CQLExplainResult struct {
	OriginalQuery   string
	ParsedAST       *CQLSelectStmt
	TranslatedQuery *Query
	EstimatedCost   float64
	Steps           []string
}

// CQLEngine is the main entry point for executing CQL queries.
type CQLEngine struct {
	db         *DB
	config     CQLConfig
	translator *CQLTranslator
	cache      map[string]*cqlCacheEntry
	mu         sync.RWMutex
}

// NewCQLEngine creates a new CQL engine.
func NewCQLEngine(db *DB, config CQLConfig) *CQLEngine {
	return &CQLEngine{
		db:         db,
		config:     config,
		translator: NewCQLTranslator(),
		cache:      make(map[string]*cqlCacheEntry),
	}
}

// Execute parses, translates, and executes a CQL query string.
func (e *CQLEngine) Execute(ctx context.Context, query string) (*Result, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("cql: engine is disabled")
	}
	if len(query) > e.config.MaxQueryLength {
		return nil, fmt.Errorf("cql: query exceeds maximum length of %d", e.config.MaxQueryLength)
	}

	// Check cache
	if e.config.CacheEnabled {
		e.mu.RLock()
		if entry, ok := e.cache[query]; ok {
			if time.Since(entry.createdAt) < e.config.DefaultTimeout {
				e.mu.RUnlock()
				return entry.result, nil
			}
		}
		e.mu.RUnlock()
	}

	// Parse
	stmt, err := e.parse(query)
	if err != nil {
		return nil, err
	}

	// Translate
	q, err := e.translator.Translate(stmt)
	if err != nil {
		return nil, fmt.Errorf("cql: translation error: %w", err)
	}

	// Apply timeout
	if ctx == nil {
		ctx = context.Background()
	}
	if e.config.DefaultTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.DefaultTimeout)
		defer cancel()
	}

	// Execute
	result, err := e.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("cql: execution error: %w", err)
	}

	// Cache result
	if e.config.CacheEnabled {
		e.mu.Lock()
		if len(e.cache) >= e.config.CacheSize {
			// Evict oldest entry
			var oldest string
			var oldestTime time.Time
			for k, v := range e.cache {
				if oldest == "" || v.createdAt.Before(oldestTime) {
					oldest = k
					oldestTime = v.createdAt
				}
			}
			delete(e.cache, oldest)
		}
		e.cache[query] = &cqlCacheEntry{result: result, createdAt: time.Now()}
		e.mu.Unlock()
	}

	return result, nil
}

// Explain returns the query execution plan without executing the query.
func (e *CQLEngine) Explain(query string) (*CQLExplainResult, error) {
	stmt, err := e.parse(query)
	if err != nil {
		return nil, err
	}

	q, err := e.translator.Translate(stmt)
	if err != nil {
		return nil, fmt.Errorf("cql: translation error: %w", err)
	}

	steps := []string{"1. Parse CQL query", "2. Build AST"}
	cost := 1.0
	if q.Aggregation != nil {
		steps = append(steps, "3. Apply aggregation: "+aggFuncName(q.Aggregation.Function))
		cost += 2.0
	}
	if len(q.Tags) > 0 {
		steps = append(steps, fmt.Sprintf("3. Filter by %d tag(s)", len(q.Tags)))
		cost += float64(len(q.Tags)) * 0.5
	}
	if len(q.GroupBy) > 0 {
		steps = append(steps, fmt.Sprintf("4. Group by %d field(s)", len(q.GroupBy)))
		cost += float64(len(q.GroupBy))
	}
	steps = append(steps, fmt.Sprintf("%d. Execute against storage", len(steps)+1))

	return &CQLExplainResult{
		OriginalQuery:   query,
		ParsedAST:       stmt,
		TranslatedQuery: q,
		EstimatedCost:   cost,
		Steps:           steps,
	}, nil
}

// Validate checks whether a CQL query is syntactically valid.
func (e *CQLEngine) Validate(query string) error {
	_, err := e.parse(query)
	return err
}

func (e *CQLEngine) parse(query string) (*CQLSelectStmt, error) {
	lexer := NewCQLLexer(strings.TrimSpace(query))
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("cql: lexer error: %w", err)
	}
	parser := NewCQLParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("cql: parse error: %w", err)
	}
	return stmt, nil
}

func cqlAggFuncName(f AggFunc) string {
	names := map[AggFunc]string{
		AggCount:      "count",
		AggSum:        "sum",
		AggMean:       "mean",
		AggMin:        "min",
		AggMax:        "max",
		AggRate:       "rate",
		AggFirst:      "first",
		AggLast:       "last",
		AggStddev:     "stddev",
		AggPercentile: "percentile",
	}
	if n, ok := names[f]; ok {
		return n
	}
	return "unknown"
}

// ensure interfaces are satisfied
var (
	_ CQLNode = (*CQLSelectStmt)(nil)
	_ CQLNode = (*CQLFromClause)(nil)
	_ CQLNode = (*CQLWhereClause)(nil)
	_ CQLNode = (*CQLWindowClause)(nil)
	_ CQLNode = (*CQLGapFillClause)(nil)
	_ CQLNode = (*CQLAlignClause)(nil)
	_ CQLNode = (*CQLAsOfJoin)(nil)
	_ CQLExpr = (*CQLColumnExpr)(nil)
	_ CQLExpr = (*CQLBinaryExpr)(nil)
	_ CQLExpr = (*CQLLiteralExpr)(nil)
	_ CQLExpr = (*CQLIdentExpr)(nil)
	_ CQLExpr = (*CQLFunctionExpr)(nil)
	_ CQLExpr = (*CQLDurationExpr)(nil)
)


