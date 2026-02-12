package cql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

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

	cols, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	if p.current().Type == TokenFrom {
		from, err := p.parseFrom()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	if p.current().Type == TokenWhere {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	if p.current().Type == TokenLast {
		p.advance()
		dur, err := p.parseDuration()
		if err != nil {
			return nil, fmt.Errorf("cql: invalid duration in LAST clause: %w", err)
		}
		now := time.Now()
		stmt.Where = mergeTimeRange(stmt.Where, now.Add(-dur), now)
	}

	if p.current().Type == TokenGroupBy {
		gb, err := p.parseGroupBy()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = gb
	}

	if p.current().Type == TokenWindow {
		win, err := p.parseWindow()
		if err != nil {
			return nil, err
		}
		stmt.Window = win
	}

	if p.current().Type == TokenGapFill {
		gf, err := p.parseGapFill()
		if err != nil {
			return nil, err
		}
		stmt.GapFill = gf
	}

	if p.current().Type == TokenAlign {
		al, err := p.parseAlign()
		if err != nil {
			return nil, err
		}
		stmt.Align = al
	}

	if p.current().Type == TokenOrderBy {
		ob, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = ob
	}

	if p.current().Type == TokenLimit {
		lim, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		stmt.Limit = lim
	}

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
		p.advance()
	}
	return cols, nil
}

func (p *CQLParser) parseFrom() (*CQLFromClause, error) {
	p.advance()
	tok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, fmt.Errorf("cql: expected metric name after FROM: %w", err)
	}
	from := &CQLFromClause{Metric: tok.Value}

	if p.current().Type == TokenAS {
		p.advance()
		alias, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("cql: expected alias after AS: %w", err)
		}
		from.Alias = alias.Value
	}

	if p.current().Type == TokenAsOfJoin {
		p.advance()
		rightTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("cql: expected metric name after ASOF JOIN: %w", err)
		}
		join := &CQLAsOfJoin{RightMetric: rightTok.Value}

		if p.current().Type == TokenAS {
			p.advance()
			if _, err := p.expect(TokenIdent); err != nil {
				return nil, fmt.Errorf("cql: expected alias after AS: %w", err)
			}
		}

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
	p.advance()
	expr, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("cql: invalid WHERE condition: %w", err)
	}
	return &CQLWhereClause{Condition: expr}, nil
}

func (p *CQLParser) parseGroupBy() ([]CQLExpr, error) {
	p.advance()
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
	p.advance()
	dur, err := p.parseDuration()
	if err != nil {
		return nil, fmt.Errorf("cql: invalid window duration: %w", err)
	}
	win := &CQLWindowClause{Duration: dur}
	return win, nil
}

func (p *CQLParser) parseGapFill() (*CQLGapFillClause, error) {
	p.advance()
	tok := p.current()
	method := "linear"
	if tok.Type == TokenIdent {
		method = strings.ToLower(tok.Value)
		p.advance()
	}
	return &CQLGapFillClause{Method: method}, nil
}

func (p *CQLParser) parseAlign() (*CQLAlignClause, error) {
	p.advance()
	tok := p.current()
	boundary := "calendar"
	if tok.Type == TokenIdent {
		boundary = strings.ToLower(tok.Value)
		p.advance()
	}
	return &CQLAlignClause{Boundary: boundary}, nil
}

func (p *CQLParser) parseOrderBy() ([]CQLOrderExpr, error) {
	p.advance()
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
	p.advance()
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

	if p.current().Type == TokenDot {
		p.advance()
		field, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("cql: expected field after dot: %w", err)
		}
		return &CQLColumnExpr{Name: tok.Value + "." + field.Value}, nil
	}

	if p.current().Type == TokenLParen {
		p.pos--
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
