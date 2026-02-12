package cql

import (
	"fmt"
	"strings"
)

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
	l.pos++
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
			l.pos = saved
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
