package cql

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	"GROUP":       TokenGroupBy,
	"WINDOW":      TokenWindow,
	"GAP_FILL":    TokenGapFill,
	"INTERPOLATE": TokenInterpolate,
	"ASOF":        TokenAsOfJoin,
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
	"ORDER":       TokenOrderBy,
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

func isDigit(ch byte) bool { return ch >= '0' && ch <= '9' }

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool { return isIdentStart(ch) || isDigit(ch) }

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

// CQLFromClause represents the FROM clause.
type CQLFromClause struct {
	Metric   string
	Alias    string
	AsOfJoin *CQLAsOfJoin
}

// CQLWhereClause represents the WHERE clause.
type CQLWhereClause struct {
	Condition CQLExpr
}

// CQLWindowClause represents the WINDOW clause.
type CQLWindowClause struct {
	Duration time.Duration
	Slide    time.Duration
}

// CQLGapFillClause represents the GAP_FILL clause.
type CQLGapFillClause struct {
	Method string // linear, previous, zero, null
	MaxGap time.Duration
}

// CQLAlignClause represents the ALIGN clause.
type CQLAlignClause struct {
	Boundary string // calendar, epoch
	Timezone string
}

// CQLAsOfJoin represents an ASOF JOIN clause.
type CQLAsOfJoin struct {
	RightMetric string
	Tolerance   time.Duration
}

// CQLColumnExpr represents a column expression with optional function and alias.
type CQLColumnExpr struct {
	Name     string
	Alias    string
	Function string
	Args     []CQLExpr
}

// CQLBinaryExpr represents a binary expression.
type CQLBinaryExpr struct {
	Left  CQLExpr
	Op    string
	Right CQLExpr
}

// CQLLiteralExpr represents a literal value.
type CQLLiteralExpr struct {
	Value       interface{}
	LiteralType string // string, number
}

// CQLIdentExpr represents an identifier expression.
type CQLIdentExpr struct {
	Name string
}

// CQLFunctionExpr represents a function call expression.
type CQLFunctionExpr struct {
	Name     string
	Args     []CQLExpr
	Distinct bool
}

// CQLDurationExpr represents a duration literal (e.g., 5m, 1h).
type CQLDurationExpr struct {
	Duration time.Duration
}

// CQLOrderExpr represents an ORDER BY expression.
type CQLOrderExpr struct {
	Expr CQLExpr
	Desc bool
}

// CQLParser is a Pratt-style recursive descent parser for CQL.
type CQLParser struct {
	tokens []CQLToken
	pos    int
}

// NewCQLParser creates a new CQL parser.
func NewCQLParser(tokens []CQLToken) *CQLParser {
	return &CQLParser{tokens: tokens}
}

func parseDurationString(s string) (time.Duration, error) {

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

// CQLTranslator converts CQL AST to internal Query objects.
type CQLTranslator struct{}

// NewCQLTranslator creates a new translator.
func NewCQLTranslator() *CQLTranslator {
	return &CQLTranslator{}
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
	executor   QueryExecutor
	config     CQLConfig
	translator *CQLTranslator
	cache      map[string]*cqlCacheEntry
	mu         sync.RWMutex
}

// NewCQLEngine creates a new CQL engine.
func NewCQLEngine(executor QueryExecutor, config CQLConfig) *CQLEngine {
	return &CQLEngine{
		executor:   executor,
		config:     config,
		translator: NewCQLTranslator(),
		cache:      make(map[string]*cqlCacheEntry),
	}
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
