// Bridge: cql_bridge.go
//
// This file bridges internal/cql/ into the public chronicle package.
// It re-exports types and wraps the CQL engine so that callers use the
// top-level chronicle API while the query language implementation stays private.
//
// Pattern: internal/cql/ (implementation) → cql_bridge.go (public API)

package chronicle

import (
	"context"

	"github.com/chronicle-db/chronicle/internal/cql"
)

// CQLEngine is a type alias for the extracted CQL engine.
type CQLEngine = cql.CQLEngine

// CQLConfig is a type alias for the extracted CQL configuration.
type CQLConfig = cql.CQLConfig

// CQLExplainResult is a type alias for the extracted CQL explain result.
type CQLExplainResult = cql.CQLExplainResult

// CQLSelectStmt is a type alias for the extracted CQL select statement.
type CQLSelectStmt = cql.CQLSelectStmt

// CQLExpr is a type alias for the extracted CQL expression interface.
type CQLExpr = cql.CQLExpr

// CQLTokenType is a type alias for the extracted CQL token type.
type CQLTokenType = cql.CQLTokenType

// CQLToken is a type alias for the extracted CQL token.
type CQLToken = cql.CQLToken

// CQLNode is a type alias for the extracted CQL node interface.
type CQLNode = cql.CQLNode

// CQLLexer is a type alias for the extracted CQL lexer.
type CQLLexer = cql.CQLLexer

// CQLParser is a type alias for the extracted CQL parser.
type CQLParser = cql.CQLParser

// CQLTranslator is a type alias for the extracted CQL translator.
type CQLTranslator = cql.CQLTranslator

// CQLFromClause is a type alias for the extracted CQL FROM clause.
type CQLFromClause = cql.CQLFromClause

// CQLWhereClause is a type alias for the extracted CQL WHERE clause.
type CQLWhereClause = cql.CQLWhereClause

// CQLWindowClause is a type alias for the extracted CQL WINDOW clause.
type CQLWindowClause = cql.CQLWindowClause

// CQLGapFillClause is a type alias for the extracted CQL GAP_FILL clause.
type CQLGapFillClause = cql.CQLGapFillClause

// CQLAlignClause is a type alias for the extracted CQL ALIGN clause.
type CQLAlignClause = cql.CQLAlignClause

// CQLAsOfJoin is a type alias for the extracted CQL ASOF JOIN clause.
type CQLAsOfJoin = cql.CQLAsOfJoin

// CQLColumnExpr is a type alias for the extracted CQL column expression.
type CQLColumnExpr = cql.CQLColumnExpr

// CQLBinaryExpr is a type alias for the extracted CQL binary expression.
type CQLBinaryExpr = cql.CQLBinaryExpr

// CQLLiteralExpr is a type alias for the extracted CQL literal expression.
type CQLLiteralExpr = cql.CQLLiteralExpr

// CQLIdentExpr is a type alias for the extracted CQL identifier expression.
type CQLIdentExpr = cql.CQLIdentExpr

// CQLFunctionExpr is a type alias for the extracted CQL function expression.
type CQLFunctionExpr = cql.CQLFunctionExpr

// CQLDurationExpr is a type alias for the extracted CQL duration expression.
type CQLDurationExpr = cql.CQLDurationExpr

// CQLOrderExpr is a type alias for the extracted CQL order expression.
type CQLOrderExpr = cql.CQLOrderExpr

// Re-export token constants for backward compatibility.
const (
	TokenSelect      = cql.TokenSelect
	TokenFrom        = cql.TokenFrom
	TokenWhere       = cql.TokenWhere
	TokenGroupBy     = cql.TokenGroupBy
	TokenWindow      = cql.TokenWindow
	TokenGapFill     = cql.TokenGapFill
	TokenInterpolate = cql.TokenInterpolate
	TokenAsOfJoin    = cql.TokenAsOfJoin
	TokenAlign       = cql.TokenAlign
	TokenRate        = cql.TokenRate
	TokenDelta       = cql.TokenDelta
	TokenIncrease    = cql.TokenIncrease
	TokenTimestamp   = cql.TokenTimestamp
	TokenDuration    = cql.TokenDuration
	TokenString      = cql.TokenString
	TokenNumber      = cql.TokenNumber
	TokenIdent       = cql.TokenIdent
	TokenStar        = cql.TokenStar
	TokenComma       = cql.TokenComma
	TokenLParen      = cql.TokenLParen
	TokenRParen      = cql.TokenRParen
	TokenDot         = cql.TokenDot
	TokenEQ          = cql.TokenEQ
	TokenNEQ         = cql.TokenNEQ
	TokenLT          = cql.TokenLT
	TokenGT          = cql.TokenGT
	TokenLTE         = cql.TokenLTE
	TokenGTE         = cql.TokenGTE
	TokenAnd         = cql.TokenAnd
	TokenOr          = cql.TokenOr
	TokenNot         = cql.TokenNot
	TokenBetween     = cql.TokenBetween
	TokenLast        = cql.TokenLast
	TokenNow         = cql.TokenNow
	TokenAS          = cql.TokenAS
	TokenTolerance   = cql.TokenTolerance
	TokenOrderBy     = cql.TokenOrderBy
	TokenLimit       = cql.TokenLimit
	TokenOffset      = cql.TokenOffset
	TokenDesc        = cql.TokenDesc
	TokenAsc         = cql.TokenAsc
	TokenEOF         = cql.TokenEOF
)

// Re-export constructors and defaults.
var (
	DefaultCQLConfig = cql.DefaultCQLConfig
	NewCQLLexer      = cql.NewCQLLexer
	NewCQLParser     = cql.NewCQLParser
	NewCQLTranslator = cql.NewCQLTranslator
)

// NewCQLEngine creates a new CQL engine backed by the given database.
func NewCQLEngine(db *DB, config CQLConfig) *CQLEngine {
	var executor cql.QueryExecutor
	if db != nil {
		executor = &dbCQLAdapter{db: db}
	}
	return cql.NewCQLEngine(executor, config)
}

// dbCQLAdapter adapts a *DB to the cql.QueryExecutor interface.
type dbCQLAdapter struct {
	db *DB
}

func (a *dbCQLAdapter) ExecuteContext(ctx context.Context, q *cql.Query) (*cql.Result, error) {
	rootQ := &Query{
		Metric:  q.Metric,
		Start:   q.Start,
		End:     q.End,
		Tags:    q.Tags,
		GroupBy: q.GroupBy,
		Limit:   q.Limit,
	}
	for _, tf := range q.TagFilters {
		rootQ.TagFilters = append(rootQ.TagFilters, TagFilter{Key: tf.Key, Values: tf.Values})
	}
	if q.Aggregation != nil {
		rootQ.Aggregation = &Aggregation{
			Function: AggFunc(q.Aggregation.Function),
			Window:   q.Aggregation.Window,
		}
	}
	result, err := a.db.ExecuteContext(ctx, rootQ)
	if err != nil {
		return nil, err
	}
	points := make([]cql.Point, len(result.Points))
	for i, p := range result.Points {
		points[i] = cql.Point{
			Metric:    p.Metric,
			Timestamp: p.Timestamp,
			Value:     p.Value,
			Tags:      p.Tags,
		}
	}
	return &cql.Result{Points: points}, nil
}
