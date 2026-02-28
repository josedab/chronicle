// Package query provides SQL-like query parsing for time-series data.
//
// It implements a lexer, parser, and AST for Chronicle's query language,
// supporting SELECT-style syntax with time-range predicates, tag filters,
// aggregations, and GROUP BY clauses. The parser produces [Query] values
// that are handed to the execution engine for evaluation against the
// underlying storage layer.
package query
