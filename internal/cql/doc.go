// Package cql implements the Chronicle Query Language parser and execution engine.
//
// CQL is Chronicle's native query language for time-series data. It supports
// SQL-like syntax with time-series extensions including downsampling, window
// functions, and tag-based filtering.
//
// The package provides lexing, parsing, AST construction, and query plan
// generation for CQL expressions.
package cql
