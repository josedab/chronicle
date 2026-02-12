package cql

import (
	"fmt"
	"strings"
)

// Translate converts a CQL AST into an internal Query.
func (t *CQLTranslator) Translate(stmt *CQLSelectStmt) (*Query, error) {
	if stmt == nil {
		return nil, fmt.Errorf("cql: nil statement")
	}
	q := &Query{}

	if stmt.From != nil {
		q.Metric = stmt.From.Metric
	}

	if stmt.Where != nil && stmt.Where.Condition != nil {
		tags, start, end := t.extractConditions(stmt.Where.Condition)
		q.Tags = tags
		q.Start = start
		q.End = end
	}

	aggFunc, found := t.extractAggregation(stmt.Columns)
	if found {
		agg := &Aggregation{Function: aggFunc}
		if stmt.Window != nil {
			agg.Window = stmt.Window.Duration
		}
		q.Aggregation = agg
	}

	for _, gb := range stmt.GroupBy {
		switch e := gb.(type) {
		case *CQLIdentExpr:
			q.GroupBy = append(q.GroupBy, e.Name)
		case *CQLColumnExpr:
			q.GroupBy = append(q.GroupBy, e.Name)
		}
	}

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

	selector := metric
	if len(matchers) > 0 {
		selector = metric + "{" + strings.Join(matchers, ",") + "}"
	}

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
