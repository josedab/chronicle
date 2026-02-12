package cql

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Execute parses, translates, and executes a CQL query string.
func (e *CQLEngine) Execute(ctx context.Context, query string) (*Result, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("cql: engine is disabled")
	}
	if len(query) > e.config.MaxQueryLength {
		return nil, fmt.Errorf("cql: query exceeds maximum length of %d", e.config.MaxQueryLength)
	}

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

	stmt, err := e.parse(query)
	if err != nil {
		return nil, err
	}

	q, err := e.translator.Translate(stmt)
	if err != nil {
		return nil, fmt.Errorf("cql: translation error: %w", err)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	if e.config.DefaultTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.DefaultTimeout)
		defer cancel()
	}

	result, err := e.executor.ExecuteContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("cql: execution error: %w", err)
	}

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
