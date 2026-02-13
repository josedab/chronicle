package chronicle

import (
	"fmt"
	"testing"
)

func TestQueryMiddlewareSingle(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewQueryMiddlewareEngine(db, DefaultQueryMiddlewareConfig())

	// Middleware that sets a limit on the query
	e.Use("limiter", 1, func(q *Query, next func(*Query) (*Result, error)) (*Result, error) {
		q.Limit = 100
		return next(q)
	})

	q := &Query{Metric: "cpu", Start: 1, End: 2}
	_, err := e.Execute(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Limit != 100 {
		t.Errorf("expected limit 100, got %d", q.Limit)
	}
}

func TestQueryMiddlewareChain(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewQueryMiddlewareEngine(db, DefaultQueryMiddlewareConfig())

	order := make([]string, 0)

	e.Use("first", 1, func(q *Query, next func(*Query) (*Result, error)) (*Result, error) {
		order = append(order, "first")
		return next(q)
	})
	e.Use("second", 2, func(q *Query, next func(*Query) (*Result, error)) (*Result, error) {
		order = append(order, "second")
		return next(q)
	})
	e.Use("third", 3, func(q *Query, next func(*Query) (*Result, error)) (*Result, error) {
		order = append(order, "third")
		return next(q)
	})

	q := &Query{Metric: "cpu", Start: 1, End: 2}
	_, err := e.Execute(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(order) != 3 {
		t.Fatalf("expected 3 middleware calls, got %d", len(order))
	}
	if order[0] != "first" || order[1] != "second" || order[2] != "third" {
		t.Errorf("unexpected order: %v", order)
	}
}

func TestQueryMiddlewareReject(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewQueryMiddlewareEngine(db, DefaultQueryMiddlewareConfig())

	e.Use("reject", 1, func(q *Query, next func(*Query) (*Result, error)) (*Result, error) {
		return nil, fmt.Errorf("query rejected")
	})

	q := &Query{Metric: "cpu", Start: 1, End: 2}
	_, err := e.Execute(q)
	if err == nil {
		t.Fatal("expected error from rejecting middleware")
	}
	if err.Error() != "query rejected" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestQueryMiddlewareEmptyPipeline(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewQueryMiddlewareEngine(db, DefaultQueryMiddlewareConfig())

	q := &Query{Metric: "cpu", Start: 1, End: 2}
	result, err := e.Execute(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result from empty pipeline")
	}
}

func TestQueryMiddlewareStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewQueryMiddlewareEngine(db, DefaultQueryMiddlewareConfig())

	e.Use("noop", 1, func(q *Query, next func(*Query) (*Result, error)) (*Result, error) {
		return next(q)
	})

	q := &Query{Metric: "cpu", Start: 1, End: 2}
	e.Execute(q)
	e.Execute(q)

	stats := e.GetStats()
	if stats.TotalExecuted != 2 {
		t.Errorf("expected 2 total executed, got %d", stats.TotalExecuted)
	}
	if stats.MiddlewareCount != 1 {
		t.Errorf("expected 1 middleware, got %d", stats.MiddlewareCount)
	}
}
