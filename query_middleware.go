package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// QueryMiddlewareConfig configures the query middleware engine.
type QueryMiddlewareConfig struct {
	Enabled        bool `json:"enabled"`
	MaxMiddlewares int  `json:"max_middlewares"`
}

// DefaultQueryMiddlewareConfig returns sensible defaults.
func DefaultQueryMiddlewareConfig() QueryMiddlewareConfig {
	return QueryMiddlewareConfig{
		Enabled:        true,
		MaxMiddlewares: 50,
	}
}

// QueryMiddlewareFunc is a function that can intercept and modify query execution.
type QueryMiddlewareFunc func(q *Query, next func(*Query) (*Result, error)) (*Result, error)

// QueryMiddlewareEntry holds a named middleware with priority.
type QueryMiddlewareEntry struct {
	Name     string              `json:"name"`
	Priority int                 `json:"priority"`
	Fn       QueryMiddlewareFunc `json:"-"`
}

// QueryMiddlewareStats holds statistics for the middleware engine.
type QueryMiddlewareStats struct {
	TotalExecuted   int64         `json:"total_executed"`
	AvgLatency      time.Duration `json:"avg_latency"`
	MiddlewareCount int           `json:"middleware_count"`
}

// QueryMiddlewareEngine manages a pipeline of query middlewares.
type QueryMiddlewareEngine struct {
	db      *DB
	config  QueryMiddlewareConfig
	running bool

	middlewares    []QueryMiddlewareEntry
	totalExecuted  int64
	totalLatencyNs int64

	mu sync.RWMutex
}

// NewQueryMiddlewareEngine creates a new query middleware engine.
func NewQueryMiddlewareEngine(db *DB, cfg QueryMiddlewareConfig) *QueryMiddlewareEngine {
	return &QueryMiddlewareEngine{
		db:          db,
		config:      cfg,
		middlewares: make([]QueryMiddlewareEntry, 0),
	}
}

// Start starts the middleware engine.
func (e *QueryMiddlewareEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop stops the middleware engine.
func (e *QueryMiddlewareEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// Use registers a middleware with the given name and priority.
func (e *QueryMiddlewareEngine) Use(name string, priority int, fn QueryMiddlewareFunc) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.middlewares) >= e.config.MaxMiddlewares {
		return fmt.Errorf("max middlewares reached (%d)", e.config.MaxMiddlewares)
	}

	e.middlewares = append(e.middlewares, QueryMiddlewareEntry{
		Name:     name,
		Priority: priority,
		Fn:       fn,
	})

	// Sort by priority (lower = earlier)
	sort.Slice(e.middlewares, func(i, j int) bool {
		return e.middlewares[i].Priority < e.middlewares[j].Priority
	})

	return nil
}

// Execute runs the query through the middleware pipeline, then executes via db.
func (e *QueryMiddlewareEngine) Execute(q *Query) (*Result, error) {
	e.mu.RLock()
	mws := make([]QueryMiddlewareEntry, len(e.middlewares))
	copy(mws, e.middlewares)
	e.mu.RUnlock()

	start := time.Now()

	// Build the chain from the end — use ExecuteContext to bypass middleware routing
	final := func(q *Query) (*Result, error) {
		if e.db != nil {
			return e.db.ExecuteContext(context.Background(), q)
		}
		return &Result{}, nil
	}

	chain := final
	for i := len(mws) - 1; i >= 0; i-- {
		mw := mws[i]
		next := chain
		chain = func(q *Query) (*Result, error) {
			return mw.Fn(q, next)
		}
	}

	result, err := chain(q)

	elapsed := time.Since(start)
	e.mu.Lock()
	e.totalExecuted++
	e.totalLatencyNs += elapsed.Nanoseconds()
	e.mu.Unlock()

	return result, err
}

// GetStats returns middleware engine statistics.
// MiddlewareCount returns the number of registered middlewares.
func (e *QueryMiddlewareEngine) MiddlewareCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.middlewares)
}

func (e *QueryMiddlewareEngine) GetStats() QueryMiddlewareStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var avg time.Duration
	if e.totalExecuted > 0 {
		avg = time.Duration(e.totalLatencyNs / e.totalExecuted)
	}

	return QueryMiddlewareStats{
		TotalExecuted:   e.totalExecuted,
		AvgLatency:      avg,
		MiddlewareCount: len(e.middlewares),
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the middleware engine.
func (e *QueryMiddlewareEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/query-middleware/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
