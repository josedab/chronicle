package chronicle

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
)

// WritePipelineConfig configures the write pipeline hooks engine.
type WritePipelineConfig struct {
	Enabled  bool `json:"enabled"`
	MaxHooks int  `json:"max_hooks"`
}

// DefaultWritePipelineConfig returns sensible defaults.
func DefaultWritePipelineConfig() WritePipelineConfig {
	return WritePipelineConfig{
		Enabled:  true,
		MaxHooks: 32,
	}
}

// WriteHook represents a hook in the write pipeline.
type WriteHook struct {
	Name    string                       `json:"name"`
	Phase   string                       `json:"phase"` // "pre" or "post"
	Handler func(Point) (Point, error)   `json:"-"`
}

// WritePipelineStats tracks write pipeline statistics.
type WritePipelineStats struct {
	TotalProcessed int64 `json:"total_processed"`
	TotalRejected  int64 `json:"total_rejected"`
	TotalErrors    int64 `json:"total_errors"`
	HookCount      int   `json:"hook_count"`
}

// WritePipelineEngine manages write pipeline hooks.
type WritePipelineEngine struct {
	db     *DB
	config WritePipelineConfig

	hooks   []WriteHook
	running bool
	stopCh  chan struct{}

	// Atomic stats to avoid lock juggling in hot path
	totalProcessed atomic.Int64
	totalRejected  atomic.Int64
	totalErrors    atomic.Int64

	mu sync.RWMutex
}

// NewWritePipelineEngine creates a new write pipeline engine.
func NewWritePipelineEngine(db *DB, cfg WritePipelineConfig) *WritePipelineEngine {
	return &WritePipelineEngine{
		db:     db,
		config: cfg,
		hooks:  make([]WriteHook, 0),
		stopCh: make(chan struct{}),
	}
}

// Start starts the write pipeline engine.
func (e *WritePipelineEngine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
}

// Stop stops the write pipeline engine.
func (e *WritePipelineEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Register adds a hook to the write pipeline.
func (e *WritePipelineEngine) Register(hook WriteHook) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.hooks) >= e.config.MaxHooks {
		return fmt.Errorf("max hooks (%d) reached", e.config.MaxHooks)
	}
	if hook.Phase != "pre" && hook.Phase != "post" {
		return fmt.Errorf("invalid phase %q: must be pre or post", hook.Phase)
	}
	e.hooks = append(e.hooks, hook)
	return nil
}

// Unregister removes a hook by name.
func (e *WritePipelineEngine) Unregister(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, h := range e.hooks {
		if h.Name == name {
			e.hooks = append(e.hooks[:i], e.hooks[i+1:]...)
			return true
		}
	}
	return false
}

// ProcessPre runs all "pre" hooks in order, returning the modified point or an error.
func (e *WritePipelineEngine) ProcessPre(p Point) (Point, error) {
	e.mu.RLock()
	hooks := make([]WriteHook, len(e.hooks))
	copy(hooks, e.hooks)
	e.mu.RUnlock()

	current := p
	for _, h := range hooks {
		if h.Phase != "pre" {
			continue
		}
		result, err := h.Handler(current)
		if err != nil {
			e.totalRejected.Add(1)
			e.totalErrors.Add(1)
			return Point{}, fmt.Errorf("hook %q rejected point: %w", h.Name, err)
		}
		current = result
	}

	e.totalProcessed.Add(1)
	return current, nil
}

// ProcessPost runs all "post" hooks (fire-and-forget).
// Panics in individual hooks are recovered to prevent crashing the write path.
func (e *WritePipelineEngine) ProcessPost(p Point) {
	e.mu.RLock()
	hooks := make([]WriteHook, len(e.hooks))
	copy(hooks, e.hooks)
	e.mu.RUnlock()

	for _, h := range hooks {
		if h.Phase != "post" {
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("post-write hook panicked", "hook", h.Name, "panic", r)
					e.totalErrors.Add(1)
				}
			}()
			if _, err := h.Handler(p); err != nil {
				slog.Warn("post-write hook error", "hook", h.Name, "error", err)
				e.totalErrors.Add(1)
			}
		}()
	}
}

// Stats returns aggregate statistics.
func (e *WritePipelineEngine) Stats() WritePipelineStats {
	e.mu.RLock()
	hookCount := len(e.hooks)
	e.mu.RUnlock()

	return WritePipelineStats{
		TotalProcessed: e.totalProcessed.Load(),
		TotalRejected:  e.totalRejected.Load(),
		TotalErrors:    e.totalErrors.Load(),
		HookCount:      hookCount,
	}
}

// ListHooks returns the registered hooks.
func (e *WritePipelineEngine) ListHooks() []WriteHook {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]WriteHook, len(e.hooks))
	copy(out, e.hooks)
	return out
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *WritePipelineEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/write-hooks/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		hooks := e.ListHooks()
		type hookInfo struct {
			Name  string `json:"name"`
			Phase string `json:"phase"`
		}
		infos := make([]hookInfo, len(hooks))
		for i, h := range hooks {
			infos[i] = hookInfo{Name: h.Name, Phase: h.Phase}
		}
		json.NewEncoder(w).Encode(infos)
	})
	mux.HandleFunc("/api/v1/write-hooks/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
