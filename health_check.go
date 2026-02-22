package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// HealthCheckConfig configures the health check engine.
type HealthCheckConfig struct {
	Enabled       bool          `json:"enabled"`
	CheckInterval time.Duration `json:"check_interval"`
}

// DefaultHealthCheckConfig returns sensible defaults.
func DefaultHealthCheckConfig() HealthCheckConfig {
	return HealthCheckConfig{
		Enabled:       true,
		CheckInterval: 15 * time.Second,
	}
}

// ComponentHealth represents the health of a single component.
type ComponentHealth struct {
	Name      string        `json:"name"`
	Status    string        `json:"status"`
	Message   string        `json:"message"`
	LastCheck time.Time     `json:"last_check"`
	Latency   time.Duration `json:"latency"`
}

// HealthCheckStatus represents the overall system health.
type HealthCheckStatus struct {
	Overall    string            `json:"overall"`
	Components []ComponentHealth `json:"components"`
	Uptime     time.Duration     `json:"uptime"`
	Version    string            `json:"version"`
}

// HealthCheckEngine manages health checks.
type HealthCheckEngine struct {
	db        *DB
	config    HealthCheckConfig
	mu        sync.RWMutex
	checkers  map[string]func() ComponentHealth
	startTime time.Time
	running   bool
	stopCh    chan struct{}
}

// NewHealthCheckEngine creates a new engine with built-in component checks.
func NewHealthCheckEngine(db *DB, cfg HealthCheckConfig) *HealthCheckEngine {
	e := &HealthCheckEngine{
		db:       db,
		config:   cfg,
		checkers: make(map[string]func() ComponentHealth),
		stopCh:   make(chan struct{}),
	}
	// Built-in components default to healthy
	e.checkers["storage"] = func() ComponentHealth {
		return ComponentHealth{Name: "storage", Status: "healthy", Message: "ok", LastCheck: time.Now()}
	}
	e.checkers["wal"] = func() ComponentHealth {
		return ComponentHealth{Name: "wal", Status: "healthy", Message: "ok", LastCheck: time.Now()}
	}
	e.checkers["index"] = func() ComponentHealth {
		return ComponentHealth{Name: "index", Status: "healthy", Message: "ok", LastCheck: time.Now()}
	}
	return e
}

// Start starts the engine.
func (e *HealthCheckEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.startTime = time.Now()
	e.mu.Unlock()
}

// Stop stops the engine.
func (e *HealthCheckEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// AddComponent registers a health check function for a named component.
func (e *HealthCheckEngine) AddComponent(name string, checker func() ComponentHealth) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.checkers[name] = checker
}

// Check runs all health checks and returns the overall status.
func (e *HealthCheckEngine) Check() HealthCheckStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	status := HealthCheckStatus{
		Overall: "healthy",
		Version: "1.0.0",
		Uptime:  time.Since(e.startTime),
	}

	for _, checker := range e.checkers {
		start := time.Now()
		ch := checker()
		ch.Latency = time.Since(start)
		status.Components = append(status.Components, ch)

		if ch.Status == "unhealthy" {
			status.Overall = "unhealthy"
		} else if ch.Status == "degraded" && status.Overall == "healthy" {
			status.Overall = "degraded"
		}
	}

	return status
}

// IsReady returns true if all components are healthy or degraded.
func (e *HealthCheckEngine) IsReady() bool {
	status := e.Check()
	return status.Overall != "unhealthy"
}

// IsLive returns true if the engine is running.
func (e *HealthCheckEngine) IsLive() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

// RegisterHTTPHandlers registers HTTP endpoints under /api/v1/health/.
// Note: /health, /health/ready, /health/live are registered in http_routes_admin.go
// and delegate to this engine. These endpoints provide the API-versioned alternative.
func (e *HealthCheckEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/health/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Check())
	})
	mux.HandleFunc("/api/v1/health/components", func(w http.ResponseWriter, r *http.Request) {
		status := e.Check()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status.Components)
	})
}
