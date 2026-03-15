package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// HealthCheckConfig configures the health check engine.
type HealthCheckConfig struct {
	Enabled       bool          `json:"enabled"`
	CheckInterval time.Duration `json:"check_interval"`

	// Performance thresholds — when exceeded the component is reported as degraded.
	MaxWriteLatency time.Duration `json:"max_write_latency"`
	MaxQueryLatency time.Duration `json:"max_query_latency"`
	MaxWALSizeBytes int64         `json:"max_wal_size_bytes"`
}

// DefaultHealthCheckConfig returns sensible defaults.
func DefaultHealthCheckConfig() HealthCheckConfig {
	return HealthCheckConfig{
		Enabled:         true,
		CheckInterval:   15 * time.Second,
		MaxWriteLatency: 100 * time.Millisecond,
		MaxQueryLatency: 500 * time.Millisecond,
		MaxWALSizeBytes: 256 * 1024 * 1024, // 256 MB
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
	e.checkers["database"] = e.checkDatabase
	e.checkers["storage"] = e.checkStorage
	e.checkers["wal"] = e.checkWAL
	e.checkers["index"] = e.checkIndex
	e.checkers["write_latency"] = e.checkWriteLatency
	e.checkers["query_latency"] = e.checkQueryLatency
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

func (e *HealthCheckEngine) checkDatabase() ComponentHealth {
	ch := ComponentHealth{Name: "database", LastCheck: time.Now()}
	if e.db == nil {
		ch.Status = "unhealthy"
		ch.Message = "database handle is nil"
		return ch
	}
	if e.db.isClosed() {
		ch.Status = "unhealthy"
		ch.Message = "database is closed"
		return ch
	}
	ch.Status = "healthy"
	ch.Message = "ok"
	return ch
}

func (e *HealthCheckEngine) checkStorage() ComponentHealth {
	ch := ComponentHealth{Name: "storage", LastCheck: time.Now()}
	if e.db == nil || e.db.path == "" {
		ch.Status = "degraded"
		ch.Message = "no storage path configured"
		return ch
	}
	info, err := os.Stat(e.db.path)
	if err != nil {
		ch.Status = "unhealthy"
		ch.Message = fmt.Sprintf("storage stat failed: %v", err)
		return ch
	}
	ch.Status = "healthy"
	ch.Message = fmt.Sprintf("size=%d bytes", info.Size())
	return ch
}

func (e *HealthCheckEngine) checkWAL() ComponentHealth {
	ch := ComponentHealth{Name: "wal", LastCheck: time.Now()}
	if e.db == nil || e.db.wal == nil {
		ch.Status = "unhealthy"
		ch.Message = "WAL not initialized"
		return ch
	}
	pos := e.db.wal.Position()
	ch.Status = "healthy"
	ch.Message = fmt.Sprintf("position=%d bytes", pos)

	if e.config.MaxWALSizeBytes > 0 && pos > e.config.MaxWALSizeBytes {
		ch.Status = "degraded"
		ch.Message = fmt.Sprintf("WAL size %d exceeds threshold %d", pos, e.config.MaxWALSizeBytes)
	}
	return ch
}

func (e *HealthCheckEngine) checkIndex() ComponentHealth {
	ch := ComponentHealth{Name: "index", LastCheck: time.Now()}
	if e.db == nil || e.db.index == nil {
		ch.Status = "unhealthy"
		ch.Message = "index not initialized"
		return ch
	}
	metrics := len(e.db.Metrics())
	series := e.db.SeriesCount()
	ch.Status = "healthy"
	ch.Message = fmt.Sprintf("metrics=%d, series=%d", metrics, series)
	return ch
}

func (e *HealthCheckEngine) checkWriteLatency() ComponentHealth {
	ch := ComponentHealth{Name: "write_latency", LastCheck: time.Now()}
	if e.db == nil || e.db.isClosed() {
		ch.Status = "unhealthy"
		ch.Message = "database unavailable"
		return ch
	}
	probe := Point{
		Metric:    "__health_probe__",
		Value:     0,
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"probe": "health"},
	}
	start := time.Now()
	err := e.db.Write(probe)
	latency := time.Since(start)

	if err != nil {
		ch.Status = "degraded"
		ch.Message = fmt.Sprintf("write probe failed: %v", err)
		return ch
	}
	ch.Status = "healthy"
	ch.Message = fmt.Sprintf("latency=%s", latency)
	if e.config.MaxWriteLatency > 0 && latency > e.config.MaxWriteLatency {
		ch.Status = "degraded"
		ch.Message = fmt.Sprintf("write latency %s exceeds threshold %s", latency, e.config.MaxWriteLatency)
	}
	return ch
}

func (e *HealthCheckEngine) checkQueryLatency() ComponentHealth {
	ch := ComponentHealth{Name: "query_latency", LastCheck: time.Now()}
	if e.db == nil || e.db.isClosed() {
		ch.Status = "unhealthy"
		ch.Message = "database unavailable"
		return ch
	}
	metrics := e.db.Metrics()
	if len(metrics) == 0 {
		ch.Status = "healthy"
		ch.Message = "no metrics to probe"
		return ch
	}
	start := time.Now()
	_, err := e.db.Execute(&Query{
		Metric: metrics[0],
		Start:  time.Now().Add(-time.Minute).UnixNano(),
		End:    time.Now().UnixNano(),
		Limit:  1,
	})
	latency := time.Since(start)

	if err != nil {
		ch.Status = "degraded"
		ch.Message = fmt.Sprintf("query probe failed: %v", err)
		return ch
	}
	ch.Status = "healthy"
	ch.Message = fmt.Sprintf("latency=%s", latency)
	if e.config.MaxQueryLatency > 0 && latency > e.config.MaxQueryLatency {
		ch.Status = "degraded"
		ch.Message = fmt.Sprintf("query latency %s exceeds threshold %s", latency, e.config.MaxQueryLatency)
	}
	return ch
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
