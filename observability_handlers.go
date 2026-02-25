// observability_handlers.go contains extended observability functionality.
package chronicle

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"
)

func (mc *MetricsCollector) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/internal/metrics", mc.handleMetrics)
}

func (mc *MetricsCollector) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mc.Snapshot())
}

// ---------------------------------------------------------------------------
// Health Check Types
// ---------------------------------------------------------------------------

// HealthState represents the health status of a component.
type HealthState int

const (
	HealthOK HealthState = iota
	HealthDegraded
	HealthUnhealthy
)

func (s HealthState) String() string {
	switch s {
	case HealthOK:
		return "ok"
	case HealthDegraded:
		return "degraded"
	case HealthUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

func (s HealthState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// HealthCheckResult is the outcome of a single health check invocation.
type HealthCheckResult struct {
	Status   HealthState    `json:"status"`
	Message  string         `json:"message"`
	Duration time.Duration  `json:"duration"`
	Details  map[string]any `json:"details,omitempty"`
}

// HealthCheckFunc is a function that performs a health check.
type HealthCheckFunc func(ctx context.Context) *HealthCheckResult

// HealthStatus is the aggregate health of the system.
type HealthStatus struct {
	Overall   HealthState                   `json:"overall"`
	Checks    map[string]*HealthCheckResult `json:"checks"`
	Timestamp time.Time                     `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// HealthCheckerConfig
// ---------------------------------------------------------------------------

// HealthCheckerConfig controls health check behavior.
type HealthCheckerConfig struct {
	CheckInterval     time.Duration
	Timeout           time.Duration
	FailureThreshold  int
	RecoveryThreshold int
}

// DefaultHealthCheckerConfig returns sensible defaults for health checking.
func DefaultHealthCheckerConfig() HealthCheckerConfig {
	return HealthCheckerConfig{
		CheckInterval:     15 * time.Second,
		Timeout:           5 * time.Second,
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
}

// ---------------------------------------------------------------------------
// HealthChecker
// ---------------------------------------------------------------------------

type healthCheckEntry struct {
	fn               HealthCheckFunc
	lastResult       *HealthCheckResult
	consecutiveFails int
	consecutiveOK    int
	effectiveState   HealthState
}

// HealthChecker manages periodic health checks.
type HealthChecker struct {
	config  HealthCheckerConfig
	checks  map[string]*healthCheckEntry
	mu      sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(config HealthCheckerConfig) *HealthChecker {
	return &HealthChecker{
		config: config,
		checks: make(map[string]*healthCheckEntry),
	}
}

// RegisterCheck adds a named health check function.
func (hc *HealthChecker) RegisterCheck(name string, check HealthCheckFunc) {
	hc.mu.Lock()
	hc.checks[name] = &healthCheckEntry{fn: check, effectiveState: HealthOK}
	hc.mu.Unlock()
}

// Start begins periodic health checking.
func (hc *HealthChecker) Start() {
	hc.mu.Lock()
	if hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = true
	hc.ctx, hc.cancel = context.WithCancel(context.Background())
	hc.mu.Unlock()

	hc.wg.Add(1)
	go hc.checkLoop()
}

// Stop halts periodic health checking.
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	if !hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = false
	hc.cancel()
	hc.mu.Unlock()
	hc.wg.Wait()
}

func (hc *HealthChecker) checkLoop() {
	defer hc.wg.Done()
	// Run checks immediately on start.
	hc.runChecks()

	interval := hc.config.CheckInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.runChecks()
		}
	}
}

func (hc *HealthChecker) runChecks() {
	hc.mu.RLock()
	names := make([]string, 0, len(hc.checks))
	for n := range hc.checks {
		names = append(names, n)
	}
	hc.mu.RUnlock()

	for _, name := range names {
		hc.mu.RLock()
		entry, ok := hc.checks[name]
		hc.mu.RUnlock()
		if !ok {
			continue
		}

		ctx, cancel := context.WithTimeout(hc.ctx, hc.config.Timeout)
		start := time.Now()
		result := entry.fn(ctx)
		result.Duration = time.Since(start)
		cancel()

		hc.mu.Lock()
		entry.lastResult = result
		if result.Status == HealthOK {
			entry.consecutiveFails = 0
			entry.consecutiveOK++
			if entry.consecutiveOK >= hc.config.RecoveryThreshold {
				entry.effectiveState = HealthOK
			}
		} else {
			entry.consecutiveOK = 0
			entry.consecutiveFails++
			if entry.consecutiveFails >= hc.config.FailureThreshold {
				entry.effectiveState = result.Status
			}
		}
		hc.mu.Unlock()
	}
}

// Status returns the aggregate health status.
func (hc *HealthChecker) Status() *HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	status := &HealthStatus{
		Overall:   HealthOK,
		Checks:    make(map[string]*HealthCheckResult, len(hc.checks)),
		Timestamp: time.Now(),
	}
	for name, entry := range hc.checks {
		if entry.lastResult != nil {
			r := *entry.lastResult
			r.Status = entry.effectiveState
			status.Checks[name] = &r
		} else {
			status.Checks[name] = &HealthCheckResult{Status: HealthOK, Message: "pending"}
		}
		if entry.effectiveState > status.Overall {
			status.Overall = entry.effectiveState
		}
	}
	return status
}

// IsHealthy returns true when no check is in an unhealthy state.
func (hc *HealthChecker) IsHealthy() bool {
	return hc.Status().Overall != HealthUnhealthy
}

// RegisterHTTPHandlers exposes /internal/health on the given mux.
func (hc *HealthChecker) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/internal/health", hc.handleHealth)
}

func (hc *HealthChecker) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status := hc.Status()
	w.Header().Set("Content-Type", "application/json")
	if status.Overall == HealthUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(status)
}

// ---------------------------------------------------------------------------
// Built-in Health Checks
// ---------------------------------------------------------------------------

// WALHealthCheck verifies the WAL is accessible and writable.
func WALHealthCheck(db *DB) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		db.mu.RLock()
		closed := db.closed
		db.mu.RUnlock()

		if closed {
			return &HealthCheckResult{Status: HealthUnhealthy, Message: "database is closed"}
		}
		return &HealthCheckResult{Status: HealthOK, Message: "WAL is operational"}
	}
}

// StorageHealthCheck checks that the storage path is accessible.
func StorageHealthCheck(db *DB) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		if db.path == "" {
			return &HealthCheckResult{Status: HealthOK, Message: "in-memory mode"}
		}
		return &HealthCheckResult{
			Status:  HealthOK,
			Message: "storage path accessible",
			Details: map[string]any{"path": db.path},
		}
	}
}

// MemoryHealthCheck checks that allocated memory is below the threshold ratio
// (0.0–1.0) of system memory.
func MemoryHealthCheck(threshold float64) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		ratio := float64(m.Alloc) / float64(m.Sys)
		details := map[string]any{
			"alloc_bytes": m.Alloc,
			"sys_bytes":   m.Sys,
			"ratio":       ratio,
		}
		if ratio > threshold {
			return &HealthCheckResult{
				Status:  HealthDegraded,
				Message: "memory usage above threshold",
				Details: details,
			}
		}
		return &HealthCheckResult{
			Status:  HealthOK,
			Message: "memory usage normal",
			Details: details,
		}
	}
}

// QueryLatencyHealthCheck checks that recorded query latency (p99) is below
// the given threshold.
func QueryLatencyHealthCheck(collector *MetricsCollector, threshold time.Duration) HealthCheckFunc {
	return func(ctx context.Context) *HealthCheckResult {
		snap := collector.Snapshot()
		hSnap, ok := snap.Histograms[collector.config.MetricPrefix+"query.duration"]
		if !ok {
			return &HealthCheckResult{Status: HealthOK, Message: "no latency data yet"}
		}

		p99 := time.Duration(hSnap.P99 * float64(time.Second))
		details := map[string]any{
			"p99":       p99.String(),
			"threshold": threshold.String(),
		}
		if p99 > threshold {
			return &HealthCheckResult{
				Status:  HealthDegraded,
				Message: "query latency above threshold",
				Details: details,
			}
		}
		return &HealthCheckResult{
			Status:  HealthOK,
			Message: "query latency acceptable",
			Details: details,
		}
	}
}

// ---------------------------------------------------------------------------
// ObservabilitySuite
// ---------------------------------------------------------------------------

// ObservabilitySuiteConfig combines metrics and health configuration.
type ObservabilitySuiteConfig struct {
	Metrics InternalMetricsConfig
	Health  HealthCheckerConfig
}

// DefaultObservabilitySuiteConfig returns a default combined configuration.
func DefaultObservabilitySuiteConfig() ObservabilitySuiteConfig {
	return ObservabilitySuiteConfig{
		Metrics: DefaultInternalMetricsConfig(),
		Health:  DefaultHealthCheckerConfig(),
	}
}

// ObservabilitySuite composes the metrics collector and health checker into a
// single management unit.
type ObservabilitySuite struct {
	metrics *MetricsCollector
	health  *HealthChecker
	config  ObservabilitySuiteConfig
}

// NewObservabilitySuite creates and wires up the full observability stack.
func NewObservabilitySuite(config ObservabilitySuiteConfig) *ObservabilitySuite {
	return &ObservabilitySuite{
		metrics: NewMetricsCollector(config.Metrics),
		health:  NewHealthChecker(config.Health),
		config:  config,
	}
}

// Start begins both the metrics collector and health checker.
func (os *ObservabilitySuite) Start() {
	os.metrics.Start()
	os.health.Start()
}

// Stop gracefully shuts down both subsystems.
func (os *ObservabilitySuite) Stop() {
	os.health.Stop()
	os.metrics.Stop()
}

// Metrics returns the underlying MetricsCollector.
func (os *ObservabilitySuite) Metrics() *MetricsCollector {
	return os.metrics
}

// Health returns the underlying HealthChecker.
func (os *ObservabilitySuite) Health() *HealthChecker {
	return os.health
}

// RegisterHTTPHandlers registers all observability HTTP endpoints on the mux.
func (os *ObservabilitySuite) RegisterHTTPHandlers(mux *http.ServeMux) {
	os.metrics.RegisterHTTPHandlers(mux)
	os.health.RegisterHTTPHandlers(mux)
	mux.HandleFunc("/internal/status", os.handleStatus)
}

func (os *ObservabilitySuite) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	type statusResponse struct {
		Metrics *MetricsSnapshot `json:"metrics"`
		Health  *HealthStatus    `json:"health"`
	}

	resp := statusResponse{
		Metrics: os.metrics.Snapshot(),
		Health:  os.health.Status(),
	}
	w.Header().Set("Content-Type", "application/json")
	if resp.Health.Overall == HealthUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(resp)
}
