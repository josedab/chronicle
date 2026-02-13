package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ConnectionPoolConfig configures the connection pool engine.
type ConnectionPoolConfig struct {
	MaxSize             int           `json:"max_size"`
	MinIdle             int           `json:"min_idle"`
	MaxIdleTime         time.Duration `json:"max_idle_time"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

// DefaultConnectionPoolConfig returns sensible defaults.
func DefaultConnectionPoolConfig() ConnectionPoolConfig {
	return ConnectionPoolConfig{
		MaxSize:             100,
		MinIdle:             5,
		MaxIdleTime:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
	}
}

// PooledConnection represents a connection in the pool.
type PooledConnection struct {
	ID        string    `json:"id"`
	Backend   string    `json:"backend"`
	CreatedAt time.Time `json:"created_at"`
	LastUsed  time.Time `json:"last_used"`
	InUse     bool      `json:"in_use"`
	Healthy   bool      `json:"healthy"`
}

// ConnectionPoolStats holds statistics for the connection pool.
type ConnectionPoolStats struct {
	TotalConnections  int   `json:"total_connections"`
	ActiveConnections int   `json:"active_connections"`
	IdleConnections   int   `json:"idle_connections"`
	TotalAcquires     int64 `json:"total_acquires"`
	TotalReleases     int64 `json:"total_releases"`
}

// ConnectionPoolEngine manages a pool of connections.
type ConnectionPoolEngine struct {
	db      *DB
	config  ConnectionPoolConfig
	running bool

	connections   map[string]*PooledConnection
	nextID        int
	totalAcquires int64
	totalReleases int64

	mu sync.RWMutex
}

// NewConnectionPoolEngine creates a new connection pool engine.
func NewConnectionPoolEngine(db *DB, cfg ConnectionPoolConfig) *ConnectionPoolEngine {
	return &ConnectionPoolEngine{
		db:          db,
		config:      cfg,
		connections: make(map[string]*PooledConnection),
	}
}

// Start starts the connection pool engine.
func (e *ConnectionPoolEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop stops the connection pool engine.
func (e *ConnectionPoolEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// Acquire obtains a connection for the given backend.
func (e *ConnectionPoolEngine) Acquire(backend string) (*PooledConnection, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Try to find an idle connection for this backend
	for _, conn := range e.connections {
		if conn.Backend == backend && !conn.InUse && conn.Healthy {
			conn.InUse = true
			conn.LastUsed = time.Now()
			e.totalAcquires++
			return conn, nil
		}
	}

	// Check pool size limit
	if len(e.connections) >= e.config.MaxSize {
		return nil, fmt.Errorf("connection pool exhausted (max %d)", e.config.MaxSize)
	}

	// Create a new connection
	e.nextID++
	id := fmt.Sprintf("conn-%d", e.nextID)
	conn := &PooledConnection{
		ID:        id,
		Backend:   backend,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		InUse:     true,
		Healthy:   true,
	}
	e.connections[id] = conn
	e.totalAcquires++

	return conn, nil
}

// Release returns a connection to the pool.
func (e *ConnectionPoolEngine) Release(connID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	conn, ok := e.connections[connID]
	if !ok {
		return fmt.Errorf("connection %q not found", connID)
	}

	conn.InUse = false
	conn.LastUsed = time.Now()
	e.totalReleases++
	return nil
}

// HealthCheck marks idle connections that have exceeded max idle time as unhealthy.
func (e *ConnectionPoolEngine) HealthCheck() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	marked := 0
	now := time.Now()
	for _, conn := range e.connections {
		if !conn.InUse && now.Sub(conn.LastUsed) > e.config.MaxIdleTime {
			conn.Healthy = false
			marked++
		}
	}
	return marked
}

// GetStats returns connection pool statistics.
func (e *ConnectionPoolEngine) GetStats() ConnectionPoolStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	active := 0
	idle := 0
	for _, conn := range e.connections {
		if conn.InUse {
			active++
		} else {
			idle++
		}
	}

	return ConnectionPoolStats{
		TotalConnections:  len(e.connections),
		ActiveConnections: active,
		IdleConnections:   idle,
		TotalAcquires:     e.totalAcquires,
		TotalReleases:     e.totalReleases,
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the connection pool engine.
func (e *ConnectionPoolEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/connection-pool/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
