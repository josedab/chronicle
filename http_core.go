package chronicle

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// QueryExecutor provides query execution capabilities.
// This interface allows HTTP handlers to be tested independently of the DB.
type QueryExecutor interface {
	Execute(q *Query) (*Result, error)
	ExecuteContext(ctx context.Context, q *Query) (*Result, error)
}

// DataWriter provides data writing capabilities.
// This interface allows HTTP handlers to be tested independently of the DB.
type DataWriter interface {
	Write(p Point) error
	WriteBatch(points []Point) error
}

// MetricLister provides metric listing capabilities.
type MetricLister interface {
	Metrics() []string
}

// SchemaManager provides schema management capabilities.
type SchemaManager interface {
	RegisterSchema(schema MetricSchema) error
	UnregisterSchema(name string)
	GetSchema(name string) *MetricSchema
	ListSchemas() []MetricSchema
}

// Ensure DB implements the interfaces
var (
	_ QueryExecutor = (*DB)(nil)
	_ DataWriter    = (*DB)(nil)
	_ MetricLister  = (*DB)(nil)
	_ SchemaManager = (*DB)(nil)
)

type httpServer struct {
	srv *http.Server
}

type writeRequest struct {
	Points []Point `json:"points"`
}

type queryRequest struct {
	Query       string            `json:"query"`
	Metric      string            `json:"metric"`
	Start       int64             `json:"start"`
	End         int64             `json:"end"`
	Tags        map[string]string `json:"tags"`
	Aggregation string            `json:"aggregation,omitempty"`
	Window      string            `json:"window,omitempty"`
}

type queryResponse struct {
	Points []Point `json:"points"`
}

const (
	// maxBodySize is the maximum allowed request body size (10MB)
	maxBodySize = 10 * 1024 * 1024
)

// rateLimiter implements a simple token bucket rate limiter per IP
type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	rate     int           // requests per window
	window   time.Duration // time window
	cleanup  time.Duration // cleanup interval
}

type visitor struct {
	tokens    int
	lastReset time.Time
}

// newRateLimiter creates a rate limiter with the given rate per window.
func newRateLimiter(rate int, window time.Duration) *rateLimiter {
	rl := &rateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		window:   window,
		cleanup:  window * 2,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *rateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanup)
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, v := range rl.visitors {
			if now.Sub(v.lastReset) > rl.cleanup {
				delete(rl.visitors, ip)
			}
		}
		rl.mu.Unlock()
	}
}

func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	v, exists := rl.visitors[ip]
	if !exists {
		rl.visitors[ip] = &visitor{tokens: rl.rate - 1, lastReset: now}
		return true
	}

	if now.Sub(v.lastReset) >= rl.window {
		v.tokens = rl.rate - 1
		v.lastReset = now
		return true
	}

	if v.tokens > 0 {
		v.tokens--
		return true
	}

	return false
}

// getClientIP extracts the client IP from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// Check X-Real-IP header
	xri := r.Header.Get("X-Real-IP")
	if xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// rateLimitMiddleware wraps a handler with rate limiting
func rateLimitMiddleware(rl *rateLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := getClientIP(r)
		if !rl.allow(ip) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

// authenticator handles API key authentication
type authenticator struct {
	enabled      bool
	apiKeys      map[string]bool
	readOnlyKeys map[string]bool
	excludePaths map[string]bool
}

func newAuthenticator(cfg *AuthConfig) *authenticator {
	a := &authenticator{
		apiKeys:      make(map[string]bool),
		readOnlyKeys: make(map[string]bool),
		excludePaths: make(map[string]bool),
	}

	if cfg == nil || !cfg.Enabled {
		a.enabled = false
		return a
	}

	a.enabled = true
	for _, key := range cfg.APIKeys {
		a.apiKeys[key] = true
	}
	for _, key := range cfg.ReadOnlyKeys {
		a.readOnlyKeys[key] = true
	}
	for _, path := range cfg.ExcludePaths {
		a.excludePaths[path] = true
	}
	// Always allow health endpoint without auth
	a.excludePaths["/health"] = true

	return a
}

// extractAPIKey extracts the API key from the request
func extractAPIKey(r *http.Request) string {
	// Check Authorization header (Bearer token)
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}

	// Check X-API-Key header
	if key := r.Header.Get("X-API-Key"); key != "" {
		return key
	}

	// Check query parameter
	return r.URL.Query().Get("api_key")
}

// isWriteOperation returns true if the request is a write operation
func isWriteOperation(r *http.Request) bool {
	if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
		path := r.URL.Path
		// Query endpoints are reads even with POST
		if strings.HasPrefix(path, "/query") ||
			strings.HasPrefix(path, "/api/v1/query") ||
			strings.HasPrefix(path, "/graphql") {
			return false
		}
		return true
	}
	return false
}

// authMiddleware wraps a handler with authentication
func authMiddleware(auth *authenticator, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !auth.enabled {
			next(w, r)
			return
		}

		// Check if path is excluded from auth
		if auth.excludePaths[r.URL.Path] {
			next(w, r)
			return
		}

		apiKey := extractAPIKey(r)
		if apiKey == "" {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}

		// Check if it's a full-access key
		if auth.apiKeys[apiKey] {
			next(w, r)
			return
		}

		// Check if it's a read-only key
		if auth.readOnlyKeys[apiKey] {
			if isWriteOperation(r) {
				http.Error(w, "read-only API key cannot perform write operations", http.StatusForbidden)
				return
			}
			next(w, r)
			return
		}

		http.Error(w, "invalid API key", http.StatusUnauthorized)
	}
}

// middlewareWrapper wraps handlers with authentication and rate limiting
type middlewareWrapper func(h http.HandlerFunc) http.HandlerFunc

// setupWriteRoutes configures write-related endpoints
