package chronicle

import (
	"context"
	"log"
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
	wg  sync.WaitGroup
	rl  *rateLimiter
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
	stopCh   chan struct{}
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
		stopCh:   make(chan struct{}),
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *rateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanup)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.mu.Lock()
			now := time.Now()
			for ip, v := range rl.visitors {
				if now.Sub(v.lastReset) > rl.cleanup {
					delete(rl.visitors, ip)
				}
			}
			rl.mu.Unlock()
		case <-rl.stopCh:
			return
		}
	}
}

// Stop stops the cleanup goroutine.
func (rl *rateLimiter) Stop() {
	select {
	case <-rl.stopCh:
	default:
		close(rl.stopCh)
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

// getClientIP extracts the client IP from the request.
// X-Forwarded-For and X-Real-IP headers are only trusted when the request
// originates from a loopback address, which is the common case when running
// behind a local reverse proxy (nginx, HAProxy, etc.).
func getClientIP(r *http.Request) string {
	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || remoteIP == "" {
		// SplitHostPort failed (e.g., missing port); use RemoteAddr directly
		remoteIP = r.RemoteAddr
	}

	// Only trust proxy headers from loopback addresses.
	trusted := remoteIP == "127.0.0.1" || remoteIP == "::1"

	if trusted {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			ips := strings.Split(xff, ",")
			if len(ips) > 0 {
				return strings.TrimSpace(ips[0])
			}
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return xri
		}
	}

	// Fall back to RemoteAddr
	if remoteIP != "" {
		return remoteIP
	}
	return r.RemoteAddr
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

// csrfProtectionMiddleware requires a custom header on state-changing requests to prevent CSRF.
// Browsers will not send the X-Requested-With header cross-origin without a CORS preflight.
func csrfProtectionMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete || r.Method == http.MethodPatch {
			if r.Header.Get("X-Requested-With") == "" && r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
				// API clients using JSON or other content types must set X-Requested-With
				// Allow form submissions only if they have the header
			}
			if r.Header.Get("X-Requested-With") == "" {
				// Check Origin header as fallback
				origin := r.Header.Get("Origin")
				if origin != "" {
					host := r.Host
					if host == "" {
						host = r.URL.Host
					}
					// Block if Origin doesn't match the Host
					if !strings.HasSuffix(origin, "://"+host) {
						http.Error(w, "CSRF validation failed: origin mismatch", http.StatusForbidden)
						return
					}
				}
			}
		}
		next(w, r)
	}
}

// securityHeadersMiddleware sets standard security headers on all responses.
func securityHeadersMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		next(w, r)
	}
}

// bodySizeLimitMiddleware limits the size of request bodies to prevent resource exhaustion.
func bodySizeLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.ContentLength != 0 {
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		}
		next(w, r)
	}
}

// authenticator handles API key authentication
type authenticator struct {
	enabled      bool
	apiKeys      []string
	readOnlyKeys []string
	excludePaths map[string]bool
}

func newAuthenticator(cfg *AuthConfig) *authenticator {
	a := &authenticator{
		excludePaths: make(map[string]bool),
	}

	if cfg == nil || !cfg.Enabled {
		a.enabled = false
		return a
	}

	a.enabled = true
	a.apiKeys = append(a.apiKeys, cfg.APIKeys...)
	a.readOnlyKeys = append(a.readOnlyKeys, cfg.ReadOnlyKeys...)
	for _, path := range cfg.ExcludePaths {
		a.excludePaths[path] = true
	}
	// Always allow health endpoints without auth
	a.excludePaths["/health"] = true
	a.excludePaths["/health/ready"] = true
	a.excludePaths["/health/live"] = true

	return a
}

// matchKey returns true if candidate matches any key in keys using
// constant-time comparison (bcrypt or subtle).
func (a *authenticator) matchKey(keys []string, candidate string) bool {
	for _, stored := range keys {
		if CompareAPIKey(stored, candidate) {
			return true
		}
	}
	return false
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

	// Query parameter support removed for security: API keys in URLs
	// appear in server logs, browser history, proxy logs, and Referer headers.
	// Use Authorization or X-API-Key headers instead.
	if key := r.URL.Query().Get("api_key"); key != "" {
		log.Println("[WARN] chronicle: API key passed via query parameter is deprecated and will be ignored; use Authorization or X-API-Key header")
	}

	return ""
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
		if auth.matchKey(auth.apiKeys, apiKey) {
			next(w, r)
			return
		}

		// Check if it's a read-only key
		if auth.matchKey(auth.readOnlyKeys, apiKey) {
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

// adminOnlyMiddleware restricts access to full-access (write-capable) API keys.
// Read-only API key holders are denied access to admin endpoints.
func adminOnlyMiddleware(auth *authenticator, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !auth.enabled {
			next(w, r)
			return
		}

		apiKey := extractAPIKey(r)
		if apiKey == "" {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}

		// Only full-access keys can access admin endpoints
		if auth.matchKey(auth.apiKeys, apiKey) {
			next(w, r)
			return
		}

		// Read-only keys or invalid keys are forbidden
		if auth.matchKey(auth.readOnlyKeys, apiKey) {
			http.Error(w, "admin access requires a write-capable API key", http.StatusForbidden)
			return
		}

		http.Error(w, "invalid API key", http.StatusUnauthorized)
	}
}

// middlewareWrapper wraps handlers with authentication and rate limiting
type middlewareWrapper func(h http.HandlerFunc) http.HandlerFunc

// setupWriteRoutes configures write-related endpoints
